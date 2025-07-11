#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <string>

#include "buffer/buffer_manager.h"
#include "common/macros.h"
#include "storage/file.h"
#include <chrono>
#include <ctime>
#include <thread>
#include <stack>

uint64_t wake_timeout_ = 100;
uint64_t timeout_ = 2;

using namespace std;

void __print(int x) {cerr << x;}
void __print(long x) {cerr << x;}
void __print(long long x) {cerr << x;}
void __print(unsigned x) {cerr << x;}
void __print(unsigned long x) {cerr << x;}
void __print(unsigned long long x) {cerr << x;}
void __print(float x) {cerr << x;}
void __print(double x) {cerr << x;}
void __print(long double x) {cerr << x;}
void __print(char x) {cerr << '\'' << x << '\'';}
void __print(const char *x) {cerr << '\"' << x << '\"';}
void __print(const string &x) {cerr << '\"' << x << '\"';}
void __print(bool x) {cerr << (x ? "true" : "false");}

template<typename T, typename V>
void __print(const pair<T, V> &x) {cerr << '{'; __print(x.first); cerr << ','; __print(x.second); cerr << '}';}
template<typename T>
void __print(const T &x) {int f = 0; cerr << '{'; for (auto &i: x) cerr << (f++ ? "," : ""), __print(i); cerr << "}";}
void _print() {cerr << "]\n";}
template <typename T, typename... V>
void _print(T t, V... v) {__print(t); if (sizeof...(v)) cerr << ", "; _print(v...);}
#ifndef ONLINE_JUDGE
#define debug(x...) cerr << "[" << #x << "] = ["; _print(x)
#else
#define debug(x...)
#endif

namespace buzzdb {

char* BufferFrame::get_data() { return data.data(); }

BufferFrame::BufferFrame()
    : page_id(INVALID_PAGE_ID),
      frame_id(INVALID_FRAME_ID),
      dirty(false),
      exclusive(false) {
    // std::cout << "DEBUG: Created BufferFrame with INVALID_PAGE_ID" << std::endl;
}

BufferFrame::BufferFrame(const BufferFrame& other)
    : page_id(other.page_id),
      frame_id(other.frame_id),
      data(other.data),
      dirty(other.dirty),
      exclusive(other.exclusive) {
    // std::cout << "DEBUG: Copied BufferFrame for page " << page_id << " frame " << frame_id << std::endl;
}

BufferFrame& BufferFrame::operator=(BufferFrame other) {
  // std::cout << "DEBUG: Assigning BufferFrame from page " << other.page_id << " to page " << this->page_id << std::endl;
  std::swap(this->page_id, other.page_id);
  std::swap(this->frame_id, other.frame_id);
  std::swap(this->data, other.data);
  std::swap(this->dirty, other.dirty);
  std::swap(this->exclusive, other.exclusive);
  // std::cout << "DEBUG: After assignment, this page is " << this->page_id << " frame " << this->frame_id << std::endl;
  return *this;
}

BufferManager::BufferManager(size_t page_size, size_t page_count) {
    // std::cout << "DEBUG: Creating BufferManager with " << page_count << " pages of size " << page_size << std::endl;
    capacity_ = page_count;
    page_size_ = page_size;

    pool_.resize(capacity_);
    for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
        pool_[frame_id].reset(new BufferFrame());
        pool_[frame_id]->data.resize(page_size_);
        pool_[frame_id]->frame_id = frame_id;
        // std::cout << "DEBUG: Initialized frame " << frame_id << std::endl;
    }
    // std::cout << "DEBUG: BufferManager initialization complete" << std::endl;
}

BufferManager::~BufferManager() {
    // std::cout << "DEBUG: BufferManager destructor called, flushing all pages" << std::endl;
    flush_all_pages();
}

uint64_t BufferManager::find_victim_frame() {
    // std::cout << "DEBUG: Looking for victim frame, clock hand at " << clock_hand_ << std::endl;
    // First pass: look for empty frames
    for (size_t i = 0; i < capacity_; i++) {
        uint64_t frame_id = (clock_hand_ + i) % capacity_;
        if (pool_[frame_id]->page_id == INVALID_PAGE_ID) {
            clock_hand_ = (frame_id + 1) % capacity_;
            // std::cout << "DEBUG: Found empty victim frame " << frame_id << std::endl;
            return frame_id;
        }
    }
    
    // Second pass: look for frames that aren't locked
    for (size_t i = 0; i < capacity_; i++) {
        uint64_t frame_id = (clock_hand_ + i) % capacity_;
        
        auto lock_it = frame_locks_.find(frame_id);
        auto usage_it = page_usage_count_.find(frame_id);
        
        if ((lock_it == frame_locks_.end() || 
                (lock_it->second.exclusive_owner == INVALID_TXN_ID && 
                lock_it->second.shared_owners.empty())) &&
            (usage_it == page_usage_count_.end() || usage_it->second == 0)) {
            
            clock_hand_ = (frame_id + 1) % capacity_;
            // std::cout << "DEBUG: Found unlocked victim frame " << frame_id << " with page " << pool_[frame_id]->page_id << std::endl;
            return frame_id;
        }
    }
    
    // If all frames are locked, we have to throw an exception
    // std::cout << "DEBUG: All frames are locked, buffer is full" << std::endl;
    throw buffer_full_error();
}

bool BufferManager::detect_deadlock(uint64_t txn_id) {
    // std::cout << "DEBUG: Checking for deadlock involving txn " << txn_id << std::endl;
    // Using depth-first search to detect cycles in the wait-for graph
    std::set<uint64_t> visited;
    std::stack<uint64_t> stack;
    
    stack.push(txn_id);
    
    while (!stack.empty()) {
        uint64_t current_txn = stack.top();
        stack.pop();
        
        if (visited.find(current_txn) != visited.end()) {
            continue;
        }
        
        visited.insert(current_txn);
        
        // Find what this transaction is waiting for
        auto it = txn_waiting_for_.find(current_txn);
        if (it == txn_waiting_for_.end()) {
            continue;  // Not waiting for anything
        }
        
        uint64_t frame_waiting_for = it->second;
        // std::cout << "DEBUG: Txn " << current_txn << " is waiting for frame " << frame_waiting_for << std::endl;
        
        // Find transactions holding locks on this frame
        auto lock_it = frame_locks_.find(frame_waiting_for);
        if (lock_it == frame_locks_.end()) {
            continue;  // No locks on this frame
        }
        
        // Check if any transaction that has a lock on this frame is waiting for txn_id
        // which would create a cycle
        if (lock_it->second.exclusive_owner != INVALID_TXN_ID) {
            // std::cout << "DEBUG: Frame " << frame_waiting_for << " has exclusive owner txn " << lock_it->second.exclusive_owner << std::endl;
            if (lock_it->second.exclusive_owner == txn_id) {
                std::cout << "DEBUG: Deadlock detected! Cycle involves txn " << txn_id << std::endl;
                return true;  // Cycle detected
            }
            
            // Add the exclusive owner to the stack
            stack.push(lock_it->second.exclusive_owner);
        }
        
        // Check all shared owners
        for (uint64_t owner_txn : lock_it->second.shared_owners) {
            // std::cout << "DEBUG: Frame " << frame_waiting_for << " has shared owner txn " << owner_txn << std::endl;
            if (owner_txn == txn_id) {
                std::cout << "DEBUG: Deadlock detected! Cycle involves txn " << txn_id << std::endl;
                return true;  // Cycle detected
            }
            
            // Add shared owner to the stack
            stack.push(owner_txn);
        }
    }
    
    // std::cout << "DEBUG: No deadlock detected for txn " << txn_id << std::endl;
    return false;  // No cycle found
}

/**
This API is called when the transaction txn_id wants to acquire lock on page (page_id).
The exclusive boolean defines whether the transaction needs a shared lock (when false) or
an exclusive lock (when true).
*/
BufferFrame& BufferManager::fix_page(UNUSED_ATTRIBUTE uint64_t txn_id, UNUSED_ATTRIBUTE uint64_t page_id, UNUSED_ATTRIBUTE bool exclusive) {
    // std::cout << "DEBUG: fix_page called for txn " << txn_id << ", page " << page_id << ", exclusive=" << exclusive << std::endl;
    std::unique_lock<std::mutex> lock(global_mutex_);

    // First check if the page is already in the buffer
    for (size_t i = 0; i < capacity_; ++i) {
        if (pool_[i]->page_id == page_id) {
            uint64_t frame_id = i;
            // std::cout << "DEBUG: Page " << page_id << " found in frame " << frame_id << std::endl;
            bool can_acquire = false;

            if (exclusive) {
                // need to try for exclusive lock
                if ((frame_locks_.find(frame_id) == frame_locks_.end() 
                    || (frame_locks_[frame_id].exclusive_owner == INVALID_TXN_ID
                        && frame_locks_[frame_id].shared_owners.empty())) 
                || (frame_locks_[frame_id].exclusive_owner == txn_id)) {
                    // std::cout << "DEBUG: Txn " << txn_id << " can acquire exclusive lock on frame " << frame_id << std::endl;
                    can_acquire = true;
                }
                else if (frame_locks_[frame_id].exclusive_owner == INVALID_TXN_ID 
                    && frame_locks_[frame_id].shared_owners.size() == 1 
                    && frame_locks_[frame_id].shared_owners.count(txn_id) == 1) {
                    // only this txn has shared lock
                    // so it can be upgraded according to doc
                    // std::cout << "DEBUG: Txn " << txn_id << " can upgrade shared lock to exclusive on frame " << frame_id << std::endl;
                    can_acquire = true;
                }
            } 
            else {
                if (frame_locks_.find(frame_id) == frame_locks_.end() 
                    || frame_locks_[frame_id].exclusive_owner == INVALID_TXN_ID 
                    || frame_locks_[frame_id].exclusive_owner == txn_id) {
                    // essentially can only acquire shared lock
                    // if it was the exclusive owner
                    // or if no other txn has exclusive lock on it
                    // std::cout << "DEBUG: Txn " << txn_id << " can acquire shared lock on frame " << frame_id << std::endl;
                    can_acquire = true;
                }
            }
            
            if (can_acquire) {
                if (exclusive) {
                    frame_locks_[frame_id].shared_owners.erase(txn_id);
                    frame_locks_[frame_id].exclusive_owner = txn_id;
                    // std::cout << "DEBUG: Txn " << txn_id << " acquired exclusive lock on frame " << frame_id << std::endl;
                }
                else {
                    if (frame_locks_[frame_id].exclusive_owner != txn_id) {
                        frame_locks_[frame_id].shared_owners.insert(txn_id);
                        // std::cout << "DEBUG: Txn " << txn_id << " acquired shared lock on frame " << frame_id << std::endl;
                    } else {
                        // std::cout << "DEBUG: Txn " << txn_id << " already has exclusive lock on frame " << frame_id << std::endl;
                    }
                }
                
                txn_locks_[txn_id].insert(page_id);
                pool_[i]->exclusive = exclusive;
                
                ++page_usage_count_[frame_id];
                std::cout << "DEBUG: Usage count for frame " << frame_id << " increased to " << page_usage_count_[frame_id] << std::endl;
                
                return *pool_[i];
            }
            
            // need to wait for lock now
            std::cout << "DEBUG: Txn " << txn_id << " needs to wait for lock on frame " << frame_id << std::endl;
            
            txn_waiting_for_[txn_id] = frame_id;
            
            if (detect_deadlock(txn_id)) {
                txn_waiting_for_.erase(txn_id);
                std::cout << "DEBUG: Aborting txn " << txn_id << " to resolve deadlock" << std::endl;
                throw transaction_abort_error();
            }
            
            auto start_time = std::chrono::steady_clock::now();
            
            while (1) {
                lock.unlock();
                std::cout << "DEBUG: Txn " << txn_id << " sleeping while waiting for frame " << frame_id << std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                lock.lock();
                
                if (exclusive) {
                    // need to try for exclusive lock
                    if ((frame_locks_.find(frame_id) == frame_locks_.end() 
                        || (frame_locks_[frame_id].exclusive_owner == INVALID_TXN_ID
                            && frame_locks_[frame_id].shared_owners.empty())) 
                    || (frame_locks_[frame_id].exclusive_owner == txn_id)) {
                        std::cout << "DEBUG: Txn " << txn_id << " can now acquire exclusive lock on frame " << frame_id << std::endl;
                        break;
                    }
                    else if (frame_locks_[frame_id].exclusive_owner == INVALID_TXN_ID 
                        && frame_locks_[frame_id].shared_owners.size() == 1 
                        && frame_locks_[frame_id].shared_owners.count(txn_id) == 1) {
                        // only this txn has shared lock
                        // so it can be upgraded according to doc
                        std::cout << "DEBUG: Txn " << txn_id << " can now upgrade shared lock to exclusive on frame " << frame_id << std::endl;
                        break;
                    }
                } 
                else {
                    if (frame_locks_.find(frame_id) == frame_locks_.end() 
                        || frame_locks_[frame_id].exclusive_owner == INVALID_TXN_ID 
                        || frame_locks_[frame_id].exclusive_owner == txn_id) {
                        // essentially can only acquire shared lock
                        // if it was the exclusive owner
                        // or if no other txn has exclusive lock on it
                        // std::cout << "DEBUG: Txn " << txn_id << " can now acquire shared lock on frame " << frame_id << std::endl;
                        break;
                    }
                }
                auto current_time = std::chrono::steady_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    current_time - start_time).count();
                
                if (elapsed > 2000) {
                    txn_waiting_for_.erase(txn_id);
                    // std::cout << "DEBUG: Txn " << txn_id << " timed out waiting for lock on frame " << frame_id << std::endl;
                    throw transaction_abort_error();
                }
            }
            
            txn_waiting_for_.erase(txn_id);
            
            if (exclusive) {
                // If upgrading from shared, remove shared lock
                frame_locks_[frame_id].shared_owners.erase(txn_id);
                frame_locks_[frame_id].exclusive_owner = txn_id;
                // std::cout << "DEBUG: Txn " << txn_id << " acquired exclusive lock on frame " << frame_id << " after waiting" << std::endl;
            } else {
                // Add shared lock if not already exclusive owner
                if (frame_locks_[frame_id].exclusive_owner != txn_id) {
                    frame_locks_[frame_id].shared_owners.insert(txn_id);
                    // std::cout << "DEBUG: Txn " << txn_id << " acquired shared lock on frame " << frame_id << " after waiting" << std::endl;
                }
            }
            
            // Record that this transaction locked this page
            txn_locks_[txn_id].insert(page_id);
            
            // Update frame state
            pool_[i]->exclusive = exclusive;
            
            // Increment usage counter
            page_usage_count_[frame_id]++;
            // std::cout << "DEBUG: Usage count for frame " << frame_id << " increased to " << page_usage_count_[frame_id] << " after waiting" << std::endl;
            
            return *pool_[i];
        }
    }

    // Page not in buffer, need to load it
    // std::cout << "DEBUG: Page " << page_id << " not found in buffer, looking for victim frame" << std::endl;
    uint64_t frame_id;
    try {
        frame_id = find_victim_frame();
    } catch (const buffer_full_error& e) {
        // std::cout << "DEBUG: Failed to find victim frame: " << e.what() << std::endl;
        throw;  // Propagate the error
    }
    
    // If victim frame has a page and it's dirty, write it back to disk
    if (pool_[frame_id]->page_id != INVALID_PAGE_ID && pool_[frame_id]->dirty) {
        // std::cout << "DEBUG: Victim frame " << frame_id << " has dirty page " << pool_[frame_id]->page_id << ", writing to disk" << std::endl;
        write_frame(frame_id);
    }
    
    // Load the new page
    // std::cout << "DEBUG: Loading page " << page_id << " into frame " << frame_id << std::endl;
    pool_[frame_id]->page_id = page_id;
    pool_[frame_id]->dirty = false;
    pool_[frame_id]->exclusive = exclusive;
    
    // Read page from disk
    try {
        read_frame(frame_id);
        // std::cout << "DEBUG: Successfully read page " << page_id << " from disk into frame " << frame_id << std::endl;
    } catch (const std::exception& e) {
        // std::cout << "DEBUG: Error reading page " << page_id << " from disk: " << e.what() << std::endl;
        throw;
    }
    
    // Initialize lock information
    frame_locks_[frame_id].shared_owners.clear();
    if (exclusive) {
        frame_locks_[frame_id].exclusive_owner = txn_id;
        // std::cout << "DEBUG: Txn " << txn_id << " acquired exclusive lock on new frame " << frame_id << std::endl;
    } else {
        frame_locks_[frame_id].exclusive_owner = INVALID_TXN_ID;
        frame_locks_[frame_id].shared_owners.insert(txn_id);
        // std::cout << "DEBUG: Txn " << txn_id << " acquired shared lock on new frame " << frame_id << std::endl;
    }
    
    // Record that this transaction locked this page
    txn_locks_[txn_id].insert(page_id);
    
    // Initialize usage counter
    page_usage_count_[frame_id] = 1;
    // std::cout << "DEBUG: Usage count for new frame " << frame_id << " set to 1" << std::endl;
    
    return *pool_[frame_id];
}

/**
This API is called by the tester to unfix a page acquired by a transaction.
It releases all the locks acquired by the txn_id on this page and if dirty
flushes that to disk.
*/
void BufferManager::unfix_page(UNUSED_ATTRIBUTE uint64_t txn_id, UNUSED_ATTRIBUTE BufferFrame& page, UNUSED_ATTRIBUTE bool is_dirty) {
    // std::cout << "DEBUG: unfix_page called for txn " << txn_id << ", page " << page.page_id << ", is_dirty=" << is_dirty << std::endl;
    std::lock_guard<std::mutex> lock(global_mutex_);

    uint64_t frame_id = page.frame_id;
    
    // Check if frame_id is valid
    if (frame_id >= capacity_ || pool_[frame_id]->page_id == INVALID_PAGE_ID) {
        // std::cout << "DEBUG: Invalid frame_id " << frame_id << " in unfix_page" << std::endl;
        return;
    }
    
    // Find the actual frame by page_id (safer)
    for (size_t i = 0; i < capacity_; i++) {
        if (pool_[i]->page_id == page.page_id) {
            if (is_dirty) {
                pool_[i]->dirty = true;
                // std::cout << "DEBUG: Marked frame " << i << " with page " << page.page_id << " as dirty" << std::endl;
            }
            
            // Decrement usage counter
            if (page_usage_count_.find(i) != page_usage_count_.end()) {
                --page_usage_count_[i];
                // std::cout << "DEBUG: Decreased usage count for frame " << i << " to " << page_usage_count_[i] << std::endl;
                if (page_usage_count_[i] == 0) {
                    page_usage_count_.erase(i);
                    // std::cout << "DEBUG: Removed usage count for frame " << i << " (reached 0)" << std::endl;
                }
            }
            return;
        }
    }
    
    std::cout << "DEBUG: Warning: Page " << page.page_id << " not found in buffer pool during unfix" << std::endl;
}

void BufferManager::flush_all_pages() {
    // std::cout << "DEBUG: flush_all_pages called" << std::endl;
    for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
        if (pool_[frame_id]->dirty == true) {
            // std::cout << "DEBUG: Flushing dirty page " << pool_[frame_id]->page_id << " from frame " << frame_id << std::endl;
            write_frame(frame_id);
        }
    }
    // std::cout << "DEBUG: All pages flushed" << std::endl;
}


void BufferManager::discard_all_pages() {
    // std::cout << "DEBUG: discard_all_pages called" << std::endl;
    for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
        pool_[frame_id].reset(new BufferFrame());
        pool_[frame_id]->page_id = INVALID_PAGE_ID;
        pool_[frame_id]->dirty = false;
        pool_[frame_id]->data.resize(page_size_);
        // std::cout << "DEBUG: Reset frame " << frame_id << std::endl;
    }
    
    frame_locks_.clear();
    txn_locks_.clear();
    txn_waiting_for_.clear();
    page_usage_count_.clear();
    // std::cout << "DEBUG: Cleared all locks and usage counts" << std::endl;
}

void BufferManager::discard_pages(uint64_t txn_id) {
    // std::cout << "DEBUG: discard_pages called for txn " << txn_id << std::endl;
    std::lock_guard<std::mutex> lock(global_mutex_);
    auto it = txn_locks_.find(txn_id);
    if (it != txn_locks_.end()) {
        // std::cout << "DEBUG: Txn " << txn_id << " has " << it->second.size() << " pages to check for discarding" << std::endl;
        for (uint64_t page_id : it->second) {
            // Find corresponding frame by page_id
            for (size_t i = 0; i < capacity_; i++) {
                if (pool_[i]->page_id == page_id) {
                    // Use i directly as the frame_id
                    uint64_t frame_id = i;
                    // std::cout << "DEBUG: Checking frame " << frame_id << " with page " << page_id << " for discarding" << std::endl;
                    
                    // If this transaction had exclusive lock and made changes, discard them
                    if (frame_locks_[frame_id].exclusive_owner == txn_id && pool_[i]->dirty) {
                        // Reload page from disk to discard changes
                        // std::cout << "DEBUG: Discarding changes to page " << page_id << " by reloading from disk" << std::endl;
                        try {
                            read_frame(frame_id);
                            pool_[i]->dirty = false;
                        } catch (const std::exception& e) {
                            // std::cout << "DEBUG: Error reloading page " << page_id << ": " << e.what() << std::endl;
                            // For new pages that don't exist on disk, just reset the frame
                            if (pool_[i]->dirty) {
                                pool_[i]->dirty = false;
                                std::fill(pool_[i]->data.begin(), pool_[i]->data.end(), 0);
                            }
                        }
                    }
                    break;
                }
            }
        }
    } 
    
}

void BufferManager::flush_pages_internal(uint64_t txn_id) {
    // std::cout << "DEBUG: flush_pages_internal called for txn " << txn_id << std::endl;
    // Check which pages this transaction modified
    auto it = txn_locks_.find(txn_id);
    if (it != txn_locks_.end()) {
        std::cout << "DEBUG: Txn " << txn_id << " has " << it->second.size() << " pages to check for flushing" << std::endl;
        for (uint64_t page_id : it->second) {
            // Find corresponding frame
            for (size_t i = 0; i < capacity_; i++) {
                if (pool_[i]->page_id == page_id) {
                    uint64_t frame_id = i; // Use i directly, not pool_[i]->frame_id
                    std::cout << "DEBUG: Checking frame " << frame_id << " with page " << page_id << " for flushing" << std::endl;
                    
                    // If page is dirty and this transaction holds the exclusive lock, flush it
                    if (pool_[i]->dirty && frame_locks_[frame_id].exclusive_owner == txn_id) {
                        std::cout << "DEBUG: Flushing dirty page " << page_id << " from frame " << frame_id << std::endl;
                        write_frame(frame_id);
                        pool_[i]->dirty = false;
                    }
                    break;
                }
            }
        }
    } 
    // else {
    //     std::cout << "DEBUG: No pages to flush for txn " << txn_id << std::endl;
    // }
}

void BufferManager::flush_pages(UNUSED_ATTRIBUTE uint64_t txn_id) {
    // std::cout << "DEBUG: flush_pages called for txn " << txn_id << std::endl;
    // Flush all dirty pages acquired by the transaction to disk
    flush_pages_internal(txn_id);
}

void BufferManager::transaction_complete(UNUSED_ATTRIBUTE uint64_t txn_id) {
    // Free all the locks acquired by the transaction
    // std::cout << "DEBUG: transaction_complete called for txn " << txn_id << std::endl;
    
    // First flush all dirty pages to ensure data is written to disk
    {
        std::lock_guard<std::mutex> lock(global_mutex_);
        auto it = txn_locks_.find(txn_id);
        if (it != txn_locks_.end()) {
            for (uint64_t page_id : it->second) {
                for (size_t i = 0; i < capacity_; i++) {
                    if (pool_[i]->page_id == page_id) {
                        if (pool_[i]->dirty && frame_locks_[i].exclusive_owner == txn_id) {
                            // std::cout << "DEBUG: Transaction complete: Flushing dirty page " << page_id << " from frame " << i << std::endl;
                            write_frame(i);
                            pool_[i]->dirty = false;
                        }
                        break;
                    }
                }
            }
        }
    }
    
    // Now release locks
    std::lock_guard<std::mutex> lock(global_mutex_);
    
    // Release all locks held by this transaction
    auto it = txn_locks_.find(txn_id);
    if (it != txn_locks_.end()) {
        // std::cout << "DEBUG: Txn " << txn_id << " has " << it->second.size() << " pages to release locks for" << std::endl;
        for (uint64_t page_id : it->second) {
            // Find corresponding frame
            for (size_t i = 0; i < capacity_; i++) {
                if (pool_[i]->page_id == page_id) {
                    uint64_t frame_id = i;
                    
                    // Release locks
                    if (frame_locks_[frame_id].exclusive_owner == txn_id) {
                        // std::cout << "DEBUG: Releasing exclusive lock on frame " << frame_id << " for txn " << txn_id << std::endl;
                        frame_locks_[frame_id].exclusive_owner = INVALID_TXN_ID;
                    }
                    if (frame_locks_[frame_id].shared_owners.erase(txn_id) > 0) {
                        std::cout << "DEBUG: Releasing shared lock on frame " << frame_id << " for txn " << txn_id << std::endl;
                    }
                    break;
                }
            }
        }
        
        // Clear transaction's lock set
        // std::cout << "DEBUG: Clearing lock set for txn " << txn_id << std::endl;
        txn_locks_.erase(it);
    }
    
    // Clean up any waiting info
    if (txn_waiting_for_.erase(txn_id) > 0) {
        // std::cout << "DEBUG: Removed waiting info for txn " << txn_id << std::endl;
    }
}

void BufferManager::transaction_abort(UNUSED_ATTRIBUTE uint64_t txn_id) {
    // Free all the locks acquired by the transaction
    // std::cout << "DEBUG: transaction_abort called for txn " << txn_id << std::endl;
    
    discard_pages(txn_id);
        
    std::lock_guard<std::mutex> lock(global_mutex_);
    
    auto it = txn_locks_.find(txn_id);
    if (it != txn_locks_.end()) {
        // std::cout << "DEBUG: Txn " << txn_id << " has " << it->second.size() << " pages to release locks for during abort" << std::endl;
        for (uint64_t page_id : it->second) {
            // Find corresponding frame
            for (size_t i = 0; i < capacity_; i++) {
                if (pool_[i]->page_id == page_id) {
                    uint64_t frame_id = pool_[i]->frame_id;
                    
                    // Release locks
                    if (frame_locks_[frame_id].exclusive_owner == txn_id) {
                        // std::cout << "DEBUG: Releasing exclusive lock on frame " << frame_id << " for aborted txn " << txn_id << std::endl;
                        frame_locks_[frame_id].exclusive_owner = INVALID_TXN_ID;
                    }
                    if (frame_locks_[frame_id].shared_owners.erase(txn_id) > 0) {
                        // std::cout << "DEBUG: Releasing shared lock on frame " << frame_id << " for aborted txn " << txn_id << std::endl;
                    }
                    break;
                }
            }
        }        
        // std::cout << "DEBUG: Clearing lock set for aborted txn " << txn_id << std::endl;
        txn_locks_.erase(it);
    }
    
    // Clean up any waiting info
    if (txn_waiting_for_.erase(txn_id) > 0) {
        std::cout << "DEBUG: Removed waiting info for aborted txn " << txn_id << std::endl;
    }
    // std::cout << "DEBUG: Flushing all pages after abort" << std::endl;
    flush_all_pages();
}


void BufferManager::read_frame(uint64_t frame_id) {
  // std::cout << "DEBUG: Reading frame " << frame_id << " (page " << pool_[frame_id]->page_id << ") from disk" << std::endl;
  std::lock_guard<std::mutex> file_guard(file_use_mutex_);

  auto segment_id = get_segment_id(pool_[frame_id]->page_id);
  auto file_handle =
      File::open_file(std::to_string(segment_id).c_str(), File::WRITE);
  size_t start = get_segment_page_id(pool_[frame_id]->page_id) * page_size_;
  // std::cout << "DEBUG: Reading from segment " << segment_id << " at offset " << start << " for page " << pool_[frame_id]->page_id << std::endl;
  
  try {
    file_handle->read_block(start, page_size_, pool_[frame_id]->data.data());
    // std::cout << "DEBUG: Successfully read frame " << frame_id << " from disk" << std::endl;
  } catch (const std::exception& e) {
    // std::cout << "DEBUG: Error reading frame " << frame_id << " from disk: " << e.what() << std::endl;
    throw;
  }
}

void BufferManager::write_frame(uint64_t frame_id) {
  // std::cout << "DEBUG: Writing frame " << frame_id << " (page " << pool_[frame_id]->page_id << ") to disk" << std::endl;
  std::lock_guard<std::mutex> file_guard(file_use_mutex_);

  auto segment_id = get_segment_id(pool_[frame_id]->page_id);
  auto file_handle =
      File::open_file(std::to_string(segment_id).c_str(), File::WRITE);
  size_t start = get_segment_page_id(pool_[frame_id]->page_id) * page_size_;
  // std::cout << "DEBUG: Writing to segment " << segment_id << " at offset " << start << " for page " << pool_[frame_id]->page_id << std::endl;
  
  try {
    file_handle->write_block(pool_[frame_id]->data.data(), start, page_size_);
    // std::cout << "DEBUG: Successfully wrote frame " << frame_id << " to disk" << std::endl;
  } catch (const std::exception& e) {
    // std::cout << "DEBUG: Error writing frame " << frame_id << " to disk: " << e.what() << std::endl;
    throw;
  }
}


}  // namespace buzzdb