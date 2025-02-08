
#include "log/log_manager.h"

#include <string.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <set>
#include <vector>

#include "buffer/buffer_manager.h"
#include "common/macros.h"
#include "storage/test_file.h"

namespace buzzdb {

struct LogRecordData {
    LogManager::LogRecordType type;
    uint64_t txn_id{0};

    // For UPDATE_RECORD
    // page_id, length, offset, before_img, after_img
    uint64_t page_id{0};
    uint64_t length{0};
    uint64_t offset{0};
    std::vector<char> before_img;
    std::vector<char> after_img;

    size_t log_offset{0};
    size_t record_size{0};
};

/**
 * Functionality of the buffer manager that might be handy

 Flush all the dirty pages to the disk
        buffer_manager.flush_all_pages():

 Write @data of @length at an @offset the buffer page @page_id
        BufferFrame& frame = buffer_manager.fix_page(page_id, true);
        memcpy(&frame.get_data()[offset], data, length);
        buffer_manager.unfix_page(frame, true);

 * Read and Write from/to the log_file
   log_file_->read_block(offset, size, data);

   Usage:
   uint64_t txn_id;
   log_file_->read_block(offset, sizeof(uint64_t), reinterpret_cast<char *>(&txn_id));
   log_file_->write_block(reinterpret_cast<char *> (&txn_id), offset, sizeof(uint64_t));
 */

LogManager::LogManager(File* log_file) {
    log_file_ = log_file;
    log_record_type_to_count[LogRecordType::ABORT_RECORD] = 0;
    log_record_type_to_count[LogRecordType::COMMIT_RECORD] = 0;
    log_record_type_to_count[LogRecordType::UPDATE_RECORD] = 0;
    log_record_type_to_count[LogRecordType::BEGIN_RECORD] = 0;
    log_record_type_to_count[LogRecordType::CHECKPOINT_RECORD] = 0;
}

LogManager::~LogManager() {}

void LogManager::reset(File* log_file) {
    log_file_ = log_file;
    current_offset_ = 0;
    txn_id_to_first_log_record.clear();
    log_record_type_to_count.clear();
}

// Convert enum to integral for writing to disk.
static inline uint8_t encodeRecordType(LogManager::LogRecordType t) {
  return static_cast<uint8_t>(t);
}
static inline LogManager::LogRecordType decodeRecordType(uint8_t x) {
  return static_cast<LogManager::LogRecordType>(x);
}


void LogManager::WriteToLog(const void* src, size_t len) {
    size_t old_size = log_file_->size();
    if (current_offset_ + len > old_size) {
        log_file_->resize(old_size + len);
    }
    log_file_->write_block(reinterpret_cast<const char*>(src), current_offset_, len);
    current_offset_ += len;
    return;
}


/// Get log records
uint64_t LogManager::get_total_log_records() {
    uint64_t total = 0;
    for (auto& it : log_record_type_to_count) {
        total += it.second;
    }
    return total;
}

uint64_t LogManager::get_total_log_records_of_type(UNUSED_ATTRIBUTE LogRecordType type) {
    return log_record_type_to_count[type];
}

/**
 * Increment the ABORT_RECORD count.
 * Rollback the provided transaction.
 * Add abort log record to the log file.
 * Remove from the active transactions.
 */
void LogManager::log_abort(UNUSED_ATTRIBUTE uint64_t txn_id,
                           UNUSED_ATTRIBUTE BufferManager& buffer_manager) {
    uint8_t encodedRecord = encodeRecordType(LogRecordType::ABORT_RECORD);
    WriteToLog(&encodedRecord, sizeof(encodedRecord));
    WriteToLog(&txn_id, sizeof(txn_id));
    log_record_type_to_count[LogRecordType::ABORT_RECORD]++;
    rollback_txn(txn_id, buffer_manager);
    txn_id_to_first_log_record.erase(txn_id);
    return;
}

/**
 * Increment the COMMIT_RECORD count
 * Add commit log record to the log file
 * Remove from the active transactions
 */
void LogManager::log_commit(UNUSED_ATTRIBUTE uint64_t txn_id) {
    uint8_t encodedRecord = encodeRecordType(LogRecordType::COMMIT_RECORD);
    WriteToLog(&encodedRecord, sizeof(encodedRecord));
    WriteToLog(&txn_id, sizeof(txn_id));
    log_record_type_to_count[LogRecordType::COMMIT_RECORD]++;
    txn_id_to_first_log_record.erase(txn_id);
    return;
}

/**
 * Increment the UPDATE_RECORD count
 * Add the update log record to the log file
 * @param txn_id		transaction id
 * @param page_id		buffer page id
 * @param length		length of the update tuple
 * @param offset 		offset to the tuple in the buffer page
 * @param before_img	before image of the buffer page at the given offset
 * @param after_img		after image of the buffer page at the given offset
 */
void LogManager::log_update(UNUSED_ATTRIBUTE uint64_t txn_id, UNUSED_ATTRIBUTE uint64_t page_id,
                            UNUSED_ATTRIBUTE uint64_t length, UNUSED_ATTRIBUTE uint64_t offset,
                            UNUSED_ATTRIBUTE std::byte* before_img,
                            UNUSED_ATTRIBUTE std::byte* after_img) {
    uint8_t encodedRecord = encodeRecordType(LogRecordType::UPDATE_RECORD);
    WriteToLog(&encodedRecord, sizeof(encodedRecord));
    WriteToLog(&txn_id, sizeof(txn_id));
    WriteToLog(&page_id, sizeof(page_id));
    WriteToLog(&length, sizeof(length));
    WriteToLog(&offset, sizeof(offset));
    WriteToLog(before_img, length);
    WriteToLog(after_img, length);

    log_record_type_to_count[LogRecordType::UPDATE_RECORD]++;

    return;
}

/**
 * Increment the BEGIN_RECORD count
 * Add the begin log record to the log file
 * Add to the active transactions
 */
void LogManager::log_txn_begin(UNUSED_ATTRIBUTE uint64_t txn_id) {
    uint8_t encodedRecord = encodeRecordType(LogRecordType::BEGIN_RECORD);
    WriteToLog(&encodedRecord, sizeof(encodedRecord));
    WriteToLog(&txn_id, sizeof(txn_id));
    log_record_type_to_count[LogRecordType::BEGIN_RECORD]++;
    // write type of log record
    if (txn_id_to_first_log_record.begin() == txn_id_to_first_log_record.end()) {
        txn_id_to_first_log_record[txn_id] = current_offset_;
    }
    return;
}

/**
 * Increment the CHECKPOINT_RECORD count
 * Flush all dirty pages to the disk (USE: buffer_manager.flush_all_pages())
 * Add the checkpoint log record to the log file
 */
void LogManager::log_checkpoint(UNUSED_ATTRIBUTE BufferManager& buffer_manager) {
    buffer_manager.flush_all_pages();
    uint8_t encodedRecord = encodeRecordType(LogRecordType::CHECKPOINT_RECORD);
    WriteToLog(&encodedRecord, sizeof(encodedRecord));
    log_record_type_to_count[LogRecordType::CHECKPOINT_RECORD]++;
    return;
}
std::vector<LogRecordData> read_all_logs(File* file) {
    std::vector<LogRecordData> logs;
    size_t offset = 0;
    size_t file_size = file->size();

    while (offset < file_size) {
        LogRecordData log;
        uint8_t record_type;
        file->read_block(offset, sizeof(uint8_t), reinterpret_cast<char*>(&record_type));
        log.type = decodeRecordType(record_type);  // Use the decode function
        offset += sizeof(uint8_t);

        // Debug output
        std::cout << "Reading log record at offset " << offset << " type: " << static_cast<int>(record_type) << std::endl;

        switch(log.type) {
            case LogManager::LogRecordType::ABORT_RECORD:
            case LogManager::LogRecordType::COMMIT_RECORD:
            case LogManager::LogRecordType::BEGIN_RECORD:
                file->read_block(offset, sizeof(uint64_t), reinterpret_cast<char*>(&log.txn_id));
                offset += sizeof(uint64_t);
                break;

            case LogManager::LogRecordType::UPDATE_RECORD:
                file->read_block(offset, sizeof(uint64_t), reinterpret_cast<char*>(&log.txn_id));
                offset += sizeof(uint64_t);

                file->read_block(offset, sizeof(uint64_t), reinterpret_cast<char*>(&log.page_id));
                offset += sizeof(uint64_t);

                file->read_block(offset, sizeof(uint64_t), reinterpret_cast<char*>(&log.length));
                offset += sizeof(uint64_t);

                file->read_block(offset, sizeof(uint64_t), reinterpret_cast<char*>(&log.offset));
                offset += sizeof(uint64_t);

                log.before_img.resize(log.length);
                file->read_block(offset, log.length, log.before_img.data());
                offset += log.length;

                log.after_img.resize(log.length);
                file->read_block(offset, log.length, log.after_img.data());
                offset += log.length;
                break;

            case LogManager::LogRecordType::CHECKPOINT_RECORD:
                // Checkpoint record has no additional data
                break;

            default:
                // std::cout << "Invalid log record type: " << static_cast<int>(record_type) << std::endl;
                return logs;  // Return what we have so far if we hit an invalid record
        }

        logs.push_back(log);
    }
    return logs;
}

void debugLogs(const LogRecordData& log) {
    std::cout << "----------------------------------------" << std::endl;
    std::cout << "Log Record Type: ";
    switch(log.type) {
        case LogManager::LogRecordType::BEGIN_RECORD:
            std::cout << "BEGIN";
            break;
        case LogManager::LogRecordType::COMMIT_RECORD:
            std::cout << "COMMIT";
            break;
        case LogManager::LogRecordType::ABORT_RECORD:
            std::cout << "ABORT";
            break;
        case LogManager::LogRecordType::UPDATE_RECORD:
            std::cout << "UPDATE";
            break;
        case LogManager::LogRecordType::CHECKPOINT_RECORD:
            std::cout << "CHECKPOINT";
            break;
        default:
            std::cout << "INVALID";
    }
    std::cout << std::endl;

    std::cout << "Transaction ID: " << log.txn_id << std::endl;

    if (log.type == LogManager::LogRecordType::UPDATE_RECORD) {
        std::cout << "Page ID: " << log.page_id << std::endl;
        std::cout << "Length: " << log.length << std::endl;
        std::cout << "Offset: " << log.offset << std::endl;
        std::cout << "Before Image Size: " << log.before_img.size() << std::endl;
        std::cout << "After Image Size: " << log.after_img.size() << std::endl;
    }
    std::cout << "----------------------------------------" << std::endl;
}

/**
 * @Analysis Phase:
 * 		1. Get the active transactions and commited transactions
 * 		2. Restore the txn_id_to_first_log_record
 * @Redo Phase:
 * 		1. Redo the entire log tape to restore the buffer page
 * 		2. For UPDATE logs: write the after_img to the buffer page
 * 		3. For ABORT logs: rollback the transactions
 * 	@Undo Phase
 * 		1. Rollback the transactions which are active and not commited
 */
void LogManager::recovery(UNUSED_ATTRIBUTE BufferManager& buffer_manager) {
    assert(log_file_->size() > 0);
    std::vector<LogRecordData> logs = read_all_logs(log_file_);
    std::set<uint64_t> active_txns;
    std::set<uint64_t> commited_txns;
    std::set<uint64_t> aborted_txns;

    // Analysis Phase
    for (auto& log: logs) {
        // debug log
        // debugLogs(log);
        switch(log.type) {
            case LogRecordType::BEGIN_RECORD:
                active_txns.insert(log.txn_id);
                break;
            case LogRecordType::COMMIT_RECORD:
                active_txns.erase(log.txn_id);
                commited_txns.insert(log.txn_id);
                break;
            case LogRecordType::ABORT_RECORD:
                active_txns.erase(log.txn_id);
                aborted_txns.insert(log.txn_id);
                break;
            case LogRecordType::UPDATE_RECORD:
            case LogRecordType::CHECKPOINT_RECORD: break;
            default:
                // std::cout << "Invalid log record type" << std::endl;
                break;
        }
    }

    // first do redo, then undo
    for (auto& log: logs) {
        if (log.type == LogRecordType::UPDATE_RECORD) {
            // if commited or inflight txn
            if (commited_txns.find(log.txn_id) != commited_txns.end() ||
                active_txns.find(log.txn_id) != active_txns.end()) {
                    BufferFrame& frame = buffer_manager.fix_page(log.page_id, true);
                    memcpy(&frame.get_data()[log.offset], log.after_img.data(), log.length);
                    buffer_manager.unfix_page(frame, true);
                }
        }
    }

    std::set<uint64_t> transactions_to_undo;
    transactions_to_undo.insert(active_txns.begin(), active_txns.end());
    transactions_to_undo.insert(aborted_txns.begin(), aborted_txns.end());

    // undo phase -> we rollback txn that are inflight and not committed
    for (auto& txn_id: transactions_to_undo) {
        std::cout << "Txn Id: " << txn_id << std::endl;
        for (int i = (int) logs.size() - 1; i >= 0; --i) {
            if (logs[i].txn_id == txn_id) {
                if (logs[i].type == LogRecordType::UPDATE_RECORD) {
                    BufferFrame& frame = buffer_manager.fix_page(logs[i].page_id, true);
                    memcpy(&frame.get_data()[logs[i].offset], logs[i].before_img.data(), logs[i].length);
                    buffer_manager.unfix_page(frame, true);
                }
                if (logs[i].type == LogRecordType::BEGIN_RECORD) {
                    break;
                }
            }
        }
    }
}

/**
 * Use txn_id_to_first_log_record to get the begin of the current transaction
 * Walk through the log tape and rollback the changes by writing the before
 * image of the tuple on the buffer page.
 * Note: There might be other transactions' log records interleaved, so be careful to
 * only undo the changes corresponding to current transactions.
 */
void LogManager::rollback_txn(UNUSED_ATTRIBUTE uint64_t txn_id,
                              UNUSED_ATTRIBUTE BufferManager& buffer_manager) {
    std::vector<LogRecordData> logs = read_all_logs(log_file_);
    for (int i = (int) logs.size() - 1; i >= 0; --i) {
        if (logs[i].txn_id == txn_id) {
            if (logs[i].type == LogRecordType::UPDATE_RECORD) {
                BufferFrame& frame = buffer_manager.fix_page(logs[i].page_id, true);
                memcpy(&frame.get_data()[logs[i].offset], logs[i].before_img.data(), logs[i].length);
                buffer_manager.unfix_page(frame, true);
            }
            if (logs[i].type == LogRecordType::BEGIN_RECORD) {
                break;
            }
        }
    }
}

}  // namespace buzzdb
