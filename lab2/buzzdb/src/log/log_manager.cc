#include "log/log_manager.h"

#include <string.h>

#include <cassert>
#include <cstddef>
#include <iostream>
#include <set>
#include <unordered_map>
#include <vector>

#include "common/macros.h"
#include "storage/test_file.h"
#include "buffer/buffer_manager.h"

namespace buzzdb {

/**
 * In this reference solution, we store each log record
 * as follows (in binary) at log_file_->current_offset_:
 *
 *  1)  1 byte  : record_type  (LogRecordType as uint8_t)
 *  2)  8 bytes : txn_id
 *
 *  For UPDATE_RECORDs, we also store:
 *     8 bytes : page_id
 *     8 bytes : length
 *     8 bytes : offset
 *     length bytes : before_img
 *     length bytes : after_img
 *
 *  For other record types, we store no additional fields.
 *
 *  We always flush after each record is written.
 *
 *  For Recovery we do a standard ARIES approach:
 *   - read all records from the beginning into memory
 *   - Analysis: figure out which txns are committed/aborted/in-flight
 *   - Redo: for committed or in-flight transactions, re-apply the after_img
 *   - Undo: for all in-flight transactions, scan backwards and apply the before_img
 *            for each of that transaction's updates.
 *
 *  immediate rollback (log_abort) calls rollback_txn, which does a backward
 *  scan for the single transaction as well, rewriting the before-images to disk.
 *
 *  We also store the first offset of each transaction's log records
 *  in txn_id_to_first_log_record so we can skip scanning from the start
 *  if we want.  In this sample code, we do a simpler full-file pass each time.
 */

// For memory usage, we store ephemeral data about each record in memory:
struct LogRecordData {
  LogManager::LogRecordType type;
  uint64_t txn_id{0};

  // For update:
  uint64_t page_id{0};
  uint64_t length{0};
  uint64_t offset{0};
  std::vector<char> before_img;
  std::vector<char> after_img;

  // offset in log file where this record begins
  size_t log_offset{0};
  // size in bytes
  size_t record_size{0};
};

// Convert enum to integral for writing to disk.
static inline uint8_t encodeRecordType(LogManager::LogRecordType t) {
  return static_cast<uint8_t>(t);
}
static inline LogManager::LogRecordType decodeRecordType(uint8_t x) {
  return static_cast<LogManager::LogRecordType>(x);
}

LogManager::LogManager(File* log_file) {
  log_file_ = log_file;
  current_offset_ = 0;

  log_record_type_to_count[LogRecordType::ABORT_RECORD] = 0;
  log_record_type_to_count[LogRecordType::COMMIT_RECORD] = 0;
  log_record_type_to_count[LogRecordType::UPDATE_RECORD] = 0;
  log_record_type_to_count[LogRecordType::BEGIN_RECORD] = 0;
  log_record_type_to_count[LogRecordType::CHECKPOINT_RECORD] = 0;
}

LogManager::~LogManager() = default;

void LogManager::reset(File* log_file) {
  log_file_ = log_file;
  current_offset_ = 0;
  txn_id_to_first_log_record.clear();
  log_record_type_to_count[LogRecordType::ABORT_RECORD] = 0;
  log_record_type_to_count[LogRecordType::COMMIT_RECORD] = 0;
  log_record_type_to_count[LogRecordType::UPDATE_RECORD] = 0;
  log_record_type_to_count[LogRecordType::BEGIN_RECORD] = 0;
  log_record_type_to_count[LogRecordType::CHECKPOINT_RECORD] = 0;
}

uint64_t LogManager::get_total_log_records() {
  uint64_t sum = 0;
  for (auto &kv : log_record_type_to_count) {
    sum += kv.second;
  }
  return sum;
}

uint64_t LogManager::get_total_log_records_of_type(LogRecordType type) {
  return log_record_type_to_count[type];
}

// A small helper that writes exactly 'len' bytes from 'src' to the
// log file at 'current_offset_' and increments current_offset_.
void LogManager::WriteToLog(const void* src, size_t len) {
  // enlarge file if needed:
  size_t old_size = log_file_->size();
  if (current_offset_ + len > old_size) {
    log_file_->resize(current_offset_ + len);
  }
  // write:
  log_file_->write_block(reinterpret_cast<const char*>(src), current_offset_, len);
  current_offset_ += len;
}

// We also define a helper that flushes the file fully.  For a real
// system, we might only do an fdatasync or similar, but we can just
// rely on flush here (the posix_file is opened O_SYNC in this code).
// In other words, in the interest of the test's simplicity, we won't
// do partial flush logic here.

void LogManager::log_txn_begin(uint64_t txn_id) {
  // We only store record_type (1 byte) and txn_id (8 bytes).
  uint8_t rec_type = encodeRecordType(LogRecordType::BEGIN_RECORD);
  WriteToLog(&rec_type, sizeof(rec_type));
  WriteToLog(&txn_id, sizeof(txn_id));

  ForceFlush(log_file_);

  log_record_type_to_count[LogRecordType::BEGIN_RECORD] += 1;

  // remember first log offset if not present
  if (txn_id_to_first_log_record.find(txn_id) == txn_id_to_first_log_record.end()) {
    txn_id_to_first_log_record[txn_id] = current_offset_;
  }
}

void LogManager::log_commit(uint64_t txn_id) {
  uint8_t rec_type = encodeRecordType(LogRecordType::COMMIT_RECORD);
  WriteToLog(&rec_type, sizeof(rec_type));
  WriteToLog(&txn_id, sizeof(txn_id));

  ForceFlush(log_file_);

  log_record_type_to_count[LogRecordType::COMMIT_RECORD] += 1;
}

void LogManager::log_update(uint64_t txn_id, uint64_t page_id, uint64_t length,
                            uint64_t offset, std::byte* before_img,
                            std::byte* after_img) {
  // record_type, txn_id, page_id, length, offset, then the two images
  uint8_t rec_type = encodeRecordType(LogRecordType::UPDATE_RECORD);
  WriteToLog(&rec_type, sizeof(rec_type));
  WriteToLog(&txn_id, sizeof(txn_id));
  WriteToLog(&page_id, sizeof(page_id));
  WriteToLog(&length, sizeof(length));
  WriteToLog(&offset, sizeof(offset));

  // then before_img, after_img, each 'length' bytes
  WriteToLog(before_img, length);
  WriteToLog(after_img, length);

  ForceFlush(log_file_);

  log_record_type_to_count[LogRecordType::UPDATE_RECORD] += 1;

  // remember first log offset if not present
  if (txn_id_to_first_log_record.find(txn_id) == txn_id_to_first_log_record.end()) {
    txn_id_to_first_log_record[txn_id] = current_offset_;
  }
}

// We'll do an immediate rollback in log_abort: write an ABORT record
// then call rollback_txn, removing from active txns if needed.
void LogManager::log_abort(uint64_t txn_id, BufferManager &buffer_manager) {
  // Write an ABORT record
  uint8_t rec_type = encodeRecordType(LogRecordType::ABORT_RECORD);
  WriteToLog(&rec_type, sizeof(rec_type));
  WriteToLog(&txn_id, sizeof(txn_id));

  ForceFlush(log_file_);

  log_record_type_to_count[LogRecordType::ABORT_RECORD] += 1;

  // Now do an immediate rollback
  rollback_txn(txn_id, buffer_manager);
}

void LogManager::log_checkpoint(BufferManager& buffer_manager) {
  // flush all pages
  buffer_manager.flush_all_pages();

  // write checkpoint record
  uint8_t rec_type = encodeRecordType(LogRecordType::CHECKPOINT_RECORD);
  // We'll store a dummy txn_id (0) in file
  uint64_t dummy_txn = 0;
  WriteToLog(&rec_type, sizeof(rec_type));
  WriteToLog(&dummy_txn, sizeof(dummy_txn));

  ForceFlush(log_file_);

  log_record_type_to_count[LogRecordType::CHECKPOINT_RECORD] += 1;
}

/**
 * To parse the entire log, we do the following:
 *   We'll read until we cannot read a full 1 byte + 8 bytes (type + txn_id).
 *   Then depending on record type, read further fields.
 */
static bool ReadBytes(File* file, size_t offset, void* dest, size_t len) {
  // If offset+len > file->size(), then we can't read
  if (offset + len > file->size()) {
    return false;  // can't read
  }
  file->read_block(offset, len, reinterpret_cast<char*>(dest));
  return true;
}

std::vector<LogRecordData> ReadAllRecords(File* file) {
  std::vector<LogRecordData> records;
  size_t off = 0;
  size_t fsize = file->size();

  while (off + 1 + 8 <= fsize) {
    LogRecordData rec;
    rec.log_offset = off;
    // 1 byte type
    uint8_t raw_type;
    if (!ReadBytes(file, off, &raw_type, 1)) break;
    off += 1;
    rec.type = decodeRecordType(raw_type);

    // 8 bytes txn_id
    if (!ReadBytes(file, off, &rec.txn_id, 8)) break;
    off += 8;

    rec.record_size = 1 + 8;

    if (rec.type == LogManager::LogRecordType::UPDATE_RECORD) {
      // read page_id, length, offset
      if (!ReadBytes(file, off, &rec.page_id, 8)) break;
      off += 8;
      if (!ReadBytes(file, off, &rec.length, 8)) break;
      off += 8;
      if (!ReadBytes(file, off, &rec.offset, 8)) break;
      off += 8;

      rec.record_size += 8 + 8 + 8;

      // read the before/after images
      rec.before_img.resize(rec.length);
      rec.after_img.resize(rec.length);
      if (!ReadBytes(file, off, rec.before_img.data(), rec.length)) break;
      off += rec.length;
      if (!ReadBytes(file, off, rec.after_img.data(), rec.length)) break;
      off += rec.length;

      rec.record_size += 2 * rec.length;
    } else {
      // no additional data for COMMIT,ABORT,BEGIN,CHECKPOINT
    }

    records.push_back(rec);
  }

  return records;
}

/**
 * ARIES Recovery approach:
 * 1) We parse the entire log from beginning to end into a vector<LogRecordData>.
 * 2) Analysis: track which transactions are begun, which are committed, which are aborted.
 * 3) Redo: for each update in the order they appear, if the transaction is either
 *    committed or incomplete (in-flight), we apply the after_img to the page (like repeating history).
 * 4) Undo: for each transaction that is incomplete (in-flight), we scan the log in reverse order,
 *    for that transaction's updates, applying the before_img.
 */
void LogManager::recovery(BufferManager& buffer_manager) {
  size_t fsize = log_file_->size();
  if (fsize == 0) {
    // nothing to do
    return;
  }

  // parse entire log
  auto records = ReadAllRecords(log_file_);

  // analysis
  std::set<uint64_t> in_flight;
  std::set<uint64_t> committed;
  std::set<uint64_t> aborted;

  for (auto &r : records) {
    switch(r.type) {
      case LogRecordType::BEGIN_RECORD:
        in_flight.insert(r.txn_id);
        break;
      case LogRecordType::COMMIT_RECORD:
        if (in_flight.find(r.txn_id) != in_flight.end()) {
          in_flight.erase(r.txn_id);
        }
        committed.insert(r.txn_id);
        break;
      case LogRecordType::ABORT_RECORD:
        if (in_flight.find(r.txn_id) != in_flight.end()) {
          in_flight.erase(r.txn_id);
        }
        aborted.insert(r.txn_id);
        break;
      case LogRecordType::UPDATE_RECORD:
      case LogRecordType::CHECKPOINT_RECORD:
        // do nothing
        break;
      default:
        break;
    }
  }

  // redo
  for (auto &r : records) {
    if (r.type == LogRecordType::UPDATE_RECORD) {
      // we redo the update if that txn either ended commit or is still in-flight
      if (committed.find(r.txn_id) != committed.end() ||
          in_flight.find(r.txn_id) != in_flight.end()) {
        // apply after_img
        BufferFrame& frame = buffer_manager.fix_page(r.page_id, true);
        memcpy(&frame.get_data()[r.offset], r.after_img.data(), r.length);
        buffer_manager.unfix_page(frame, true);
      }
    }
  }

  // undo
  // For each in-flight transaction, we skip if it ended up in 'committed' or 'aborted'
  // Actually we want those in in_flight sets. Then we do a backward pass.
  // We'll gather the relevant updates in reverse order
  // We then apply the before_img for them.
  // That effectively aborts them.
  for (auto txn_id : in_flight) {
    // do a backward pass
    for (int i = (int)records.size()-1; i >= 0; i--) {
      auto &r = records[i];
      if (r.type == LogRecordType::UPDATE_RECORD && r.txn_id == txn_id) {
        // apply before_img
        BufferFrame& frame = buffer_manager.fix_page(r.page_id, true);
        memcpy(&frame.get_data()[r.offset], r.before_img.data(), r.length);
        buffer_manager.unfix_page(frame, true);
      }
      if (r.type == LogRecordType::BEGIN_RECORD && r.txn_id == txn_id) {
        // we've undone the entire transaction
        break;
      }
    }
  }
}

// In log_manager.cc:
void LogManager::ForceFlush(File* file) {
  (void)file;  // Mark parameter unused
  // In POSIX O_SYNC mode, writes are automatically on stable storage,
  // so we do nothing else here.
}

/**
 * Roll back the transaction with the given txn_id immediately, by scanning the log
 * backward and applying the before_img for each of that transaction's updates,
 * stopping after we reach the BEGIN_RECORD for that transaction.
 */
void LogManager::rollback_txn(uint64_t txn_id, BufferManager& buffer_manager) {
  // parse entire log
  auto records = ReadAllRecords(log_file_);

  // do a backward pass
  for (int i = (int)records.size()-1; i >= 0; i--) {
    auto &r = records[i];
    if (r.txn_id == txn_id) {
      if (r.type == LogRecordType::UPDATE_RECORD) {
        // apply before image
        BufferFrame& frame = buffer_manager.fix_page(r.page_id, true);
        memcpy(&frame.get_data()[r.offset], r.before_img.data(), r.length);
        buffer_manager.unfix_page(frame, true);
      }
      if (r.type == LogRecordType::BEGIN_RECORD) {
        // done
        break;
      }
    }
  }
}

}  // namespace buzzdb
