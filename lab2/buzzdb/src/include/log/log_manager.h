// log_manager.h

#pragma once

#include <cstddef>
#include <map>
#include <string>
#include "buffer/buffer_manager.h"
#include "storage/file.h"

namespace buzzdb {

class LogManager {
 public:
  enum class LogRecordType {
    INVALID_RECORD_TYPE,
    ABORT_RECORD,
    COMMIT_RECORD,
    UPDATE_RECORD,
    BEGIN_RECORD,
    CHECKPOINT_RECORD
  };

  // Constructor / Destructor
  LogManager(File* log_file);
  ~LogManager();

  // Logging methods
  void log_abort(uint64_t txn_id, BufferManager& buffer_manager);
  void log_commit(uint64_t txn_id);
  void log_update(uint64_t txn_id, uint64_t page_id, uint64_t length,
                  uint64_t offset, std::byte* before_img, std::byte* after_img);
  void log_txn_begin(uint64_t txn_id);
  void log_checkpoint(BufferManager& buffer_manager);

  // Recovery
  void recovery(BufferManager& buffer_manager);
  void rollback_txn(uint64_t txn_id, BufferManager& buffer_manager);

  // Reset state (simulate crash)
  void reset(File* log_file);

  // Stats for the unit tests
  uint64_t get_total_log_records();
  uint64_t get_total_log_records_of_type(LogRecordType type);

 private:
  File* log_file_;
  size_t current_offset_ = 0;

  // This map tracks the first log offset for each txn.
  std::map<uint64_t, uint64_t> txn_id_to_first_log_record;
  // Tally of record types for tests
  std::map<LogRecordType, uint64_t> log_record_type_to_count;

  // Helper to actually write len bytes from src into log_file_ at current_offset_.
  void WriteToLog(const void* src, size_t len);

  // Helper to flush writes. In O_SYNC mode this is often a no-op, but we keep it for clarity.
  void ForceFlush(File* file);
};

}  // namespace buzzdb
