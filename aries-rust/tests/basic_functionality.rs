use aries_rust::{
    buffer::BufferManager,
    log_mod::LogManager,
    transaction::TransactionManager,
    storage::{MemoryFile, FileMode, File},
    common::PageID,
};
use std::path::Path;

#[test]
fn test_file_operations() {
    // Create a memory file and test basic operations
    let mut file = MemoryFile::new(FileMode::WRITE);
    
    // Test writing
    file.write_block(b"hello world", 0).unwrap();
    assert_eq!(file.size().unwrap(), 11);
    
    // Test reading
    let data = file.read_block(0, 5).unwrap();
    assert_eq!(&data, b"hello");
}

#[test]
fn test_transaction_lifecycle() {
    // Create temporary log file
    let log_path = Path::new("test_log.dat");
    
    // Set up necessary components
    let mut buffer_manager = BufferManager::new(4096, 10);
    let log_manager = LogManager::new(log_path).unwrap();
    let mut txn_manager = TransactionManager::new(log_manager, buffer_manager);
    
    // Start a transaction
    let txn_id = txn_manager.start_txn().unwrap();
    
    // Commit it
    txn_manager.commit_txn(txn_id).unwrap();
    
    // Clean up
    std::fs::remove_file(log_path).ok();
}