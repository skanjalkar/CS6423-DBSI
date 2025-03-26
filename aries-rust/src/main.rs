use log::{info, LevelFilter};
use env_logger::Builder;
use std::path::Path;

use aries_rust::{
    buffer::BufferManager,
    log_mod::LogManager,
    transaction::TransactionManager,
    common::PageID,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    Builder::new()
        .filter_level(LevelFilter::Info)
        .init();

    info!("Aries Protocol Implementation in Rust");

    // Create temporary paths for our files
    let log_path = Path::new("temp_log.dat");
    
    // Initialize components
    let mut buffer_manager = BufferManager::new(4096, 100);
    let log_manager = LogManager::new(log_path)?;
    let mut txn_manager = TransactionManager::new(log_manager, buffer_manager);
    
    // Start a transaction
    let txn_id = txn_manager.start_txn()?;
    info!("Started transaction {}", txn_id.0);
    
    // Commit the transaction
    txn_manager.commit_txn(txn_id)?;
    info!("Committed transaction {}", txn_id.0);
    
    // Clean up temporary files
    if log_path.exists() {
        std::fs::remove_file(log_path)?;
    }
    
    Ok(())
}