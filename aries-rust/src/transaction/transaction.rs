// src/transaction/mod.rs
use std::collections::{HashSet, HashMap};

use crate::common::{TransactionID, PageID, Result, BuzzDBError};
use crate::log_mod::LogManager;
use crate::buffer::BufferManager;

#[derive(Debug)]
pub struct Transaction {
    pub id: TransactionID,
    pub started: bool,
    pub modified_pages: HashSet<PageID>,
}

impl Transaction {
    pub fn new(id: TransactionID) -> Self {
        Self {
            id,
            started: true,
            modified_pages: HashSet::new(),
        }
    }

    pub fn add_modified_page(&mut self, page_id: PageID) {
        self.modified_pages.insert(page_id);
    }
}

pub struct TransactionManager {
    next_txn_id: u64,
    active_transactions: HashMap<TransactionID, Transaction>,
    log_manager: LogManager,
    buffer_manager: BufferManager,
}

impl TransactionManager {
    pub fn new(log_manager: LogManager, buffer_manager: BufferManager) -> Self {
        Self {
            next_txn_id: 0,
            active_transactions: HashMap::new(),
            log_manager,
            buffer_manager,
        }
    }

    pub fn start_txn(&mut self) -> Result<TransactionID> {
        let txn_id = TransactionID(self.next_txn_id);
        self.next_txn_id += 1;
        
        let txn = Transaction::new(txn_id);
        self.active_transactions.insert(txn_id, txn);
        
        self.log_manager.log_txn_begin(txn_id)?;
        
        Ok(txn_id)
    }

    pub fn commit_txn(&mut self, txn_id: TransactionID) -> Result<()> {
        if let Some(txn) = self.active_transactions.remove(&txn_id) {
            // Flush all modified pages
            for page_id in txn.modified_pages {
                self.buffer_manager.flush_page(page_id)?;
            }
            
            self.log_manager.log_commit(txn_id)?;
        } else {
            return Err(BuzzDBError::Other(format!("Transaction {} not found", txn_id.0)));
        }
        
        Ok(())
    }

    pub fn abort_txn(&mut self, txn_id: TransactionID) -> Result<()> {
        if let Some(txn) = self.active_transactions.remove(&txn_id) {
            // Discard all modified pages
            for page_id in txn.modified_pages {
                self.buffer_manager.discard_page(page_id)?;
            }
            
            self.log_manager.log_abort(txn_id, &mut self.buffer_manager)?;
        } else {
            return Err(BuzzDBError::Other(format!("Transaction {} not found", txn_id.0)));
        }
        
        Ok(())
    }

    pub fn add_modified_page(&mut self, txn_id: TransactionID, page_id: PageID) -> Result<()> {
        if let Some(txn) = self.active_transactions.get_mut(&txn_id) {
            txn.add_modified_page(page_id);
            Ok(())
        } else {
            Err(BuzzDBError::Other(format!("Transaction {} not found", txn_id.0)))
        }
    }
}
