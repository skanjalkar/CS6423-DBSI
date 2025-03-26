use thiserror::Error; 

#[derive(Debug, Error)]
pub enum BuzzDBError {
    #[error("Not implemented")]
    NotImplemented,
    
    #[error("Buffer full")]
    BufferFull,
    
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    
    #[error("Other error: {0}")]
    Other(String),
    
    #[error("Invalid slot index: {0}")]
    InvalidSlotIndex(usize),
    
    #[error("Slot {0} is empty")]
    EmptySlot(usize),
    
    #[error("Deserialization error")]
    DeserializationError,
    
    #[error("Page {0} is full")]
    PageFull(u64),
    
    #[error("Page {0} not found")]
    PageNotFound(u64),
    
    #[error("Page size exceeded: got {0}, max {1}")]
    PageSizeExceeded(usize, usize),
}

pub type Result<T> = std::result::Result<T, BuzzDBError>;
