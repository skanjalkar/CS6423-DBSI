// src/storage/file.rs

use std::fs::{File as StdFile, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::common::{BuzzDBError, Result};

/// File access mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileMode {
    /// Read-only access
    READ,
    /// Read-write access
    WRITE,
}

/// File abstraction for storage operations
pub trait File {
    /// Returns the mode the file was opened with
    fn get_mode(&self) -> FileMode;
    
    /// Returns the current size of the file in bytes
    fn size(&self) -> Result<usize>;
    
    /// Resizes the file to the new size
    /// If new_size is smaller than current size, the file is truncated
    /// If new_size is larger, the file is extended with zeros
    fn resize(&mut self, new_size: usize) -> Result<()>;
    
    /// Reads a block from the file at the given offset
    fn read_block(&mut self, offset: usize, size: usize) -> Result<Vec<u8>>;
    
    /// Writes a block to the file at the given offset
    fn write_block(&mut self, block: &[u8], offset: usize) -> Result<()>;
}

/// Implementation of File for standard filesystem operations
pub struct PosixFile {
    mode: FileMode,
    file: StdFile,
    cached_size: usize,
}

impl PosixFile {
    /// Creates a new PosixFile with the given path and mode
    pub fn new(path: &Path, mode: FileMode) -> Result<Self> {
        let file = match mode {
            FileMode::READ => OpenOptions::new()
                .read(true)
                .open(path)
                .map_err(|e| BuzzDBError::IOError(e))?,
            FileMode::WRITE => OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)
                .map_err(|e| BuzzDBError::IOError(e))?,
        };
        
        let metadata = file.metadata().map_err(|e| BuzzDBError::IOError(e))?;
        let cached_size = metadata.len() as usize;
        
        Ok(Self {
            mode,
            file,
            cached_size,
        })
    }
    
    /// Creates a temporary file that will be deleted when dropped
    pub fn make_temporary() -> Result<Self> {
        use std::env::temp_dir;
        use uuid::Uuid;
        
        let temp_path = temp_dir().join(format!("buzzdb-temp-{}.tmp", Uuid::new_v4()));
        
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&temp_path)
            .map_err(|e| BuzzDBError::IOError(e))?;
            
        // Delete the file when it's closed
        std::fs::remove_file(&temp_path).map_err(|e| BuzzDBError::IOError(e))?;
        
        Ok(Self {
            mode: FileMode::WRITE,
            file,
            cached_size: 0,
        })
    }
}

impl File for PosixFile {
    fn get_mode(&self) -> FileMode {
        self.mode
    }
    
    fn size(&self) -> Result<usize> {
        Ok(self.cached_size)
    }
    
    fn resize(&mut self, new_size: usize) -> Result<()> {
        if new_size == self.cached_size {
            return Ok(());
        }
        
        if self.mode == FileMode::READ {
            return Err(BuzzDBError::Other("Cannot resize a read-only file".to_string()));
        }
        
        self.file.set_len(new_size as u64).map_err(|e| BuzzDBError::IOError(e))?;
        self.cached_size = new_size;
        
        Ok(())
    }
    
    fn read_block(&mut self, offset: usize, size: usize) -> Result<Vec<u8>> {
        if offset + size > self.cached_size {
            return Err(BuzzDBError::Other(format!(
                "Attempt to read past end of file: offset={}, size={}, filesize={}",
                offset, size, self.cached_size
            )));
        }
        
        let mut buffer = vec![0u8; size];
        self.file.seek(SeekFrom::Start(offset as u64)).map_err(|e| BuzzDBError::IOError(e))?;
        self.file.read_exact(&mut buffer).map_err(|e| BuzzDBError::IOError(e))?;
        
        Ok(buffer)
    }
    
    fn write_block(&mut self, block: &[u8], offset: usize) -> Result<()> {
        if self.mode == FileMode::READ {
            return Err(BuzzDBError::Other("Cannot write to a read-only file".to_string()));
        }
        
        if offset + block.len() > self.cached_size {
            self.resize(offset + block.len())?;
        }
        
        self.file.seek(SeekFrom::Start(offset as u64)).map_err(|e| BuzzDBError::IOError(e))?;
        self.file.write_all(block).map_err(|e| BuzzDBError::IOError(e))?;
        self.file.flush().map_err(|e| BuzzDBError::IOError(e))?;
        
        Ok(())
    }
}

/// In-memory implementation of File for testing
pub struct MemoryFile {
    mode: FileMode,
    data: Vec<u8>,
}

impl MemoryFile {
    /// Creates a new empty MemoryFile with the given mode
    pub fn new(mode: FileMode) -> Self {
        Self {
            mode,
            data: Vec::new(),
        }
    }
    
    /// Creates a MemoryFile with the given initial data
    pub fn with_data(data: Vec<u8>, mode: FileMode) -> Self {
        Self {
            mode,
            data,
        }
    }
    
    /// Returns a reference to the underlying data
    pub fn get_data(&self) -> &[u8] {
        &self.data
    }
}

impl File for MemoryFile {
    fn get_mode(&self) -> FileMode {
        self.mode
    }
    
    fn size(&self) -> Result<usize> {
        Ok(self.data.len())
    }
    
    fn resize(&mut self, new_size: usize) -> Result<()> {
        if self.mode == FileMode::READ {
            return Err(BuzzDBError::Other("Cannot resize a read-only file".to_string()));
        }
        
        self.data.resize(new_size, 0);
        Ok(())
    }
    
    fn read_block(&mut self, offset: usize, size: usize) -> Result<Vec<u8>> {
        if offset + size > self.data.len() {
            return Err(BuzzDBError::Other(format!(
                "Attempt to read past end of file: offset={}, size={}, filesize={}",
                offset, size, self.data.len()
            )));
        }
        
        Ok(self.data[offset..offset + size].to_vec())
    }
    
    fn write_block(&mut self, block: &[u8], offset: usize) -> Result<()> {
        if self.mode == FileMode::READ {
            return Err(BuzzDBError::Other("Cannot write to a read-only file".to_string()));
        }
        
        if offset + block.len() > self.data.len() {
            self.resize(offset + block.len())?;
        }
        
        self.data[offset..offset + block.len()].copy_from_slice(block);
        Ok(())
    }
}
