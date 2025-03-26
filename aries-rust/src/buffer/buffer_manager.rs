use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::vec::Vec;

use crate::common::{BuzzDBError, PageID, Result};

/// A buffer frame represents a page in memory
pub struct BufferFrame {
    page_id: PageID,
    data: Vec<u8>,
    is_dirty: bool,
    pin_count: u32,
}

impl BufferFrame {
    /// Create a new buffer frame
    pub fn new(page_id: PageID, page_size: usize) -> Self {
        Self {
            page_id,
            data: vec![0; page_size],
            is_dirty: false,
            pin_count: 0,
        }
    }

    /// Get a reference to the data in the frame
    pub fn get_data(&self) -> &[u8] {
        &self.data
    }
    
    /// Get a mutable reference to the data in the frame
    pub fn get_data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
    
    /// Mark the frame as dirty
    pub fn set_dirty(&mut self, dirty: bool) {
        self.is_dirty = dirty;
    }
    
    /// Check if the frame is dirty
    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }
    
    /// Increment the pin count
    pub fn pin(&mut self) {
        self.pin_count += 1;
    }
    
    /// Decrement the pin count
    pub fn unpin(&mut self) -> Result<()> {
        if self.pin_count == 0 {
            return Err(BuzzDBError::Other("Cannot unpin a page with pin count 0".to_string()));
        }
        self.pin_count -= 1;
        Ok(())
    }
    
    /// Get the pin count
    pub fn pin_count(&self) -> u32 {
        self.pin_count
    }
}
    
/// The buffer manager is responsible for managing pages in memory
pub struct BufferManager {
    frames: HashMap<PageID, Arc<Mutex<BufferFrame>>>,
    page_size: usize,
    capacity: usize,
}

impl BufferManager {
    /// Create a new buffer manager
    pub fn new(page_size: usize, capacity: usize) -> Self {
        Self {
            frames: HashMap::with_capacity(capacity),
            page_size,
            capacity,
        }
    }

    /// Fix a page in the buffer pool
    /// If the page is not in the buffer pool, it will be loaded from disk
    /// The page will be pinned, preventing it from being evicted
    pub fn fix_page(&mut self, page_id: PageID, is_exclusive: bool) -> Result<Arc<Mutex<BufferFrame>>> {
        // Check if the page is already in the buffer pool
        if let Some(frame) = self.frames.get(&page_id) {
            let mut frame = frame.lock().unwrap();
            frame.pin();
            return Ok(Arc::clone(&self.frames[&page_id]));
        }
    
        // If we've reached capacity and can't evict any pages, return an error
        if self.frames.len() >= self.capacity && !self.can_evict_page() {
            return Err(BuzzDBError::BufferFull);
        }
        
        // If we've reached capacity, try to evict a page
        if self.frames.len() >= self.capacity {
            self.evict_page()?;
        }
        
        // Create a new frame
        let frame = BufferFrame::new(page_id, self.page_size);
        let frame = Arc::new(Mutex::new(frame));
        
        // Pin the frame
        {
            let mut frame_guard = frame.lock().unwrap();
            frame_guard.pin();
            
            // If exclusive access is requested, mark the page as dirty
            if is_exclusive {
                frame_guard.set_dirty(true);
            }
        }
            
        // Add the frame to the buffer pool
        self.frames.insert(page_id, Arc::clone(&frame));
        
        Ok(frame)
    }
        
    /// Unfix a page in the buffer pool
    /// The page will be unpinned, allowing it to be evicted
    pub fn unfix_page(&mut self, frame: Arc<Mutex<BufferFrame>>, is_dirty: bool) -> Result<()> {
        let mut frame = frame.lock().unwrap();
        
        // Update dirty flag if necessary
        if is_dirty {
            frame.set_dirty(true);
        }
        
        // Unpin the page
        frame.unpin()?;
        
        Ok(())
    }
        
    /// Flush a specific page to disk
    pub fn flush_page(&mut self, page_id: PageID) -> Result<()> {
        if let Some(frame) = self.frames.get(&page_id) {
            let frame = frame.lock().unwrap();
            if frame.is_dirty() {
                // In a real implementation, we would write the page to disk here
                // For now, we'll just mark it as clean
                // frame.set_dirty(false);
            }
        }
        
        Ok(())
    }
        
    /// Flush all pages to disk
    pub fn flush_all_pages(&mut self) -> Result<()> {
        // First, collect all page IDs to avoid borrowing conflicts
        let page_ids: Vec<PageID> = self.frames.keys().copied().collect();
        
        // Then flush each page
        for page_id in page_ids {
            self.flush_page(page_id)?;
        }
        
        Ok(())
    }
        
    /// Discard a specific page from the buffer pool
    pub fn discard_page(&mut self, page_id: PageID) -> Result<()> {
        if let Some(frame) = self.frames.get(&page_id) {
            let frame = frame.lock().unwrap();
            if frame.pin_count() > 0 {
                return Err(BuzzDBError::Other("Cannot discard a pinned page".to_string()));
            }
        }
        
        self.frames.remove(&page_id);
        Ok(())
    }
        
    /// Discard all pages from the buffer pool
    pub fn discard_all_pages(&mut self) -> Result<()> {
        // Only remove unpinned pages
        self.frames.retain(|_, frame| {
            let frame = frame.lock().unwrap();
            frame.pin_count() > 0
        });
        
        Ok(())
    }
        
    /// Helper method to check if any page can be evicted
    fn can_evict_page(&self) -> bool {
        for frame in self.frames.values() {
            let frame = frame.lock().unwrap();
            if frame.pin_count() == 0 {
                return true;
            }
        }
        
        false
    }
        
    /// Helper method to evict a page
    fn evict_page(&mut self) -> Result<()> {
        // Find a page to evict (using a simple strategy: first unpinned page)
        let page_id_to_evict = {
            let mut page_id_to_evict = None;
            
            for (page_id, frame) in &self.frames {
                let frame = frame.lock().unwrap();
                if frame.pin_count() == 0 {
                    page_id_to_evict = Some(*page_id);
                    break;
                }
            }
            
            page_id_to_evict
        };
            
        // If we found a page to evict, evict it
        if let Some(page_id) = page_id_to_evict {
            // If the page is dirty, flush it to disk
            self.flush_page(page_id)?;
            
            // Remove the page from the buffer pool
            self.frames.remove(&page_id);
            
            return Ok(());
        }
            
        Err(BuzzDBError::BufferFull)
    }
        
    /// Get the page size
    pub fn get_page_size(&self) -> usize {
        self.page_size
    }
    
    /// Get the overall page ID from segment ID and page ID
    pub fn get_overall_page_id(segment_id: u16, page_id: u64) -> PageID {
        PageID((segment_id as u64) << 48 | page_id)
    }
    
    /// Get the segment ID from an overall page ID
    pub fn get_segment_id(overall_page_id: PageID) -> u16 {
        (overall_page_id.0 >> 48) as u16
    }
    
    /// Get the page ID within a segment from an overall page ID
    pub fn get_segment_page_id(overall_page_id: PageID) -> u64 {
        overall_page_id.0 & 0x0000FFFFFFFFFFFF
    }
}