use serde::{Serialize, Deserialize};
use crate::common::{PageID, RecordID, BuzzDBError, Result};

#[derive(Debug, Serialize, Deserialize)]
pub struct SlottedPage {
    pub page_id: PageID,
    pub slots: Vec<Option<RecordID>>, // Each slot can hold a record ID or be empty
}

impl SlottedPage {
    pub fn new(page_id: PageID, num_slots: usize) -> Self {
        Self {
            page_id,
            slots: vec![None; num_slots],
        }
    }

    pub fn allocate_slot(&mut self, record_id: RecordID) -> Option<usize> {
        for (index, slot) in self.slots.iter_mut().enumerate() {
            if slot.is_none() {
                *slot = Some(record_id);
                return Some(index);
            }
        }
        None // No empty slot available
    }

    pub fn deallocate_slot(&mut self, slot_index: usize) -> Result<()> {
        if slot_index >= self.slots.len() {
            return Err(BuzzDBError::InvalidSlotIndex(slot_index));
        }
        self.slots[slot_index] = None;
        Ok(())
    }

    pub fn get_record_id(&self, slot_index: usize) -> Result<RecordID> {
        if slot_index >= self.slots.len() {
            return Err(BuzzDBError::InvalidSlotIndex(slot_index));
        }
        
        match self.slots[slot_index] {
            Some(record_id) => Ok(record_id),
            None => Err(BuzzDBError::EmptySlot(slot_index)),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Serialization failed")
    }

    pub fn deserialize(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data).map_err(|_| BuzzDBError::DeserializationError)
    }
}
