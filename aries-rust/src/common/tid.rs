use serde::{Serialize, Deserialize};
use std::cmp::Ordering;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TID {
    pub page_id: u64,
    pub slot_id: u64,
}

impl TID {
    pub fn new(page_id: u64, slot_id: u64) -> Self {
        Self { page_id, slot_id }
    }
}

impl PartialOrd for TID {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TID {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.page_id.cmp(&other.page_id) {
            Ordering::Equal => self.slot_id.cmp(&other.slot_id),
            other => other,
        }
    }
}
