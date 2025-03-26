use log::{info, LevelFilter};
use env_logger::Builder;

mod buffer;
mod heap;
mod log_mod;
mod storage;
mod transaction;
mod common;

fn main() {
    Builder::new()
        .filter_level(LevelFilter::Info) // Set the logging level
        .init();

    info!("Aries Protocol Implementation in Rust");

    // Basic test scenario (can be expanded later)
    // Example:
    // let buffer_manager = buffer::BufferManager::new(4096, 100);
    // info!("Buffer manager initialized");
}