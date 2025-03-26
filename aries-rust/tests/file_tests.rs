use aries_rust::storage::{FileMode, MemoryFile, File};

#[test]
fn test_file_resize() {
    let mut file = MemoryFile::new(FileMode::WRITE);
    
    // Initial size should be zero
    assert_eq!(file.size().unwrap(), 0);
    
    // Resize to 100 bytes
    file.resize(100).unwrap();
    assert_eq!(file.size().unwrap(), 100);
    
    // Write something and read it back
    let test_data = b"test data";
    file.write_block(test_data, 50).unwrap();
    
    let read_data = file.read_block(50, test_data.len()).unwrap();
    assert_eq!(read_data, test_data);
}