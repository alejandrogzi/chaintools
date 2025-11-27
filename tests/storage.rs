use chaintools::storage::{ByteSlice, SharedBytes};
use std::path::Path;

#[test]
fn shared_bytes_from_owned() {
    let data = vec![1, 2, 3, 4, 5];
    let shared = SharedBytes::from_owned(data);

    assert_eq!(shared.as_slice(), &[1, 2, 3, 4, 5]);
}

#[test]
fn shared_bytes_clone() {
    let data = vec![1, 2, 3, 4, 5];
    let shared1 = SharedBytes::from_owned(data);
    let shared2 = shared1.clone();

    assert_eq!(shared1.as_slice(), shared2.as_slice());
}

#[test]
fn byte_slice_creation() {
    let storage = SharedBytes::from_owned(vec![0, 10, 20, 30, 40]);
    let slice = ByteSlice::new(storage, 1..4);

    assert_eq!(slice.as_bytes(), &[10, 20, 30]);
}

#[test]
fn byte_slice_full_range() {
    let storage = SharedBytes::from_owned(vec![10, 20, 30]);
    let slice = ByteSlice::new(storage, 0..3);

    assert_eq!(slice.as_bytes(), &[10, 20, 30]);
}

#[test]
fn byte_slice_single_element() {
    let storage = SharedBytes::from_owned(vec![10, 20, 30]);
    let slice = ByteSlice::new(storage, 1..2);

    assert_eq!(slice.as_bytes(), &[20]);
}

#[test]
fn byte_slice_empty_range() {
    let storage = SharedBytes::from_owned(vec![10, 20, 30]);
    let slice = ByteSlice::new(storage, 1..1);

    assert_eq!(slice.as_bytes(), &[]);
}

#[test]
fn byte_slice_valid_utf8() {
    let storage = SharedBytes::from_owned(b"hello world".to_vec());
    let slice = ByteSlice::new(storage, 6..11); // "world"

    assert_eq!(slice.as_str(), Some("world"));
}

#[test]
fn byte_slice_invalid_utf8() {
    let storage = SharedBytes::from_owned(vec![0, 128, 255]);
    let slice = ByteSlice::new(storage, 0..3);

    assert_eq!(slice.as_str(), None);
}

#[test]
fn byte_slice_clone() {
    let storage = SharedBytes::from_owned(vec![10, 20, 30, 40]);
    let slice1 = ByteSlice::new(storage.clone(), 1..3);
    let slice2 = slice1.clone();

    assert_eq!(slice1.as_bytes(), slice2.as_bytes());
}

#[test]
fn is_gz_path_detection() {
    assert!(chaintools::storage::is_gz_path(Path::new("file.txt.gz")));
    assert!(chaintools::storage::is_gz_path(Path::new(
        "path/to/file.chain.gz"
    )));
    assert!(!chaintools::storage::is_gz_path(Path::new("file.txt")));
    assert!(!chaintools::storage::is_gz_path(Path::new("file.gz.txt")));
    assert!(!chaintools::storage::is_gz_path(Path::new("file")));
    assert!(!chaintools::storage::is_gz_path(Path::new("")));
}

#[test]
fn gzip_feature_error() {
    let error = chaintools::storage::gzip_feature_error();

    match error {
        chaintools::ChainError::Unsupported { msg } => {
            assert_eq!(msg, "gzip support disabled; enable the `gzip` feature");
        }
        _ => panic!("Expected Unsupported error"),
    }
}

#[test]
fn shared_bytes_debug_format() {
    let shared = SharedBytes::from_owned(vec![1, 2, 3]);
    let _debug_str = format!("{:?}", shared);
    // Just check that it can be formatted as debug
    let _debug_str = format!("{:?}", shared);
}

#[test]
fn byte_slice_debug_format() {
    let storage = SharedBytes::from_owned(vec![1, 2, 3]);
    let slice = ByteSlice::new(storage, 0..2);
    let debug_str = format!("{:?}", slice);
    assert!(debug_str.contains("ByteSlice"));
}
