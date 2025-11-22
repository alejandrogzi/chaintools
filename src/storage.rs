use std::ops::Range;
use std::sync::Arc;

use crate::ChainError;

#[cfg(feature = "mmap")]
use memmap2::Mmap;

/// Shared byte storage. Keeps parsing zero-copy while remaining lifetime-safe.
#[derive(Debug, Clone)]
pub enum SharedBytes {
    #[cfg(feature = "mmap")]
    Mmap(Arc<Mmap>),
    Owned(Arc<Vec<u8>>),
}

impl SharedBytes {
    /// Returns the entire buffer as a byte slice.
    ///
    /// This provides a view into the underlying storage, whether it is a
    /// memory-mapped file or an owned vector.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::storage::SharedBytes;
    ///
    /// let data = vec![10, 20, 30];
    /// let shared_bytes = SharedBytes::from_owned(data);
    ///
    /// assert_eq!(shared_bytes.as_slice(), &[10, 20, 30]);
    /// ```
    pub fn as_slice(&self) -> &[u8] {
        match self {
            #[cfg(feature = "mmap")]
            SharedBytes::Mmap(m) => &m[..],
            SharedBytes::Owned(buf) => buf.as_slice(),
        }
    }

    #[cfg(feature = "mmap")]
    /// Creates a `SharedBytes` instance from a memory map.
    ///
    /// This is used to wrap a memory-mapped file in a reference-counted
    /// pointer, allowing for safe, zero-copy sharing of the data.
    ///
    /// This function is only available when the `mmap` feature is enabled.
    pub(crate) fn from_mmap(mmap: Mmap) -> Self {
        SharedBytes::Mmap(Arc::new(mmap))
    }

    /// Creates a `SharedBytes` instance from an owned `Vec<u8>`.
    ///
    /// The vector is wrapped in a reference-counted pointer to allow for
    /// cheap, lifetime-safe cloning and sharing.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::storage::SharedBytes;
    ///
    /// let data = vec![1, 2, 3, 4, 5];
    /// let shared_bytes = SharedBytes::from_owned(data);
    ///
    /// assert_eq!(shared_bytes.as_slice(), &[1, 2, 3, 4, 5]);
    /// ```
    pub fn from_owned(data: Vec<u8>) -> Self {
        SharedBytes::Owned(Arc::new(data))
    }
}

/// A lightweight, clonable view into a subsection of a `SharedBytes` buffer.
///
/// This struct holds a reference-counted pointer to the underlying storage
/// and a `Range` indicating the specific slice it represents. This makes it
/// cheap to pass around, as it doesn't own the actual data.
///
/// # Examples
///
/// ```ignore
/// use chaintools::storage::{ByteSlice, SharedBytes};
///
/// let storage = SharedBytes::from_owned(vec![0, 10, 20, 30, 40]);
///
/// // Create a slice representing the bytes 10, 20, 30
/// let byte_slice = ByteSlice::new(storage, 1..4);
///
/// assert_eq!(byte_slice.as_bytes(), &[10, 20, 30]);
///
/// // Clones are cheap
/// let another_slice = byte_slice.clone();
/// assert_eq!(another_slice.as_bytes(), &[10, 20, 30]);
/// ```
#[derive(Debug, Clone)]
pub struct ByteSlice {
    storage: SharedBytes,
    range: Range<usize>,
}

impl ByteSlice {
    /// Creates a new `ByteSlice`.
    ///
    /// This is a lightweight operation, as it only clones the reference
    /// to the underlying storage and stores the range.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::storage::{ByteSlice, SharedBytes};
    ///
    /// let storage = SharedBytes::from_owned(vec![0, 10, 20, 30, 40]);
    ///
    /// // Create a slice representing the bytes 10, 20, 30
    /// let byte_slice = ByteSlice::new(storage, 1..4);
    ///
    /// assert_eq!(byte_slice.as_bytes(), &[10, 20, 30]);
    /// ```
    pub fn new(storage: SharedBytes, range: Range<usize>) -> Self {
        ByteSlice { storage, range }
    }

    /// Returns the byte slice represented by this `ByteSlice`.
    ///
    /// This method provides a view into the portion of the underlying
    /// storage that this slice represents.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::storage::{ByteSlice, SharedBytes};
    ///
    /// let storage = SharedBytes::from_owned(vec![0, 10, 20, 30, 40]);
    /// let byte_slice = ByteSlice::new(storage, 1..4);
    ///
    /// assert_eq!(byte_slice.as_bytes(), &[10, 20, 30]);
    /// ```
    pub fn as_bytes(&self) -> &[u8] {
        &self.storage.as_slice()[self.range.clone()]
    }

    /// Attempts to convert the byte slice to a UTF-8 string slice.
    ///
    /// This will return `Some(&str)` if the slice contains valid UTF-8,
    /// and `None` otherwise.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use chaintools::storage::{ByteSlice, SharedBytes};
    ///
    /// // A slice with valid UTF-8
    /// let storage1 = SharedBytes::from_owned(b"hello world".to_vec());
    /// let slice1 = ByteSlice::new(storage1, 6..11);
    /// assert_eq!(slice1.as_str(), Some("world"));
    ///
    /// // A slice with invalid UTF-8
    /// let storage2 = SharedBytes::from_owned(vec![0, 128, 255]);
    /// let slice2 = ByteSlice::new(storage2, 0..3);
    /// assert_eq!(slice2.as_str(), None);
    /// ```
    pub fn as_str(&self) -> Option<&str> {
        std::str::from_utf8(self.as_bytes()).ok()
    }
}

/// Checks if a file path has a `.gz` extension.
///
/// This is a small helper to detect gzip-compressed files without requiring
/// a hard dependency on a compression library.
///
/// # Examples
///
/// ```ignore
/// use std::path::Path;
/// use chaintools::storage::is_gz_path;
///
/// assert!(is_gz_path(Path::new("some/file.txt.gz")));
/// assert!(!is_gz_path(Path::new("another/file.txt")));
/// assert!(!is_gz_path(Path::new("no_extension")));
/// ```
pub fn is_gz_path(path: &std::path::Path) -> bool {
    path.extension().is_some_and(|ext| ext == "gz")
}

/// Constructs a `ChainError::Unsupported` for when the `gzip` feature is needed.
///
/// This provides a consistent error message for operations that require
/// gzip support when the feature is not compiled in.
///
/// # Examples
///
/// ```ignore
/// use chaintools::storage::gzip_feature_error;
/// use chaintools::ChainError;
///
/// let error = gzip_feature_error();
///
/// match error {
///     ChainError::Unsupported { msg } => {
///         assert_eq!(msg, "gzip support disabled; enable the `gzip` feature");
///     },
///     _ => panic!("Expected an Unsupported error"),
/// }
/// ```
pub fn gzip_feature_error() -> ChainError {
    ChainError::Unsupported {
        msg: "gzip support disabled; enable the `gzip` feature".into(),
    }
}
