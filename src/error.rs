use std::borrow::Cow;

/// Error types for chain parsing and processing.
///
/// Represents various error conditions that can occur during chain file parsing,
/// I/O operations, or unsupported features.
///
/// # Variants
///
/// * `Io` - I/O errors from file operations
/// * `Format` - Parsing errors with byte offset and error message
/// * `Unsupported` - Feature or format not supported
///
/// # Examples
///
/// ```
/// use chaintools::ChainError;
/// use std::io;
///
/// let io_err = ChainError::Io(io::Error::new(io::ErrorKind::NotFound, "file not found"));
/// let format_err = ChainError::Format {
///     offset: 100,
///     msg: "invalid chain format".into()
/// };
/// ```
#[derive(Debug)]
pub enum ChainError {
    Io(std::io::Error),
    Format {
        offset: usize,
        msg: Cow<'static, str>,
    },
    #[allow(dead_code)]
    Unsupported {
        msg: Cow<'static, str>,
    },
}

impl From<std::io::Error> for ChainError {
    /// Converts I/O errors to ChainError::Io variant.
    ///
    /// # Arguments
    ///
    /// * `value` - The I/O error to convert
    ///
    /// # Output
    ///
    /// Returns a `ChainError::Io` wrapping the original error
    ///
    /// # Examples
    ///
    /// ```
    /// use chaintools::ChainError;
    /// use std::io;
    ///
    /// let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
    /// let chain_err: ChainError = io_err.into();
    ///
    /// match chain_err {
    ///     ChainError::Io(_) => println!("Got I/O error"),
    ///     _ => println!("Got other error"),
    /// }
    /// ```
    fn from(value: std::io::Error) -> Self {
        ChainError::Io(value)
    }
}
