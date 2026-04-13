use chaintools::ChainError;
use std::io;

#[test]
fn chain_error_io_creation() {
    let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
    let chain_err = ChainError::from(io_err);

    match chain_err {
        ChainError::Io(err) => {
            assert_eq!(err.kind(), io::ErrorKind::NotFound);
            assert!(format!("{:?}", err).contains("NotFound"));
        }
        _ => panic!("Expected Io error variant"),
    }
}

#[test]
fn chain_error_format_creation() {
    let error = ChainError::Format {
        offset: 100,
        msg: "invalid chain format".into(),
    };

    match error {
        ChainError::Format { offset, msg } => {
            assert_eq!(offset, 100);
            assert_eq!(msg, "invalid chain format");
        }
        _ => panic!("Expected Format error variant"),
    }
}

#[test]
fn chain_error_unsupported_creation() {
    let error = ChainError::Unsupported {
        msg: "feature not available".into(),
    };

    match error {
        ChainError::Unsupported { msg } => {
            assert_eq!(msg, "feature not available");
        }
        _ => panic!("Expected Unsupported error variant"),
    }
}

#[test]
fn chain_error_debug_format() {
    let io_err = ChainError::Io(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "access denied",
    ));
    let format_err = ChainError::Format {
        offset: 50,
        msg: "parse error".into(),
    };
    let unsupported_err = ChainError::Unsupported {
        msg: "not implemented".into(),
    };

    let io_debug = format!("{:?}", io_err);
    let format_debug = format!("{:?}", format_err);
    let unsupported_debug = format!("{:?}", unsupported_err);

    assert!(io_debug.contains("Io"));
    assert!(format_debug.contains("Format"));
    assert!(unsupported_debug.contains("Unsupported"));
}

#[test]
fn chain_error_from_io_convenience() {
    // Test various IO error kinds
    let not_found = ChainError::from(io::Error::new(io::ErrorKind::NotFound, "missing"));
    let permission = ChainError::from(io::Error::new(io::ErrorKind::PermissionDenied, "denied"));
    let invalid = ChainError::from(io::Error::new(io::ErrorKind::InvalidData, "bad data"));

    match not_found {
        ChainError::Io(err) => assert_eq!(err.kind(), io::ErrorKind::NotFound),
        _ => panic!("Expected Io error"),
    }

    match permission {
        ChainError::Io(err) => assert_eq!(err.kind(), io::ErrorKind::PermissionDenied),
        _ => panic!("Expected Io error"),
    }

    match invalid {
        ChainError::Io(err) => assert_eq!(err.kind(), io::ErrorKind::InvalidData),
        _ => panic!("Expected Io error"),
    }
}

#[test]
fn chain_error_structure_comparison() {
    let err1 = ChainError::Format {
        offset: 100,
        msg: "same error".into(),
    };

    let err2 = ChainError::Format {
        offset: 100,
        msg: "same error".into(),
    };

    let err3 = ChainError::Format {
        offset: 200,
        msg: "different offset".into(),
    };

    // Note: ChainError doesn't implement PartialEq, so we can't directly compare
    // But we can test that they have the same structure
    match (&err1, &err2) {
        (
            ChainError::Format {
                offset: o1,
                msg: m1,
            },
            ChainError::Format {
                offset: o2,
                msg: m2,
            },
        ) => {
            assert_eq!(o1, o2);
            assert_eq!(m1, m2);
        }
        _ => panic!("Both should be Format errors"),
    }

    match (&err1, &err3) {
        (
            ChainError::Format {
                offset: o1,
                msg: m1,
            },
            ChainError::Format {
                offset: o2,
                msg: m2,
            },
        ) => {
            assert_ne!(o1, o2); // Different offsets
            assert_ne!(m1, m2); // Different messages
        }
        _ => panic!("Both should be Format errors"),
    }
}
