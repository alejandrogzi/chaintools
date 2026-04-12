use chaintools::storage::{ByteSlice, SharedBytes};
use chaintools::{Chain, Strand};

#[test]
fn strand_creation() {
    let plus = Strand::Plus;
    let minus = Strand::Minus;

    assert_eq!(plus, Strand::Plus);
    assert_eq!(minus, Strand::Minus);
    assert_ne!(plus, minus);
}

#[test]
fn strand_copy_and_clone() {
    let original = Strand::Plus;
    let copied = original;
    let cloned = original.clone();

    assert_eq!(original, copied);
    assert_eq!(original, cloned);
}

#[test]
fn chain_creation() {
    let storage = SharedBytes::from_owned(b"chrYchr5".to_vec());
    let reference_name = ByteSlice::new(storage.clone(), 0..4);
    let query_name = ByteSlice::new(storage.clone(), 4..8);

    let chain = Chain {
        score: 4900,
        reference_name,
        reference_size: 58368225,
        reference_strand: Strand::Plus,
        reference_start: 25985403,
        reference_end: 25985638,
        query_name,
        query_size: 151006098,
        query_strand: Strand::Minus,
        query_start: 43257292,
        query_end: 43257528,
        id: 1,
        blocks: chaintools::BlockSlice::new(
            std::sync::Arc::new(vec![
                chaintools::Block {
                    size: 9,
                    gap_reference: 1,
                    gap_query: 0,
                },
                chaintools::Block {
                    size: 10,
                    gap_reference: 0,
                    gap_query: 5,
                },
            ]),
            0..2,
        ),
    };

    assert_eq!(chain.score, 4900);
    assert_eq!(chain.reference_name.as_str(), Some("chrY"));
    assert_eq!(chain.query_name.as_str(), Some("chr5"));
    assert_eq!(chain.reference_strand, Strand::Plus);
    assert_eq!(chain.query_strand, Strand::Minus);
    assert_eq!(chain.id, 1);
    assert_eq!(chain.blocks.as_slice().len(), 2);
}

#[test]
fn chain_clone() {
    let storage = SharedBytes::from_owned(b"chrYchr5".to_vec());
    let reference_name = ByteSlice::new(storage.clone(), 0..4);
    let query_name = ByteSlice::new(storage.clone(), 4..7);

    let chain = Chain {
        score: 100,
        reference_name,
        reference_size: 1000,
        reference_strand: Strand::Plus,
        reference_start: 0,
        reference_end: 100,
        query_name,
        query_size: 1000,
        query_strand: Strand::Plus,
        query_start: 0,
        query_end: 100,
        id: 1,
        blocks: chaintools::BlockSlice::new(
            std::sync::Arc::new(vec![chaintools::Block {
                size: 100,
                gap_reference: 0,
                gap_query: 0,
            }]),
            0..1,
        ),
    };

    let cloned = chain.clone();
    assert_eq!(chain.score, cloned.score);
    assert_eq!(chain.id, cloned.id);
    assert_eq!(
        chain.reference_name.as_str(),
        cloned.reference_name.as_str()
    );
    assert_eq!(chain.query_name.as_str(), cloned.query_name.as_str());
}

#[test]
fn chain_debug_format() {
    let storage = SharedBytes::from_owned(b"chrYchr5".to_vec());
    let reference_name = ByteSlice::new(storage.clone(), 0..4);
    let query_name = ByteSlice::new(storage.clone(), 4..7);

    let chain = Chain {
        score: 100,
        reference_name,
        reference_size: 1000,
        reference_strand: Strand::Plus,
        reference_start: 0,
        reference_end: 100,
        query_name,
        query_size: 1000,
        query_strand: Strand::Plus,
        query_start: 0,
        query_end: 100,
        id: 1,
        blocks: chaintools::BlockSlice::new(
            std::sync::Arc::new(vec![chaintools::Block {
                size: 100,
                gap_reference: 0,
                gap_query: 0,
            }]),
            0..1,
        ),
    };

    let debug_str = format!("{:?}", chain);
    assert!(debug_str.contains("Chain"));
    assert!(debug_str.contains("score: 100"));
    assert!(debug_str.contains("id: 1"));
}
