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
    let t_name = ByteSlice::new(storage.clone(), 0..4);
    let q_name = ByteSlice::new(storage.clone(), 4..8);

    let chain = Chain {
        score: 4900,
        t_name,
        t_size: 58368225,
        t_strand: Strand::Plus,
        t_start: 25985403,
        t_end: 25985638,
        q_name,
        q_size: 151006098,
        q_strand: Strand::Minus,
        q_start: 43257292,
        q_end: 43257528,
        id: 1,
        blocks: chaintools::BlockSlice::new(
            std::sync::Arc::new(vec![
                chaintools::Block {
                    size: 9,
                    dt: 1,
                    dq: 0,
                },
                chaintools::Block {
                    size: 10,
                    dt: 0,
                    dq: 5,
                },
            ]),
            0..2,
        ),
    };

    assert_eq!(chain.score, 4900);
    assert_eq!(chain.t_name.as_str(), Some("chrY"));
    assert_eq!(chain.q_name.as_str(), Some("chr5"));
    assert_eq!(chain.t_strand, Strand::Plus);
    assert_eq!(chain.q_strand, Strand::Minus);
    assert_eq!(chain.id, 1);
    assert_eq!(chain.blocks.as_slice().len(), 2);
}

#[test]
fn chain_clone() {
    let storage = SharedBytes::from_owned(b"chrYchr5".to_vec());
    let t_name = ByteSlice::new(storage.clone(), 0..4);
    let q_name = ByteSlice::new(storage.clone(), 4..7);

    let chain = Chain {
        score: 100,
        t_name,
        t_size: 1000,
        t_strand: Strand::Plus,
        t_start: 0,
        t_end: 100,
        q_name,
        q_size: 1000,
        q_strand: Strand::Plus,
        q_start: 0,
        q_end: 100,
        id: 1,
        blocks: chaintools::BlockSlice::new(
            std::sync::Arc::new(vec![chaintools::Block {
                size: 100,
                dt: 0,
                dq: 0,
            }]),
            0..1,
        ),
    };

    let cloned = chain.clone();
    assert_eq!(chain.score, cloned.score);
    assert_eq!(chain.id, cloned.id);
    assert_eq!(chain.t_name.as_str(), cloned.t_name.as_str());
    assert_eq!(chain.q_name.as_str(), cloned.q_name.as_str());
}

#[test]
fn chain_debug_format() {
    let storage = SharedBytes::from_owned(b"chrYchr5".to_vec());
    let t_name = ByteSlice::new(storage.clone(), 0..4);
    let q_name = ByteSlice::new(storage.clone(), 4..7);

    let chain = Chain {
        score: 100,
        t_name,
        t_size: 1000,
        t_strand: Strand::Plus,
        t_start: 0,
        t_end: 100,
        q_name,
        q_size: 1000,
        q_strand: Strand::Plus,
        q_start: 0,
        q_end: 100,
        id: 1,
        blocks: chaintools::BlockSlice::new(
            std::sync::Arc::new(vec![chaintools::Block {
                size: 100,
                dt: 0,
                dq: 0,
            }]),
            0..1,
        ),
    };

    let debug_str = format!("{:?}", chain);
    assert!(debug_str.contains("Chain"));
    assert!(debug_str.contains("score: 100"));
    assert!(debug_str.contains("id: 1"));
}
