use chaintools::{AbsoluteBlock, Block, BlockSlice, absolute_to_dense_blocks};
use std::sync::Arc;

#[test]
fn block_creation() {
    let block = Block {
        size: 100,
        gap_reference: 50,
        gap_query: 30,
    };

    assert_eq!(block.size, 100);
    assert_eq!(block.gap_reference, 50);
    assert_eq!(block.gap_query, 30);
}

#[test]
fn block_equality() {
    let block1 = Block {
        size: 100,
        gap_reference: 50,
        gap_query: 30,
    };

    let block2 = Block {
        size: 100,
        gap_reference: 50,
        gap_query: 30,
    };

    let block3 = Block {
        size: 200,
        gap_reference: 50,
        gap_query: 30,
    };

    assert_eq!(block1, block2);
    assert_ne!(block1, block3);
}

#[test]
fn block_slice_creation() {
    let blocks = vec![
        Block {
            size: 100,
            gap_reference: 0,
            gap_query: 0,
        },
        Block {
            size: 50,
            gap_reference: 10,
            gap_query: 5,
        },
        Block {
            size: 75,
            gap_reference: 20,
            gap_query: 15,
        },
    ];

    let storage = Arc::new(blocks);
    let slice = BlockSlice::new(storage.clone(), 0..2);

    assert_eq!(slice.as_slice().len(), 2);
    assert_eq!(slice.as_slice()[0].size, 100);
    assert_eq!(slice.as_slice()[1].size, 50);
}

#[test]
fn block_slice_full_range() {
    let blocks = vec![
        Block {
            size: 100,
            gap_reference: 0,
            gap_query: 0,
        },
        Block {
            size: 50,
            gap_reference: 10,
            gap_query: 5,
        },
    ];

    let storage = Arc::new(blocks);
    let slice = BlockSlice::new(storage.clone(), 0..2);

    let block_slice = slice.as_slice();
    assert_eq!(block_slice.len(), 2);
    assert_eq!(block_slice[0].size, 100);
    assert_eq!(block_slice[1].size, 50);
}

#[test]
fn block_slice_single_element() {
    let blocks = vec![
        Block {
            size: 100,
            gap_reference: 0,
            gap_query: 0,
        },
        Block {
            size: 50,
            gap_reference: 10,
            gap_query: 5,
        },
    ];

    let storage = Arc::new(blocks);
    let slice = BlockSlice::new(storage.clone(), 1..2);

    let block_slice = slice.as_slice();
    assert_eq!(block_slice.len(), 1);
    assert_eq!(block_slice[0].size, 50);
}

#[test]
fn block_slice_empty_range() {
    let blocks = vec![
        Block {
            size: 100,
            gap_reference: 0,
            gap_query: 0,
        },
        Block {
            size: 50,
            gap_reference: 10,
            gap_query: 5,
        },
    ];

    let storage = Arc::new(blocks);
    let slice = BlockSlice::new(storage.clone(), 1..1);

    assert_eq!(slice.as_slice().len(), 0);
}

#[test]
fn block_slice_clone() {
    let blocks = vec![Block {
        size: 100,
        gap_reference: 0,
        gap_query: 0,
    }];
    let storage = Arc::new(blocks);
    let slice = BlockSlice::new(storage.clone(), 0..1);

    let cloned_slice = slice.clone();
    assert_eq!(slice.as_slice().len(), cloned_slice.as_slice().len());
    assert_eq!(slice.as_slice()[0], cloned_slice.as_slice()[0]);
}

#[test]
fn absolute_block_lengths_and_validation() {
    let block = AbsoluteBlock {
        reference_start: 10,
        reference_end: 25,
        query_start: 100,
        query_end: 115,
    };

    assert_eq!(block.reference_len(), 15);
    assert_eq!(block.query_len(), 15);
    assert_eq!(block.aligned_len(), Some(15));
    assert!(block.is_gapless_match_block());
    assert!(block.validate().is_ok());
}

#[test]
fn absolute_to_dense_blocks_computes_gaps() {
    let blocks = [
        AbsoluteBlock {
            reference_start: 10,
            reference_end: 20,
            query_start: 50,
            query_end: 60,
        },
        AbsoluteBlock {
            reference_start: 25,
            reference_end: 35,
            query_start: 61,
            query_end: 71,
        },
    ];

    let dense = absolute_to_dense_blocks(&blocks).expect("convert absolute blocks");
    assert_eq!(
        dense,
        vec![
            Block {
                size: 10,
                gap_reference: 5,
                gap_query: 1,
            },
            Block {
                size: 10,
                gap_reference: 0,
                gap_query: 0,
            },
        ]
    );
}

#[test]
fn absolute_to_dense_blocks_rejects_overlaps() {
    let blocks = [
        AbsoluteBlock {
            reference_start: 10,
            reference_end: 20,
            query_start: 50,
            query_end: 60,
        },
        AbsoluteBlock {
            reference_start: 19,
            reference_end: 30,
            query_start: 61,
            query_end: 72,
        },
    ];

    let err = absolute_to_dense_blocks(&blocks).unwrap_err();
    assert!(err.to_string().contains("reference coordinates overlap"));
}

#[test]
fn absolute_to_dense_blocks_rejects_length_mismatch() {
    let blocks = [AbsoluteBlock {
        reference_start: 10,
        reference_end: 20,
        query_start: 50,
        query_end: 61,
    }];

    let err = absolute_to_dense_blocks(&blocks).unwrap_err();
    assert!(err.to_string().contains("lengths differ"));
}
