use chaintools::{Block, BlockSlice};
use std::sync::Arc;

#[test]
fn block_creation() {
    let block = Block {
        size: 100,
        dt: 50,
        dq: 30,
    };

    assert_eq!(block.size, 100);
    assert_eq!(block.dt, 50);
    assert_eq!(block.dq, 30);
}

#[test]
fn block_equality() {
    let block1 = Block {
        size: 100,
        dt: 50,
        dq: 30,
    };

    let block2 = Block {
        size: 100,
        dt: 50,
        dq: 30,
    };

    let block3 = Block {
        size: 200,
        dt: 50,
        dq: 30,
    };

    assert_eq!(block1, block2);
    assert_ne!(block1, block3);
}

#[test]
fn block_slice_creation() {
    let blocks = vec![
        Block {
            size: 100,
            dt: 0,
            dq: 0,
        },
        Block {
            size: 50,
            dt: 10,
            dq: 5,
        },
        Block {
            size: 75,
            dt: 20,
            dq: 15,
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
            dt: 0,
            dq: 0,
        },
        Block {
            size: 50,
            dt: 10,
            dq: 5,
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
            dt: 0,
            dq: 0,
        },
        Block {
            size: 50,
            dt: 10,
            dq: 5,
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
            dt: 0,
            dq: 0,
        },
        Block {
            size: 50,
            dt: 10,
            dq: 5,
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
        dt: 0,
        dq: 0,
    }];
    let storage = Arc::new(blocks);
    let slice = BlockSlice::new(storage.clone(), 0..1);

    let cloned_slice = slice.clone();
    assert_eq!(slice.as_slice().len(), cloned_slice.as_slice().len());
    assert_eq!(slice.as_slice()[0], cloned_slice.as_slice()[0]);
}
