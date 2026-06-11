// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

//! Chain input/output.
//!
//! Everything that turns bytes into chains and back: the byte
//! [`storage`] backing (mmap/gzip), the whole-file [`reader`], the low-memory
//! [`stream`]ing reader, the chain [`writer`], and the random-access
//! [`index`]. Parsing primitives live in the sibling [`crate::parser`] module.

pub mod reader;
pub mod storage;
pub mod stream;
pub mod writer;

#[cfg(feature = "index")]
pub mod index;
