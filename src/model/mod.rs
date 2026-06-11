// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

//! Core chain data model.
//!
//! The fundamental, dependency-free types shared by every other part of the
//! crate: the [`chain`] records ([`Chain`](chain::Chain),
//! [`Strand`](chain::Strand)), the [`block`] alignment primitives
//! ([`Block`](block::Block), [`AbsoluteBlock`](block::AbsoluteBlock),
//! [`BlockSlice`](block::BlockSlice),
//! [`absolute_to_dense_blocks`](block::absolute_to_dense_blocks)), and the
//! crate-wide [`error`] type ([`ChainError`](error::ChainError)).

pub mod block;
pub mod chain;
pub mod error;
