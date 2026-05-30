// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

//! Core chain data model.
//!
//! The fundamental, dependency-free types shared by every other part of the
//! crate: the [`chain`] records ([`Chain`](chain::Chain),
//! [`Strand`](chain::Strand)), the [`block`] alignment primitives
//! ([`Block`](block::Block), [`BlockSlice`](block::BlockSlice)), and the
//! crate-wide [`error`] type ([`ChainError`](error::ChainError)).

pub mod block;
pub mod chain;
pub mod error;
