// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

//! UCSC `chainScore`-compatible rescoring subsystem.
//!
//! A faithful re-implementation of kent's scoring: piecewise-linear gap costs
//! ([`gapcalc`]), the DNA score matrix ([`scoring`]), and the per-chain
//! rescoring engine ([`chainscore`]) that combines them. See the parent
//! [`crate::seq`] module for sequence access.

pub mod chainscore;
pub mod gapcalc;
pub mod scoring;
