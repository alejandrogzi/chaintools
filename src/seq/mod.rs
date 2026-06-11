// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

//! Sequence access and sequence-based analysis (the `sequence` feature).
//!
//! [`sequence`] provides random-access resolution of `.2bit`/FASTA inputs
//! ([`SequenceResolver`](sequence::SequenceResolver),
//! [`SequenceCache`](sequence::SequenceCache)). The engines built on top of it
//! live here too: [`antirepeat`] (repeat/degeneracy filtering) and the
//! [`score`] subsystem (UCSC `chainScore`-compatible rescoring).

pub mod antirepeat;
pub mod revcomp;
pub mod score;
pub mod sequence;
