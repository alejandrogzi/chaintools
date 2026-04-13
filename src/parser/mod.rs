// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

pub(crate) mod common;
pub(crate) mod sequential;

#[cfg(feature = "parallel")]
mod parallel;

#[cfg(any(feature = "parallel", feature = "index"))]
pub(crate) use sequential::locate_chain_ranges;
pub(crate) use sequential::parse_chains_sequential;

#[cfg(feature = "parallel")]
pub(crate) use parallel::parse_chains_parallel;
