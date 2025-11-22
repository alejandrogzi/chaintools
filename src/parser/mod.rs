pub(crate) mod common;
mod sequential;

#[cfg(feature = "parallel")]
mod parallel;

#[cfg(any(feature = "parallel", feature = "index"))]
pub(crate) use sequential::locate_chain_ranges;
pub(crate) use sequential::parse_chains_sequential;

#[cfg(feature = "parallel")]
pub(crate) use parallel::parse_chains_parallel;
