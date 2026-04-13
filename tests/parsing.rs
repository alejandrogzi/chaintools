// Integration tests for parsing functionality
// Tests the public API that uses the private parser modules

use chaintools::{Reader, StreamingReader};

const COMPLEX_CHAIN: &str = "chain 1000 chr1 10000 + 1000 2000 chr2 8000 - 500 1500 42\n\
100 10 5\n\
200 0 20\n\
50 15 0\n\
300 5 10\n\
150 0 0\n\
\n";

const MALFORMED_CHAINS: &str = "invalid line\n\
chain 100 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n\
50\n\
\n\
chain missing fields\n\
\n";

#[test]
fn parse_complex_chain() {
    let reader = Reader::from_owned_bytes(COMPLEX_CHAIN.as_bytes().to_vec()).unwrap();
    assert_eq!(reader.len(), 1);

    let chain = reader.chains().next().unwrap();
    assert_eq!(chain.score, 1000);
    assert_eq!(chain.id, 42);
    assert_eq!(chain.reference_name.as_str(), Some("chr1"));
    assert_eq!(chain.query_name.as_str(), Some("chr2"));
    assert_eq!(chain.reference_strand, chaintools::Strand::Plus);
    assert_eq!(chain.query_strand, chaintools::Strand::Minus);

    // Check blocks
    let blocks = chain.blocks.as_slice();
    assert_eq!(blocks.len(), 5);

    assert_eq!(blocks[0].size, 100);
    assert_eq!(blocks[0].gap_reference, 10);
    assert_eq!(blocks[0].gap_query, 5);

    assert_eq!(blocks[1].size, 200);
    assert_eq!(blocks[1].gap_reference, 0);
    assert_eq!(blocks[1].gap_query, 20);

    assert_eq!(blocks[2].size, 50);
    assert_eq!(blocks[2].gap_reference, 15);
    assert_eq!(blocks[2].gap_query, 0);

    assert_eq!(blocks[3].size, 300);
    assert_eq!(blocks[3].gap_reference, 5);
    assert_eq!(blocks[3].gap_query, 10);

    assert_eq!(blocks[4].size, 150);
    assert_eq!(blocks[4].gap_reference, 0);
    assert_eq!(blocks[4].gap_query, 0);
}

#[test]
fn parse_malformed_chains() {
    let result = Reader::from_owned_bytes(MALFORMED_CHAINS.as_bytes().to_vec());
    assert!(result.is_err(), "Should fail on malformed input");
}

#[test]
fn streaming_parse_complex_chain() {
    use std::io::{BufReader, Cursor};

    let cursor = Cursor::new(COMPLEX_CHAIN.as_bytes());
    let mut reader = StreamingReader::new(BufReader::new(cursor));

    let chain = reader
        .next_chain()
        .unwrap()
        .expect("Should parse complex chain");
    assert_eq!(chain.score, 1000);
    assert_eq!(chain.blocks.len(), 5);

    let result = reader.next_chain().unwrap();
    assert!(result.is_none(), "Should return None after parsing");
}

#[test]
fn parse_chain_with_large_coordinates() {
    let data = "chain 9223372036854775807 chr1 4294967295 + 0 4294967295 chr2 4294967295 + 0 4294967295 18446744073709551615\n4294967295\n4294967295\n\n";

    let reader = Reader::from_owned_bytes(data.as_bytes().to_vec()).unwrap();
    let chain = reader.chains().next().unwrap();

    assert_eq!(chain.score, 9223372036854775807); // i64::MAX
    assert_eq!(chain.reference_size, 4294967295); // u32::MAX
    assert_eq!(chain.query_size, 4294967295); // u32::MAX
    assert_eq!(chain.id, 18446744073709551615); // u64::MAX
    assert_eq!(chain.blocks.as_slice()[0].size, 4294967295);
}

#[test]
fn parse_chain_with_minimal_blocks() {
    let data = "chain 1 chr1 100 + 0 100 chr2 100 + 0 100 1\n1\n\n";

    let reader = Reader::from_owned_bytes(data.as_bytes().to_vec()).unwrap();
    let chain = reader.chains().next().unwrap();

    assert_eq!(chain.score, 1);
    assert_eq!(chain.blocks.as_slice().len(), 1);
    assert_eq!(chain.blocks.as_slice()[0].size, 1);
    assert_eq!(chain.blocks.as_slice()[0].gap_reference, 0);
    assert_eq!(chain.blocks.as_slice()[0].gap_query, 0);
}

#[test]
fn parse_chain_with_zero_size_blocks() {
    let data = "chain 1 chr1 100 + 0 100 chr2 100 + 0 100 1\n0 10 5\n10 0 0\n\n";

    let reader = Reader::from_owned_bytes(data.as_bytes().to_vec()).unwrap();
    let chain = reader.chains().next().unwrap();

    assert_eq!(chain.blocks.as_slice().len(), 2);
    assert_eq!(chain.blocks.as_slice()[0].size, 0);
    assert_eq!(chain.blocks.as_slice()[0].gap_reference, 10);
    assert_eq!(chain.blocks.as_slice()[0].gap_query, 5);
}

#[test]
fn parse_multiple_chains_with_different_strands() {
    let data = "chain 100 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n50\n50\n\n\
chain 200 chr1 1000 - 0 100 chr2 1000 - 0 100 2\n60\n60\n\n\
chain 300 chr1 1000 + 0 100 chr2 1000 - 0 100 3\n70\n70\n\n\
chain 400 chr1 1000 - 0 100 chr2 1000 + 0 100 4\n80\n80\n\n";

    let reader = Reader::from_owned_bytes(data.as_bytes().to_vec()).unwrap();
    assert_eq!(reader.len(), 4);

    let chains: Vec<_> = reader.chains().collect();

    assert_eq!(chains[0].reference_strand, chaintools::Strand::Plus);
    assert_eq!(chains[0].query_strand, chaintools::Strand::Plus);

    assert_eq!(chains[1].reference_strand, chaintools::Strand::Minus);
    assert_eq!(chains[1].query_strand, chaintools::Strand::Minus);

    assert_eq!(chains[2].reference_strand, chaintools::Strand::Plus);
    assert_eq!(chains[2].query_strand, chaintools::Strand::Minus);

    assert_eq!(chains[3].reference_strand, chaintools::Strand::Minus);
    assert_eq!(chains[3].query_strand, chaintools::Strand::Plus);
}
