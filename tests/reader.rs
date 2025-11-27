use chaintools::{Reader, StreamingReader};
use std::io::BufReader;

const SAMPLE_CHAIN: &str =
    "chain 4900 chrY 58368225 + 25985403 25985638 chr5 151006098 - 43257292 43257528 1\n\
9 1 0\n\
10 0 5\n\
61 4 0\n\
16 0 4\n\
42 3 0\n\
16 0 8\n\
14 1 0\n\
3 7 0\n\
48\n\
\n";

const MULTIPLE_CHAINS: &str = "chain 100 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n50\n50\n\n\
chain 200 chr1 1000 + 100 200 chr2 1000 + 100 200 2\n60\n60\n\n";

#[test]
fn reader_from_owned_bytes() {
    let reader = Reader::from_owned_bytes(SAMPLE_CHAIN.as_bytes().to_vec()).unwrap();

    assert_eq!(reader.len(), 1);
    assert!(!reader.is_empty());

    let chain = reader.chains().next().unwrap();
    assert_eq!(chain.score, 4900);
    assert_eq!(chain.t_name.as_str(), Some("chrY"));
    assert_eq!(chain.q_name.as_str(), Some("chr5"));
    assert_eq!(chain.blocks.as_slice().len(), 9);
}

#[test]
fn reader_multiple_chains() {
    let reader = Reader::from_owned_bytes(MULTIPLE_CHAINS.as_bytes().to_vec()).unwrap();

    assert_eq!(reader.len(), 2);

    let chains: Vec<_> = reader.chains().collect();
    assert_eq!(chains[0].score, 100);
    assert_eq!(chains[0].id, 1);
    assert_eq!(chains[1].score, 200);
    assert_eq!(chains[1].id, 2);
}

#[test]
fn reader_empty_input() {
    let reader = Reader::from_owned_bytes(b"".to_vec()).unwrap();

    assert_eq!(reader.len(), 0);
    assert!(reader.is_empty());

    let chains: Vec<_> = reader.chains().collect();
    assert_eq!(chains.len(), 0);
}

#[test]
fn reader_chains_iterator() {
    let reader = Reader::from_owned_bytes(MULTIPLE_CHAINS.as_bytes().to_vec()).unwrap();

    let mut count = 0;
    let mut total_score = 0;

    for chain in reader.chains() {
        count += 1;
        total_score += chain.score;
    }

    assert_eq!(count, 2);
    assert_eq!(total_score, 300);
}

#[test]
fn reader_chain_details() {
    let reader = Reader::from_owned_bytes(SAMPLE_CHAIN.as_bytes().to_vec()).unwrap();
    let chain = reader.chains().next().unwrap();

    // Test target sequence info
    assert_eq!(chain.t_name.as_str(), Some("chrY"));
    assert_eq!(chain.t_size, 58368225);
    assert_eq!(chain.t_strand, chaintools::Strand::Plus);
    assert_eq!(chain.t_start, 25985403);
    assert_eq!(chain.t_end, 25985638);

    // Test query sequence info
    assert_eq!(chain.q_name.as_str(), Some("chr5"));
    assert_eq!(chain.q_size, 151006098);
    assert_eq!(chain.q_strand, chaintools::Strand::Minus);
    assert_eq!(chain.q_start, 43257292);
    assert_eq!(chain.q_end, 43257528);

    // Test blocks
    assert_eq!(chain.blocks.as_slice().len(), 9);
    assert_eq!(chain.blocks.as_slice()[0].size, 9);
    assert_eq!(chain.blocks.as_slice()[0].dt, 1);
    assert_eq!(chain.blocks.as_slice()[0].dq, 0);
}

#[test]
fn reader_block_details() {
    let reader = Reader::from_owned_bytes(SAMPLE_CHAIN.as_bytes().to_vec()).unwrap();
    let chain = reader.chains().next().unwrap();
    let blocks = chain.blocks.as_slice();

    // Test specific blocks
    assert_eq!(blocks[0].size, 9);
    assert_eq!(blocks[0].dt, 1);
    assert_eq!(blocks[0].dq, 0);

    assert_eq!(blocks[1].size, 10);
    assert_eq!(blocks[1].dt, 0);
    assert_eq!(blocks[1].dq, 5);

    // Last block should have dt=0, dq=0
    let last_block = blocks.last().unwrap();
    assert_eq!(last_block.size, 48);
    assert_eq!(last_block.dt, 0);
    assert_eq!(last_block.dq, 0);
}

#[test]
fn reader_from_path() {
    use std::fs;

    // Create a temporary file
    let temp_file = "test_temp.chain";
    fs::write(temp_file, SAMPLE_CHAIN).expect("Should write temp file");

    let reader = Reader::from_path(temp_file);
    assert!(reader.is_ok(), "Should create reader from path");

    let reader = reader.unwrap();
    assert_eq!(reader.len(), 1);

    // Clean up
    fs::remove_file(temp_file).expect("Should remove temp file");
}

#[test]
fn reader_zero_copy_names() {
    let reader = Reader::from_owned_bytes(SAMPLE_CHAIN.as_bytes().to_vec()).unwrap();
    let chain = reader.chains().next().unwrap();

    // Names should be zero-copy references into the original buffer
    assert_eq!(chain.t_name.as_str(), Some("chrY"));
    assert_eq!(chain.q_name.as_str(), Some("chr5"));

    // Test that the byte slices are correct
    assert_eq!(chain.t_name.as_bytes(), b"chrY");
    assert_eq!(chain.q_name.as_bytes(), b"chr5");
}

#[test]
fn reader_strand_variants() {
    let data_plus = "chain 100 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n50\n50\n\n";
    let data_minus = "chain 100 chr1 1000 - 0 100 chr2 1000 - 0 100 1\n50\n50\n\n";

    // Test plus strand
    let reader = Reader::from_owned_bytes(data_plus.as_bytes().to_vec()).unwrap();
    let chain = reader.chains().next().unwrap();
    assert_eq!(chain.t_strand, chaintools::Strand::Plus);
    assert_eq!(chain.q_strand, chaintools::Strand::Plus);

    // Test minus strand
    let reader = Reader::from_owned_bytes(data_minus.as_bytes().to_vec()).unwrap();
    let chain = reader.chains().next().unwrap();
    assert_eq!(chain.t_strand, chaintools::Strand::Minus);
    assert_eq!(chain.q_strand, chaintools::Strand::Minus);
}

#[test]
fn reader_large_numbers() {
    let data = "chain 9223372036854775807 chr1 4294967295 + 0 4294967295 chr2 4294967295 + 0 4294967295 18446744073709551615\n100\n100\n\n";

    let reader = Reader::from_owned_bytes(data.as_bytes().to_vec()).unwrap();
    let chain = reader.chains().next().unwrap();

    assert_eq!(chain.score, 9223372036854775807); // i64::MAX
    assert_eq!(chain.t_size, 4294967295); // u32::MAX
    assert_eq!(chain.q_size, 4294967295); // u32::MAX
    assert_eq!(chain.id, 18446744073709551615); // u64::MAX
}

#[test]
fn reader_from_owned_bytes_parses() {
    let reader = Reader::from_owned_bytes(SAMPLE_CHAIN.as_bytes().to_vec()).unwrap();
    assert_eq!(reader.len(), 1);
    let chain = reader.chains().next().unwrap();
    assert_eq!(chain.t_name.as_str(), Some("chrY"));
    assert_eq!(chain.q_name.as_str(), Some("chr5"));
    assert_eq!(chain.blocks.as_slice().len(), 9);
    assert_eq!(chain.blocks.as_slice().last().unwrap().dq, 0);
}

#[test]
fn streaming_reader_parses() {
    let cursor = std::io::Cursor::new(SAMPLE_CHAIN.as_bytes());
    let mut reader = StreamingReader::new(BufReader::new(cursor));
    let chain = reader.next_chain().unwrap().expect("chain present");
    assert_eq!(chain.t_name, b"chrY");
    assert_eq!(chain.q_name, b"chr5");
    assert_eq!(chain.blocks.len(), 9);
    assert!(reader.next_chain().unwrap().is_none());
}

#[cfg(feature = "index")]
#[test]
fn index_spans_match() {
    let idx = chaintools::ChainIndex::from_owned(SAMPLE_CHAIN.as_bytes().to_vec()).unwrap();
    assert_eq!(idx.len(), 1);
    let span = idx.spans()[0];
    assert!(idx.chain_bytes(0).is_some());
    assert_eq!(span.offset, 0);
}

#[cfg(feature = "index")]
#[test]
fn index_chain_bytes() {
    let idx = chaintools::ChainIndex::from_owned(SAMPLE_CHAIN.as_bytes().to_vec()).unwrap();
    assert_eq!(idx.len(), 1);
    assert!(idx.chain_bytes(0).is_some());
}
