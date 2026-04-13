use chaintools::{StreamItem, StreamingReader};
use std::io::{BufReader, Cursor};

const SIMPLE_CHAIN: &str = "chain 100 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n50\n50\n\n";

#[test]
fn streaming_reader_creation() {
    let cursor = Cursor::new(SIMPLE_CHAIN.as_bytes());
    let _reader = StreamingReader::new(BufReader::new(cursor));

    // Reader should be created successfully
    // We can't test internal state directly since it's private
}

#[test]
fn streaming_reader_parse_single_chain() {
    let cursor = Cursor::new(SIMPLE_CHAIN.as_bytes());
    let mut reader = StreamingReader::new(BufReader::new(cursor));

    let chain = reader.next_chain().unwrap().expect("Should parse a chain");
    assert_eq!(chain.score, 100);
    assert_eq!(chain.reference_name, b"chr1");
    assert_eq!(chain.query_name, b"chr2");
    assert_eq!(chain.blocks.len(), 2);
    assert_eq!(chain.blocks[0].size, 50);
}

#[test]
fn streaming_reader_empty_input() {
    let cursor = Cursor::new("".as_bytes());
    let mut reader = StreamingReader::new(BufReader::new(cursor));

    let result = reader.next_chain().unwrap();
    assert!(result.is_none(), "Should return None for empty input");
}

#[test]
fn streaming_reader_multiple_chains() {
    let data = "chain 100 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n50\n50\n\n\
                chain 200 chr1 1000 + 100 200 chr2 1000 + 100 200 2\n60\n60\n\n";

    let cursor = Cursor::new(data.as_bytes());
    let mut reader = StreamingReader::new(BufReader::new(cursor));

    let chain1 = reader
        .next_chain()
        .unwrap()
        .expect("Should parse first chain");
    assert_eq!(chain1.score, 100);
    assert_eq!(chain1.id, 1);

    let chain2 = reader
        .next_chain()
        .unwrap()
        .expect("Should parse second chain");
    assert_eq!(chain2.score, 200);
    assert_eq!(chain2.id, 2);

    let result = reader.next_chain().unwrap();
    assert!(result.is_none(), "Should return None after all chains");
}

#[test]
fn streaming_reader_skip_blank_lines() {
    let data = "\n\n\
                chain 100 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n50\n50\n\n\
                \n\n";

    let cursor = Cursor::new(data.as_bytes());
    let mut reader = StreamingReader::new(BufReader::new(cursor));

    let chain = reader
        .next_chain()
        .unwrap()
        .expect("Should parse chain despite blank lines");
    assert_eq!(chain.score, 100);

    let result = reader.next_chain().unwrap();
    assert!(result.is_none(), "Should return None after chain");
}

#[test]
fn streaming_reader_next_item_returns_metadata_lines() {
    let data =
        "#meta-one\n\n#meta-two\r\nchain 100 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n50\n50\n\n";

    let cursor = Cursor::new(data.as_bytes());
    let mut reader = StreamingReader::new(BufReader::new(cursor));

    match reader.next_item().unwrap().expect("metadata item") {
        StreamItem::MetaLine(line) => assert_eq!(line, b"#meta-one"),
        StreamItem::Header(_) => panic!("expected metadata line"),
    }

    match reader.next_item().unwrap().expect("second metadata item") {
        StreamItem::MetaLine(line) => assert_eq!(line, b"#meta-two"),
        StreamItem::Header(_) => panic!("expected metadata line"),
    }

    match reader.next_item().unwrap().expect("header item") {
        StreamItem::Header(header) => assert_eq!(header.score, 100),
        StreamItem::MetaLine(_) => panic!("expected header"),
    }
}

#[test]
fn streaming_reader_next_header_skips_metadata_lines() {
    let data = "#meta\nchain 100 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n50\n50\n\n";

    let cursor = Cursor::new(data.as_bytes());
    let mut reader = StreamingReader::new(BufReader::new(cursor));

    let chain = reader.next_chain().unwrap().expect("should parse a chain");
    assert_eq!(chain.score, 100);
    assert_eq!(chain.id, 1);
}

#[test]
fn streaming_reader_chain_with_multiple_blocks() {
    let data = "chain 100 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n10\n20\n30\n\n";

    let cursor = Cursor::new(data.as_bytes());
    let mut reader = StreamingReader::new(BufReader::new(cursor));

    let chain = reader.next_chain().unwrap().expect("Should parse chain");
    assert_eq!(chain.blocks.len(), 3);
    assert_eq!(chain.blocks[0].size, 10);
    assert_eq!(chain.blocks[1].size, 20);
    assert_eq!(chain.blocks[2].size, 30);
}

#[test]
fn streaming_reader_chain_with_gaps() {
    let data = "chain 100 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n10 5 3\n20 0 10\n\n";

    let cursor = Cursor::new(data.as_bytes());
    let mut reader = StreamingReader::new(BufReader::new(cursor));

    let chain = reader.next_chain().unwrap().expect("Should parse chain");
    assert_eq!(chain.blocks.len(), 2);

    assert_eq!(chain.blocks[0].size, 10);
    assert_eq!(chain.blocks[0].gap_reference, 5);
    assert_eq!(chain.blocks[0].gap_query, 3);

    assert_eq!(chain.blocks[1].size, 20);
    assert_eq!(chain.blocks[1].gap_reference, 0);
    assert_eq!(chain.blocks[1].gap_query, 10);
}

#[test]
fn streaming_reader_strand_parsing() {
    let data_plus = "chain 100 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n50\n50\n\n";
    let data_minus = "chain 100 chr1 1000 - 0 100 chr2 1000 - 0 100 1\n50\n50\n\n";

    // Test plus strand
    let cursor = Cursor::new(data_plus.as_bytes());
    let mut reader = StreamingReader::new(BufReader::new(cursor));
    let chain = reader.next_chain().unwrap().expect("Should parse chain");
    assert_eq!(chain.reference_strand, chaintools::Strand::Plus);
    assert_eq!(chain.query_strand, chaintools::Strand::Plus);

    // Test minus strand
    let cursor = Cursor::new(data_minus.as_bytes());
    let mut reader = StreamingReader::new(BufReader::new(cursor));
    let chain = reader.next_chain().unwrap().expect("Should parse chain");
    assert_eq!(chain.reference_strand, chaintools::Strand::Minus);
    assert_eq!(chain.query_strand, chaintools::Strand::Minus);
}

#[test]
fn streaming_reader_error_on_no_blocks() {
    let data = "chain 100 chr1 1000 + 0 100 chr2 1000 + 0 100 1\n\n";

    let cursor = Cursor::new(data.as_bytes());
    let mut reader = StreamingReader::new(BufReader::new(cursor));

    let result = reader.next_chain();
    assert!(
        result.is_err(),
        "Should return error for chain without blocks"
    );
}

#[test]
fn streaming_reader_from_path() {
    use std::fs;

    // Create a temporary file
    let temp_file = "test_temp.chain";
    fs::write(temp_file, SIMPLE_CHAIN).expect("Should write temp file");

    let reader = StreamingReader::from_path(temp_file);
    assert!(reader.is_ok(), "Should create reader from path");

    // Clean up
    fs::remove_file(temp_file).expect("Should remove temp file");
}
