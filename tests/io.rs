use chaintools::{Reader, StreamingReader};

#[cfg(feature = "index")]
use chaintools::ChainIndex;

const BASIC_CHAIN: &str = "tests/data/basic.chain";
#[cfg(feature = "gzip")]
const BASIC_CHAIN_GZ: &str = "tests/data/basic.chain.gz";

#[test]
fn reader_reads_plain_chain_file() {
    let reader = Reader::from_path(BASIC_CHAIN).expect("should read plain chain file");
    assert_eq!(reader.len(), 2);

    let chains: Vec<_> = reader.chains().collect();
    assert_eq!(chains[0].score, 100);
    assert_eq!(chains[0].id, 1);
    assert_eq!(chains[1].score, 200);
    assert_eq!(chains[1].id, 2);
}

#[test]
fn streaming_reader_reads_plain_chain_file() {
    let mut reader =
        StreamingReader::from_path(BASIC_CHAIN).expect("should build streaming reader");
    let mut scores = Vec::new();

    while let Some(chain) = reader.next_chain().expect("should parse chain from stream") {
        scores.push(chain.score);
    }

    assert_eq!(scores, vec![100, 200]);
}

#[cfg(feature = "gzip")]
#[test]
fn reader_reads_gzip_chain_file() {
    let reader = Reader::from_path(BASIC_CHAIN_GZ).expect("should read gzipped chain file");
    assert_eq!(reader.len(), 2);

    let chain_ids: Vec<_> = reader.chains().map(|c| c.id).collect();
    assert_eq!(chain_ids, vec![1, 2]);
}

#[cfg(feature = "gzip")]
#[test]
fn streaming_reader_reads_gzip_chain_file() {
    let mut reader =
        StreamingReader::from_path(BASIC_CHAIN_GZ).expect("should build streaming reader");
    let mut ids = Vec::new();

    while let Some(chain) = reader.next_chain().expect("should parse gzipped chain") {
        ids.push(chain.id);
    }

    assert_eq!(ids, vec![1, 2]);
}

#[cfg(feature = "mmap")]
#[test]
fn reader_from_path_with_mmap_enabled() {
    // When mmap is enabled, Reader::from_path uses it for plain files.
    let reader = Reader::from_path(BASIC_CHAIN).expect("should read via mmap");
    assert_eq!(reader.len(), 2);

    let first = reader.chains().next().expect("first chain present");
    assert_eq!(first.score, 100);
    assert_eq!(first.id, 1);
}

#[cfg(feature = "index")]
#[test]
fn chain_index_over_plain_file() {
    let index = ChainIndex::from_path(BASIC_CHAIN).expect("should index plain chain file");
    assert_eq!(index.len(), 2);

    let spans = index.spans();
    assert_eq!(spans.len(), 2);
    assert!(index.chain_bytes(0).unwrap().starts_with(b"chain 100 chr1"));
    assert!(index.chain_bytes(1).unwrap().starts_with(b"chain 200 chr1"));
}

#[cfg(all(feature = "index", feature = "gzip"))]
#[test]
fn chain_index_over_gzip_file() {
    let index = ChainIndex::from_path(BASIC_CHAIN_GZ).expect("should index gzipped chain file");
    assert_eq!(index.len(), 2);

    let chain = std::str::from_utf8(index.chain_bytes(1).expect("second chain bytes")).unwrap();
    assert!(
        chain.contains("chain 200 chr1 1000 + 100 200"),
        "unexpected chain content: {}",
        chain
    );
}
