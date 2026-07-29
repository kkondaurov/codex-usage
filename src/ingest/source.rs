use anyhow::{Context, Result};
use std::ffi::OsStr;
use std::fs::{File, Metadata};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
#[cfg(not(test))]
use std::time::Duration;

// Bound individual JSONL records so a missing newline cannot grow memory
// without limit. Large payloads are parsed for metadata but are never retained
// in the SQLite projection.
pub(super) const MAX_JSONL_LINE_BYTES: usize = 32 * 1024 * 1024;

// A live scan shares the machine with latency-sensitive HTTP reads. Briefly
// sleeping after each bounded slice gives those foreground workers regular
// CPU and storage scheduling opportunities without changing which source
// bytes are fingerprinted or where a file transaction commits.
pub(super) const BACKGROUND_INGEST_SLICE_BYTES: u64 = 8 * 1024 * 1024;
#[cfg(not(test))]
pub(super) const BACKGROUND_INGEST_PAUSE: Duration = Duration::from_millis(1);

pub(super) fn is_compressed_rollout_path(path: &Path) -> bool {
    path.file_name()
        .and_then(OsStr::to_str)
        .is_some_and(|name| name.ends_with(".jsonl.zst"))
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum IngestPacing {
    #[default]
    Unpaced,
    Background,
}

#[derive(Debug)]
struct IngestPacer {
    mode: IngestPacing,
    slice_bytes: u64,
    bytes_in_slice: u64,
    #[cfg(test)]
    pauses: u64,
}

impl Default for IngestPacer {
    fn default() -> Self {
        Self::new(IngestPacing::Unpaced)
    }
}

impl IngestPacer {
    fn new(mode: IngestPacing) -> Self {
        Self {
            mode,
            slice_bytes: BACKGROUND_INGEST_SLICE_BYTES,
            bytes_in_slice: 0,
            #[cfg(test)]
            pauses: 0,
        }
    }

    fn account(&mut self, bytes: u64) {
        if self.mode == IngestPacing::Unpaced || bytes == 0 {
            return;
        }
        let total = self.bytes_in_slice.saturating_add(bytes);
        let pauses = total / self.slice_bytes;
        self.bytes_in_slice = total % self.slice_bytes;
        for _ in 0..pauses {
            self.pause();
        }
    }

    fn pause(&mut self) {
        #[cfg(test)]
        {
            self.pauses = self.pauses.saturating_add(1);
        }
        #[cfg(not(test))]
        std::thread::sleep(BACKGROUND_INGEST_PAUSE);
    }

    #[cfg(test)]
    fn with_slice(mode: IngestPacing, slice_bytes: u64) -> Self {
        assert!(slice_bytes > 0);
        let mut pacer = Self::new(mode);
        pacer.slice_bytes = slice_bytes;
        pacer
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct FileIdentity {
    pub(super) ctime_ns: Option<i64>,
    pub(super) device_id: Option<i64>,
    pub(super) inode: Option<i64>,
}

impl FileIdentity {
    pub(super) fn is_complete(self) -> bool {
        self.ctime_ns.is_some() && self.device_id.is_some() && self.inode.is_some()
    }

    pub(super) fn same_file(self, other: Self) -> bool {
        self.is_complete()
            && other.is_complete()
            && self.device_id == other.device_id
            && self.inode == other.inode
    }
}

#[cfg(unix)]
pub(super) fn file_identity(metadata: &Metadata) -> FileIdentity {
    let ctime_ns = metadata
        .ctime()
        .checked_mul(1_000_000_000)
        .and_then(|seconds| seconds.checked_add(metadata.ctime_nsec()));
    FileIdentity {
        ctime_ns,
        device_id: i64::try_from(metadata.dev()).ok(),
        inode: i64::try_from(metadata.ino()).ok(),
    }
}

#[cfg(not(unix))]
pub(super) fn file_identity(_metadata: &Metadata) -> FileIdentity {
    FileIdentity::default()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CapturedExtent {
    size: u64,
    modified_ns: u64,
    identity: FileIdentity,
}

impl CapturedExtent {
    pub(super) fn size(self) -> u64 {
        self.size
    }

    pub(super) fn modified_ns(self) -> u64 {
        self.modified_ns
    }

    pub(super) fn identity(self) -> FileIdentity {
        self.identity
    }
}

pub(super) struct SourceSnapshot {
    path: PathBuf,
    file: File,
    extent: CapturedExtent,
    pacer: IngestPacer,
    compressed: bool,
    materialized: bool,
}

impl SourceSnapshot {
    pub(super) fn open(path: &Path) -> Result<Self> {
        let file =
            File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        let metadata = file
            .metadata()
            .with_context(|| format!("failed to stat opened source {}", path.display()))?;
        let modified_ns = metadata
            .modified()
            .ok()
            .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|value| value.as_nanos().min(u64::MAX as u128) as u64)
            .unwrap_or_default();
        let compressed = is_compressed_rollout_path(path);
        Ok(Self {
            path: path.to_path_buf(),
            extent: CapturedExtent {
                size: metadata.len(),
                modified_ns,
                identity: file_identity(&metadata),
            },
            file,
            pacer: IngestPacer::default(),
            compressed,
            materialized: !compressed,
        })
    }

    pub(super) fn set_pacing(&mut self, pacing: IngestPacing) {
        self.pacer = IngestPacer::new(pacing);
    }

    pub(super) fn is_compressed(&self) -> bool {
        self.compressed
    }

    pub(super) fn ensure_materialized(&mut self) -> io::Result<()> {
        if self.materialized {
            return Ok(());
        }
        self.file.rewind()?;
        let mut output = tempfile::tempfile()?;
        let mut buffer = vec![0_u8; 256 * 1024];
        let mut size = 0_u64;
        {
            let mut decoder = zstd::stream::read::Decoder::new(&mut self.file)?;
            loop {
                let read = decoder.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                output.write_all(&buffer[..read])?;
                size = size.checked_add(read as u64).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("{} decompressed size overflowed", self.path.display()),
                    )
                })?;
                self.pacer.account(read as u64);
            }
        }
        output.flush()?;
        output.rewind()?;
        self.file = output;
        self.extent.size = size;
        self.materialized = true;
        Ok(())
    }

    pub(super) fn extent(&self) -> CapturedExtent {
        self.extent
    }

    pub(super) fn read_exact_at(&mut self, offset: u64, buffer: &mut [u8]) -> io::Result<()> {
        self.ensure_materialized()?;
        let end = offset.checked_add(buffer.len() as u64).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{} source range overflowed", self.path.display()),
            )
        })?;
        if end > self.extent.size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{} source range exceeds captured extent",
                    self.path.display()
                ),
            ));
        }
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(buffer)?;
        self.pacer.account(buffer.len() as u64);
        Ok(())
    }

    pub(super) fn jsonl_from(&mut self, offset: u64) -> io::Result<CapturedJsonlReader<'_>> {
        if self.compressed && !self.materialized && offset == 0 {
            self.file.rewind()?;
            let decoder = zstd::stream::read::Decoder::new(&mut self.file)?;
            return Ok(CapturedJsonlReader {
                reader: Box::new(BufReader::new(decoder)),
                pacer: &mut self.pacer,
            });
        }
        self.ensure_materialized()?;
        let remaining = self.extent.size.checked_sub(offset).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{} source offset exceeds captured extent",
                    self.path.display()
                ),
            )
        })?;
        self.file.seek(SeekFrom::Start(offset))?;
        Ok(CapturedJsonlReader {
            reader: Box::new(BufReader::new((&mut self.file).take(remaining))),
            pacer: &mut self.pacer,
        })
    }
}

pub(super) struct CapturedJsonlReader<'a> {
    reader: Box<dyn BufRead + 'a>,
    pacer: &'a mut IngestPacer,
}

impl CapturedJsonlReader<'_> {
    pub(super) fn next_bounded_line(
        &mut self,
        buffer: &mut Vec<u8>,
        limit: usize,
    ) -> io::Result<BoundedLine> {
        if self.pacer.mode == IngestPacing::Unpaced {
            return read_bounded_line(&mut self.reader, buffer, limit);
        }
        let reader = &mut self.reader;
        let pacer = &mut self.pacer;
        read_bounded_line_with_accounting(reader, buffer, limit, |bytes| pacer.account(bytes))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BoundedLine {
    Eof,
    Complete { len: u64, oversized: bool },
    Incomplete { len: u64, oversized: bool },
}

pub(super) fn read_bounded_line<R: BufRead>(
    reader: &mut R,
    buffer: &mut Vec<u8>,
    limit: usize,
) -> io::Result<BoundedLine> {
    read_bounded_line_with_accounting(reader, buffer, limit, |_| {})
}

fn read_bounded_line_with_accounting<R: BufRead>(
    reader: &mut R,
    buffer: &mut Vec<u8>,
    limit: usize,
    mut account: impl FnMut(u64),
) -> io::Result<BoundedLine> {
    buffer.clear();
    let mut len = 0_u64;
    let mut oversized = false;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(if len == 0 {
                BoundedLine::Eof
            } else {
                BoundedLine::Incomplete { len, oversized }
            });
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(available.len(), |index| index + 1);
        if !oversized {
            if buffer.len().saturating_add(take) <= limit {
                buffer.extend_from_slice(&available[..take]);
            } else {
                oversized = true;
                buffer.clear();
            }
        }
        reader.consume(take);
        account(take as u64);
        len = len.saturating_add(take as u64);
        if newline.is_some() {
            return Ok(BoundedLine::Complete { len, oversized });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Write};

    #[test]
    fn compressed_snapshot_exposes_logical_jsonl_extent() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source.jsonl.zst");
        let output = File::create(&path).unwrap();
        let mut encoder = zstd::stream::write::Encoder::new(output, 3).unwrap();
        encoder.write_all(b"old\n").unwrap();
        encoder.finish().unwrap();

        let mut snapshot = SourceSnapshot::open(&path).unwrap();
        snapshot.ensure_materialized().unwrap();
        assert_eq!(snapshot.extent().size(), 4);
        let mut reader = snapshot.jsonl_from(0).unwrap();
        let mut buffer = Vec::new();
        assert_eq!(
            reader.next_bounded_line(&mut buffer, 16).unwrap(),
            BoundedLine::Complete {
                len: 4,
                oversized: false
            }
        );
        assert_eq!(buffer, b"old\n");
        assert_eq!(
            reader.next_bounded_line(&mut buffer, 16).unwrap(),
            BoundedLine::Eof
        );
    }

    #[test]
    fn source_snapshot_reader_stops_at_captured_extent_after_append() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source.jsonl");
        std::fs::write(&path, b"old\n").unwrap();
        let mut snapshot = SourceSnapshot::open(&path).unwrap();
        let mut file = File::options().append(true).open(&path).unwrap();
        file.write_all(b"new\n").unwrap();
        drop(file);

        let mut reader = snapshot.jsonl_from(0).unwrap();
        let mut buffer = Vec::new();
        assert_eq!(
            reader.next_bounded_line(&mut buffer, 16).unwrap(),
            BoundedLine::Complete {
                len: 4,
                oversized: false
            }
        );
        assert_eq!(buffer, b"old\n");
        assert_eq!(
            reader.next_bounded_line(&mut buffer, 16).unwrap(),
            BoundedLine::Eof
        );
    }

    #[test]
    fn background_pacer_preserves_exact_thresholds_and_remainder() {
        let mut pacer = IngestPacer::with_slice(IngestPacing::Background, 8);
        pacer.account(7);
        assert_eq!(pacer.pauses, 0);
        assert_eq!(pacer.bytes_in_slice, 7);

        pacer.account(1);
        assert_eq!(pacer.pauses, 1);
        assert_eq!(pacer.bytes_in_slice, 0);

        pacer.account(19);
        assert_eq!(pacer.pauses, 3);
        assert_eq!(pacer.bytes_in_slice, 3);

        let mut unpaced = IngestPacer::with_slice(IngestPacing::Unpaced, 8);
        unpaced.account(u64::MAX);
        assert_eq!(unpaced.pauses, 0);
        assert_eq!(unpaced.bytes_in_slice, 0);
    }

    #[test]
    fn captured_reads_share_one_background_pacing_budget() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source.jsonl");
        std::fs::write(&path, b"abcd\nef\n").unwrap();
        let mut snapshot = SourceSnapshot::open(&path).unwrap();
        snapshot.pacer = IngestPacer::with_slice(IngestPacing::Background, 4);

        let mut buffer = Vec::new();
        {
            let mut reader = snapshot.jsonl_from(0).unwrap();
            assert_eq!(
                reader.next_bounded_line(&mut buffer, 16).unwrap(),
                BoundedLine::Complete {
                    len: 5,
                    oversized: false
                }
            );
            assert_eq!(
                reader.next_bounded_line(&mut buffer, 16).unwrap(),
                BoundedLine::Complete {
                    len: 3,
                    oversized: false
                }
            );
        }
        assert_eq!(snapshot.pacer.pauses, 2);
        assert_eq!(snapshot.pacer.bytes_in_slice, 0);

        let mut exact = [0_u8; 4];
        snapshot.read_exact_at(0, &mut exact).unwrap();
        assert_eq!(&exact, b"abcd");
        assert_eq!(snapshot.pacer.pauses, 3);
        assert_eq!(snapshot.pacer.bytes_in_slice, 0);
    }

    #[test]
    fn oversized_line_accounts_each_buffered_chunk_while_it_is_drained() {
        let mut source = vec![b'x'; 20];
        source.push(b'\n');
        let mut reader = BufReader::with_capacity(4, source.as_slice());
        let mut buffer = Vec::new();
        let mut charges = Vec::new();

        let line = read_bounded_line_with_accounting(&mut reader, &mut buffer, 3, |bytes| {
            charges.push(bytes)
        })
        .unwrap();

        assert_eq!(
            line,
            BoundedLine::Complete {
                len: 21,
                oversized: true
            }
        );
        assert!(buffer.is_empty());
        assert_eq!(charges, [4, 4, 4, 4, 4, 1]);
    }

    #[test]
    fn source_snapshot_rejects_ranges_beyond_captured_extent() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source.jsonl");
        std::fs::write(&path, b"data").unwrap();
        let mut snapshot = SourceSnapshot::open(&path).unwrap();
        let mut exact = [0_u8; 2];
        snapshot.read_exact_at(2, &mut exact).unwrap();
        assert_eq!(&exact, b"ta");

        let mut beyond = [0_u8; 2];
        let error = snapshot.read_exact_at(3, &mut beyond).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn source_snapshot_reports_short_read_after_in_place_truncation() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source.jsonl");
        std::fs::write(&path, b"captured").unwrap();
        let mut snapshot = SourceSnapshot::open(&path).unwrap();
        File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(3)
            .unwrap();

        let mut captured = [0_u8; 8];
        let error = snapshot.read_exact_at(0, &mut captured).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[cfg(unix)]
    #[test]
    fn source_snapshot_keeps_opened_inode_after_rename_over() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source.jsonl");
        let replacement = temp.path().join("replacement.jsonl");
        std::fs::write(&path, b"old\n").unwrap();
        std::fs::write(&replacement, b"new\n").unwrap();
        let mut snapshot = SourceSnapshot::open(&path).unwrap();
        std::fs::rename(&replacement, &path).unwrap();

        let mut old = [0_u8; 4];
        snapshot.read_exact_at(0, &mut old).unwrap();
        assert_eq!(&old, b"old\n");
        let mut current = SourceSnapshot::open(&path).unwrap();
        let mut new = [0_u8; 4];
        current.read_exact_at(0, &mut new).unwrap();
        assert_eq!(&new, b"new\n");
        assert!(
            !snapshot
                .extent()
                .identity()
                .same_file(current.extent().identity())
        );
    }

    #[test]
    fn file_identity_distinguishes_metadata_equality_from_same_opened_file() {
        let original = FileIdentity {
            ctime_ns: Some(10),
            device_id: Some(20),
            inode: Some(30),
        };
        let changed_metadata = FileIdentity {
            ctime_ns: Some(11),
            ..original
        };
        let replacement = FileIdentity {
            inode: Some(31),
            ..changed_metadata
        };
        let incomplete = FileIdentity {
            ctime_ns: None,
            ..original
        };

        assert_ne!(original, changed_metadata);
        assert!(original.same_file(changed_metadata));
        assert!(changed_metadata.same_file(original));
        assert!(!original.same_file(replacement));
        assert!(!original.same_file(incomplete));
        assert!(!incomplete.same_file(original));
    }

    #[test]
    fn bounded_line_reader_drains_complete_records_and_marks_incomplete_tails() {
        let input = b"0123456789\n{}\n1234567\nabcdefghij";
        let mut reader = BufReader::new(input.as_slice());
        let mut buffer = Vec::new();

        assert_eq!(
            read_bounded_line(&mut reader, &mut buffer, 8).unwrap(),
            BoundedLine::Complete {
                len: 11,
                oversized: true
            }
        );
        assert!(buffer.is_empty());
        assert_eq!(
            read_bounded_line(&mut reader, &mut buffer, 8).unwrap(),
            BoundedLine::Complete {
                len: 3,
                oversized: false
            }
        );
        assert_eq!(buffer, b"{}\n");
        assert_eq!(
            read_bounded_line(&mut reader, &mut buffer, 8).unwrap(),
            BoundedLine::Complete {
                len: 8,
                oversized: false
            }
        );
        assert_eq!(buffer, b"1234567\n");
        assert_eq!(
            read_bounded_line(&mut reader, &mut buffer, 8).unwrap(),
            BoundedLine::Incomplete {
                len: 10,
                oversized: true
            }
        );
        assert!(buffer.is_empty());
    }
}
