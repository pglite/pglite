use crate::types::{ColumnDef, Value};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

pub const WAL_MAGIC: &[u8; 4] = b"PGL2";
pub const WAL_MAGIC_V1: &[u8; 4] = b"PGL1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalRecord {
    Begin,
    Commit,
    Rollback,
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
    },
    DropTable {
        name: String,
    },
    Insert {
        table: String,
        row: Vec<Value>,
    },
    Update {
        table: String,
        pk: i64,
        col_idx: usize,
        new_val: Value,
    },
    Delete {
        table: String,
        pk: i64,
    },
    CommentOnTable {
        table: String,
        comment: Option<String>,
    },
    CommentOnColumn {
        table: String,
        column: String,
        comment: Option<String>,
    },
    AlterTableAddColumn {
        table: String,
        column: ColumnDef,
        default_val: Value,
    },
    AlterTableDropColumn {
        table: String,
        column: String,
    },
}

use std::time::Instant;

const BATCH_FLUSH_SIZE: usize = 64 * 1024; // 64 KB in-memory buffer
const MAX_FLUSH_INTERVAL_MS: u128 = 20;   // 20 ms batch interval

pub struct WalManager {
    pub filepath: Option<PathBuf>,
    pub writer: Option<BufWriter<File>>,
    pub buffer: Vec<WalRecord>,
    pub pending_bytes: Vec<u8>,
    pub last_flush: Instant,
}

impl WalManager {
    pub fn new(db_path: &str) -> Self {
        if db_path == ":memory:" || db_path.starts_with(":memory:") {
            return Self {
                filepath: None,
                writer: None,
                buffer: Vec::new(),
                pending_bytes: Vec::new(),
                last_flush: Instant::now(),
            };
        }

        let v2_rwal_path = PathBuf::from(format!("{}.v2.rwal", db_path));
        let rwal_path = PathBuf::from(format!("{}.rwal", db_path));
        let legacy_wal_path = PathBuf::from(format!("{}.wal", db_path));

        // Use .v2.rwal or .rwal. If legacy .wal exists with PGL magic, recover and migrate it.
        let wal_path = if v2_rwal_path.exists() {
            v2_rwal_path
        } else if rwal_path.exists() {
            rwal_path
        } else if legacy_wal_path.exists() {
            let has_pgl_magic = File::open(&legacy_wal_path).ok().and_then(|f| {
                unsafe { memmap2::Mmap::map(&f).ok() }
            }).map(|mmap| mmap.len() >= 4 && (&mmap[0..4] == WAL_MAGIC || &mmap[0..4] == WAL_MAGIC_V1)).unwrap_or(false);

            if has_pgl_magic {
                let _ = std::fs::rename(&legacy_wal_path, &rwal_path);
                if rwal_path.exists() {
                    rwal_path
                } else {
                    legacy_wal_path
                }
            } else {
                rwal_path
            }
        } else {
            rwal_path
        };

        let mut recovered_records: Vec<WalRecord> = Vec::new();

        // Zero-Copy WAL Recovery via Memory-Mapped File (mmap)
        if let Ok(file) = File::open(&wal_path) {
            if file.metadata().map(|m| m.len() > 0).unwrap_or(false) {
                if let Ok(mmap) = unsafe { memmap2::Mmap::map(&file) } {
                    if mmap.len() >= 4 && (&mmap[0..4] == WAL_MAGIC || &mmap[0..4] == WAL_MAGIC_V1) {
                        // High-Speed Binary Zero-Copy Stream Deserialization
                        let mut cursor = 4;
                        while cursor + 4 <= mmap.len() {
                            let len = u32::from_le_bytes(
                                mmap[cursor..cursor + 4].try_into().unwrap_or([0; 4])
                            ) as usize;
                            cursor += 4;
                            if cursor + len <= mmap.len() {
                                if let Ok(record) = bincode::deserialize::<WalRecord>(&mmap[cursor..cursor + len]) {
                                    recovered_records.push(record);
                                }
                                cursor += len;
                            } else {
                                break;
                            }
                        }
                    } else if !mmap.is_empty() && mmap[0] == b'{' {
                        // Legacy JSON format fallback
                        if let Ok(text) = std::str::from_utf8(&mmap) {
                            for line in text.lines() {
                                let line = line.trim();
                                if !line.is_empty() {
                                    if let Ok(record) = serde_json::from_str::<WalRecord>(line) {
                                        recovered_records.push(record);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let is_new = !wal_path.exists() || std::fs::metadata(&wal_path).map(|m| m.len() == 0).unwrap_or(true);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .ok();

        if is_new {
            if let Some(f) = &mut file {
                let _ = f.write_all(WAL_MAGIC);
                let _ = f.flush();
            }
        }

        let writer = file.map(BufWriter::new);

        Self {
            filepath: Some(wal_path),
            writer,
            buffer: recovered_records,
            pending_bytes: Vec::with_capacity(BATCH_FLUSH_SIZE),
            last_flush: Instant::now(),
        }
    }

    pub fn append(&mut self, record: WalRecord) {
        if self.writer.is_some() {
            if let Ok(bytes) = bincode::serialize(&record) {
                let len = bytes.len() as u32;
                self.pending_bytes.extend_from_slice(&len.to_le_bytes());
                self.pending_bytes.extend_from_slice(&bytes);
            }
        }
        self.buffer.push(record);

        // Asynchronous Batching: Flush when buffer threshold reached or interval elapsed
        if self.pending_bytes.len() >= BATCH_FLUSH_SIZE || self.last_flush.elapsed().as_millis() >= MAX_FLUSH_INTERVAL_MS {
            self.flush_pending();
        }
    }

    fn flush_pending(&mut self) {
        if self.pending_bytes.is_empty() {
            return;
        }
        if let Some(writer) = &mut self.writer {
            let _ = writer.write_all(&self.pending_bytes);
            let _ = writer.flush();
        }
        self.pending_bytes.clear();
        self.last_flush = Instant::now();
    }

    pub fn flush(&mut self) {
        self.flush_pending();
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self.pending_bytes.clear();
        if let Some(path) = &self.filepath {
            let _ = std::fs::remove_file(path);
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(path)
                .ok();
            self.writer = file.map(BufWriter::new);
        }
        self.last_flush = Instant::now();
    }
}

impl Drop for WalManager {
    fn drop(&mut self) {
        self.flush_pending();
    }
}
