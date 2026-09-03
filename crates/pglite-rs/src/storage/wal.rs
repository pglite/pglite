use crate::types::Value;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalRecord {
    Begin,
    Commit,
    Rollback,
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
}

use std::time::Instant;

const BATCH_FLUSH_SIZE: usize = 64 * 1024; // 64 KB in-memory buffer
const MAX_FLUSH_INTERVAL_MS: u128 = 20;   // 20 ms batch interval

pub struct WalManager {
    filepath: Option<PathBuf>,
    writer: Option<BufWriter<File>>,
    buffer: Vec<WalRecord>,
    pending_bytes: Vec<u8>,
    last_flush: Instant,
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

        let wal_path = PathBuf::from(format!("{}.wal", db_path));
        let mut recovered_records: Vec<WalRecord> = Vec::new();

        // Check if existing WAL file is in legacy JSON format
        if let Ok(data) = std::fs::read(&wal_path) {
            if !data.is_empty() {
                if data[0] == b'{' {
                    // Legacy JSON format: parse lines and convert to binary
                    if let Ok(text) = std::str::from_utf8(&data) {
                        for line in text.lines() {
                            let line = line.trim();
                            if !line.is_empty() {
                                if let Ok(record) = serde_json::from_str::<WalRecord>(line) {
                                    recovered_records.push(record);
                                }
                            }
                        }
                    }

                    // Rewrite file immediately into new compact binary format
                    if let Ok(mut f) = OpenOptions::new().write(true).truncate(true).open(&wal_path) {
                        for record in &recovered_records {
                            if let Ok(bytes) = bincode::serialize(record) {
                                let len = bytes.len() as u32;
                                let _ = f.write_all(&len.to_le_bytes());
                                let _ = f.write_all(&bytes);
                            }
                        }
                    }
                }
            }
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .ok();

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
