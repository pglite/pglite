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

pub struct WalManager {
    filepath: Option<PathBuf>,
    writer: Option<BufWriter<File>>,
    buffer: Vec<WalRecord>,
}

impl WalManager {
    pub fn new(db_path: &str) -> Self {
        if db_path == ":memory:" || db_path.starts_with(":memory:") {
            return Self {
                filepath: None,
                writer: None,
                buffer: Vec::new(),
            };
        }

        let wal_path = PathBuf::from(format!("{}.wal", db_path));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .ok();

        let writer = file.map(BufWriter::new);

        Self {
            filepath: Some(wal_path),
            writer,
            buffer: Vec::new(),
        }
    }

    pub fn append(&mut self, record: WalRecord) {
        if let Some(writer) = &mut self.writer {
            if let Ok(json) = serde_json::to_string(&record) {
                let _ = writeln!(writer, "{}", json);
            }
        }
        self.buffer.push(record);
    }

    pub fn flush(&mut self) {
        if let Some(writer) = &mut self.writer {
            let _ = writer.flush();
        }
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
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
    }
}
