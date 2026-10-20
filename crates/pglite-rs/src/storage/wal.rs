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
    // ---- Các biến thể dưới đây chỉ được THÊM VÀO CUỐI: thứ tự là một phần của định dạng
    // file (bincode ghi chỉ số biến thể). Binary cũ gặp chúng sẽ bỏ qua bản ghi đó.
    /// ROLLBACK ghi từ bản này trở đi: được hoàn tác khi replay. `Rollback` cũ vẫn bị bỏ qua
    /// để DB cũ mở ra đúng như trước.
    TxRollback,
    Savepoint {
        name: String,
    },
    RollbackToSavepoint {
        name: String,
    },
    ReleaseSavepoint {
        name: String,
    },
    /// UPDATE/DELETE trên bảng có khoá chính không phải số (UUID, text).
    UpdateByKey {
        table: String,
        key: Value,
        col_idx: usize,
        new_val: Value,
    },
    DeleteByKey {
        table: String,
        key: Value,
    },
    /// UPDATE/DELETE trên bảng không có khoá chính: nhận diện dòng bằng toàn bộ giá trị cũ.
    UpdateByRow {
        table: String,
        old_row: Vec<Value>,
        updates: Vec<(usize, Value)>,
    },
    DeleteByRow {
        table: String,
        row: Vec<Value>,
    },
    /// Luôn ghi ngay sau `Begin`, đánh dấu giao dịch mở từ bản này trở đi: nếu không có
    /// COMMIT/ROLLBACK ở cuối file (crash giữa giao dịch) thì bị hoàn tác khi replay. Giao dịch
    /// chỉ có `Begin` (file cũ) vẫn được giữ như trước. Binary cũ bỏ qua bản ghi này và vẫn thấy `Begin`.
    TxBegin,
    /// Thay đổi catalog (ràng buộc, index, ALTER COLUMN, đổi tên): `payload` = bincode của
    /// `catalog::SchemaOp`. Để dạng byte thô để bản này luôn giải mã được khung, kể cả khi bản sau
    /// thêm loại op mới (op lạ chỉ bị bỏ qua). Binary cũ bỏ qua cả khung.
    Schema {
        version: u8,
        payload: Vec<u8>,
    },
}

#[derive(Serialize)]
pub enum WalRecordRef<'a> {
    Begin,
    Commit,
    Rollback,
    CreateTable {
        name: &'a str,
        columns: &'a [ColumnDef],
    },
    DropTable {
        name: &'a str,
    },
    Insert {
        table: &'a str,
        row: &'a [Value],
    },
    Update {
        table: &'a str,
        pk: i64,
        col_idx: usize,
        new_val: &'a Value,
    },
    Delete {
        table: &'a str,
        pk: i64,
    },
    CommentOnTable {
        table: &'a str,
        comment: Option<&'a str>,
    },
    CommentOnColumn {
        table: &'a str,
        column: &'a str,
        comment: Option<&'a str>,
    },
    AlterTableAddColumn {
        table: &'a str,
        column: &'a ColumnDef,
        default_val: &'a Value,
    },
    AlterTableDropColumn {
        table: &'a str,
        column: &'a str,
    },
    TxRollback,
    Savepoint {
        name: &'a str,
    },
    RollbackToSavepoint {
        name: &'a str,
    },
    ReleaseSavepoint {
        name: &'a str,
    },
    UpdateByKey {
        table: &'a str,
        key: &'a Value,
        col_idx: usize,
        new_val: &'a Value,
    },
    DeleteByKey {
        table: &'a str,
        key: &'a Value,
    },
    UpdateByRow {
        table: &'a str,
        old_row: &'a [Value],
        updates: &'a [(usize, Value)],
    },
    DeleteByRow {
        table: &'a str,
        row: &'a [Value],
    },
    TxBegin,
    Schema {
        version: u8,
        payload: &'a [u8],
    },
}


impl WalRecord {
    /// Bản ghi đổi schema (không phải dữ liệu hay điều khiển giao dịch).
    pub fn is_ddl(&self) -> bool {
        matches!(
            self,
            WalRecord::CreateTable { .. }
                | WalRecord::DropTable { .. }
                | WalRecord::CommentOnTable { .. }
                | WalRecord::CommentOnColumn { .. }
                | WalRecord::AlterTableAddColumn { .. }
                | WalRecord::AlterTableDropColumn { .. }
                | WalRecord::Schema { .. }
        )
    }
}

#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;

const BATCH_FLUSH_SIZE: usize = 64 * 1024; // 64 KB in-memory buffer
#[cfg(not(target_arch = "wasm32"))]
const MAX_FLUSH_INTERVAL_MS: u128 = 20;   // 20 ms batch interval

/// Số biến thể `WalRecord` mà bản này biết (chỉ số bincode 0..KNOWN_VARIANTS).
pub const KNOWN_VARIANTS: u32 = 22;
/// Khoảng rác tối đa được quét để tìm lại ranh giới khung sau một chỗ hỏng.
const RESYNC_WINDOW: usize = 64 * 1024 * 1024;

/// Khung đủ độ dài, không giải mã được nhưng có chỉ số biến thể "của tương lai": nhiều khả năng
/// do binary mới hơn ghi. Loại khung này luôn được bỏ qua nguyên vẹn, không bao giờ bị xoá.
fn is_future_variant(frame: &[u8]) -> bool {
    frame.len() >= 4 && {
        let idx = u32::from_le_bytes(frame[0..4].try_into().unwrap());
        (KNOWN_VARIANTS..KNOWN_VARIANTS + 64).contains(&idx)
    }
}

/// Kết quả đọc chuỗi khung `[u32 len][bincode]` (không gồm 4 byte magic).
#[derive(Debug, Default)]
pub struct ScanResult {
    pub records: Vec<WalRecord>,
    /// Số khung đủ độ dài nhưng không giải mã được (bị bỏ qua như trước).
    pub undecodable: usize,
    /// Offset của chỗ hỏng đáng ngờ đầu tiên: khung không giải mã được (trừ biến thể tương lai)
    /// hoặc khung cụt.
    pub first_suspicious: Option<usize>,
    /// Khung cuối bị cụt (ghi dở khi crash/đầy đĩa); `valid_end` là chỗ cắt an toàn.
    pub truncated_tail: bool,
    pub valid_end: usize,
}

/// Đọc khung theo đúng cách bản cũ đọc: khung không giải mã được thì bỏ qua, dừng ở khung cụt.
pub fn scan_frames(data: &[u8]) -> ScanResult {
    let mut res = ScanResult::default();
    let mut cursor = 0usize;
    while cursor < data.len() {
        if cursor + 4 > data.len() {
            res.truncated_tail = true;
            res.first_suspicious.get_or_insert(cursor);
            break;
        }
        let len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
        let body = cursor + 4;
        if len > data.len() - body {
            res.truncated_tail = true;
            res.first_suspicious.get_or_insert(cursor);
            break;
        }
        let frame = &data[body..body + len];
        match bincode::deserialize::<WalRecord>(frame) {
            Ok(record) => {
                // Giải mã được nhưng mã hoá lại không ra đúng byte: có thể là khung cụt cũ bị
                // ghi nối phía sau. Vẫn đọc như bản cũ, chỉ đánh dấu để thử tìm lại ranh giới.
                if res.first_suspicious.is_none() && !roundtrips(&record, frame) {
                    res.first_suspicious = Some(cursor);
                }
                res.records.push(record)
            }
            Err(_) => {
                res.undecodable += 1;
                if !is_future_variant(frame) {
                    res.first_suspicious.get_or_insert(cursor);
                }
            }
        }
        cursor = body + len;
        res.valid_end = cursor;
    }
    res
}

fn roundtrips(record: &WalRecord, frame: &[u8]) -> bool {
    bincode::serialize(record).map_or(false, |b| b.as_slice() == frame)
}

/// Chuỗi khung từ `start` có hợp lệ tới đúng EOF không. Khung đầu phải giải mã được và mã hoá
/// lại ra đúng từng byte, nên rác ngẫu nhiên gần như không thể khớp nhầm; các khung sau chỉ cần
/// giải mã được hoặc là biến thể tương lai (giống khi đọc bình thường).
fn exact_chain(data: &[u8], start: usize) -> Option<Vec<WalRecord>> {
    let mut cursor = start;
    let mut records = Vec::new();
    while cursor < data.len() {
        if cursor + 4 > data.len() {
            return None;
        }
        let len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
        let body = cursor + 4;
        if len == 0 || len > data.len() - body {
            return None;
        }
        let frame = &data[body..body + len];
        match bincode::deserialize::<WalRecord>(frame) {
            Ok(record) => {
                if cursor == start && !roundtrips(&record, frame) {
                    return None;
                }
                records.push(record);
            }
            Err(_) if cursor != start && is_future_variant(frame) => {}
            Err(_) => return None,
        }
        cursor = body + len;
    }
    if records.is_empty() { None } else { Some(records) }
}

/// Cách sửa file WAL sau khi đọc.
#[derive(Debug, PartialEq)]
pub enum Repair {
    None,
    /// Cắt bỏ khung cụt ở cuối: data[..at] giữ nguyên.
    TruncateAt(usize),
    /// Bỏ đoạn rác data[from..to] (khung cụt mà bản cũ đã ghi nối tiếp phía sau), giữ phần còn lại.
    DropRange(usize, usize),
}

/// Đọc khung và quyết định có cần sửa file không. Trả về (records, số khung bị bỏ qua, cách sửa).
/// File lành đọc ra đúng như bản cũ đọc.
pub fn recover_frames(data: &[u8]) -> (Vec<WalRecord>, usize, Repair) {
    let scan = scan_frames(data);
    let Some(p) = scan.first_suspicious else {
        return (scan.records, scan.undecodable, Repair::None);
    };
    // Khung tại p có độ dài hợp lệ thì khung kế tiếp bắt đầu đúng ranh giới.
    let boundary_after_p = (p + 4 <= data.len())
        .then(|| u32::from_le_bytes(data[p..p + 4].try_into().unwrap()) as usize)
        .filter(|&len| len <= data.len() - p - 4)
        .map(|len| p + 4 + len);
    let end = data.len().min(p.saturating_add(4 + RESYNC_WINDOW));
    for o in p + 1..end {
        if Some(o) == boundary_after_p {
            if exact_chain(data, o).is_some() {
                // Chỉ một khung hỏng được bỏ qua, phần sau vẫn đúng ranh giới: giữ cách đọc cũ.
                break;
            }
            continue;
        }
        if let Some(chain) = exact_chain(data, o) {
            let mut records = scan_frames(&data[..p]).records;
            records.extend(chain);
            return (records, scan.undecodable, Repair::DropRange(p, o));
        }
    }
    if scan.truncated_tail {
        return (scan.records, scan.undecodable, Repair::TruncateAt(scan.valid_end));
    }
    (scan.records, scan.undecodable, Repair::None)
}

fn sync_disabled_by_env() -> bool {
    std::env::var("PGLITE_SYNC")
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "off" | "0" | "false" | "no"))
        .unwrap_or(false)
}

fn frame_bytes(records: &[WalRecord]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(WAL_MAGIC);
    for r in records {
        if let Ok(bytes) = bincode::serialize(r) {
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(&bytes);
        }
    }
    out
}

/// Ghi file mới qua file tạm + fsync + rename để không bao giờ để lại file dở.
fn write_atomic(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension(format!(
        "{}.tmp",
        path.extension().and_then(|e| e.to_str()).unwrap_or("wal")
    ));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent() {
        if let Ok(d) = File::open(dir) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn backup_path(path: &std::path::Path) -> PathBuf {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    PathBuf::from(format!("{}.corrupt-{}.bak", path.display(), ts))
}

fn parse_json_lines(bytes: &[u8]) -> Vec<WalRecord> {
    let mut out = Vec::new();
    if let Ok(text) = std::str::from_utf8(bytes) {
        for line in text.lines() {
            let line = line.trim();
            if !line.is_empty() {
                if let Ok(record) = serde_json::from_str::<WalRecord>(line) {
                    out.push(record);
                }
            }
        }
    }
    out
}

pub struct WalManager {
    pub filepath: Option<PathBuf>,
    pub writer: Option<BufWriter<File>>,
    pub pending_bytes: Vec<u8>,
    /// fsync tại mỗi điểm commit (COMMIT, ROLLBACK, lệnh autocommit). Mặc định bật;
    /// tắt bằng `PGLITE_SYNC=off` hoặc `PGlite::set_sync(false)`.
    pub sync: bool,
    /// Group commit: điểm commit chỉ ghi xuống file, fsync dồn lại cho tới `sync_now()`.
    /// Người gọi chỉ được báo thành công cho client sau khi `sync_now()` trả về Ok.
    pub defer_sync: bool,
    /// Lỗi ghi đầu tiên. Sau đó mọi lệnh ghi đều báo lỗi để bộ nhớ không lệch thêm khỏi đĩa.
    poisoned: Option<String>,
    /// Độ dài file sau lần ghi thành công gần nhất (để cắt bỏ khung dở khi ghi lỗi).
    durable_len: u64,
    unsynced: bool,
    /// Tăng mỗi khi có bản ghi được thêm; executor dùng để biết lệnh có ghi gì không.
    pub appended: u64,
    /// Tăng mỗi khi có bản ghi DDL (đổi schema) được thêm. Lệnh lỗi đã ghi DDL thì không bị cắt
    /// khỏi WAL, vì DDL không hoàn tác được trong bộ nhớ.
    pub ddl_appended: u64,
    /// Độ dài file tại điểm commit gần nhất: không bao giờ cắt WAL ngắn hơn mốc này.
    committed_len: u64,
    #[cfg(not(target_arch = "wasm32"))]
    pub last_flush: Instant,
    /// Bảng tạm (`CREATE TEMP TABLE`, viết thường): chỉ sống trong phiên, mọi bản ghi của chúng bị bỏ
    /// qua nên không bao giờ xuống đĩa (file DB không đổi, kể cả khi sau này có bảng thật trùng tên).
    pub temp_tables: std::collections::HashSet<String>,
}

impl WalManager {
    /// Bản ghi thuộc một bảng tạm: không ghi. `DropTable` của bảng tạm còn gỡ tên khỏi danh sách.
    fn skip_temp(&mut self, table: Option<&str>, is_drop: bool) -> bool {
        let Some(t) = table else { return false };
        if self.temp_tables.is_empty() {
            return false;
        }
        let key = t.to_lowercase();
        if !self.temp_tables.contains(&key) {
            return false;
        }
        if is_drop {
            self.temp_tables.remove(&key);
        }
        true
    }

    fn empty(filepath: Option<PathBuf>, writer: Option<BufWriter<File>>, durable_len: u64) -> Self {
        Self {
            filepath,
            writer,
            pending_bytes: Vec::with_capacity(BATCH_FLUSH_SIZE),
            sync: !sync_disabled_by_env(),
            defer_sync: false,
            poisoned: None,
            durable_len,
            unsynced: false,
            appended: 0,
            ddl_appended: 0,
            committed_len: durable_len,
            #[cfg(not(target_arch = "wasm32"))]
            last_flush: Instant::now(),
            temp_tables: std::collections::HashSet::new(),
        }
    }

    pub fn new(db_path: &str) -> (Self, Vec<WalRecord>) {
        if db_path == ":memory:" || db_path.starts_with(":memory:") {
            return (Self::empty(None, None, 0), Vec::new());
        }

        let v2_rwal_path = PathBuf::from(format!("{}.v2.rwal", db_path));
        let rwal_path = PathBuf::from(format!("{}.rwal", db_path));
        let legacy_wal_path = PathBuf::from(format!("{}.wal", db_path));

        // Use .v2.rwal or .rwal. If legacy .wal exists with PGL magic, recover and migrate it.
        let wal_path = if v2_rwal_path.exists() {
            v2_rwal_path
        } else if rwal_path.exists() {
            if std::fs::read(&legacy_wal_path).map(|b| b.first() == Some(&b'{')).unwrap_or(false) {
                // Bản trước bỏ qua file JSON này và tạo .rwal riêng: không trộn hai nguồn.
                eprintln!(
                    "[pglite-rs] WARNING: legacy JSON WAL {} was not loaded because {} already exists",
                    legacy_wal_path.display(),
                    rwal_path.display()
                );
            }
            rwal_path
        } else if legacy_wal_path.exists() {
            let head = std::fs::read(&legacy_wal_path).unwrap_or_default();
            let has_pgl_magic = head.len() >= 4 && (&head[0..4] == WAL_MAGIC || &head[0..4] == WAL_MAGIC_V1);

            if has_pgl_magic {
                let _ = std::fs::rename(&legacy_wal_path, &rwal_path);
                if rwal_path.exists() {
                    rwal_path
                } else {
                    legacy_wal_path
                }
            } else if head.first() == Some(&b'{') {
                // WAL JSON cũ: chuyển sang .rwal nhị phân, giữ file gốc làm bản lưu.
                let records = parse_json_lines(&head);
                match write_atomic(&rwal_path, &frame_bytes(&records)) {
                    Ok(()) => {
                        let _ = std::fs::rename(&legacy_wal_path, format!("{}.migrated", legacy_wal_path.display()));
                        rwal_path
                    }
                    Err(e) => {
                        eprintln!("[pglite-rs] WARNING: could not migrate legacy JSON WAL {}: {}", legacy_wal_path.display(), e);
                        legacy_wal_path
                    }
                }
            } else {
                rwal_path
            }
        } else {
            rwal_path
        };

        #[allow(unused_mut)]
        let mut recovered_records: Vec<WalRecord> = Vec::new();

        #[cfg(not(target_arch = "wasm32"))]
        if let Ok(file) = File::open(&wal_path) {
            if file.metadata().map(|m| m.len() > 0).unwrap_or(false) {
                if let Ok(mmap) = unsafe { memmap2::Mmap::map(&file) } {
                    if mmap.len() >= 4 && (&mmap[0..4] == WAL_MAGIC || &mmap[0..4] == WAL_MAGIC_V1) {
                        let (records, skipped, repair) = recover_frames(&mmap[4..]);
                        recovered_records = records;
                        if skipped > 0 && repair == Repair::None {
                            // Vẫn mở DB như trước, nhưng không còn bỏ qua trong im lặng.
                            eprintln!(
                                "[pglite-rs] WARNING: {} WAL record(s) in {} could not be decoded and were skipped",
                                skipped,
                                wal_path.display()
                            );
                        }
                        if repair != Repair::None {
                            let fixed: Vec<u8> = match repair {
                                Repair::TruncateAt(at) => mmap[..4 + at].to_vec(),
                                Repair::DropRange(from, to) => {
                                    let mut v = mmap[..4 + from].to_vec();
                                    v.extend_from_slice(&mmap[4 + to..]);
                                    v
                                }
                                Repair::None => unreachable!(),
                            };
                            let original = mmap.to_vec();
                            drop(mmap);
                            Self::repair_file(&wal_path, &original, &fixed, &repair);
                        }
                    } else if !mmap.is_empty() && mmap[0] == b'{' {
                        // Legacy JSON format fallback
                        recovered_records = parse_json_lines(&mmap);
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
                let _ = f.sync_all();
            }
        }
        let durable_len = file.as_ref().and_then(|f| f.metadata().ok()).map(|m| m.len()).unwrap_or(0);

        let writer = file.map(BufWriter::new);
        (Self::empty(Some(wal_path), writer, durable_len), recovered_records)
    }

    /// Lưu bản sao file gốc rồi thay bằng bản đã sửa. Lỗi ở đây không chặn việc mở DB.
    #[cfg(not(target_arch = "wasm32"))]
    fn repair_file(path: &std::path::Path, original: &[u8], fixed: &[u8], repair: &Repair) {
        let backup = backup_path(path);
        if let Err(e) = write_atomic(&backup, original) {
            eprintln!("[pglite-rs] WARNING: WAL {} needs repair ({:?}) but backup failed: {}", path.display(), repair, e);
            return;
        }
        match write_atomic(path, fixed) {
            Ok(()) => eprintln!(
                "[pglite-rs] WARNING: repaired WAL {} ({:?}, {} -> {} bytes); original saved to {}",
                path.display(),
                repair,
                original.len(),
                fixed.len(),
                backup.display()
            ),
            Err(e) => eprintln!("[pglite-rs] WARNING: could not repair WAL {}: {}", path.display(), e),
        }
    }

    fn push_frame(&mut self, bytes: &[u8]) {
        self.pending_bytes.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        self.pending_bytes.extend_from_slice(bytes);
    }

    fn maybe_flush(&mut self) {
        let should_flush = {
            #[cfg(not(target_arch = "wasm32"))]
            {
                self.pending_bytes.len() >= BATCH_FLUSH_SIZE || self.last_flush.elapsed().as_millis() >= MAX_FLUSH_INTERVAL_MS
            }
            #[cfg(target_arch = "wasm32")]
            {
                self.pending_bytes.len() >= BATCH_FLUSH_SIZE
            }
        };

        if should_flush {
            // Lỗi được giữ trong `poisoned` và trả về ở lệnh hiện tại (xem `Executor::execute`).
            let _ = self.flush_pending();
        }
    }

    pub fn append(&mut self, record: WalRecord) {
        let (table, is_drop) = match &record {
            WalRecord::CreateTable { name, .. } => (Some(name.as_str()), false),
            WalRecord::DropTable { name } => (Some(name.as_str()), true),
            WalRecord::Insert { table, .. }
            | WalRecord::Update { table, .. }
            | WalRecord::Delete { table, .. }
            | WalRecord::CommentOnTable { table, .. }
            | WalRecord::CommentOnColumn { table, .. }
            | WalRecord::AlterTableAddColumn { table, .. }
            | WalRecord::AlterTableDropColumn { table, .. }
            | WalRecord::UpdateByKey { table, .. }
            | WalRecord::DeleteByKey { table, .. }
            | WalRecord::UpdateByRow { table, .. }
            | WalRecord::DeleteByRow { table, .. } => (Some(table.as_str()), false),
            _ => (None, false),
        };
        if self.skip_temp(table, is_drop) {
            return;
        }
        self.appended += 1;
        if record.is_ddl() {
            self.ddl_appended += 1;
        }
        if self.writer.is_some() {
            if let Ok(bytes) = bincode::serialize(&record) {
                self.push_frame(&bytes);
            }
        }
        self.maybe_flush();
    }

    pub fn append_ref<'a>(&mut self, record: WalRecordRef<'a>) {
        let (table, is_drop) = match &record {
            WalRecordRef::CreateTable { name, .. } => (Some(*name), false),
            WalRecordRef::DropTable { name } => (Some(*name), true),
            WalRecordRef::Insert { table, .. }
            | WalRecordRef::Update { table, .. }
            | WalRecordRef::Delete { table, .. }
            | WalRecordRef::CommentOnTable { table, .. }
            | WalRecordRef::CommentOnColumn { table, .. }
            | WalRecordRef::AlterTableAddColumn { table, .. }
            | WalRecordRef::AlterTableDropColumn { table, .. }
            | WalRecordRef::UpdateByKey { table, .. }
            | WalRecordRef::DeleteByKey { table, .. }
            | WalRecordRef::UpdateByRow { table, .. }
            | WalRecordRef::DeleteByRow { table, .. } => (Some(*table), false),
            _ => (None, false),
        };
        if self.skip_temp(table, is_drop) {
            return;
        }
        self.appended += 1;
        if self.writer.is_some() {
            if let Ok(bytes) = bincode::serialize(&record) {
                self.push_frame(&bytes);
            }
        }
        self.maybe_flush();
    }

    /// Lỗi ghi đã xảy ra (nếu có). DB cần được mở lại.
    pub fn error(&self) -> Option<&str> {
        self.poisoned.as_deref()
    }

    fn poison(&mut self, msg: String) -> String {
        let full = format!("WAL write failed, database must be reopened: {}", msg);
        self.poisoned.get_or_insert(full.clone());
        self.pending_bytes.clear();
        full
    }

    fn flush_pending(&mut self) -> Result<(), String> {
        if let Some(e) = &self.poisoned {
            return Err(e.clone());
        }
        if self.pending_bytes.is_empty() {
            return Ok(());
        }
        let n = self.pending_bytes.len() as u64;
        let result = match &mut self.writer {
            Some(writer) => writer.write_all(&self.pending_bytes).and_then(|_| writer.flush()),
            None => Ok(()),
        };
        if let Err(e) = result {
            // Cắt khung dở để file luôn kết thúc đúng ranh giới.
            if let Some(writer) = &mut self.writer {
                let _ = writer.get_ref().set_len(self.durable_len);
            }
            return Err(self.poison(e.to_string()));
        }
        self.durable_len += n;
        self.unsynced = true;
        self.pending_bytes.clear();
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.last_flush = Instant::now();
        }
        Ok(())
    }

    /// Ghi buffer xuống file (chưa fsync).
    pub fn flush(&mut self) -> Result<(), String> {
        self.flush_pending()
    }

    /// Điểm commit: ghi buffer và fsync (nếu bật `sync`).
    pub fn commit_point(&mut self) -> Result<(), String> {
        self.flush_pending()?;
        if !self.defer_sync {
            self.sync_now()?;
        }
        self.committed_len = self.durable_len;
        Ok(())
    }

    /// fsync phần đã ghi xuống file mà chưa fsync (nếu bật `sync`).
    pub fn sync_now(&mut self) -> Result<(), String> {
        if let Some(e) = &self.poisoned {
            return Err(e.clone());
        }
        if self.unsynced {
            if self.sync {
                if let Some(writer) = &self.writer {
                    if let Err(e) = writer.get_ref().sync_data() {
                        return Err(self.poison(e.to_string()));
                    }
                }
            }
            self.unsynced = false;
        }
        Ok(())
    }

    /// Vị trí cuối WAL (gồm cả phần còn trong buffer).
    pub fn position(&self) -> u64 {
        self.durable_len + self.pending_bytes.len() as u64
    }

    /// Bỏ mọi bản ghi từ `pos` trở đi (lệnh lỗi giữa chừng). Chỉ được khi `pos` nằm sau điểm commit
    /// gần nhất, tức phần bị bỏ chưa từng được chốt. Trả về `false` (không làm gì) nếu không được.
    pub fn truncate_to(&mut self, pos: u64) -> bool {
        if self.poisoned.is_some() || pos < self.committed_len || pos > self.position() {
            return false;
        }
        if pos >= self.durable_len {
            self.pending_bytes.truncate((pos - self.durable_len) as usize);
            return true;
        }
        self.pending_bytes.clear();
        if let Some(writer) = &mut self.writer {
            let res = writer.flush().and_then(|_| writer.get_ref().set_len(pos));
            if let Err(e) = res {
                self.poison(e.to_string());
                return false;
            }
        }
        self.durable_len = pos;
        true
    }

    /// Còn dữ liệu chưa ghi hoặc chưa fsync.
    pub fn has_pending(&self) -> bool {
        self.poisoned.is_none() && (!self.pending_bytes.is_empty() || self.unsynced)
    }

    pub fn clear(&mut self) {
        self.pending_bytes.clear();
        if let Some(path) = &self.filepath {
            let _ = std::fs::remove_file(path);
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .ok();
            if let Some(f) = &mut file {
                // Thiếu magic thì lần mở sau bỏ qua toàn bộ file.
                let _ = f.write_all(WAL_MAGIC);
                let _ = f.sync_all();
            }
            self.durable_len = WAL_MAGIC.len() as u64;
            self.committed_len = self.durable_len;
            self.writer = file.map(BufWriter::new);
        }
        self.unsynced = false;
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.last_flush = Instant::now();
        }
    }
}

impl Drop for WalManager {
    fn drop(&mut self) {
        let _ = self.commit_point();
        let _ = self.sync_now();
    }
}
