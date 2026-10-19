//! Hồi quy cho các lỗi làm mất/hỏng dữ liệu: WAL ghi dở, lỗi ghi, khoá chính trùng, UPDATE đổi
//! khoá, TRUNCATE trong giao dịch, WAL JSON cũ, DEFAULT không tất định khi ADD COLUMN.
use pglite_rs::PGlite;
use std::io::Write;
use std::path::{Path, PathBuf};

fn temp_db(tag: &str) -> (PathBuf, String) {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("pglite_loss_{}_{}_{}", tag, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("t.db").to_str().unwrap().to_string();
    (dir, db)
}

fn rows(db: &mut PGlite, sql: &str) -> Vec<serde_json::Value> {
    db.query(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).rows
}

fn col(db: &mut PGlite, sql: &str, c: &str) -> Vec<serde_json::Value> {
    rows(db, sql).into_iter().map(|r| r[c].clone()).collect()
}

fn exec_all(db: &mut PGlite, sqls: &[&str]) {
    for sql in sqls {
        db.exec(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

fn backups(dir: &Path) -> usize {
    std::fs::read_dir(dir).unwrap().flatten().filter(|e| e.file_name().to_string_lossy().contains(".corrupt-")).count()
}

/// Kết quả các truy vấn trước khi đóng phải trùng với sau khi mở lại.
fn assert_same_after_reopen(path: &str, checks: &[&str], f: impl FnOnce(&mut PGlite)) -> PGlite {
    let mut before = Vec::new();
    {
        let mut db = PGlite::open(path).unwrap();
        f(&mut db);
        for q in checks {
            before.push(rows(&mut db, q));
        }
    }
    let mut db = PGlite::open(path).unwrap();
    for (q, b) in checks.iter().zip(before) {
        assert_eq!(rows(&mut db, q), b, "khác nhau trước/sau khi mở lại: {q}");
    }
    db
}

const SEED: &[&str] = &["CREATE TABLE t (id SERIAL PRIMARY KEY, v TEXT)", "INSERT INTO t (v) VALUES ('before')"];
const AFTER: &[&str] = &[
    "INSERT INTO t (v) VALUES ('after0')",
    "INSERT INTO t (v) VALUES ('after1')",
    "UPDATE t SET v = 'before-updated' WHERE id = 1",
    "INSERT INTO t (v) VALUES ('after2')",
];

fn wal_bytes(path: &str) -> Vec<u8> {
    std::fs::read(format!("{path}.rwal")).unwrap()
}

#[test]
fn torn_tail_does_not_swallow_later_writes() {
    let (dir, path) = temp_db("torn");
    exec_all(&mut PGlite::open(&path).unwrap(), SEED);
    // Crash giữa lúc ghi: độ dài khung + một phần thân
    let mut f = std::fs::OpenOptions::new().append(true).open(format!("{path}.rwal")).unwrap();
    f.write_all(&[200, 0, 0, 0, 1, 2, 3]).unwrap();
    drop(f);
    {
        let mut db = PGlite::open(&path).unwrap();
        assert_eq!(col(&mut db, "SELECT v FROM t", "v"), vec!["before"]);
        exec_all(&mut db, AFTER);
    }
    assert_eq!(backups(&dir), 1, "phải giữ bản sao file gốc trước khi cắt");
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(col(&mut db, "SELECT v FROM t ORDER BY id", "v"), vec!["before-updated", "after0", "after1", "after2"]);
    assert_eq!(backups(&dir), 1, "lần mở thứ hai không cần sửa gì nữa");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn tail_shorter_than_length_prefix_is_cut() {
    let (dir, path) = temp_db("torn_len");
    exec_all(&mut PGlite::open(&path).unwrap(), SEED);
    std::fs::OpenOptions::new().append(true).open(format!("{path}.rwal")).unwrap().write_all(&[9, 0]).unwrap();
    exec_all(&mut PGlite::open(&path).unwrap(), &["INSERT INTO t (v) VALUES ('x')"]);
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(col(&mut db, "SELECT v FROM t ORDER BY id", "v"), vec!["before", "x"]);
    let _ = std::fs::remove_dir_all(dir);
}

/// File đã bị lỗi cũ làm hỏng: khung cụt rồi các bản ghi mới bị nối phía sau. Bản mới phải cứu
/// lại được các bản ghi phía sau.
#[test]
fn wal_damaged_by_old_engine_is_resynced() {
    let (dir_a, a) = temp_db("dmg_a");
    let (dir_b, b) = temp_db("dmg_b");
    exec_all(&mut PGlite::open(&a).unwrap(), SEED);
    {
        let mut db = PGlite::open(&b).unwrap();
        exec_all(&mut db, SEED);
        exec_all(&mut db, AFTER);
    }
    let good = wal_bytes(&a);
    let full = wal_bytes(&b);
    assert_eq!(&full[..good.len()], &good[..], "hai DB phải ghi giống hệt nhau");
    for garbage in [&[200u8, 0, 0, 0, 1, 2, 3][..], &[40, 0, 0, 0, 7][..], &[1, 0][..], &[5, 0, 0, 0, 9, 9, 9, 9, 9][..]] {
        let (dir, path) = temp_db("dmg");
        let mut damaged = good.clone();
        damaged.extend_from_slice(garbage);
        damaged.extend_from_slice(&full[good.len()..]);
        std::fs::write(format!("{path}.rwal"), &damaged).unwrap();
        {
            let mut db = PGlite::open(&path).unwrap();
            assert_eq!(
                col(&mut db, "SELECT v FROM t ORDER BY id", "v"),
                vec!["before-updated", "after0", "after1", "after2"],
                "rác {garbage:?}"
            );
            exec_all(&mut db, &["INSERT INTO t (v) VALUES ('new')"]);
        }
        // Khung rác có độ dài khớp đúng ranh giới thì chỉ bị bỏ qua như bản cũ, không cần sửa file.
        let whole_frame = garbage.len() >= 4 && u32::from_le_bytes(garbage[0..4].try_into().unwrap()) as usize == garbage.len() - 4;
        assert_eq!(backups(&dir), if whole_frame { 0 } else { 1 }, "rác {garbage:?}");
        let mut db = PGlite::open(&path).unwrap();
        assert_eq!(col(&mut db, "SELECT v FROM t ORDER BY id", "v"), vec!["before-updated", "after0", "after1", "after2", "new"]);
        assert_eq!(backups(&dir), if whole_frame { 0 } else { 1 }, "lần mở sau không sửa thêm");
        let _ = std::fs::remove_dir_all(dir);
    }
    let _ = std::fs::remove_dir_all(dir_a);
    let _ = std::fs::remove_dir_all(dir_b);
}

/// Khung còn nguyên nhưng là biến thể mà bản này chưa biết (do binary mới hơn ghi): bỏ qua như
/// trước, file không bị cắt hay sửa.
#[test]
fn future_record_variant_is_skipped_not_removed() {
    let (dir, path) = temp_db("future");
    exec_all(&mut PGlite::open(&path).unwrap(), SEED);
    let mut frame = Vec::new();
    let body = [(pglite_rs::storage::wal::KNOWN_VARIANTS + 3).to_le_bytes().to_vec(), vec![1, 2, 3, 4, 5, 6]].concat();
    frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
    frame.extend_from_slice(&body);
    std::fs::OpenOptions::new().append(true).open(format!("{path}.rwal")).unwrap().write_all(&frame).unwrap();
    exec_all(&mut PGlite::open(&path).unwrap(), &["INSERT INTO t (v) VALUES ('x')"]);
    let len_before = wal_bytes(&path).len();
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(col(&mut db, "SELECT v FROM t ORDER BY id", "v"), vec!["before", "x"]);
    drop(db);
    assert_eq!(wal_bytes(&path).len(), len_before);
    assert_eq!(backups(&dir), 0);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn known_variant_count_matches_enum() {
    use pglite_rs::storage::wal::{WalRecord, KNOWN_VARIANTS};
    let last = bincode::serialize(&WalRecord::TxBegin).unwrap();
    assert_eq!(u32::from_le_bytes(last[0..4].try_into().unwrap()), KNOWN_VARIANTS - 1);
}

/// Mọi fixture do bản cũ ghi đọc ra mà không bị coi là hỏng.
#[test]
fn legacy_fixtures_are_never_repaired() {
    let fx = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    for name in ["legacy_v2", "legacy_tx", "legacy_open_begin", "legacy_dup_pk"] {
        let bytes = std::fs::read(fx.join(format!("{name}.db.rwal"))).unwrap();
        let (records, skipped, repair) = pglite_rs::storage::wal::recover_frames(&bytes[4..]);
        assert_eq!(repair, pglite_rs::storage::wal::Repair::None, "{name}");
        assert_eq!(skipped, 0, "{name}");
        assert!(!records.is_empty(), "{name}");
    }
}

#[test]
fn write_error_is_reported_and_file_stays_clean() {
    let (dir, path) = temp_db("write_err");
    let mut db = PGlite::open(&path).unwrap();
    exec_all(&mut db, SEED);
    // Thay writer bằng file chỉ đọc: mọi lần ghi sau đó đều lỗi
    let ro = std::fs::File::open(format!("{path}.rwal")).unwrap();
    db.executor_mut().storage.wal.writer = Some(std::io::BufWriter::new(ro));
    let err = db.exec("INSERT INTO t (v) VALUES ('lost')", &[]).unwrap_err();
    assert!(err.contains("WAL write failed"), "{err}");
    assert!(db.exec("INSERT INTO t (v) VALUES ('also-lost')", &[]).is_err(), "WAL hỏng thì không được nhận ghi tiếp");
    assert!(db.query("SELECT * FROM t", &[]).is_ok(), "đọc vẫn được");
    assert!(db.try_flush().is_err());
    drop(db);
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(col(&mut db, "SELECT v FROM t", "v"), vec!["before"]);
    assert_eq!(backups(&dir), 0);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn duplicate_primary_key_is_rejected() {
    let mut db = PGlite::in_memory().unwrap();
    exec_all(&mut db, &[
        "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)",
        "INSERT INTO t VALUES (1, 'a'), (2, 'b')",
    ]);
    let err = db.exec("INSERT INTO t VALUES (1, 'dup')", &[]).unwrap_err();
    assert!(err.contains("duplicate key value violates unique constraint \"t_pkey\""), "{err}");
    // Trùng trong cùng lệnh: không dòng nào được chèn
    assert!(db.exec("INSERT INTO t VALUES (3, 'x'), (3, 'y')", &[]).is_err());
    assert!(db.exec("UPDATE t SET id = 2 WHERE id = 1", &[]).is_err());
    assert!(db.exec("UPDATE t SET id = 5", &[]).is_err());
    assert_eq!(rows(&mut db, "SELECT id, v FROM t ORDER BY id"), vec![
        serde_json::json!({"id": 1, "v": "a"}),
        serde_json::json!({"id": 2, "v": "b"}),
    ]);
    // Dời và đổi chỗ khoá trong một lệnh là hợp lệ (trạng thái cuối không trùng)
    exec_all(&mut db, &["UPDATE t SET id = id + 1", "UPDATE t SET id = 5 - id"]);
    assert_eq!(rows(&mut db, "SELECT id, v FROM t ORDER BY id"), vec![
        serde_json::json!({"id": 2, "v": "b"}),
        serde_json::json!({"id": 3, "v": "a"}),
    ]);
    // Khoá UUID/text
    exec_all(&mut db, &["CREATE TABLE u (id UUID PRIMARY KEY, v TEXT)", "INSERT INTO u VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'a')"]);
    assert!(db.exec("INSERT INTO u VALUES ('A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 'dup')", &[]).is_err());
    // Bảng không khai báo PRIMARY KEY (cột id tự đoán làm khoá): giữ hành vi cũ, cho trùng
    exec_all(&mut db, &["CREATE TABLE n (id INT, v TEXT)", "INSERT INTO n VALUES (1, 'a')", "INSERT INTO n VALUES (1, 'b')"]);
    assert_eq!(rows(&mut db, "SELECT * FROM n").len(), 2);
    // Ghi tiếp vào bảng vừa có lệnh lỗi vẫn bình thường
    exec_all(&mut db, &["INSERT INTO t VALUES (9, 'z')"]);
    assert_eq!(rows(&mut db, "SELECT * FROM t").len(), 3);
}

/// DB cũ đã chứa khoá trùng: mở ra đúng như bản cũ đọc, và DELETE/UPDATE theo khoá không còn
/// "đánh rơi" dòng còn lại.
#[test]
fn legacy_duplicate_keys_stay_reachable() {
    let fx = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let (dir, path) = temp_db("legacy_dup");
    std::fs::copy(fx.join("legacy_dup_pk.db.rwal"), format!("{path}.rwal")).unwrap();
    let expected: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(fx.join("legacy_dup_pk.expected.json")).unwrap()).unwrap();
    let mut db = PGlite::open(&path).unwrap();
    for (k, q) in [("d", "SELECT * FROM d ORDER BY id, v"), ("s", "SELECT * FROM s ORDER BY v"), ("c", "SELECT * FROM c ORDER BY id")] {
        assert_eq!(serde_json::Value::Array(rows(&mut db, q)), expected[k], "{k}");
    }
    exec_all(&mut db, &["DELETE FROM d WHERE id = 1"]);
    exec_all(&mut db, &["UPDATE d SET v = 'z' WHERE id = 1"]);
    assert_eq!(col(&mut db, "SELECT v FROM d WHERE id = 1", "v"), vec!["z"]);
    drop(db);
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(col(&mut db, "SELECT v FROM d ORDER BY id", "v"), vec!["z", "c"]);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn primary_key_shifts_survive_reopen() {
    let (dir, path) = temp_db("shift");
    let checks = ["SELECT * FROM t ORDER BY id", "SELECT * FROM u ORDER BY v"];
    let mut db = assert_same_after_reopen(&path, &checks, |db| {
        exec_all(db, &[
            "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            "UPDATE t SET id = id + 1",
            "UPDATE t SET id = 6 - id WHERE id IN (2, 4)",
            "BEGIN",
            "UPDATE t SET id = id + 10",
            "ROLLBACK",
            "BEGIN",
            "UPDATE t SET id = id * 2 WHERE id > 2",
            "COMMIT",
            "CREATE TABLE u (id TEXT PRIMARY KEY, v TEXT)",
            "INSERT INTO u VALUES ('x', '1'), ('y', '2')",
            "UPDATE u SET id = CASE WHEN id = 'x' THEN 'y' ELSE 'x' END",
        ]);
    });
    // (1a,2b,3c) -> +1 -> (2a,3b,4c) -> đổi chỗ 2/4 -> (4a,3b,2c) -> x2 khi > 2 -> (8a,6b,2c)
    assert_eq!(rows(&mut db, "SELECT id, v FROM t ORDER BY id"), vec![
        serde_json::json!({"id": 2, "v": "c"}),
        serde_json::json!({"id": 6, "v": "b"}),
        serde_json::json!({"id": 8, "v": "a"}),
    ]);
    assert_eq!(rows(&mut db, "SELECT id, v FROM u ORDER BY v"), vec![
        serde_json::json!({"id": "y", "v": "1"}),
        serde_json::json!({"id": "x", "v": "2"}),
    ]);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn truncate_is_transactional_and_durable() {
    let (dir, path) = temp_db("trunc");
    let checks = ["SELECT * FROM t ORDER BY id", "SELECT * FROM u ORDER BY v", "SELECT * FROM o ORDER BY id"];
    let mut db = assert_same_after_reopen(&path, &checks, |db| {
        exec_all(db, &[
            "CREATE TABLE t (id SERIAL PRIMARY KEY, v TEXT)",
            "INSERT INTO t (v) VALUES ('keep1'), ('keep2')",
            "BEGIN",
            "TRUNCATE t",
            "INSERT INTO t (v) VALUES ('rolled-back')",
            "ROLLBACK",
            "CREATE TABLE u (id UUID PRIMARY KEY, v TEXT)",
            "COMMENT ON TABLE u IS 'bang u'",
            "INSERT INTO u VALUES ('11111111-1111-1111-1111-111111111111', 'old')",
            "TRUNCATE TABLE u RESTART IDENTITY CASCADE",
            "INSERT INTO u VALUES ('22222222-2222-2222-2222-222222222222', 'new')",
            "UPDATE u SET v = 'wrong' WHERE id = '11111111-1111-1111-1111-111111111111'",
            "CREATE TABLE o (id SERIAL PRIMARY KEY, user_id INT)",
            "INSERT INTO o (user_id) VALUES (1), (2)",
            "BEGIN",
            "TRUNCATE o, t",
            "INSERT INTO o (user_id) VALUES (3)",
            "COMMIT",
        ]);
        assert_eq!(col(db, "SELECT v FROM t ORDER BY id", "v"), Vec::<serde_json::Value>::new());
    });
    assert_eq!(col(&mut db, "SELECT v FROM u", "v"), vec!["new"]);
    assert_eq!(col(&mut db, "SELECT user_id FROM o", "user_id"), vec![3]);
    assert!(rows(&mut db, "SELECT * FROM o WHERE user_id = 1").is_empty());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn truncate_rollback_restores_rows() {
    let mut db = PGlite::in_memory().unwrap();
    exec_all(&mut db, &[
        "CREATE TABLE t (id SERIAL PRIMARY KEY, v TEXT, owner_id INT)",
        "INSERT INTO t (v, owner_id) VALUES ('a', 1), ('b', 2)",
        "BEGIN",
        "TRUNCATE t",
        "ROLLBACK",
    ]);
    assert_eq!(col(&mut db, "SELECT v FROM t ORDER BY id", "v"), vec!["a", "b"]);
    assert_eq!(col(&mut db, "SELECT v FROM t WHERE id = 2", "v"), vec!["b"]);
    assert_eq!(col(&mut db, "SELECT v FROM t WHERE owner_id = 1", "v"), vec!["a"]);
}

#[test]
fn legacy_json_wal_is_migrated() {
    let (dir, path) = temp_db("json");
    let lines = [
        r#"{"CreateTable":{"name":"t","columns":[{"name":"id","data_type":"Serial","is_primary_key":true,"is_nullable":false,"default_value":null},{"name":"v","data_type":"Text","is_primary_key":false,"is_nullable":true,"default_value":null}]}}"#,
        r#"{"Insert":{"table":"t","row":[{"Int":1},{"Text":"from-json"}]}}"#,
    ];
    std::fs::write(format!("{path}.wal"), lines.join("\n") + "\n").unwrap();
    {
        let mut db = PGlite::open(&path).unwrap();
        assert_eq!(col(&mut db, "SELECT v FROM t", "v"), vec!["from-json"]);
        exec_all(&mut db, &["INSERT INTO t (v) VALUES ('new')"]);
    }
    assert!(Path::new(&format!("{path}.wal.migrated")).exists());
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(col(&mut db, "SELECT v FROM t ORDER BY id", "v"), vec!["from-json", "new"]);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn volatile_default_on_add_column_survives_reopen() {
    let (dir, path) = temp_db("add_col");
    let checks = ["SELECT * FROM t ORDER BY id", "SELECT * FROM n ORDER BY v"];
    let mut db = assert_same_after_reopen(&path, &checks, |db| {
        exec_all(db, &[
            "CREATE TABLE t (id SERIAL PRIMARY KEY, v TEXT)",
            "INSERT INTO t (v) VALUES ('a'), ('b'), ('c')",
            "ALTER TABLE t ADD COLUMN token UUID DEFAULT gen_random_uuid()",
            "ALTER TABLE t ADD COLUMN flag BOOLEAN DEFAULT true",
            "CREATE TABLE n (v TEXT)",
            "INSERT INTO n (v) VALUES ('x'), ('y')",
            "ALTER TABLE n ADD COLUMN token UUID DEFAULT gen_random_uuid()",
        ]);
    });
    let tokens = col(&mut db, "SELECT token FROM t", "token");
    assert_eq!(tokens.len(), 3);
    assert!(tokens.iter().all(|t| !t.is_null()));
    let _ = std::fs::remove_dir_all(dir);
}
