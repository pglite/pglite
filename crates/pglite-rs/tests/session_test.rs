//! Đợt 13: `COPY` (API `copy_from` / `copy_to`), cursor, LISTEN/NOTIFY, READ ONLY, SET LOCAL.
use pglite_rs::PGlite;

fn temp_db(tag: &str) -> (std::path::PathBuf, String) {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("pglite_session_{}_{}_{}", tag, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("s.db").to_str().unwrap().to_string();
    (dir, path)
}

fn exec(db: &mut PGlite, sqls: &[&str]) {
    for sql in sqls {
        db.exec(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

#[test]
fn copy_text_and_csv_roundtrip_and_survive_reopen() {
    let (dir, path) = temp_db("copy");
    {
        let mut db = PGlite::open(&path).unwrap();
        exec(&mut db, &["CREATE TABLE c (id INT PRIMARY KEY, name TEXT, note TEXT)"]);
        let n = db.copy_from("COPY c FROM STDIN", "1\talice\t\\N\n2\tb\\tob\tline\\nbreak\n\\.\n").unwrap();
        assert_eq!(n, 2);
        let n = db
            .copy_from("COPY c (id, name, note) FROM STDIN WITH (FORMAT csv, HEADER true)", "id,name,note\n3,\"car,ol\",\"say \"\"hi\"\"\"\n4,,\"\"\n")
            .unwrap();
        assert_eq!(n, 2);
        let text = db.copy_to("COPY c TO STDOUT").unwrap();
        assert_eq!(text, "1\talice\t\\N\n2\tb\\tob\tline\\nbreak\n3\tcar,ol\tsay \"hi\"\n4\t\\N\t\n");
        let csv = db.copy_to("COPY (SELECT id, name FROM c WHERE id > 2 ORDER BY id) TO STDOUT WITH (FORMAT csv, HEADER)").unwrap();
        assert_eq!(csv, "id,name\n3,\"car,ol\"\n4,\n");
        // ràng buộc vẫn được kiểm: trùng khoá → lỗi, không dòng nào của lô được ghi
        assert!(db.copy_from("COPY c FROM STDIN", "5\tx\ty\n1\tdup\tz\n").is_err());
        // số cột sai
        let e = db.copy_from("COPY c FROM STDIN", "6\tonly\n").unwrap_err();
        assert!(e.contains("missing data"), "{e}");
        // SQL thường: COPY TO STDOUT trả 0 dòng, COPY FROM STDIN không có dữ liệu thì lỗi
        assert_eq!(db.query("COPY c TO STDOUT", &[]).unwrap().rows.len(), 0);
        assert!(db.exec("COPY c FROM STDIN", &[]).is_err());
    }
    let mut db = PGlite::open(&path).unwrap();
    let r = db.query("SELECT count(*) AS n FROM c", &[]).unwrap();
    assert_eq!(r.rows[0]["n"].as_i64(), Some(4));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn copy_file_paths() {
    let (dir, path) = temp_db("copyfile");
    let mut db = PGlite::open(&path).unwrap();
    exec(&mut db, &["CREATE TABLE f (a INT, b TEXT)", "INSERT INTO f VALUES (1, 'x'), (2, NULL)"]);
    let file = dir.join("out.csv").to_str().unwrap().to_string();
    exec(&mut db, &[&format!("COPY f TO '{}' (FORMAT csv)", file)]);
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "1,x\n2,\n");
    exec(&mut db, &["CREATE TABLE g (a INT, b TEXT)", &format!("COPY g FROM '{}' WITH CSV", file)]);
    let r = db.query("SELECT count(*) AS n, count(b) AS nb FROM g", &[]).unwrap();
    assert_eq!((r.rows[0]["n"].as_i64(), r.rows[0]["nb"].as_i64()), (Some(2), Some(1)));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn notifications_delivered_on_commit_only_to_listened_channels() {
    let mut db = PGlite::in_memory().unwrap();
    exec(&mut db, &["LISTEN jobs", "NOTIFY jobs, 'a'", "NOTIFY other, 'ignored'"]);
    assert_eq!(db.take_notifications(), vec![("jobs".to_string(), "a".to_string())]);
    assert!(db.take_notifications().is_empty());
    exec(&mut db, &["BEGIN", "SELECT pg_notify('jobs', 'b')", "NOTIFY jobs, 'b'"]);
    assert!(db.take_notifications().is_empty(), "chưa COMMIT");
    exec(&mut db, &["COMMIT"]);
    assert_eq!(db.take_notifications(), vec![("jobs".to_string(), "b".to_string())], "trùng trong giao dịch được gộp");
    exec(&mut db, &["BEGIN", "NOTIFY jobs, 'c'", "ROLLBACK", "UNLISTEN jobs", "NOTIFY jobs, 'd'"]);
    assert!(db.take_notifications().is_empty());
}

#[test]
fn cursors_read_only_and_set_local() {
    let mut db = PGlite::in_memory().unwrap();
    exec(&mut db, &["CREATE TABLE k (id INT PRIMARY KEY, v TEXT)", "INSERT INTO k VALUES (1,'a'),(2,'b'),(3,'c')"]);
    // cursor thường mất khi giao dịch kết thúc; WITH HOLD còn sau COMMIT
    exec(&mut db, &["BEGIN", "DECLARE c CURSOR FOR SELECT id FROM k ORDER BY id", "DECLARE h CURSOR WITH HOLD FOR SELECT id FROM k ORDER BY id DESC", "COMMIT"]);
    assert!(db.query("FETCH c", &[]).is_err());
    let r = db.query("FETCH 2 FROM h", &[]).unwrap();
    assert_eq!(r.rows.iter().map(|r| r["id"].as_i64().unwrap()).collect::<Vec<_>>(), vec![3, 2]);
    exec(&mut db, &["CLOSE h"]);
    // READ ONLY chặn ghi, giao dịch sau trở lại bình thường
    exec(&mut db, &["BEGIN READ ONLY"]);
    let e = db.exec("DELETE FROM k", &[]).unwrap_err();
    assert!(e.contains("read-only transaction"), "{e}");
    exec(&mut db, &["ROLLBACK", "BEGIN", "DELETE FROM k WHERE id = 3", "COMMIT"]);
    // SET LOCAL chỉ sống trong giao dịch; SET LOCAL ngoài giao dịch không có tác dụng
    exec(&mut db, &["SET work_mem = '16MB'", "BEGIN", "SET LOCAL work_mem = '1MB'"]);
    assert_eq!(db.query("SHOW work_mem", &[]).unwrap().rows[0]["work_mem"], "1MB");
    exec(&mut db, &["COMMIT", "SET LOCAL work_mem = '2MB'"]);
    assert_eq!(db.query("SHOW work_mem", &[]).unwrap().rows[0]["work_mem"], "16MB");
    // JSON giữ khoá viết thường như cũ
    assert!(db.query("SHOW TimeZone", &[]).unwrap().rows[0].get("timezone").is_some());
}
