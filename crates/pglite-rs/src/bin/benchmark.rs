use pglite_rs::engine::Executor;
use pglite_rs::storage::StorageEngine;
use pglite_rs::types::Value;
use std::fs;
use std::time::Instant;

fn main() {
    let db_file = "benchmark_rust.db";
    let _ = fs::remove_file(db_file);
    let _ = fs::remove_file(format!("{}.wal", db_file));

    println!("\n🦀 PGLite Native Rust Engine | Performance Benchmark Suite");
    println!("================================================================");

    let storage = StorageEngine::new(db_file.to_string());
    let mut db = Executor::new(storage);

    // 1. Schema setup
    db.execute(
        "CREATE TABLE benchmark_users (id SERIAL PRIMARY KEY, name TEXT, age INTEGER, active BOOLEAN)",
        &[],
    )
    .expect("Failed to create table");

    let total_records: usize = 500_000;
    let batch_size: usize = 1_000;

    println!("\n[1/6] PHASE: Bulk Data Ingestion (Rust Native)");
    println!("      Action: Inserting {} records", total_records);
    println!("      Config: Batch Size = {}", batch_size);

    let start_time = Instant::now();
    db.execute("BEGIN", &[]).unwrap();

    for i in (0..total_records).step_by(batch_size) {
        let mut placeholders = Vec::with_capacity(batch_size);
        let mut params = Vec::with_capacity(batch_size * 3);

        for j in 0..batch_size {
            let idx = (i + j + 1) as i64;
            let offset = j * 3;
            placeholders.push(format!("(${0}, ${1}, ${2})", offset + 1, offset + 2, offset + 3));
            params.push(Value::Text(format!("User_{}", idx)));
            params.push(Value::Int(idx % 100));
            params.push(Value::Bool(idx % 2 == 0));
        }

        let sql = format!(
            "INSERT INTO benchmark_users (name, age, active) VALUES {}",
            placeholders.join(", ")
        );
        db.execute(&sql, &params).unwrap();

        if (i + batch_size) % 100_000 == 0 || (i + batch_size) == total_records {
            let elapsed = start_time.elapsed().as_secs_f64();
            let speed = ((i + batch_size) as f64 / elapsed) as u64;
            let percent = ((i + batch_size) as f64 / total_records as f64) * 100.0;
            print!(
                "\r    ⏳ Progress: {:.1}% | Ingested: {} | Throughput: {} ops/sec",
                percent,
                i + batch_size,
                speed
            );
        }
    }

    db.execute("COMMIT", &[]).unwrap();
    let insert_duration = start_time.elapsed();
    let insert_sec = insert_duration.as_secs_f64();

    println!("\n\n✅ Ingestion Complete");
    println!("   ⏱  Total Duration: {:.3}s", insert_sec);
    println!(
        "   📈 Avg Throughput: {} ops/sec\n",
        (total_records as f64 / insert_sec) as u64
    );

    // 2. Point Lookup by PK
    println!("[2/6] PHASE: Point Lookup (Rust B-Tree Index)");
    println!("      Action: SELECT by Primary Key (id=250,000)");
    let q_start1 = Instant::now();
    let res1 = db
        .execute(
            "SELECT * FROM benchmark_users WHERE id = $1",
            &[Value::Int(250_000)],
        )
        .unwrap();
    let q_time1 = q_start1.elapsed();
    println!("   ↳ Result: {:?}", res1.rows.get(0));
    println!("   ⏱  Latency: {:.3}µs ({:.3}ms)\n", q_time1.as_micros(), q_time1.as_secs_f64() * 1000.0);

    // Point lookup stress test (10,000 queries)
    let stress_start = Instant::now();
    for id in 100_000..110_000 {
        let _ = db.execute("SELECT * FROM benchmark_users WHERE id = $1", &[Value::Int(id)]);
    }
    let stress_time = stress_start.elapsed();
    println!("   ⚡ Stress 10,000 PK Lookups: {:.2}ms (avg {:.3}µs / query | {} ops/sec)\n",
        stress_time.as_secs_f64() * 1000.0,
        stress_time.as_micros() as f64 / 10000.0,
        (10000.0 / stress_time.as_secs_f64()) as u64
    );

    // 3. Full Table Aggregation
    println!("[3/6] PHASE: Full Table Aggregation");
    println!("      Action: SELECT COUNT(*) as total FROM benchmark_users");
    let q_start2 = Instant::now();
    let res2 = db
        .execute("SELECT COUNT(*) as total FROM benchmark_users", &[])
        .unwrap();
    let q_time2 = q_start2.elapsed();
    println!("   ↳ Total Records: {:?}", res2.rows.get(0));
    println!("   ⏱  Latency: {:.3}ms\n", q_time2.as_secs_f64() * 1000.0);

    // 4. Filtered Scan (Vectorized SIMD / Rayon)
    println!("[4/6] PHASE: Filtered Scan (Vectorized Parallel Rayon)");
    println!("      Action: Complex SELECT with multiple WHERE conditions");
    let q_start3 = Instant::now();
    let res3 = db
        .execute(
            "SELECT COUNT(*) as active_users FROM benchmark_users WHERE active = true AND age > 50",
            &[],
        )
        .unwrap();
    let q_time3 = q_start3.elapsed();
    println!("   ↳ Match Count: {:?}", res3.rows.get(0));
    println!("   ⏱  Latency: {:.3}ms\n", q_time3.as_secs_f64() * 1000.0);

    // 5. Atomic Mutation
    println!("[5/6] PHASE: Atomic Mutation");
    println!("      Action: UPDATE record by Primary Key");
    let u_start = Instant::now();
    db.execute(
        "UPDATE benchmark_users SET age = $1 WHERE id = $2",
        &[Value::Int(99), Value::Int(250_000)],
    )
    .unwrap();
    let u_time = u_start.elapsed();
    println!("   ⏱  Latency: {:.3}µs ({:.3}ms)\n", u_time.as_micros(), u_time.as_secs_f64() * 1000.0);

    // 6. Record Deletion
    println!("[6/6] PHASE: Record Deletion");
    println!("      Action: DELETE record by Primary Key");
    let d_start = Instant::now();
    db.execute("DELETE FROM benchmark_users WHERE id = $1", &[Value::Int(250_000)])
        .unwrap();
    let d_time = d_start.elapsed();
    println!("   ⏱  Latency: {:.3}µs ({:.3}ms)\n", d_time.as_micros(), d_time.as_secs_f64() * 1000.0);

    let _ = fs::remove_file(db_file);
    let _ = fs::remove_file(format!("{}.wal", db_file));
    println!("✨ Rust Native Performance Suite Completed Successfully!\n");
}
