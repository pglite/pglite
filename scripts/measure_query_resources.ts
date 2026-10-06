import { PGLite, type QueryResult } from "../src/index";
import * as fs from "fs";
import * as path from "path";

// ============================================================================
// 1. DATA TYPES & INTERFACES
// ============================================================================

export interface MemorySnapshot {
  rss: number;          // Resident Set Size in MB
  heapUsed: number;     // V8/JSC Heap Used in MB
  heapTotal: number;    // V8/JSC Heap Total in MB
  external: number;     // Native C++/Rust/Buffer allocations in MB
}

export interface CpuSnapshot {
  userMicroseconds: number;
  systemMicroseconds: number;
}

export interface QueryResourceMetric {
  id: number;
  name: string;
  sql: string;
  category: "DDL" | "INSERT" | "SELECT" | "JOIN" | "AGGREGATE" | "UPDATE" | "DELETE" | "TRANSACTION" | "BURST" | "CUSTOM";
  rowCount: number;
  
  // Timing
  wallTimeMs: number;       // Elapsed real time in milliseconds
  
  // CPU Usage
  cpuUserMs: number;        // CPU time spent in user space (ms)
  cpuSystemMs: number;      // CPU time spent in kernel space (ms)
  cpuTotalMs: number;       // cpuUserMs + cpuSystemMs
  cpuPercent: number;       // (cpuTotalMs / wallTimeMs) * 100 %
  
  // RAM Usage (MB)
  ramRssBeforeMB: number;
  ramRssAfterMB: number;
  ramRssDiffMB: number;     // Delta RSS (+ is allocation, - is deallocation)
  
  ramHeapBeforeMB: number;
  ramHeapAfterMB: number;
  ramHeapDiffMB: number;    // Delta Heap Used
  
  ramExternalDiffMB: number;// Delta External (Rust NAPI bindings)
  
  peakRssMB: number;        // Peak RSS observed during query execution
  peakHeapMB: number;       // Peak Heap observed during query execution
}

export interface ProfilerOptions {
  realtimeLog?: boolean;     // Print each query's resource stats immediately
  sampleIntervalMs?: number; // Background RAM sampling interval during query (default 2ms)
  forceGcBeforeQuery?: boolean; // Run global.gc() before query if available
}

// ============================================================================
// 2. HELPER FUNCTIONS: SAMPLING & FORMATTING
// ============================================================================

export function getMemorySnapshot(): MemorySnapshot {
  const mem = process.memoryUsage();
  return {
    rss: Math.round((mem.rss / 1024 / 1024) * 1000) / 1000,
    heapUsed: Math.round((mem.heapUsed / 1024 / 1024) * 1000) / 1000,
    heapTotal: Math.round((mem.heapTotal / 1024 / 1024) * 1000) / 1000,
    external: Math.round((mem.external / 1024 / 1024) * 1000) / 1000,
  };
}

export function getCpuSnapshot(): CpuSnapshot {
  const cpu = process.cpuUsage();
  return {
    userMicroseconds: cpu.user,
    systemMicroseconds: cpu.system,
  };
}

function formatDelta(mb: number): string {
  const sign = mb > 0 ? "+" : "";
  const fixed = mb.toFixed(3);
  if (Math.abs(mb) < 0.001) return "   0.000 MB";
  if (mb > 0) return `${sign}${fixed} MB`;
  return `${fixed} MB`;
}

function truncateString(str: string, maxLength: number): string {
  const singleLine = str.replace(/\s+/g, " ").trim();
  if (singleLine.length <= maxLength) return singleLine;
  return singleLine.substring(0, maxLength - 3) + "...";
}

// ANSI Color Helpers
const C = {
  reset: "\x1b[0m",
  bold: "\x1b[1m",
  dim: "\x1b[2m",
  cyan: "\x1b[36m",
  blue: "\x1b[34m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  red: "\x1b[31m",
  magenta: "\x1b[35m",
  gray: "\x1b[90m",
  bgCyan: "\x1b[46m\x1b[30m",
  bgBlue: "\x1b[44m\x1b[37m",
};

// ============================================================================
// 3. QUERY RESOURCE PROFILER CLASS
// ============================================================================

export class QueryResourceProfiler {
  private metrics: QueryResourceMetric[] = [];
  private queryCounter = 0;
  private options: Required<ProfilerOptions>;

  constructor(options: ProfilerOptions = {}) {
    this.options = {
      realtimeLog: options.realtimeLog ?? true,
      sampleIntervalMs: options.sampleIntervalMs ?? 2,
      forceGcBeforeQuery: options.forceGcBeforeQuery ?? false,
    };
  }

  /**
   * Measure CPU and RAM consumption for an individual query execution.
   */
  public async measure<T>(
    name: string,
    sql: string,
    category: QueryResourceMetric["category"],
    queryFn: () => Promise<T>
  ): Promise<{ result: T; metric: QueryResourceMetric }> {
    this.queryCounter++;
    const queryId = this.queryCounter;

    if (this.options.forceGcBeforeQuery && typeof (globalThis as any).gc === "function") {
      (globalThis as any).gc();
    }

    // 1. Initial Snapshots
    const memBefore = getMemorySnapshot();
    const cpuBefore = process.cpuUsage();
    const hrStart = process.hrtime.bigint();

    // 2. High-frequency background sampler for Peak Memory tracking
    let peakRss = memBefore.rss;
    let peakHeap = memBefore.heapUsed;
    let sampling = true;

    const sampleTimer = setInterval(() => {
      if (!sampling) return;
      const current = process.memoryUsage();
      const currentRss = current.rss / 1024 / 1024;
      const currentHeap = current.heapUsed / 1024 / 1024;
      if (currentRss > peakRss) peakRss = currentRss;
      if (currentHeap > peakHeap) peakHeap = currentHeap;
    }, this.options.sampleIntervalMs);

    // 3. Execute the actual query
    let result: T;
    let rowCount = 0;
    try {
      result = await queryFn();

      // Determine rows returned/affected
      if (Array.isArray(result)) {
        rowCount = result.length;
      } else if (result && typeof result === "object") {
        if ("rowCount" in result && typeof (result as any).rowCount === "number") {
          rowCount = (result as any).rowCount;
        } else if ("rows" in result && Array.isArray((result as any).rows)) {
          rowCount = (result as any).rows.length;
        } else if ("affectedRows" in result && typeof (result as any).affectedRows === "number") {
          rowCount = (result as any).affectedRows;
        }
      }
    } finally {
      sampling = false;
      clearInterval(sampleTimer);
    }

    // 4. Capture Post-Query Metrics
    const hrEnd = process.hrtime.bigint();
    const cpuDiff = process.cpuUsage(cpuBefore);
    const memAfter = getMemorySnapshot();

    if (memAfter.rss > peakRss) peakRss = memAfter.rss;
    if (memAfter.heapUsed > peakHeap) peakHeap = memAfter.heapUsed;

    // 5. Compute Durations and Usage
    const wallTimeMs = Number(hrEnd - hrStart) / 1_000_000;
    const cpuUserMs = cpuDiff.user / 1_000;
    const cpuSystemMs = cpuDiff.system / 1_000;
    const cpuTotalMs = cpuUserMs + cpuSystemMs;
    const cpuPercent = wallTimeMs > 0 ? (cpuTotalMs / wallTimeMs) * 100 : 0;

    const metric: QueryResourceMetric = {
      id: queryId,
      name,
      sql,
      category,
      rowCount,
      wallTimeMs: Math.round(wallTimeMs * 1000) / 1000,
      cpuUserMs: Math.round(cpuUserMs * 1000) / 1000,
      cpuSystemMs: Math.round(cpuSystemMs * 1000) / 1000,
      cpuTotalMs: Math.round(cpuTotalMs * 1000) / 1000,
      cpuPercent: Math.round(cpuPercent * 10) / 10,
      ramRssBeforeMB: memBefore.rss,
      ramRssAfterMB: memAfter.rss,
      ramRssDiffMB: Math.round((memAfter.rss - memBefore.rss) * 1000) / 1000,
      ramHeapBeforeMB: memBefore.heapUsed,
      ramHeapAfterMB: memAfter.heapUsed,
      ramHeapDiffMB: Math.round((memAfter.heapUsed - memBefore.heapUsed) * 1000) / 1000,
      ramExternalDiffMB: Math.round((memAfter.external - memBefore.external) * 1000) / 1000,
      peakRssMB: Math.round(peakRss * 1000) / 1000,
      peakHeapMB: Math.round(peakHeap * 1000) / 1000,
    };

    this.metrics.push(metric);

    if (this.options.realtimeLog) {
      this.printSingleMetric(metric);
    }

    return { result, metric };
  }

  /**
   * Real-time stream log for a single query metric.
   */
  private printSingleMetric(m: QueryResourceMetric) {
    const idStr = `${C.cyan}[#${String(m.id).padStart(2, "0")}]${C.reset}`;
    const nameStr = `${C.bold}${m.name.padEnd(28)}${C.reset}`;
    const timeStr = `${C.yellow}⏱ ${m.wallTimeMs.toFixed(2)}ms${C.reset}`.padEnd(19);
    
    // CPU Color
    const cpuColor = m.cpuPercent > 100 ? C.red : m.cpuPercent > 50 ? C.yellow : C.green;
    const cpuStr = `${cpuColor}⚙ CPU: ${m.cpuTotalMs.toFixed(2)}ms (${m.cpuPercent.toFixed(1)}%)${C.reset}`.padEnd(28);

    // RAM Delta Color
    const ramDeltaColor = m.ramRssDiffMB > 5 ? C.red : m.ramRssDiffMB > 1 ? C.yellow : C.gray;
    const ramDiffStr = `${ramDeltaColor}ΔRSS: ${formatDelta(m.ramRssDiffMB)}${C.reset}`;
    const peakRssStr = `${C.magenta}Peak: ${m.peakRssMB.toFixed(1)}MB${C.reset}`;
    const heapStr = `${C.dim}Heap: ${m.ramHeapAfterMB.toFixed(1)}MB (${formatDelta(m.ramHeapDiffMB)})${C.reset}`;
    const rowsStr = `${C.dim}Rows: ${m.rowCount.toLocaleString()}${C.reset}`;

    console.log(` ${idStr} ${nameStr} | ${timeStr} | ${cpuStr} | ${ramDiffStr} | ${peakRssStr} | ${heapStr} | ${rowsStr}`);
  }

  /**
   * Return all recorded query metrics.
   */
  public getMetrics(): QueryResourceMetric[] {
    return this.metrics;
  }

  /**
   * Clear recorded metrics.
   */
  public reset(): void {
    this.metrics = [];
    this.queryCounter = 0;
  }

  /**
   * Print comprehensive formatted summary table and bottleneck report.
   */
  public printSummaryReport(): void {
    if (this.metrics.length === 0) {
      console.log("No metrics recorded.");
      return;
    }

    console.log("\n" + "=".repeat(128));
    console.log(`                    📊 BÁO CÁO CHI TIẾT TÀI NGUYÊN CPU & RAM Ở MỖI QUERY (PGLITE ENGINE)                    `);
    console.log("=".repeat(128) + "\n");

    // Table Header
    const headers = [
      "#",
      "Query Name",
      "Category",
      "Rows",
      "Time (ms)",
      "CPU User",
      "CPU Sys",
      "Total CPU",
      "CPU %",
      "Δ RSS (MB)",
      "Peak RSS",
      "Δ Heap",
    ];

    const colWidths = [4, 30, 11, 7, 10, 9, 8, 10, 8, 12, 10, 11];

    const headerLine = headers.map((h, i) => h.padEnd(colWidths[i])).join(" | ");
    const separatorLine = colWidths.map((w) => "-".repeat(w)).join("-+-");

    console.log(`${C.bold}${C.cyan}${headerLine}${C.reset}`);
    console.log(`${C.gray}${separatorLine}${C.reset}`);

    for (const m of this.metrics) {
      const id = String(m.id).padStart(colWidths[0]);
      const name = truncateString(m.name, colWidths[1]).padEnd(colWidths[1]);
      const cat = m.category.padEnd(colWidths[2]);
      const rows = String(m.rowCount).padStart(colWidths[3]);
      const wallTime = m.wallTimeMs.toFixed(2).padStart(colWidths[4]);
      const cpuUser = m.cpuUserMs.toFixed(2).padStart(colWidths[5]);
      const cpuSys = m.cpuSystemMs.toFixed(2).padStart(colWidths[6]);
      const cpuTot = m.cpuTotalMs.toFixed(2).padStart(colWidths[7]);
      const cpuPct = `${m.cpuPercent.toFixed(0)}%`.padStart(colWidths[8]);
      const deltaRss = formatDelta(m.ramRssDiffMB).padStart(colWidths[9]);
      const peakRss = `${m.peakRssMB.toFixed(1)}MB`.padStart(colWidths[10]);
      const deltaHeap = formatDelta(m.ramHeapDiffMB).padStart(colWidths[11]);

      console.log(
        `${id} | ${name} | ${cat} | ${rows} | ${wallTime} | ${cpuUser} | ${cpuSys} | ${cpuTot} | ${cpuPct} | ${deltaRss} | ${peakRss} | ${deltaHeap}`
      );
    }
    console.log(`${C.gray}${separatorLine}${C.reset}\n`);

    // Aggregate statistics
    const totalWallTime = this.metrics.reduce((acc, m) => acc + m.wallTimeMs, 0);
    const totalCpuTime = this.metrics.reduce((acc, m) => acc + m.cpuTotalMs, 0);
    const totalCpuUser = this.metrics.reduce((acc, m) => acc + m.cpuUserMs, 0);
    const totalCpuSys = this.metrics.reduce((acc, m) => acc + m.cpuSystemMs, 0);
    const maxRssReached = Math.max(...this.metrics.map((m) => m.peakRssMB));
    const firstMetric = this.metrics[0];
    const lastMetric = this.metrics[this.metrics.length - 1];
    const netRssChange = lastMetric.ramRssAfterMB - firstMetric.ramRssBeforeMB;
    const netHeapChange = lastMetric.ramHeapAfterMB - firstMetric.ramHeapBeforeMB;

    console.log(`${C.bold}📈 TỔNG HỢP HIỆU SUẤT TÀI NGUYÊN TOÀN BỘ PHIÊN:${C.reset}`);
    console.log(`   • Tổng số queries thực hiện       : ${C.bold}${this.metrics.length}${C.reset}`);
    console.log(`   • Tổng thời gian thực thi (Wall) : ${C.yellow}${totalWallTime.toFixed(2)} ms${C.reset}`);
    console.log(`   • Tổng CPU Time tiêu thụ          : ${C.cyan}${totalCpuTime.toFixed(2)} ms${C.reset} (User: ${totalCpuUser.toFixed(2)} ms | Sys: ${totalCpuSys.toFixed(2)} ms)`);
    console.log(`   • Tải CPU trung bình              : ${C.green}${((totalCpuTime / (totalWallTime || 1)) * 100).toFixed(1)} %${C.reset}`);
    console.log(`   • RAM RSS ban đầu                 : ${firstMetric.ramRssBeforeMB.toFixed(2)} MB`);
    console.log(`   • RAM RSS kết thúc                : ${lastMetric.ramRssAfterMB.toFixed(2)} MB (${netRssChange >= 0 ? "+" : ""}${netRssChange.toFixed(2)} MB)`);
    console.log(`   • Đỉnh RAM cao nhất (Peak RSS)    : ${C.magenta}${maxRssReached.toFixed(2)} MB${C.reset}`);
    console.log(`   • Biến thiên V8/Bun Heap          : ${netHeapChange >= 0 ? "+" : ""}${netHeapChange.toFixed(2)} MB\n`);

    // Top 3 CPU Consuming Queries
    console.log("------------------------------------------------------------------------------------------");
    console.log("🔥 TOP 3 QUERIES TIÊU TỐN NHIỀU CPU NHẤT:");
    console.log("------------------------------------------------------------------------------------------");
    const topCpu = [...this.metrics].sort((a, b) => b.cpuTotalMs - a.cpuTotalMs).slice(0, 3);
    topCpu.forEach((m, i) => {
      console.log(` ${i + 1}. ${C.bold}${m.name}${C.reset} [${m.category}]`);
      console.log(`    ⚙ CPU Time: ${C.red}${m.cpuTotalMs.toFixed(2)} ms${C.reset} (${m.cpuPercent.toFixed(1)}%) | Wall: ${m.wallTimeMs.toFixed(2)} ms | Rows: ${m.rowCount.toLocaleString()}`);
      console.log(`    📝 SQL: ${C.dim}${truncateString(m.sql, 90)}${C.reset}`);
    });

    // Top 3 RAM Consuming Queries (by Δ RSS or Peak)
    console.log("\n------------------------------------------------------------------------------------------");
    console.log("💾 TOP 3 QUERIES TĂNG TẢI RAM NHIỀU NHẤT (Δ RSS):");
    console.log("------------------------------------------------------------------------------------------");
    const topRam = [...this.metrics].sort((a, b) => b.ramRssDiffMB - a.ramRssDiffMB).slice(0, 3);
    topRam.forEach((m, i) => {
      console.log(` ${i + 1}. ${C.bold}${m.name}${C.reset} [${m.category}]`);
      console.log(`    💾 ΔRSS: ${C.magenta}${formatDelta(m.ramRssDiffMB)}${C.reset} | Peak: ${m.peakRssMB.toFixed(1)} MB | ΔHeap: ${formatDelta(m.ramHeapDiffMB)}`);
      console.log(`    📝 SQL: ${C.dim}${truncateString(m.sql, 90)}${C.reset}`);
    });

    console.log("\n" + "=".repeat(128) + "\n");
  }

  /**
   * Wrap any PGLite database instance to automatically profile every query/exec invocation.
   */
  public wrapDatabase(db: PGLite): PGLite {
    const profiler = this;

    const proxy = new Proxy(db, {
      get(target, prop, receiver) {
        if (prop === "query") {
          return async function (sql: string, params?: any, dbName?: string) {
            const label = extractQueryLabel(sql);
            const category = categorizeSql(sql);
            const { result } = await profiler.measure(label, sql, category, () =>
              target.query(sql, params, dbName)
            );
            return result;
          };
        }

        if (prop === "exec") {
          return async function (sql: string, params?: any, dbName?: string) {
            const label = extractQueryLabel(sql);
            const category = categorizeSql(sql);
            const { result } = await profiler.measure(label, sql, category, () =>
              target.exec(sql, params, dbName)
            );
            return result;
          };
        }

        if (prop === "query2") {
          return async function (sql: string, params?: any, dbName?: string) {
            const label = extractQueryLabel(sql);
            const category = categorizeSql(sql);
            const { result } = await profiler.measure(label, sql, category, () =>
              target.query2(sql, params, dbName)
            );
            return result;
          };
        }

        if (prop === "exec2") {
          return async function (sql: string, params?: any, dbName?: string) {
            const label = extractQueryLabel(sql);
            const category = categorizeSql(sql);
            const { result } = await profiler.measure(label, sql, category, () =>
              target.exec2(sql, params, dbName)
            );
            return result;
          };
        }

        const value = Reflect.get(target, prop, receiver);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });

    return proxy;
  }
}

// ============================================================================
// 4. SQL CLASSIFICATION HELPERS
// ============================================================================

function categorizeSql(sql: string): QueryResourceMetric["category"] {
  const trimmed = sql.trim().toUpperCase();
  if (trimmed.startsWith("CREATE") || trimmed.startsWith("ALTER") || trimmed.startsWith("DROP")) return "DDL";
  if (trimmed.startsWith("INSERT")) return "INSERT";
  if (trimmed.startsWith("UPDATE")) return "UPDATE";
  if (trimmed.startsWith("DELETE")) return "DELETE";
  if (trimmed.startsWith("BEGIN") || trimmed.startsWith("COMMIT") || trimmed.startsWith("ROLLBACK")) return "TRANSACTION";
  if (trimmed.includes("JOIN")) return "JOIN";
  if (trimmed.includes("GROUP BY") || trimmed.includes("COUNT(") || trimmed.includes("SUM(")) return "AGGREGATE";
  if (trimmed.startsWith("SELECT")) return "SELECT";
  return "CUSTOM";
}

function extractQueryLabel(sql: string): string {
  const clean = sql.replace(/\s+/g, " ").trim();
  const matchTable = clean.match(/(?:FROM|INTO|UPDATE|TABLE)\s+([a-zA-Z0-9_]+)/i);
  const targetTable = matchTable ? matchTable[1] : "";
  const firstWord = clean.split(" ")[0].toUpperCase();

  if (firstWord === "SELECT") {
    if (clean.includes("JOIN")) return `Join Query (${targetTable})`;
    if (clean.includes("GROUP BY")) return `Aggregate (${targetTable})`;
    if (clean.includes("WHERE id =")) return `PK Lookup (${targetTable})`;
    return `Select Query (${targetTable})`;
  }
  if (firstWord === "INSERT") return `Insert (${targetTable})`;
  if (firstWord === "UPDATE") return `Update (${targetTable})`;
  if (firstWord === "DELETE") return `Delete (${targetTable})`;
  if (firstWord === "CREATE") return `Create (${targetTable || "Object"})`;
  return `${firstWord} Query`;
}

// ============================================================================
// 5. COMPREHENSIVE BENCHMARK SCENARIOS SUITE
// ============================================================================

export async function runQueryResourceSuite(useMemoryDb: boolean = false): Promise<void> {
  const DB_FILE = useMemoryDb ? ":memory:" : "query_resource_benchmark.db";

  if (!useMemoryDb) {
    for (const f of [DB_FILE, DB_FILE + ".wal"]) {
      if (fs.existsSync(f)) {
        try { fs.unlinkSync(f); } catch {}
      }
    }
  }

  console.log("\n" + "=".repeat(128));
  console.log(`  🐘 PGLITE QUERY RESOURCE PROFILER: ĐO RAM VÀ CPU Ở MỖI QUERY`);
  console.log(`  Database Engine : PGLite Native Rust Engine (in-process NAPI)`);
  console.log(`  Target Database : ${DB_FILE}`);
  console.log(`  Platform        : ${process.platform} (${process.arch}) | Bun / Node Runtime`);
  console.log("=".repeat(128) + "\n");

  const profiler = new QueryResourceProfiler({
    realtimeLog: true,
    sampleIntervalMs: 1, // High resolution 1ms sampling for peak RAM
  });

  const rawDb = new PGLite(DB_FILE);
  const db = profiler.wrapDatabase(rawDb);

  try {
    // ------------------------------------------------------------------------
    // SCENARIO 1: DDL & SCHEMA INITIALIZATION
    // ------------------------------------------------------------------------
    console.log(`${C.bold}${C.blue}▶ GIAI ĐOẠN 1: TẠO BẢNG & ĐÁNH CHỈ MỤC (DDL & INDEXING)${C.reset}`);

    await profiler.measure(
      "DDL: Create Users Table",
      `CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT NOT NULL UNIQUE,
        department TEXT NOT NULL,
        salary NUMERIC NOT NULL,
        score FLOAT NOT NULL,
        is_active BOOLEAN NOT NULL,
        metadata JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );`,
      "DDL",
      () => rawDb.exec(`
        CREATE TABLE users (
          id SERIAL PRIMARY KEY,
          name TEXT NOT NULL,
          email TEXT NOT NULL UNIQUE,
          department TEXT NOT NULL,
          salary NUMERIC NOT NULL,
          score FLOAT NOT NULL,
          is_active BOOLEAN NOT NULL,
          metadata JSONB,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `)
    );

    await profiler.measure(
      "DDL: Create Orders Table",
      `CREATE TABLE orders (
        id SERIAL PRIMARY KEY,
        user_id INT NOT NULL REFERENCES users(id),
        order_code TEXT NOT NULL UNIQUE,
        amount NUMERIC NOT NULL,
        status TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );`,
      "DDL",
      () => rawDb.exec(`
        CREATE TABLE orders (
          id SERIAL PRIMARY KEY,
          user_id INT NOT NULL REFERENCES users(id),
          order_code TEXT NOT NULL UNIQUE,
          amount NUMERIC NOT NULL,
          status TEXT NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `)
    );

    // ------------------------------------------------------------------------
    // SCENARIO 2: DATA INSERTION (SINGLE, BATCH, BULK)
    // ------------------------------------------------------------------------
    console.log(`\n${C.bold}${C.blue}▶ GIAI ĐOẠN 2: GHI DỮ LIỆU (SINGLE INSERT, BATCH & BULK TRANSACTIONS)${C.reset}`);

    // Single Insert
    await profiler.measure(
      "Insert: Single Row",
      `INSERT INTO users (name, email, department, salary, score, is_active, metadata)
       VALUES ('Alice Admin', 'alice@test.com', 'DevOps', 95000, 98.5, true, '{"role":"lead","level":"senior"}')`,
      "INSERT",
      () => rawDb.exec(`
        INSERT INTO users (name, email, department, salary, score, is_active, metadata)
        VALUES ('Alice Admin', 'alice@test.com', 'DevOps', 95000, 98.5, true, '{"role":"lead","level":"senior"}')
      `)
    );

    // Multi-row Batch Insert (1,000 rows in single statement)
    const departments = ["Engineering", "Product", "Design", "Marketing", "Sales", "HR"];
    const batchValues: string[] = [];
    for (let i = 2; i <= 1001; i++) {
      const dept = departments[i % departments.length];
      const salary = 40000 + (i * 37) % 80000;
      const score = Math.round(((i % 100) + Math.random()) * 10) / 10;
      batchValues.push(
        `('User_${i}', 'user_${i}@example.com', '${dept}', ${salary}, ${score}, ${i % 2 === 0}, '{"level":"mid","tag":"batch"}')`
      );
    }
    const batchInsertSql = `INSERT INTO users (name, email, department, salary, score, is_active, metadata) VALUES ${batchValues.join(", ")};`;

    await profiler.measure(
      "Insert: Batch 1,000 Rows",
      "INSERT INTO users (...) VALUES 1,000 rows (Single Query Statement)",
      "INSERT",
      () => rawDb.exec(batchInsertSql)
    );

    // Large Bulk Ingestion in Transaction (9,000 more users to make 10,000 total)
    await profiler.measure(
      "Bulk Insert: 9,000 Rows (TX)",
      "BEGIN; Bulk Insert 9,000 records in 9 batches of 1,000; COMMIT;",
      "TRANSACTION",
      async () => {
        await rawDb.exec("BEGIN");
        for (let b = 0; b < 9; b++) {
          const chunkValues: string[] = [];
          for (let j = 0; j < 1000; j++) {
            const idx = 1002 + b * 1000 + j;
            const dept = departments[idx % departments.length];
            const salary = 35000 + (idx * 23) % 95000;
            const score = 50 + (idx % 50);
            chunkValues.push(
              `('User_${idx}', 'user_${idx}@enterprise.com', '${dept}', ${salary}, ${score}, ${idx % 3 !== 0}, '{"level":"staff","region":"APAC"}')`
            );
          }
          await rawDb.exec(`INSERT INTO users (name, email, department, salary, score, is_active, metadata) VALUES ${chunkValues.join(", ")};`);
        }
        return rawDb.exec("COMMIT");
      }
    );

    // Seed Orders Table (20,000 orders)
    await profiler.measure(
      "Bulk Insert: 20,000 Orders",
      "BEGIN; Insert 20,000 relational orders for 10,000 users; COMMIT;",
      "TRANSACTION",
      async () => {
        await rawDb.exec("BEGIN");
        const statuses = ["completed", "pending", "shipped", "cancelled", "refunded"];
        for (let b = 0; b < 20; b++) {
          const chunk: string[] = [];
          for (let j = 0; j < 1000; j++) {
            const orderId = b * 1000 + j + 1;
            const userId = (orderId % 10000) + 1;
            const amount = 20 + ((orderId * 13) % 1500);
            const status = statuses[orderId % statuses.length];
            chunk.push(`(${userId}, 'ORD-${orderId.toString().padStart(6, "0")}', ${amount}, '${status}')`);
          }
          await rawDb.exec(`INSERT INTO orders (user_id, order_code, amount, status) VALUES ${chunk.join(", ")};`);
        }
        return rawDb.exec("COMMIT");
      }
    );

    // ------------------------------------------------------------------------
    // SCENARIO 3: SELECT & QUERY COMPLEXITIES
    // ------------------------------------------------------------------------
    console.log(`\n${C.bold}${C.blue}▶ GIAI ĐOẠN 3: TRUY VẤN DỮ LIỆU ĐA DẠNG (SELECT, JOIN, AGGREGATE, JSONB)${C.reset}`);

    // Query 1: Index Scan - Point Primary Key Lookup
    await profiler.measure(
      "Select: PK Point Lookup",
      "SELECT * FROM users WHERE id = 5432",
      "SELECT",
      () => rawDb.query("SELECT * FROM users WHERE id = $1", [5432])
    );

    // Query 2: Full Table Scan with Range Condition
    await profiler.measure(
      "Select: Full Table Scan",
      "SELECT id, name, salary, score FROM users WHERE salary > 90000",
      "SELECT",
      () => rawDb.query("SELECT id, name, salary, score FROM users WHERE salary > 90000")
    );

    // Query 3: Multi-column Filter with Index & ORDER BY + LIMIT
    await profiler.measure(
      "Select: Filter & Sort (LIMIT 50)",
      "SELECT id, name, department, salary, score FROM users WHERE department = 'Engineering' AND score >= 80 ORDER BY salary DESC LIMIT 50",
      "SELECT",
      () => rawDb.query(
        "SELECT id, name, department, salary, score FROM users WHERE department = 'Engineering' AND score >= 80 ORDER BY salary DESC LIMIT 50"
      )
    );

    // Query 4: Two-Table INNER JOIN
    await profiler.measure(
      "Join: Users & Orders (Inner Join)",
      "SELECT u.id, u.name, u.department, o.order_code, o.amount, o.status FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed' LIMIT 1000",
      "JOIN",
      () => rawDb.query(`
        SELECT u.id, u.name, u.department, o.order_code, o.amount, o.status
        FROM users u
        INNER JOIN orders o ON u.id = o.user_id
        WHERE o.status = 'completed'
        LIMIT 1000
      `)
    );

    // Query 5: Heavy Aggregation with GROUP BY & HAVING
    await profiler.measure(
      "Aggregate: Group By Dept Summary",
      "SELECT department, COUNT(*) as user_count, ROUND(AVG(salary), 2) as avg_sal, SUM(salary) as total_sal, MAX(score) as max_score FROM users GROUP BY department HAVING COUNT(*) > 500 ORDER BY total_sal DESC",
      "AGGREGATE",
      () => rawDb.query(`
        SELECT 
          department, 
          COUNT(*) as user_count, 
          ROUND(AVG(salary), 2) as avg_sal, 
          SUM(salary) as total_sal, 
          MAX(score) as max_score 
        FROM users 
        GROUP BY department 
        HAVING COUNT(*) > 500 
        ORDER BY total_sal DESC
      `)
    );

    // Query 6: Relational Aggregation (Join + Group By)
    await profiler.measure(
      "Join + Aggregate: Revenue by Dept",
      "SELECT u.department, COUNT(o.id) as order_count, SUM(o.amount) as total_revenue, AVG(o.amount) as avg_order_val FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed' GROUP BY u.department ORDER BY total_revenue DESC",
      "JOIN",
      () => rawDb.query(`
        SELECT 
          u.department, 
          COUNT(o.id) as order_count, 
          SUM(o.amount) as total_revenue, 
          ROUND(AVG(o.amount), 2) as avg_order_val 
        FROM users u 
        JOIN orders o ON u.id = o.user_id 
        WHERE o.status = 'completed' 
        GROUP BY u.department 
        ORDER BY total_revenue DESC
      `)
    );

    // Query 7: Subquery with IN Filter
    await profiler.measure(
      "Subquery: Users with High Value Orders",
      "SELECT id, name, department FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 1400) LIMIT 200",
      "SELECT",
      () => rawDb.query(`
        SELECT id, name, department 
        FROM users 
        WHERE id IN (SELECT user_id FROM orders WHERE amount > 1400)
        LIMIT 200
      `)
    );

    // Query 8: Pattern Matching with ILIKE / LIKE
    await profiler.measure(
      "Select: String Pattern Search (ILIKE)",
      "SELECT id, name, email FROM users WHERE email LIKE '%@enterprise.com' AND name LIKE '%User_9%' LIMIT 100",
      "SELECT",
      () => rawDb.query("SELECT id, name, email FROM users WHERE email LIKE '%@enterprise.com' AND name LIKE '%User_9%' LIMIT 100")
    );

    // Query 9: JSONB Query & Field Extraction
    await profiler.measure(
      "JSONB: Field Filter & Projection",
      "SELECT id, name, metadata->>'level' as level, metadata->>'region' as region FROM users WHERE metadata->>'level' = 'staff' LIMIT 500",
      "SELECT",
      () => rawDb.query("SELECT id, name, metadata->>'level' as level, metadata->>'region' as region FROM users WHERE metadata->>'level' = 'staff' LIMIT 500")
    );

    // Query 10: Fetch Large Resultset (5,000 Rows to measure serialization RAM)
    await profiler.measure(
      "Memory Stress: Fetch 5,000 Full Rows",
      "SELECT * FROM users LIMIT 5000",
      "SELECT",
      () => rawDb.query("SELECT * FROM users LIMIT 5000")
    );

    // ------------------------------------------------------------------------
    // SCENARIO 4: MUTATIONS (UPDATE, DELETE, TRANSACTIONS)
    // ------------------------------------------------------------------------
    console.log(`\n${C.bold}${C.blue}▶ GIAI ĐOẠN 4: CẬP NHẬT & XÓA DỮ LIỆU (MUTATIONS & ACID TRANSACTIONS)${C.reset}`);

    // Batch UPDATE
    await profiler.measure(
      "Update: Batch Salary Increase",
      "UPDATE users SET salary = salary * 1.05 WHERE department = 'Engineering' AND score > 90",
      "UPDATE",
      () => rawDb.exec("UPDATE users SET salary = salary * 1.05 WHERE department = 'Engineering' AND score > 90")
    );

    // Batch DELETE
    await profiler.measure(
      "Delete: Purge Cancelled Orders",
      "DELETE FROM orders WHERE status = 'cancelled'",
      "DELETE",
      () => rawDb.exec("DELETE FROM orders WHERE status = 'cancelled'")
    );

    // ACID Transaction with Rollback
    await profiler.measure(
      "Transaction: Multi-op Rollback Test",
      "BEGIN; INSERT INTO users ...; INSERT INTO orders ...; ROLLBACK;",
      "TRANSACTION",
      async () => {
        await rawDb.exec("BEGIN");
        await rawDb.exec("INSERT INTO users (name, email, department, salary, score, is_active) VALUES ('Ghost User', 'ghost@tmp.com', 'HR', 50000, 70, false)");
        await rawDb.exec("INSERT INTO orders (user_id, order_code, amount, status) VALUES (1, 'ORD-GHOST', 999, 'pending')");
        return rawDb.exec("ROLLBACK");
      }
    );

    // ------------------------------------------------------------------------
    // SCENARIO 5: MICRO-QUERY BURST (Loop of 100 fast queries)
    // ------------------------------------------------------------------------
    console.log(`\n${C.bold}${C.blue}▶ GIAI ĐOẠN 5: CHUỖI TRUY VẤN TẦN SỐ CAO (MICRO-BURST STATS)${C.reset}`);

    await profiler.measure(
      "Burst: 100 Rapid PK Lookups",
      "FOR i = 1..100 DO: SELECT * FROM users WHERE id = (i * 97) % 10000 + 1",
      "BURST",
      async () => {
        const results = [];
        for (let i = 1; i <= 100; i++) {
          const targetId = ((i * 97) % 10000) + 1;
          const rows = await rawDb.query("SELECT id, name, department, salary FROM users WHERE id = $1", [targetId]);
          results.push(rows);
        }
        return { rowCount: 100, results };
      }
    );

  } finally {
    await rawDb.close();
    if (!useMemoryDb) {
      for (const f of [DB_FILE, DB_FILE + ".wal"]) {
        if (fs.existsSync(f)) {
          try { fs.unlinkSync(f); } catch {}
        }
      }
    }
  }

  // ------------------------------------------------------------------------
  // PRINT FINAL COMPREHENSIVE REPORT
  // ------------------------------------------------------------------------
  profiler.printSummaryReport();
}

// ============================================================================
// 6. SINGLE QUERY AD-HOC RUNNER (CLI ARGUMENTS)
// ============================================================================

export async function runCustomQueryBenchmark(
  sql: string,
  dbPath: string = ":memory:",
  iterations: number = 1,
  asJson: boolean = false
): Promise<void> {
  if (!asJson) {
    console.log("\n" + "=".repeat(100));
    console.log(`  🔍 PGLITE AD-HOC QUERY RESOURCE MEASUREMENT`);
    console.log(`  Database   : ${dbPath}`);
    console.log(`  Query      : ${sql}`);
    console.log(`  Iterations : ${iterations}`);
    console.log("=".repeat(100) + "\n");
  }

  const profiler = new QueryResourceProfiler({
    realtimeLog: !asJson && iterations === 1,
    sampleIntervalMs: 1,
  });
  const db = new PGLite(dbPath);

  try {
    const isSelect = sql.trim().toUpperCase().startsWith("SELECT") || sql.trim().toUpperCase().startsWith("WITH");
    const category = categorizeSql(sql);

    for (let i = 0; i < iterations; i++) {
      const label = iterations > 1 ? `Iteration #${i + 1}` : "Custom Query";
      await profiler.measure(label, sql, category, async () => {
        if (isSelect) {
          return await db.query(sql);
        } else {
          return await db.exec(sql);
        }
      });
    }

    const metrics = profiler.getMetrics();

    if (asJson) {
      console.log(JSON.stringify(metrics, null, 2));
      return;
    }

    if (iterations > 1) {
      profiler.printSummaryReport();
    } else {
      const metric = metrics[0];
      console.log("\n" + "-".repeat(100));
      console.log(`🎯 KẾT QUẢ ĐO CHI TIẾT CHO QUERY:`);
      console.log(`   • Wall Time         : ${metric.wallTimeMs.toFixed(3)} ms`);
      console.log(`   • CPU User Time     : ${metric.cpuUserMs.toFixed(3)} ms`);
      console.log(`   • CPU System Time   : ${metric.cpuSystemMs.toFixed(3)} ms`);
      console.log(`   • Total CPU Time    : ${metric.cpuTotalMs.toFixed(3)} ms (${metric.cpuPercent.toFixed(1)}% CPU load)`);
      console.log(`   • RAM RSS Trước     : ${metric.ramRssBeforeMB.toFixed(3)} MB`);
      console.log(`   • RAM RSS Sau       : ${metric.ramRssAfterMB.toFixed(3)} MB (Chênh lệch: ${formatDelta(metric.ramRssDiffMB)})`);
      console.log(`   • Đỉnh RAM (Peak)   : ${metric.peakRssMB.toFixed(3)} MB`);
      console.log(`   • V8/Bun Heap Trước : ${metric.ramHeapBeforeMB.toFixed(3)} MB`);
      console.log(`   • V8/Bun Heap Sau   : ${metric.ramHeapAfterMB.toFixed(3)} MB (Chênh lệch: ${formatDelta(metric.ramHeapDiffMB)})`);
      console.log(`   • External (Rust)   : Chênh lệch: ${formatDelta(metric.ramExternalDiffMB)}`);
      console.log(`   • Rows Returned     : ${metric.rowCount}`);
      console.log("-".repeat(100) + "\n");
    }
  } finally {
    await db.close();
  }
}

// ============================================================================
// 7. CLI ENTRYPOINT
// ============================================================================

async function main() {
  const args = process.argv.slice(2);

  if (args.includes("--help") || args.includes("-h")) {
    console.log(`
PGLite Query RAM & CPU Resource Profiler
Usage:
  bun run scripts/measure_query_resources.ts [options]

Options:
  --help, -h               Hiển thị trợ giúp này
  --memory                 Sử dụng cơ sở dữ liệu in-memory (:memory:) thay vì file disk
  --sql "<SQL_QUERY>"      Chạy và đo tài nguyên cho một câu query SQL cụ thể
  --db "<DB_PATH>"         Chỉ định đường dẫn database khi dùng với --sql (mặc định :memory:)
  --iterations <N>         Số lần lặp lại truy vấn để tính toán trung bình (mặc định 1)
  --json                   Xuất kết quả dưới định dạng JSON

Ví dụ:
  bun run scripts/measure_query_resources.ts
  bun run scripts/measure_query_resources.ts --memory
  bun run scripts/measure_query_resources.ts --sql "SELECT 1 + 1 as total"
  bun run scripts/measure_query_resources.ts --sql "SELECT 1" --iterations 100
  bun run scripts/measure_query_resources.ts --sql "SELECT 1" --json
    `);
    process.exit(0);
  }

  const asJson = args.includes("--json");
  const iterationsIndex = args.indexOf("--iterations");
  const iterations = iterationsIndex !== -1 && args[iterationsIndex + 1] ? parseInt(args[iterationsIndex + 1], 10) : 1;

  const sqlIndex = args.indexOf("--sql");
  if (sqlIndex !== -1 && args[sqlIndex + 1]) {
    const customSql = args[sqlIndex + 1];
    const dbIndex = args.indexOf("--db");
    const dbPath = dbIndex !== -1 && args[dbIndex + 1] ? args[dbIndex + 1] : ":memory:";
    await runCustomQueryBenchmark(customSql, dbPath, isNaN(iterations) ? 1 : iterations, asJson);
    return;
  }

  const useMemory = args.includes("--memory");
  if (asJson) {
    const profiler = new QueryResourceProfiler({ realtimeLog: false, sampleIntervalMs: 1 });
    const rawDb = new PGLite(useMemory ? ":memory:" : "query_resource_benchmark.db");
    try {
      await rawDb.exec("CREATE TABLE t (id SERIAL PRIMARY KEY, v TEXT);");
      await profiler.measure("Insert Sample", "INSERT INTO t (v) VALUES ('test')", "INSERT", () => rawDb.exec("INSERT INTO t (v) VALUES ('test')"));
      await profiler.measure("Select Sample", "SELECT * FROM t", "SELECT", () => rawDb.query("SELECT * FROM t"));
      console.log(JSON.stringify(profiler.getMetrics(), null, 2));
    } finally {
      await rawDb.close();
    }
    return;
  }

  await runQueryResourceSuite(useMemory);
}

if (import.meta.main || require.main === module) {
  main().catch((err) => {
    console.error("Benchmark failed with error:", err);
    process.exit(1);
  });
}
