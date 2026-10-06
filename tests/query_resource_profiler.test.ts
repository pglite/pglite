import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { PGLite } from "../src/index";
import { QueryResourceProfiler, getMemorySnapshot, getCpuSnapshot } from "../scripts/measure_query_resources";

describe("Query Resource Profiler (RAM & CPU per query)", () => {
  let db: PGLite;

  beforeAll(async () => {
    db = new PGLite(":memory:");
  });

  afterAll(async () => {
    await db.close();
  });

  test("getMemorySnapshot returns valid RSS, Heap and External numbers", () => {
    const mem = getMemorySnapshot();
    expect(mem.rss).toBeGreaterThan(0);
    expect(mem.heapUsed).toBeGreaterThan(0);
    expect(mem.heapTotal).toBeGreaterThanOrEqual(mem.heapUsed);
    expect(typeof mem.external).toBe("number");
  });

  test("getCpuSnapshot returns valid microsecond measurements", () => {
    const cpu = getCpuSnapshot();
    expect(cpu.userMicroseconds).toBeGreaterThanOrEqual(0);
    expect(cpu.systemMicroseconds).toBeGreaterThanOrEqual(0);
  });

  test("profiler.measure records RAM, CPU and timing for DDL queries", async () => {
    const profiler = new QueryResourceProfiler({ realtimeLog: false });

    const { metric } = await profiler.measure(
      "Create Test Table",
      "CREATE TABLE profiler_items (id SERIAL PRIMARY KEY, title TEXT, value NUMERIC);",
      "DDL",
      () => db.exec("CREATE TABLE profiler_items (id SERIAL PRIMARY KEY, title TEXT, value NUMERIC);")
    );

    expect(metric.id).toBe(1);
    expect(metric.name).toBe("Create Test Table");
    expect(metric.category).toBe("DDL");
    expect(metric.wallTimeMs).toBeGreaterThan(0);
    expect(metric.cpuTotalMs).toBeGreaterThanOrEqual(0);
    expect(metric.ramRssBeforeMB).toBeGreaterThan(0);
    expect(metric.ramRssAfterMB).toBeGreaterThan(0);
    expect(metric.peakRssMB).toBeGreaterThanOrEqual(metric.ramRssBeforeMB);
  });

  test("profiler.measure records correct metrics and rowCount on INSERT and SELECT", async () => {
    const profiler = new QueryResourceProfiler({ realtimeLog: false });

    // Insert 50 rows
    const { metric: insertMetric } = await profiler.measure(
      "Insert Batch",
      "INSERT INTO profiler_items (title, value) VALUES ...",
      "INSERT",
      async () => {
        await db.exec("BEGIN");
        for (let i = 1; i <= 50; i++) {
          await db.exec("INSERT INTO profiler_items (title, value) VALUES ($1, $2)", [`Item_${i}`, i * 10]);
        }
        return db.exec("COMMIT");
      }
    );

    expect(insertMetric.category).toBe("INSERT");
    expect(insertMetric.wallTimeMs).toBeGreaterThan(0);

    // Select query
    const { result, metric: selectMetric } = await profiler.measure(
      "Select All Items",
      "SELECT * FROM profiler_items WHERE value > 250",
      "SELECT",
      () => db.query("SELECT * FROM profiler_items WHERE value > 250")
    );

    expect(selectMetric.category).toBe("SELECT");
    expect(selectMetric.rowCount).toBe(25);
    expect(result.length).toBe(25);
    expect(selectMetric.wallTimeMs).toBeGreaterThan(0);
    expect(selectMetric.cpuTotalMs).toBeGreaterThanOrEqual(0);
    expect(typeof selectMetric.cpuPercent).toBe("number");
    expect(typeof selectMetric.ramRssDiffMB).toBe("number");
  });

  test("profiler.wrapDatabase automatically intercepts .query() and .exec()", async () => {
    const profiler = new QueryResourceProfiler({ realtimeLog: false });
    const monitoredDb = profiler.wrapDatabase(db);

    const rows = await monitoredDb.query("SELECT * FROM profiler_items WHERE id = 1");
    expect(rows.length).toBe(1);

    await monitoredDb.exec("UPDATE profiler_items SET value = 9999 WHERE id = 1");

    const metrics = profiler.getMetrics();
    expect(metrics.length).toBe(2);

    expect(metrics[0].sql).toContain("SELECT * FROM profiler_items WHERE id = 1");
    expect(metrics[0].rowCount).toBe(1);
    expect(metrics[0].wallTimeMs).toBeGreaterThan(0);

    expect(metrics[1].sql).toContain("UPDATE profiler_items SET value = 9999");
  });
});
