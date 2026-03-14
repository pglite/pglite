export const ENABLE = false;

let start = 0;
export const startBenchmarks = () => {
  if (!ENABLE) return;
  start = performance.now();
};

export const endBenchmarks = (name: string) => {
  if (!ENABLE) return;
  const end = performance.now();
  const duration = end - start;
  if (duration > 0.01) {
    console.log(`[Benchmark] ${name}: ${duration}ms`);
  }
};
