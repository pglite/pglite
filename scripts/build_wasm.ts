import { execSync } from "child_process";
import { existsSync, mkdirSync } from "fs";
import { resolve } from "path";

console.log("🚀 Building PGlite WebAssembly (WASM)...");

const projectRoot = resolve(import.meta.dir, "..");
const cratePath = resolve(projectRoot, "crates/pglite-rs");
const outDir = resolve(projectRoot, "dist/wasm");

if (!existsSync(outDir)) {
  mkdirSync(outDir, { recursive: true });
}

// 1. Cargo build wasm32 target
console.log("📦 Compiling Rust to wasm32-unknown-unknown...");
execSync(
  `cargo build --manifest-path "${cratePath}/Cargo.toml" --target wasm32-unknown-unknown --release --no-default-features --features wasm-binding`,
  { stdio: "inherit", cwd: projectRoot }
);

// 2. Generate wasm-bindgen bindings
console.log("⚡ Generating JS/TS bindings via wasm-bindgen...");
const wasmBinary = resolve(cratePath, "target/wasm32-unknown-unknown/release/pglite_rs.wasm");
const wasmBindgenBin = process.env.WASM_BINDGEN_PATH || "wasm-bindgen";

try {
  execSync(`${wasmBindgenBin} "${wasmBinary}" --out-dir "${outDir}" --target web`, {
    stdio: "inherit",
    cwd: projectRoot,
  });
} catch {
  // Fallback to ~/.cargo/bin/wasm-bindgen if not in PATH
  const fallbackBin = `${process.env.HOME}/.cargo/bin/wasm-bindgen`;
  execSync(`${fallbackBin} "${wasmBinary}" --out-dir "${outDir}" --target web`, {
    stdio: "inherit",
    cwd: projectRoot,
  });
}

console.log(`✅ WASM build successfully created in: ${outDir}`);
