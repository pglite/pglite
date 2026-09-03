import { execSync } from "child_process";
import { copyFileSync, existsSync, mkdirSync, chmodSync } from "fs";
import { resolve, join } from "path";

console.log("\n🚀 [PGLITE] Starting Unified Build (TypeScript + macOS Native + Linux x64 Native)...\n");

const ROOT_DIR = resolve(__dirname, "..");
const CRATES_DIR = join(ROOT_DIR, "crates/pglite-rs");
const DIST_DIR = join(ROOT_DIR, "dist");
const ZIG_SCRIPT = join(CRATES_DIR, "zig-x86_64.sh");

if (!existsSync(DIST_DIR)) {
  mkdirSync(DIST_DIR, { recursive: true });
}

function run(cmd: string, cwd = ROOT_DIR, env = {}) {
  console.log(`▶ ${cmd}`);
  execSync(cmd, { cwd, stdio: "inherit", env: { ...process.env, ...env } });
}

try {
  // 1. Build TypeScript and generate JS bundle + types
  console.log("\n📦 [1/4] Building TypeScript bundle & type declarations...");
  run("bun build src/index.ts src/adapters/node.ts src/adapters/browser.ts --outdir dist --target node && tsc");

  // 2. Build macOS Native Addon (darwin-arm64)
  console.log("\n🍎 [2/4] Building macOS Native Rust Addon (arm64)...");
  run(
    "cargo build --release --manifest-path crates/pglite-rs/Cargo.toml --lib",
    ROOT_DIR,
    { RUSTFLAGS: "-C link-arg=-undefined -C link-arg=dynamic_lookup" }
  );

  const macDylib = join(CRATES_DIR, "target/release/libpglite_rs.dylib");
  if (existsSync(macDylib)) {
    copyFileSync(macDylib, join(CRATES_DIR, "pglite.darwin-arm64.node"));
    copyFileSync(macDylib, join(CRATES_DIR, "pglite.node"));
    copyFileSync(macDylib, join(DIST_DIR, "pglite.darwin-arm64.node"));
    console.log("  ✓ Generated pglite.darwin-arm64.node");
  }

  // 3. Cross-compile Linux x86_64 Native Addon
  console.log("\n🐧 [3/4] Cross-compiling Linux Native Rust Addon (x86_64-unknown-linux-gnu)...");
  if (existsSync(ZIG_SCRIPT)) {
    chmodSync(ZIG_SCRIPT, 0o755);
  }
  run(
    `cargo build --target x86_64-unknown-linux-gnu --manifest-path crates/pglite-rs/Cargo.toml --lib --release`,
    ROOT_DIR,
    { RUSTFLAGS: `-C linker=${ZIG_SCRIPT}` }
  );

  const linuxSo = join(CRATES_DIR, "target/x86_64-unknown-linux-gnu/release/libpglite_rs.so");
  if (existsSync(linuxSo)) {
    copyFileSync(linuxSo, join(CRATES_DIR, "pglite.linux-x64.node"));
    copyFileSync(linuxSo, join(DIST_DIR, "pglite.linux-x64.node"));
    console.log("  ✓ Generated pglite.linux-x64.node");
  }

  // 4. Sync Native Addons to NATA backend if present
  console.log("\n🔗 [4/4] Synchronizing Native Addons to NATA backend...");
  const nataBackendDist = resolve(ROOT_DIR, "../NATA/backend/dist");
  const nataBackendWs5 = resolve(ROOT_DIR, "../NATA/backend/dist-ws5");
  const nataBackendRoot = resolve(ROOT_DIR, "../NATA/backend");

  if (existsSync(macDylib)) {
    for (const targetDir of [nataBackendDist, nataBackendWs5, nataBackendRoot]) {
      if (existsSync(targetDir)) {
        copyFileSync(macDylib, join(targetDir, "pglite.darwin-arm64.node"));
        copyFileSync(macDylib, join(targetDir, "pglite.node"));
        console.log(`  ✓ Synced macOS addon to ${targetDir}`);
      }
    }
  }

  if (existsSync(linuxSo)) {
    for (const targetDir of [nataBackendDist, nataBackendWs5, nataBackendRoot]) {
      if (existsSync(targetDir)) {
        copyFileSync(linuxSo, join(targetDir, "pglite.linux-x64.node"));
        console.log(`  ✓ Synced Linux addon to ${targetDir}`);
      }
    }
  }

  console.log("\n✨ [PGLITE] Unified Build Completed Successfully!\n");
} catch (error) {
  console.error("\n❌ Build failed:", error);
  process.exit(1);
}
