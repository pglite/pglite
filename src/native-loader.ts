import { resolve, join } from "path";
import { existsSync } from "fs";

export interface INativeBinding {
  LitePostgresNative: any;
}

let cachedBinding: INativeBinding | null = null;
let loadAttempted = false;

/**
 * Automatically detects platform and architecture to load the matching native Rust addon.
 * Supports:
 *  - macOS Apple Silicon: darwin-arm64 (.dylib / .node)
 *  - Linux x86_64: linux-x64 (.so / .node)
 */
export function getNativeBinding(): INativeBinding | null {
  if (loadAttempted) return cachedBinding;
  loadAttempted = true;

  const platform = process.platform; // 'darwin', 'linux'
  const arch = process.arch;         // 'arm64', 'x64'

  const binaryName = `pglite.${platform}-${arch}.node`;

  const searchPaths = [
    // 1. Current working directory or app directory (for Bun compiled binary on Linux)
    resolve(process.cwd(), binaryName),
    resolve(process.cwd(), "pglite.node"),
    // 2. Relative to @pglite/core package in dev / local link
    resolve(__dirname, "../crates/pglite-rs", binaryName),
    resolve(__dirname, "../crates/pglite-rs/pglite.node"),
    // 3. Inside dist or node_modules
    resolve(__dirname, binaryName),
    resolve(__dirname, "pglite.node"),
  ];

  for (const candidate of searchPaths) {
    if (existsSync(candidate)) {
      try {
        const mod = require(candidate);
        if (mod && mod.LitePostgresNative) {
          cachedBinding = mod;
          return cachedBinding;
        }
      } catch (err) {
        // Fall through to next candidate
      }
    }
  }

  // Fallback try standard require
  try {
    const mod = require("../crates/pglite-rs/pglite.node");
    if (mod && mod.LitePostgresNative) {
      cachedBinding = mod;
      return cachedBinding;
    }
  } catch { }

  return null;
}

export function isNativeAvailable(): boolean {
  return getNativeBinding() !== null;
}
