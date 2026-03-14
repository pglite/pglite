import { VFS, VFSHandle } from "../storage/engine";
import { unlinkSync, existsSync, writeFileSync, createReadStream } from "fs";
import { open } from "fs/promises";
import { tmpdir } from "os";
import { join } from "path";
import { createInterface } from "readline";

export class NodeFSAdapter implements VFS {
  async open(path: string, flags: string): Promise<VFSHandle> {
    const handle = await open(path, flags);
    return {
      read: (buf, offset, len, pos) => handle.read(buf, offset, len, pos).then(r => r.bytesRead),
      write: (buf, offset, len, pos) => {
        if (pos === -1) return handle.write(buf, offset, len).then(r => r.bytesWritten);
        return handle.write(buf, offset, len, pos).then(r => r.bytesWritten);
      },
      stat: () => handle.stat(),
      truncate: (len) => handle.truncate(len),
      close: () => handle.close(),
    };
  }
  async exists(path: string) { return existsSync(path); }
  async unlink(path: string) { if (existsSync(path)) unlinkSync(path); }
  async writeFile(path: string, data: string | Uint8Array) { writeFileSync(path, data); }
  tempDir() { return tmpdir(); }
  join(...parts: string[]) { return join(...parts); }
  async* readLines(path: string) {
    const fileStream = createReadStream(path);
    const rl = createInterface({ input: fileStream, crlfDelay: Infinity });
    for await (const line of rl) yield line;
  }
}