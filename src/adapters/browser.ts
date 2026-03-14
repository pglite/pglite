import { VFS, VFSHandle } from "../storage/engine";

export class BrowserFSAdapter implements VFS {
  private dbName = "pglite_vfs";
  private storeName = "files";
  private db: IDBDatabase | null = null;

  private async getDB(): Promise<IDBDatabase> {
    if (this.db) return this.db;
    return new Promise((resolve, reject) => {
      const request = indexedDB.open(this.dbName, 1);
      request.onupgradeneeded = () => {
        request.result.createObjectStore(this.storeName);
      };
      request.onsuccess = () => {
        this.db = request.result;
        resolve(this.db);
      };
      request.onerror = () => reject(request.error);
    });
  }

  private async getFile(path: string): Promise<Uint8Array | null> {
    const db = await this.getDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(this.storeName, "readonly");
      const request = tx.objectStore(this.storeName).get(path);
      request.onsuccess = () => resolve(request.result || null);
      request.onerror = () => reject(request.error);
    });
  }

  private async saveFile(path: string, data: Uint8Array): Promise<void> {
    const db = await this.getDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(this.storeName, "readwrite");
      const request = tx.objectStore(this.storeName).put(data, path);
      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error);
    });
  }

  async open(path: string, flags: string): Promise<VFSHandle> {
    let data = (await this.getFile(path)) || new Uint8Array(0);

    return {
      read: async (buf, offset, len, pos) => {
        const slice = data.subarray(pos, pos + len);
        buf.set(slice, offset);
        return slice.length;
      },
      write: async (buf, offset, len, pos) => {
        const writeData = buf.subarray(offset, offset + len);
        if (pos === -1 || pos >= data.length) {
          const newData = new Uint8Array(Math.max(pos === -1 ? 0 : pos, data.length) + len);
          newData.set(data);
          newData.set(writeData, pos === -1 ? data.length : pos);
          data = newData;
        } else {
          data.set(writeData, pos);
        }
        await this.saveFile(path, data);
        return len;
      },
      stat: async () => ({ size: data.length }),
      truncate: async (len) => {
        data = data.slice(0, len);
        await this.saveFile(path, data);
      },
      close: async () => {
        await this.saveFile(path, data);
      },
    };
  }

  async exists(path: string) {
    const data = await this.getFile(path);
    return data !== null;
  }

  async unlink(path: string) {
    const db = await this.getDB();
    return new Promise<void>((resolve, reject) => {
      const tx = db.transaction(this.storeName, "readwrite");
      const request = tx.objectStore(this.storeName).delete(path);
      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error);
    });
  }

  async writeFile(path: string, data: string | Uint8Array) {
    const uint8 = typeof data === "string" ? new TextEncoder().encode(data) : data;
    await this.saveFile(path, uint8);
  }

  tempDir() {
    return "/tmp";
  }

  join(...parts: string[]) {
    return parts.join("/").replace(/\/+/g, "/");
  }

  async* readLines(path: string) {
    const data = await this.getFile(path);
    if (!data) return;
    const text = new TextDecoder().decode(data);
    const lines = text.split("\n");
    for (const line of lines) yield line;
  }
}