import { VFS, VFSHandle } from "../storage/engine";

export class BrowserFSAdapter implements VFS {
  private dbName = "pglite_vfs";
  private storeName = "files";
  private db: IDBDatabase | null = null;

  private async getDB(): Promise<IDBDatabase> {
    if (this.db) {
      try {
        if (!this.db.objectStoreNames.contains(this.storeName)) {
          this.db.close();
          this.db = null;
        } else {
          return this.db;
        }
      } catch (e) {
        this.db = null;
      }
    }
    return new Promise((resolve, reject) => {
      const request = indexedDB.open(this.dbName);
      request.onupgradeneeded = () => {
        const db = request.result;
        if (!db.objectStoreNames.contains(this.storeName)) {
          db.createObjectStore(this.storeName);
        }
      };
      request.onsuccess = () => {
        const db = request.result;
        if (!db.objectStoreNames.contains(this.storeName)) {
          const currentVersion = db.version;
          db.close();
          const req2 = indexedDB.open(this.dbName, currentVersion + 1);
          req2.onupgradeneeded = () => {
            if (!req2.result.objectStoreNames.contains(this.storeName)) {
              req2.result.createObjectStore(this.storeName);
            }
          };
          req2.onsuccess = () => {
            this.db = req2.result;
            this.db.onversionchange = () => {
              this.db?.close();
              this.db = null;
            };
            resolve(this.db);
          };
          req2.onerror = () => reject(req2.error);
        } else {
          this.db = db;
          this.db.onversionchange = () => {
            this.db?.close();
            this.db = null;
          };
          resolve(this.db);
        }
      };
      request.onerror = () => reject(request.error);
    });
  }

  private async getTransaction(mode: IDBTransactionMode): Promise<IDBTransaction> {
    let db = await this.getDB();
    try {
      return db.transaction(this.storeName, mode);
    } catch (e: any) {
      if (e.name === "NotFoundError" || e.name === "InvalidStateError") {
        if (this.db) {
          try { this.db.close(); } catch (err) {}
        }
        this.db = null;
        db = await this.getDB();
        return db.transaction(this.storeName, mode);
      }
      throw e;
    }
  }

  private cache = new Map<string, { data: Uint8Array; size: number; dirty: boolean }>();
  private flushTimer: any = null;

  private scheduleFlush() {
    if (this.flushTimer) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      this.flush();
    }, 100);
  }

  public async flush(): Promise<void> {
    const dirtyFiles: any = [];
    for (const [path, file] of this.cache.entries()) {
      if (file.dirty) {
        const saveBuf = new Uint8Array(file.size);
        saveBuf.set(file.data.subarray(0, file.size));
        dirtyFiles.push({ path, data: saveBuf });
        file.dirty = false;
      }
    }
    
    if (dirtyFiles.length === 0) return;

    try {
      const tx = await this.getTransaction("readwrite");
      await new Promise<void>((resolve, reject) => {
        const store = tx.objectStore(this.storeName);
        let completed = 0;
        let hasError = false;
        
        for (const file of dirtyFiles) {
          const request = store.put(file.data, file.path);
          request.onsuccess = () => {
            completed++;
            if (completed === dirtyFiles.length && !hasError) resolve();
          };
          request.onerror = () => {
            hasError = true;
            reject(request.error);
          };
        }
      });
    } catch (e) {
      for (const file of dirtyFiles) {
        const cached = this.cache.get(file.path);
        if (cached) cached.dirty = true;
      }
      console.error("BrowserFSAdapter flush error:", e);
    }
  }

  private async getFile(path: string): Promise<Uint8Array | null> {
    if (this.cache.has(path)) {
      const cached = this.cache.get(path)!;
      return cached.data.subarray(0, cached.size);
    }
    const tx = await this.getTransaction("readonly");
    return new Promise((resolve, reject) => {
      const request = tx.objectStore(this.storeName).get(path);
      request.onsuccess = () => {
        if (request.result) {
          const data = request.result as Uint8Array;
          const copy = new Uint8Array(data.length);
          copy.set(data);
          this.cache.set(path, { data: copy, size: copy.length, dirty: false });
          resolve(copy);
        } else {
          resolve(null);
        }
      };
      request.onerror = () => reject(request.error);
    });
  }

  async open(path: string, flags: string): Promise<VFSHandle> {
    if (!this.cache.has(path)) {
      const existing = await this.getFile(path);
      if (!existing) {
        this.cache.set(path, { data: new Uint8Array(0), size: 0, dirty: false });
      }
    }

    return {
      read: async (buf, offset, len, pos) => {
        const file = this.cache.get(path)!;
        if (pos >= file.size) return 0;
        const readLen = Math.min(len, file.size - pos);
        const slice = file.data.subarray(pos, pos + readLen);
        buf.set(slice, offset);
        return readLen;
      },
      write: async (buf, offset, len, pos) => {
        const file = this.cache.get(path)!;
        const writeData = buf.subarray(offset, offset + len);
        const writePos = pos === -1 ? file.size : pos;
        const newSize = writePos + len;
        
        if (newSize > file.data.length) {
          let newCap = Math.max(file.data.length * 2, newSize + 65536);
          const newData = new Uint8Array(newCap);
          newData.set(file.data.subarray(0, file.size));
          newData.set(writeData, writePos);
          this.cache.set(path, { data: newData, size: newSize, dirty: true });
        } else {
          file.data.set(writeData, writePos);
          if (newSize > file.size) {
            file.size = newSize;
          }
          file.dirty = true;
        }
        
        this.scheduleFlush();
        return len;
      },
      stat: async () => ({ size: this.cache.get(path)!.size }),
      truncate: async (len) => {
        const file = this.cache.get(path)!;
        if (len < file.size) {
          file.size = len;
          file.dirty = true;
          this.scheduleFlush();
        }
      },
      close: async () => {
        await this.flush();
      },
    };
  }

  async exists(path: string) {
    if (this.cache.has(path)) return true;
    const data = await this.getFile(path);
    return data !== null;
  }

  async unlink(path: string) {
    this.cache.delete(path);
    const tx = await this.getTransaction("readwrite");
    return new Promise<void>((resolve, reject) => {
      const request = tx.objectStore(this.storeName).delete(path);
      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error);
    });
  }

  async writeFile(path: string, data: string | Uint8Array) {
    const uint8 = typeof data === "string" ? new TextEncoder().encode(data) : data;
    const copy = new Uint8Array(uint8.length);
    copy.set(uint8);
    this.cache.set(path, { data: copy, size: copy.length, dirty: true });
    this.scheduleFlush();
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

  static async destroyDatabase(filepath: string): Promise<void> {
    if (typeof indexedDB === "undefined") {
      throw new Error("indexedDB is not supported in this environment.");
    }
    return new Promise<void>((resolve, reject) => {
      const request = indexedDB.open("pglite_vfs");
      request.onupgradeneeded = () => {
        if (!request.result.objectStoreNames.contains("files")) {
          request.result.createObjectStore("files");
        }
      };
      request.onsuccess = () => {
        const db = request.result;
        if (!db.objectStoreNames.contains("files")) {
          db.close();
          resolve();
          return;
        }
        try {
          const tx = db.transaction("files", "readwrite");
          const store = tx.objectStore("files");
          store.delete(filepath);
          store.delete(filepath + ".wal");
          tx.oncomplete = () => {
            db.close();
            resolve();
          };
          tx.onerror = () => {
            db.close();
            reject(tx.error);
          };
        } catch (e) {
          db.close();
          reject(e);
        }
      };
      request.onerror = () => reject(request.error);
    });
  }
}