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

  private async getFile(path: string): Promise<Uint8Array | null> {
    const tx = await this.getTransaction("readonly");
    return new Promise((resolve, reject) => {
      const request = tx.objectStore(this.storeName).get(path);
      request.onsuccess = () => resolve(request.result || null);
      request.onerror = () => reject(request.error);
    });
  }

  private async saveFile(path: string, data: Uint8Array): Promise<void> {
    const tx = await this.getTransaction("readwrite");
    return new Promise((resolve, reject) => {
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
        const writePos = pos === -1 ? data.length : pos;
        if (writePos + len > data.length) {
          const newData = new Uint8Array(writePos + len);
          newData.set(data);
          newData.set(writeData, writePos);
          data = newData;
        } else {
          data.set(writeData, writePos);
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
    const tx = await this.getTransaction("readwrite");
    return new Promise<void>((resolve, reject) => {
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