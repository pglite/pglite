import { ColumnDef } from "../ast";

const PAGE_SIZE = 4096;

// Vite/Browser compatibility: Tránh import trực tiếp module 'buffer' của Node.js.
// Chúng ta sử dụng global Buffer nếu có (Node.js hoặc polyfill) hoặc dùng Uint8Array fallback.
const _Buffer =
  typeof globalThis !== "undefined" ? (globalThis as any).Buffer : undefined;

/**
 * Shim cho Buffer để hoạt động trên cả Node.js và trình duyệt.
 */
const Buffer = {
  alloc: (size: number) => {
    const buf = _Buffer ? _Buffer.alloc(size) : new Uint8Array(size);
    return extendUint8Array(buf);
  },
  allocUnsafe: (size: number) => {
    const buf = _Buffer ? _Buffer.allocUnsafe(size) : new Uint8Array(size);
    return extendUint8Array(buf);
  },
  from: (data: any, enc?: string) => {
    const buf = _Buffer
      ? _Buffer.from(data, enc)
      : typeof data === "string"
        ? new TextEncoder().encode(data)
        : new Uint8Array(data);
    return extendUint8Array(buf);
  },
  byteLength: (str: string) => {
    if (_Buffer) return _Buffer.byteLength(str);
    return new TextEncoder().encode(str).length;
  },
};

/**
 * Bổ sung các phương thức của Node.js Buffer vào Uint8Array để tương thích trình duyệt.
 */
function extendUint8Array(buf: Uint8Array): any {
  if ((buf as any).readUInt32LE) return buf;
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  Object.defineProperties(buf, {
    readUInt32LE: {
      value: (offset: number) => dv.getUint32(offset, true),
      configurable: true,
    },
    writeUInt32LE: {
      value: (val: number, offset: number) => dv.setUint32(offset, val, true),
      configurable: true,
    },
    readUInt16LE: {
      value: (offset: number) => dv.getUint16(offset, true),
      configurable: true,
    },
    writeUInt16LE: {
      value: (val: number, offset: number) => dv.setUint16(offset, val, true),
      configurable: true,
    },
    readUInt8: {
      value: (offset: number) => dv.getUint8(offset),
      configurable: true,
    },
    writeUInt8: {
      value: (val: number, offset: number) => dv.setUint8(offset, val),
      configurable: true,
    },
    readDoubleLE: {
      value: (offset: number) => dv.getFloat64(offset, true),
      configurable: true,
    },
    writeDoubleLE: {
      value: (val: number, offset: number) => dv.setFloat64(offset, val, true),
      configurable: true,
    },
    copy: {
      value: (
        target: Uint8Array,
        targetStart: number,
        srcStart: number,
        srcEnd: number,
      ) => {
        target.set(
          buf.subarray(srcStart || 0, srcEnd || buf.length),
          targetStart || 0,
        );
      },
      configurable: true,
    },
    toString: {
      value: (enc?: string, start?: number, end?: number) => {
        return new TextDecoder().decode(
          buf.subarray(start || 0, end || buf.length),
        );
      },
      configurable: true,
    },
    write: {
      value: (str: string, offset?: number, length?: number) => {
        const result = new TextEncoder().encodeInto(
          str,
          buf.subarray(
            offset || 0,
            (offset || 0) + (length || buf.length - (offset || 0)),
          ),
        );
        return result.written;
      },
      configurable: true,
    },
    subarray: {
      value: (begin?: number, end?: number) => {
        return extendUint8Array(
          Uint8Array.prototype.subarray.call(buf, begin, end),
        );
      },
      configurable: true,
    },
  });
  return buf;
}

// Định nghĩa kiểu Buffer là any để tránh lỗi TypeScript khi sử dụng các phương thức mở rộng
type Buffer = any;

export interface VFSHandle {
  read(
    buffer: Uint8Array,
    offset: number,
    length: number,
    position: number,
  ): Promise<number>;
  write(
    buffer: Uint8Array,
    offset: number,
    length: number,
    position: number,
  ): Promise<number>;
  stat(): Promise<{ size: number }>;
  truncate(length: number): Promise<void>;
  close(): Promise<void>;
}

export interface VFS {
  open(path: string, flags: string): Promise<VFSHandle>;
  exists(path: string): Promise<boolean>;
  unlink(path: string): Promise<void>;
  writeFile(path: string, data: string | Uint8Array): Promise<void>;
  tempDir(): string;
  join(...parts: string[]): string;
  readLines(path: string): AsyncIterable<string>;
}

class FileHandlePool {
  private static pool = new Map<string, VFSHandle>();
  private static refs = new Map<string, number>();
  private static opening = new Map<string, Promise<VFSHandle>>();
  private static MAX_FDS = 4096;

  static async getHandle(
    vfs: VFS,
    path: string,
    flags: string,
  ): Promise<VFSHandle> {
    const existing = this.pool.get(path);
    if (existing) {
      this.pool.delete(path);
      this.pool.set(path, existing);
      this.refs.set(path, (this.refs.get(path) || 0) + 1);
      return existing;
    }

    const inProgress = this.opening.get(path);
    if (inProgress) {
      const h = await inProgress;
      this.refs.set(path, (this.refs.get(path) || 0) + 1);
      return h;
    }

    const openPromise = (async () => {
      try {
        if (this.pool.size >= this.MAX_FDS) {
          for (const [key, h] of this.pool.entries()) {
            if ((this.refs.get(key) || 0) <= 0) {
              this.pool.delete(key);
              this.refs.delete(key);
              await h.close().catch(() => {});
              break;
            }
          }
        }
        const h = await vfs.open(path, flags);
        this.pool.set(path, h);
        return h;
      } finally {
        this.opening.delete(path);
      }
    })();

    this.opening.set(path, openPromise);
    const h = await openPromise;
    this.refs.set(path, (this.refs.get(path) || 0) + 1);
    return h;
  }

  static releaseHandle(path: string) {
    const r = this.refs.get(path) || 0;
    if (r > 0) this.refs.set(path, r - 1);
  }

  static async close(path: string) {
    const h = this.pool.get(path);
    if (h) {
      this.pool.delete(path);
      this.refs.delete(path);
      await h.close().catch(() => {});
    }
  }
}

class LRUCache<K, V> {
  private cache = new Map<K, V>();
  constructor(private capacity: number) {}
  get(key: K): V | undefined {
    const val = this.cache.get(key);
    if (val === undefined) return undefined;
    this.cache.delete(key);
    this.cache.set(key, val);
    return val;
  }
  set(key: K, val: V): void {
    if (this.cache.has(key)) this.cache.delete(key);
    else if (this.cache.size >= this.capacity) {
      const firstKey = this.cache.keys().next().value;
      if (firstKey !== undefined) this.cache.delete(firstKey);
    }
    this.cache.set(key, val);
  }
  has(key: K): boolean {
    return this.cache.has(key);
  }
  delete(key: K): void {
    this.cache.delete(key);
  }
  clear(): void {
    this.cache.clear();
  }
}

class WAL {
  private handle?: VFSHandle;
  private buffer: Buffer;
  private bufferOffset: number = 0;
  private flushTimer: any = null;
  private static readonly MAX_WAL_BUFFER = 65536;

  constructor(
    private vfs: VFS,
    private filepath: string,
  ) {
    this.buffer = Buffer.allocUnsafe(WAL.MAX_WAL_BUFFER);
  }

  async open() {
    if (this.handle !== undefined) return;
    if (!(await this.vfs.exists(this.filepath)))
      await this.vfs.writeFile(this.filepath, new Uint8Array(0));
    this.handle = await this.vfs.open(this.filepath, "a+");
  }

  private scheduleFlush() {
    if (this.flushTimer) return;
    this.flushTimer = setTimeout(() => this.flush(), 10);
  }

  public async flush() {
    if (this.bufferOffset === 0) return;
    if (!this.handle) await this.open();

    await this.handle!.write(this.buffer, 0, this.bufferOffset, -1);
    this.bufferOffset = 0;

    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
  }

  async log(pageId: number, data: Buffer) {
    const size = 8 + data.length;

    if (this.bufferOffset + size > WAL.MAX_WAL_BUFFER) {
      await this.flush();
    }

    if (size > WAL.MAX_WAL_BUFFER) {
      if (!this.handle) await this.open();
      const header = Buffer.allocUnsafe(8);
      header.writeUInt32LE(pageId, 0);
      header.writeUInt32LE(data.length, 4);
      await this.handle!.write(header, 0, 8, -1);
      await this.handle!.write(data, 0, data.length, -1);
    } else {
      this.buffer.writeUInt32LE(pageId, this.bufferOffset);
      this.buffer.writeUInt32LE(data.length, this.bufferOffset + 4);
      data.copy(this.buffer, this.bufferOffset + 8);
      this.bufferOffset += size;
      this.scheduleFlush();
    }
  }

  async logBatch(pages: { pageId: number; data: Buffer }[]) {
    if (pages.length === 0) return;
    for (const p of pages) {
      const size = 8 + p.data.length;
      if (this.bufferOffset + size > WAL.MAX_WAL_BUFFER) {
        await this.flush();
      }
      if (size > WAL.MAX_WAL_BUFFER) {
        if (!this.handle) await this.open();
        const header = Buffer.allocUnsafe(8);
        header.writeUInt32LE(p.pageId, 0);
        header.writeUInt32LE(p.data.length, 4);
        await this.handle!.write(header, 0, 8, -1);
        await this.handle!.write(p.data, 0, p.data.length, -1);
      } else {
        this.buffer.writeUInt32LE(p.pageId, this.bufferOffset);
        this.buffer.writeUInt32LE(p.data.length, this.bufferOffset + 4);
        p.data.copy(this.buffer, this.bufferOffset + 8);
        this.bufferOffset += size;
      }
    }
    this.scheduleFlush();
  }

  async clear() {
    await this.flush();
    if (!this.handle) await this.open();
    await this.handle!.truncate(0);
  }

  async close() {
    await this.flush();
    if (this.handle !== undefined) {
      await this.handle.close();
      this.handle = undefined;
    }
  }
}

class Pager {
  private static pagers = new Map<string, Pager>();
  private refCount = 0;

  public static get(vfs: VFS, filepath: string): Pager {
    let pager = this.pagers.get(filepath);
    if (!pager) {
      pager = new Pager(vfs, filepath);
      this.pagers.set(filepath, pager);
    }
    pager.refCount++;
    return pager;
  }

  public numPages: number = 0;
  public firstFreePage: number = 0xffffffff;
  private txNumPagesBackup: number = 0;
  private txFirstFreePageBackup: number = 0xffffffff;
  private cache = new LRUCache<number, Buffer>(2048);
  private wal: WAL;
  private dirtyPages = new Map<number, Buffer>();
  private handle?: VFSHandle;
  private initialized = false;

  private constructor(
    private vfs: VFS,
    private filepath: string,
  ) {
    this.wal = new WAL(vfs, filepath + ".wal");
  }

  async init() {
    if (this.initialized) return;

    if (await this.vfs.exists(this.filepath + ".wal")) {
      try {
        const walHandle = await this.vfs.open(this.filepath + ".wal", "r");
        const stat = await walHandle.stat();
        if (stat.size > 0) {
          if (!(await this.vfs.exists(this.filepath))) {
            await this.vfs.writeFile(this.filepath, new Uint8Array(0));
          }
          const dbHandle = await FileHandlePool.getHandle(
            this.vfs,
            this.filepath,
            "r+",
          );

          let offset = 0;
          const headerBuf = Buffer.alloc(8);
          while (offset < stat.size) {
            const bytesRead = await walHandle.read(headerBuf, 0, 8, offset);
            if (bytesRead < 8) break;
            const pageId = headerBuf.readUInt32LE(0);
            const dataLen = headerBuf.readUInt32LE(4);

            if (offset + 8 + dataLen > stat.size) break;

            const dataBuf = Buffer.alloc(dataLen);
            await walHandle.read(dataBuf, 0, dataLen, offset + 8);

            await dbHandle.write(dataBuf, 0, dataLen, pageId * PAGE_SIZE);
            offset += 8 + dataLen;
          }
        }
        await walHandle.close();
        await this.vfs.writeFile(this.filepath + ".wal", new Uint8Array(0));
      } catch (e) {
        console.warn("WAL recovery failed", e);
      }
    }

    await this.wal.open();
    if (!(await this.vfs.exists(this.filepath))) {
      await this.vfs.writeFile(this.filepath, new Uint8Array(0));
    }

    this.handle = await FileHandlePool.getHandle(this.vfs, this.filepath, "r+");
    const stat = await this.handle.stat();
    this.numPages = Math.floor(stat.size / PAGE_SIZE);

    if (this.numPages === 0) {
      const masterPage = Buffer.alloc(PAGE_SIZE);
      masterPage.writeUInt32LE(0xffffffff, 16);
      await this.writePage(0, masterPage);
      this.numPages = 1;
      this.firstFreePage = 0xffffffff;
    } else {
      const p0 = await this.readPage(0);
      this.firstFreePage = p0.readUInt32LE(16);
      if (this.firstFreePage === 0) this.firstFreePage = 0xffffffff;
    }
    this.txNumPagesBackup = this.numPages;
    this.txFirstFreePageBackup = this.firstFreePage;
    this.initialized = true;
  }

  async readPage(pageId: number): Promise<Buffer> {
    if (this.dirtyPages.has(pageId)) return this.dirtyPages.get(pageId)!;
    const cached = this.cache.get(pageId);
    if (cached) return cached;

    const buf = Buffer.alloc(PAGE_SIZE);
    if (pageId < this.numPages) {
      if (!this.handle) await this.init();
      await this.handle!.read(buf, 0, PAGE_SIZE, pageId * PAGE_SIZE);
    }
    this.cache.set(pageId, buf);
    return buf;
  }

  async writePage(pageId: number, data: Buffer): Promise<void> {
    this.dirtyPages.set(pageId, data);
    this.cache.set(pageId, data);
    if (pageId >= this.numPages) {
      this.numPages = pageId + 1;
    }
  }

  async allocatePage(): Promise<number> {
    let pageId: number;
    if (this.firstFreePage !== 0xffffffff && this.firstFreePage !== 0) {
      pageId = this.firstFreePage;
      const buf = await this.readPage(pageId);
      this.firstFreePage = buf.readUInt32LE(0);
      
      const p0 = await this.readPage(0);
      p0.writeUInt32LE(this.firstFreePage, 16);
      await this.writePage(0, p0);
    } else {
      pageId = this.numPages;
      this.numPages++;
    }
    const buf = Buffer.alloc(PAGE_SIZE);
    buf.writeUInt32LE(0xffffffff, 0);
    buf.writeUInt16LE(0, 4);
    buf.writeUInt16LE(PAGE_SIZE, 6);
    await this.writePage(pageId, buf);
    return pageId;
  }

  async freePage(pageId: number): Promise<void> {
    const buf = Buffer.alloc(PAGE_SIZE);
    buf.writeUInt32LE(this.firstFreePage, 0);
    await this.writePage(pageId, buf);
    
    this.firstFreePage = pageId;
    const p0 = await this.readPage(0);
    p0.writeUInt32LE(this.firstFreePage, 16);
    await this.writePage(0, p0);
  }

  async flush() {
    if (this.dirtyPages.size === 0) return;
    if (!this.handle) await this.init();

    const sortedPageIds = Array.from(this.dirtyPages.keys()).sort(
      (a, b) => a - b,
    );

    const walPages = sortedPageIds.map((pageId) => ({
      pageId,
      data: this.dirtyPages.get(pageId)!,
    }));
    await this.wal.logBatch(walPages);
    await this.wal.flush();

    let i = 0;
    while (i < sortedPageIds.length) {
      let j = i;
      while (
        j + 1 < sortedPageIds.length &&
        sortedPageIds[j + 1] === sortedPageIds[j]! + 1
      ) {
        j++;
      }

      const startPageId = sortedPageIds[i]!;
      const count = j - i + 1;

      if (count === 1) {
        const data = this.dirtyPages.get(startPageId);
        if (data) {
          await this.handle!.write(data, 0, PAGE_SIZE, startPageId * PAGE_SIZE);
        }
      } else {
        const batchBuffer = Buffer.allocUnsafe(count * PAGE_SIZE);
        let actualCount = 0;
        for (let k = 0; k < count; k++) {
          const pid = sortedPageIds[i + k]!;
          const data = this.dirtyPages.get(pid);
          if (data) {
            data.copy(batchBuffer, actualCount * PAGE_SIZE);
            actualCount++;
          }
        }
        if (actualCount > 0) {
          await this.handle!.write(
            batchBuffer,
            0,
            actualCount * PAGE_SIZE,
            startPageId * PAGE_SIZE,
          );
        }
      }
      i = j + 1;
    }

    this.dirtyPages.clear();
    await this.wal.clear();
    this.txNumPagesBackup = this.numPages;
    this.txFirstFreePageBackup = this.firstFreePage;
  }

  async clearDirty() {
    for (const pageId of this.dirtyPages.keys()) {
      this.cache.delete(pageId);
    }
    this.dirtyPages.clear();
    await this.wal.clear();
    this.numPages = this.txNumPagesBackup;
    this.firstFreePage = this.txFirstFreePageBackup;
  }

  public async close() {
    this.refCount--;
    if (this.refCount <= 0) {
      if (this.handle) {
        FileHandlePool.releaseHandle(this.filepath);
        await FileHandlePool.close(this.filepath);
        this.handle = undefined;
      }
      await this.wal.close();
      Pager.pagers.delete(this.filepath);
      this.initialized = false;
    }
  }

  public async destroy() {
    this.refCount--;
    if (this.refCount <= 0) {
      if (this.handle) {
        FileHandlePool.releaseHandle(this.filepath);
        await FileHandlePool.close(this.filepath);
        this.handle = undefined;
      }
      await this.wal.close();
      Pager.pagers.delete(this.filepath);
      this.initialized = false;
      if (await this.vfs.exists(this.filepath)) {
        await this.vfs.unlink(this.filepath);
      }
      if (await this.vfs.exists(this.filepath + ".wal")) {
        await this.vfs.unlink(this.filepath + ".wal");
      }
    }
  }
}

class SlottedPage {
  constructor(
    public buf: Buffer,
    isNew: boolean = false,
  ) {
    // Only initialize if explicitly new or if it's a completely empty buffer
    // Checking byte 6 (freeSpacePointer) is unreliable for B-Tree nodes
    const isEmpty =
      this.buf.readUInt32LE(0) === 0 && this.buf.readUInt32LE(4) === 0;
    if (isNew || isEmpty) {
      this.buf.writeUInt32LE(0xffffffff, 0);
      this.buf.writeUInt16LE(0, 4);
      this.buf.writeUInt16LE(PAGE_SIZE, 6);
    }
  }

  get nextPageId() {
    return this.buf.readUInt32LE(0);
  }
  set nextPageId(id: number) {
    this.buf.writeUInt32LE(id, 0);
  }

  get numSlots() {
    return this.buf.readUInt16LE(4);
  }
  set numSlots(n: number) {
    this.buf.writeUInt16LE(n, 4);
  }

  get freeSpacePointer() {
    return this.buf.readUInt16LE(6);
  }
  set freeSpacePointer(p: number) {
    this.buf.writeUInt16LE(p, 6);
  }

  getFreeSpace() {
    return this.freeSpacePointer - (8 + this.numSlots * 4);
  }

  insertTuple(data: Buffer): number {
    if (data.length + 4 > this.getFreeSpace()) return -1;
    this.freeSpacePointer -= data.length;
    data.copy(this.buf, this.freeSpacePointer);
    const slotIdx = this.numSlots;
    const slotOffset = 8 + slotIdx * 4;
    this.buf.writeUInt16LE(this.freeSpacePointer, slotOffset);
    this.buf.writeUInt16LE(data.length, slotOffset + 2);
    this.numSlots++;
    return slotIdx;
  }

  getTuple(targetSlotIdx: number): { offset: number; len: number; data: Buffer; slotIdx: number } | null {
    if (targetSlotIdx >= this.numSlots) return null;
    const slotOffset = 8 + targetSlotIdx * 4;
    const dataOffset = this.buf.readUInt16LE(slotOffset);
    if (dataOffset === 0) return null;
    const dataLen = this.buf.readUInt16LE(slotOffset + 2);
    return {
      offset: dataOffset,
      len: dataLen,
      data: this.buf.subarray(dataOffset, dataOffset + dataLen),
      slotIdx: targetSlotIdx
    };
  }

  getTuples(): {
    offset: number;
    len: number;
    data: Buffer;
    slotIdx: number;
  }[] {
    const buffer = this.buf;
    const num = buffer.readUInt16LE(4);
    const result = [];
    for (let i = 0; i < num; i++) {
      const slotOffset = 8 + i * 4;
      const dataOffset = buffer.readUInt16LE(slotOffset);
      if (dataOffset === 0) continue;

      const dataLen = buffer.readUInt16LE(slotOffset + 2);
      result.push({
        offset: dataOffset,
        len: dataLen,
        data: buffer.subarray(dataOffset, dataOffset + dataLen),
        slotIdx: i,
      });
    }
    return result;
  }

  deleteTuple(slotIdx: number) {
    this.buf.writeUInt16LE(0, 8 + slotIdx * 4);
  }

  updateTuple(slotIdx: number, data: Buffer): boolean {
    const slotOffset = 8 + slotIdx * 4;
    const oldLen = this.buf.readUInt16LE(slotOffset + 2);
    if (data.length <= oldLen) {
      const dataOffset = this.buf.readUInt16LE(slotOffset);
      data.copy(this.buf, dataOffset);
      this.buf.writeUInt16LE(data.length, slotOffset + 2);
      return true;
    } else {
      // Allocate new space but retain the same slot index to prevent index corruption
      if (data.length > this.getFreeSpace()) return false;
      this.freeSpacePointer -= data.length;
      data.copy(this.buf, this.freeSpacePointer);
      this.buf.writeUInt16LE(this.freeSpacePointer, slotOffset);
      this.buf.writeUInt16LE(data.length, slotOffset + 2);
      return true;
    }
  }
}

class BTree {
  private static readonly nodeCache = new WeakMap<Buffer, any>();

  constructor(
    private pager: Pager,
    public rootPageId: number,
  ) {}

  private async fetchNode(pageId: number): Promise<any> {
    const buf = await this.pager.readPage(pageId);
    let node = BTree.nodeCache.get(buf);
    if (!node) {
      node = this.deserializeNode(pageId, buf);
      BTree.nodeCache.set(buf, node);
    }
    return node;
  }

  private getNodeSize(node: any): number {
    let size = 7;
    for (let i = 0; i < node.keys.length; i++) {
      const typeInd = typeof node.keys[i] === "number" ? "N" : "S";
      const keyStr = typeInd + String(node.keys[i]);
      size += 2 + Buffer.byteLength(keyStr) + (node.isLeaf ? 6 : 4);
    }
    if (!node.isLeaf) size += 4;
    return size;
  }

  private serializeNode(node: any, buf: Buffer) {
    buf.fill(0, 0, 7); // Reset header area
    buf.writeUInt8(node.isLeaf ? 1 : 0, 0);
    buf.writeUInt16LE(node.keys.length, 1);
    buf.writeUInt32LE(node.nextLeaf || 0xffffffff, 3);
    let offset = 7;
    let actualNumKeys = node.keys.length;
    for (let i = 0; i < node.keys.length; i++) {
      const isNum = typeof node.keys[i] === "number";
      const typeInd = isNum ? "N" : "S";
      const keyStr = typeInd + String(node.keys[i]);
      const kLen = Buffer.byteLength(keyStr);
      if (offset + 2 + kLen + (node.isLeaf ? 6 : 4) > PAGE_SIZE) {
        actualNumKeys = i;
        buf.writeUInt16LE(actualNumKeys, 1);
        break;
      }

      buf.writeUInt16LE(kLen, offset);
      offset += 2;
      buf.write(keyStr, offset);
      offset += kLen;
      if (node.isLeaf) {
        buf.writeUInt32LE(node.vals[i].pageId, offset);
        offset += 4;
        buf.writeUInt16LE(node.vals[i].slotIdx, offset);
        offset += 2;
      } else {
        buf.writeUInt32LE(node.children[i], offset);
        offset += 4;
      }
    }
    if (!node.isLeaf && offset + 4 <= PAGE_SIZE) {
      buf.writeUInt32LE(node.children[actualNumKeys], offset);
    }
  }

  private deserializeNode(pageId: number, buf: Buffer) {
    if (!buf || buf.length < 7)
      return {
        pageId,
        isLeaf: true,
        keys: [],
        vals: [],
        children: [],
        nextLeaf: 0xffffffff,
      };

    const isLeaf = buf.readUInt8(0) === 1;
    const numKeys = buf.readUInt16LE(1);
    const nextLeaf = buf.readUInt32LE(3);
    let offset = 7;
    const keys = [];
    const vals = [];
    const children = [];
    for (let i = 0; i < numKeys; i++) {
      const kLen = buf.readUInt16LE(offset);
      const kStr = buf.toString("utf-8", offset + 2, offset + 2 + kLen);
      offset += 2 + kLen;
      const typeInd = kStr[0];
      const valStr = kStr.substring(1);
      // Legacy fallback if type indicator is missing (for older files if any)
      if (typeInd !== "N" && typeInd !== "S") {
        keys.push(isNaN(Number(kStr)) ? kStr : Number(kStr));
      } else {
        keys.push(typeInd === "N" ? Number(valStr) : valStr);
      }
      if (isLeaf) {
        vals.push({
          pageId: buf.readUInt32LE(offset),
          slotIdx: buf.readUInt16LE(offset + 4),
        });
        offset += 6;
      } else {
        children.push(buf.readUInt32LE(offset));
        offset += 4;
      }
    }
    if (!isLeaf) children.push(buf.readUInt32LE(offset));
    return { pageId, isLeaf, keys, vals, children, nextLeaf };
  }

  async get(key: any): Promise<{ pageId: number; slotIdx: number } | null> {
    if (this.rootPageId === 0 || this.rootPageId === 0xffffffff) return null;
    let currId = this.rootPageId;

    while (currId !== 0xffffffff && currId !== 0) {
      const node = await this.fetchNode(currId);
      
      let left = 0;
      let right = node.keys.length - 1;
      let foundIdx = -1;
      let insertIdx = node.keys.length;

      while (left <= right) {
        const mid = (left + right) >> 1;
        const midKey = node.keys[mid];
        if (midKey === key) {
          foundIdx = mid;
          break;
        } else if (midKey < key) {
          left = mid + 1;
        } else {
          insertIdx = mid;
          right = mid - 1;
        }
      }

      if (node.isLeaf) {
        if (foundIdx !== -1) return node.vals[foundIdx];
        return null;
      } else {
        if (foundIdx !== -1) {
          currId = node.children[foundIdx + 1];
        } else {
          currId = node.children[insertIdx];
        }
      }
    }
    return null;
  }

  async insert(
    key: any,
    val: { pageId: number; slotIdx: number },
  ): Promise<number> {
    if (this.rootPageId === 0 || this.rootPageId === 0xffffffff) {
      this.rootPageId = await this.pager.allocatePage();
      const buf = await this.pager.readPage(this.rootPageId);
      const node = {
        isLeaf: true,
        keys: [key],
        vals: [val],
        children: [],
        nextLeaf: 0xffffffff,
      };
      this.serializeNode(node, buf);
      await this.pager.writePage(this.rootPageId, buf);
      BTree.nodeCache.set(buf, node);
      return this.rootPageId;
    }

    const path = [];
    let currId = this.rootPageId;
    while (true) {
      const node = await this.fetchNode(currId);
      path.push(node);
      if (node.isLeaf) break;
      
      let left = 0;
      let right = node.keys.length - 1;
      let foundIdx = -1;
      let insertIdx = node.keys.length;

      while (left <= right) {
        const mid = (left + right) >> 1;
        const midKey = node.keys[mid];
        if (midKey === key) {
          foundIdx = mid;
          break;
        } else if (midKey < key) {
          left = mid + 1;
        } else {
          insertIdx = mid;
          right = mid - 1;
        }
      }

      if (foundIdx !== -1) currId = node.children[foundIdx + 1];
      else currId = node.children[insertIdx];
    }

    const leaf = path[path.length - 1];
    let left = 0;
    let right = leaf.keys.length - 1;
    let foundIdx = -1;
    let insertIdx = leaf.keys.length;

    while (left <= right) {
      const mid = (left + right) >> 1;
      const midKey = leaf.keys[mid];
      if (midKey === key) {
        foundIdx = mid;
        break;
      } else if (midKey < key) {
        left = mid + 1;
      } else {
        insertIdx = mid;
        right = mid - 1;
      }
    }

    if (foundIdx !== -1) {
      leaf.vals[foundIdx] = val;
    } else {
      leaf.keys.splice(insertIdx, 0, key);
      leaf.vals.splice(insertIdx, 0, val);
    }

    if (
      this.getNodeSize({
        isLeaf: true,
        keys: [key],
        vals: [val],
        children: [],
        nextLeaf: 0xffffffff,
      }) > PAGE_SIZE
    ) {
      throw new Error("Index key is too large to fit in a B-Tree page");
    }

    let currNode = leaf;
    while (
      currNode.keys.length > 32 ||
      this.getNodeSize(currNode) > PAGE_SIZE
    ) {
      const splitIdx = Math.floor(currNode.keys.length / 2);
      if (splitIdx === 0)
        throw new Error("Index key too large causing infinite split");
      const rightPageId = await this.pager.allocatePage();
      const rightBuf = await this.pager.readPage(rightPageId);
      const rightNode = {
        pageId: rightPageId,
        isLeaf: currNode.isLeaf,
        keys: currNode.keys.splice(splitIdx),
        vals: currNode.isLeaf ? currNode.vals.splice(splitIdx) : [],
        children: currNode.isLeaf ? [] : currNode.children.splice(splitIdx + 1),
        nextLeaf: currNode.isLeaf ? currNode.nextLeaf : 0xffffffff,
      };
      if (currNode.isLeaf) {
        rightNode.nextLeaf = currNode.nextLeaf;
        currNode.nextLeaf = rightPageId;
      }
      BTree.nodeCache.set(rightBuf, rightNode);

      let promoteKey = currNode.isLeaf
        ? rightNode.keys[0]
        : rightNode.keys.shift();

      const currBuf = await this.pager.readPage(currNode.pageId);
      this.serializeNode(currNode, currBuf);
      await this.pager.writePage(currNode.pageId, currBuf);

      this.serializeNode(rightNode, rightBuf);
      await this.pager.writePage(rightPageId, rightBuf);

      path.pop();
      if (path.length === 0) {
        this.rootPageId = await this.pager.allocatePage();
        const rootBuf = await this.pager.readPage(this.rootPageId);
        const rootNode = {
          pageId: this.rootPageId,
          isLeaf: false,
          keys: [promoteKey],
          vals: [],
          children: [currNode.pageId, rightNode.pageId],
          nextLeaf: 0xffffffff,
        };
        this.serializeNode(rootNode, rootBuf);
        await this.pager.writePage(this.rootPageId, rootBuf);
        BTree.nodeCache.set(rootBuf, rootNode);
        break;
      } else {
        const parent = path[path.length - 1];
        let pLeft = 0;
        let pRight = parent.keys.length - 1;
        let pIdx = parent.keys.length;
        while (pLeft <= pRight) {
          const mid = (pLeft + pRight) >> 1;
          if (parent.keys[mid] < promoteKey) {
            pLeft = mid + 1;
          } else {
            pIdx = mid;
            pRight = mid - 1;
          }
        }
        parent.keys.splice(pIdx, 0, promoteKey);
        parent.children.splice(pIdx + 1, 0, rightNode.pageId);
        currNode = parent;
      }
    }

    if (currNode.keys.length <= 32) {
      const buf = await this.pager.readPage(currNode.pageId);
      this.serializeNode(currNode, buf);
      await this.pager.writePage(currNode.pageId, buf);
    }

    return this.rootPageId;
  }

  async delete(key: any) {
    if (this.rootPageId === 0 || this.rootPageId === 0xffffffff) return;
    const path = [];
    let currId = this.rootPageId;
    while (currId !== 0xffffffff && currId !== 0) {
      const node = await this.fetchNode(currId);
      path.push(node);
      if (node.isLeaf) break;
      
      let left = 0;
      let right = node.keys.length - 1;
      let foundIdx = -1;
      let insertIdx = node.keys.length;

      while (left <= right) {
        const mid = (left + right) >> 1;
        const midKey = node.keys[mid];
        if (midKey === key) {
          foundIdx = mid;
          break;
        } else if (midKey < key) {
          left = mid + 1;
        } else {
          insertIdx = mid;
          right = mid - 1;
        }
      }
      
      if (foundIdx !== -1) currId = node.children[foundIdx + 1];
      else currId = node.children[insertIdx];
    }

    const leaf = path[path.length - 1];
    if (leaf && leaf.isLeaf) {
      let left = 0;
      let right = leaf.keys.length - 1;
      let foundIdx = -1;

      while (left <= right) {
        const mid = (left + right) >> 1;
        const midKey = leaf.keys[mid];
        if (midKey === key) {
          foundIdx = mid;
          break;
        } else if (midKey < key) {
          left = mid + 1;
        } else {
          right = mid - 1;
        }
      }

      if (foundIdx !== -1) {
        leaf.keys.splice(foundIdx, 1);
        leaf.vals.splice(foundIdx, 1);
        const buf = await this.pager.readPage(leaf.pageId);
        this.serializeNode(leaf, buf);
        await this.pager.writePage(leaf.pageId, buf);
      }
    }
  }
}

export interface TableData {
  columns: ColumnDef[];
  sequence?: number;
  firstPage: number;
  lastPage: number;
  indexRootPage?: number;
  comment?: string;
  pkColumn?: string | null;
  uniqueColumns?: string[];
  referencingColumns?: any[];
}

interface DbMetadata {
  name: string;
  nsp_f: number;
  nsp_l: number;
  cls_f: number;
  cls_l: number;
  att_f: number;
  att_l: number;
  dsc_f: number;
  dsc_l: number;
  ad_f: number;
  ad_l: number;
  idx_f: number;
  idx_l: number;
  nspIdx: number;
  clsIdx: number;
}

export class StorageEngine {
  private pager: Pager;
  public vfs: VFS;
  // Metadata caches shared across database contexts in this cluster
  private static dbMetaCache = new LRUCache<string, DbMetadata>(1000);
  private static tableCache = new LRUCache<string, TableData>(500);
  private static schemaCache = new Map<string, string[]>();
  private static pkIndexes = new LRUCache<string, BTree>(500);
  private tempTables = new Map<string, any[]>();
  private inTransaction = false;
  private txDbMetaBackup: string | null = null;
  private txClusterMetaBackup: string | null = null;

  private currentDbName?: string;
  private clusterInitialized = false;
  private dbMeta!: DbMetadata;
  private clusterCatalogDef!: TableData;
  private pgNamespaceDef!: TableData;
  private pgClassDef!: TableData;
  private pgAttributeDef!: TableData;
  private pgDescriptionDef!: TableData;
  private pgAttrdefDef!: TableData;
  private pgIndexDef!: TableData;

  // Static Column Definitions for catalogs
  private static readonly CLUSTER_CATALOG_COLS: ColumnDef[] = [
    { name: "name", dataType: "TEXT", isPrimaryKey: true },
    { name: "nsp_f", dataType: "NUMBER", isPrimaryKey: false },
    { name: "nsp_l", dataType: "NUMBER", isPrimaryKey: false },
    { name: "cls_f", dataType: "NUMBER", isPrimaryKey: false },
    { name: "cls_l", dataType: "NUMBER", isPrimaryKey: false },
    { name: "att_f", dataType: "NUMBER", isPrimaryKey: false },
    { name: "att_l", dataType: "NUMBER", isPrimaryKey: false },
    { name: "dsc_f", dataType: "NUMBER", isPrimaryKey: false },
    { name: "dsc_l", dataType: "NUMBER", isPrimaryKey: false },
    { name: "ad_f", dataType: "NUMBER", isPrimaryKey: false },
    { name: "ad_l", dataType: "NUMBER", isPrimaryKey: false },
    { name: "idx_f", dataType: "NUMBER", isPrimaryKey: false },
    { name: "idx_l", dataType: "NUMBER", isPrimaryKey: false },
    { name: "nspIdx", dataType: "NUMBER", isPrimaryKey: false },
    { name: "clsIdx", dataType: "NUMBER", isPrimaryKey: false },
  ];

  private static readonly PG_NAMESPACE_COLS: ColumnDef[] = [
    { name: "oid", dataType: "NUMBER", isPrimaryKey: true },
    { name: "nspname", dataType: "TEXT", isPrimaryKey: false, isUnique: true },
  ];

  private static readonly PG_CLASS_COLS: ColumnDef[] = [
    { name: "oid", dataType: "NUMBER", isPrimaryKey: true },
    { name: "relname", dataType: "TEXT", isPrimaryKey: false },
    { name: "relnamespace", dataType: "NUMBER", isPrimaryKey: false },
    { name: "relfirstpage", dataType: "NUMBER", isPrimaryKey: false },
    { name: "rellastpage", dataType: "NUMBER", isPrimaryKey: false },
    { name: "relindexroot", dataType: "NUMBER", isPrimaryKey: false },
    { name: "relsequence", dataType: "NUMBER", isPrimaryKey: false },
    { name: "relkind", dataType: "TEXT", isPrimaryKey: false },
  ];

  private static readonly PG_ATTRIBUTE_COLS: ColumnDef[] = [
    { name: "attrelid", dataType: "NUMBER", isPrimaryKey: false },
    { name: "attname", dataType: "TEXT", isPrimaryKey: false },
    { name: "atttypid", dataType: "TEXT", isPrimaryKey: false },
    { name: "attnum", dataType: "NUMBER", isPrimaryKey: false },
    { name: "attnotnull", dataType: "BOOLEAN", isPrimaryKey: false },
    { name: "attprimary", dataType: "BOOLEAN", isPrimaryKey: false },
    { name: "attunique", dataType: "BOOLEAN", isPrimaryKey: false },
    { name: "attref_table", dataType: "TEXT", isPrimaryKey: false },
    { name: "attref_col", dataType: "TEXT", isPrimaryKey: false },
    { name: "attref_on_delete", dataType: "TEXT", isPrimaryKey: false },
    { name: "attref_on_update", dataType: "TEXT", isPrimaryKey: false },
    { name: "attdef", dataType: "TEXT", isPrimaryKey: false },
    { name: "atttypmod", dataType: "NUMBER", isPrimaryKey: false },
    { name: "attisdropped", dataType: "BOOLEAN", isPrimaryKey: false },
  ];

  private static readonly PG_DESCRIPTION_COLS: ColumnDef[] = [
    { name: "objoid", dataType: "NUMBER", isPrimaryKey: false },
    { name: "objsubid", dataType: "NUMBER", isPrimaryKey: false },
    { name: "description", dataType: "TEXT", isPrimaryKey: false },
    { name: "objname", dataType: "TEXT", isPrimaryKey: false },
    { name: "column_name", dataType: "TEXT", isPrimaryKey: false },
  ];

  private static readonly PG_ATTRDEF_COLS: ColumnDef[] = [
    { name: "adrelid", dataType: "NUMBER", isPrimaryKey: false },
    { name: "adnum", dataType: "NUMBER", isPrimaryKey: false },
    { name: "adbin", dataType: "TEXT", isPrimaryKey: false },
  ];

  private static readonly PG_INDEX_COLS: ColumnDef[] = [
    { name: "indexrelid", dataType: "NUMBER", isPrimaryKey: true },
    { name: "indrelid", dataType: "NUMBER", isPrimaryKey: false },
    { name: "indkey", dataType: "JSON", isPrimaryKey: false },
    { name: "indisprimary", dataType: "BOOLEAN", isPrimaryKey: false },
    { name: "indisunique", dataType: "BOOLEAN", isPrimaryKey: false },
  ];

  constructor(
    vfs: VFS,
    public filepath: string,
  ) {
    this.vfs = vfs;
    this.pager = Pager.get(vfs, filepath);
  }

  private async initCluster() {
    if (this.clusterInitialized) return;
    await this.pager.init();

    // Page 0 acts as a Cluster Directory Pointer (supports 1M+ databases via B-Tree)
    const page0 = await this.pager.readPage(0);
    const magic = page0.toString("utf8", 0, 4);

    let clusterHeader;
    if (magic !== "LPGC") {
      // Bootstrap Cluster Catalog
      const dataPage = await this.pager.allocatePage();
      const indexPage = await this.pager.allocatePage();

      const buf = Buffer.alloc(PAGE_SIZE);
      buf.writeUInt8(1, 0);
      buf.writeUInt16LE(0, 1);
      buf.writeUInt32LE(0xffffffff, 3);
      await this.pager.writePage(indexPage, buf);

      clusterHeader = { f: dataPage, l: dataPage, idx: indexPage };
      const hBuf = Buffer.alloc(PAGE_SIZE);
      hBuf.write("LPGC", 0, 4, "utf8");
      hBuf.writeUInt32LE(clusterHeader.f, 4);
      hBuf.writeUInt32LE(clusterHeader.l, 8);
      hBuf.writeUInt32LE(clusterHeader.idx, 12);
      hBuf.writeUInt32LE(this.pager.firstFreePage, 16);
      await this.pager.writePage(0, hBuf);
    } else {
      clusterHeader = {
        f: page0.readUInt32LE(4),
        l: page0.readUInt32LE(8),
        idx: page0.readUInt32LE(12),
      };
    }

    this.clusterCatalogDef = {
      columns: StorageEngine.CLUSTER_CATALOG_COLS,
      firstPage: clusterHeader.f,
      lastPage: clusterHeader.l,
      sequence: 0,
      indexRootPage: clusterHeader.idx,
    };
    this.clusterInitialized = true;
  }

  public async init(dbName: string) {
    if (this.currentDbName === dbName) return;
    await this.initCluster();

    // Lookup specific database meta in Cluster Directory (O(log N))
    const cacheKey = `${this.filepath}:${dbName}`;
    let meta = StorageEngine.dbMetaCache.get(cacheKey) || null;

    if (!meta) {
      const btree = new BTree(
        this.pager,
        this.clusterCatalogDef.indexRootPage!,
      );
      const loc = await btree.get(dbName);

      if (loc) {
        const buf = await this.pager.readPage(loc.pageId);
        const page = new SlottedPage(buf);
        const t = page.getTuple(loc.slotIdx);
        if (t) {
          const resolved = this.isOverflow(t.data)
            ? await this.resolveOverflow(t.data)
            : t.data;
          meta = this.deserializeRow(
            StorageEngine.CLUSTER_CATALOG_COLS,
            resolved,
          );
          // Persist found meta to cache
          if (meta) StorageEngine.dbMetaCache.set(cacheKey, meta);
        }
      }
    }

    if (!meta) {
      // Bootstrap new database in this cluster
      const catalogPages = [];
      for (let i = 0; i < 8; i++) {
        const pid = await this.pager.allocatePage();
        const buf = await this.pager.readPage(pid);
        const page = new SlottedPage(buf);
        if (i >= 6) {
          // Index pages (nspIdx, clsIdx)
          page.buf.fill(0);
          page.buf.writeUInt8(1, 0);
          page.buf.writeUInt16LE(0, 1);
          page.buf.writeUInt32LE(0xffffffff, 3);
        }
        await this.pager.writePage(pid, page.buf);
        catalogPages.push(pid);
      }

      meta = {
        name: dbName,
        nsp_f: catalogPages[0]!,
        nsp_l: catalogPages[0]!,
        cls_f: catalogPages[1]!,
        cls_l: catalogPages[1]!,
        att_f: catalogPages[2]!,
        att_l: catalogPages[2]!,
        dsc_f: catalogPages[3]!,
        dsc_l: catalogPages[3]!,
        ad_f: catalogPages[4]!,
        ad_l: catalogPages[4]!,
        idx_f: catalogPages[5]!,
        idx_l: catalogPages[5]!,
        nspIdx: catalogPages[6]!,
        clsIdx: catalogPages[7]!,
      };

      await this.insertRowIntoCatalog(this.clusterCatalogDef, meta);

      // Initialize Catalog logic requires metadata
      this.dbMeta = meta;
      StorageEngine.dbMetaCache.set(cacheKey, meta);
      this.refreshCatalogDefs();

      // Initial schemas
      await this.insertRowIntoCatalog(this.pgNamespaceDef, {
        oid: 2200,
        nspname: "public",
      });
      await this.insertRowIntoCatalog(this.pgNamespaceDef, {
        oid: 11,
        nspname: "pg_catalog",
      });
      await this.insertRowIntoCatalog(this.pgNamespaceDef, {
        oid: 12345,
        nspname: "information_schema",
      });

      // Initial tables
      const systemTables = [
        { name: "pg_namespace", def: this.pgNamespaceDef, nsp: 11 },
        { name: "pg_class", def: this.pgClassDef, nsp: 11 },
        { name: "pg_attribute", def: this.pgAttributeDef, nsp: 11 },
        { name: "pg_description", def: this.pgDescriptionDef, nsp: 11 },
        { name: "pg_attrdef", def: this.pgAttrdefDef, nsp: 11 },
        { name: "pg_index", def: this.pgIndexDef, nsp: 11 },
      ];

      for (const sys of systemTables) {
        await this.insertRowIntoCatalog(this.pgClassDef, {
          oid: sys.def.firstPage,
          relname: sys.name,
          relnamespace: sys.nsp,
          relfirstpage: sys.def.firstPage,
          rellastpage: sys.def.lastPage,
          relindexroot: sys.def.indexRootPage,
          relsequence: sys.def.sequence,
          relkind: "r",
        });
        for (let i = 0; i < (sys.def.columns?.length || 0); i++) {
          const col = sys.def.columns?.[i]!;
          await this.insertRowIntoCatalog(this.pgAttributeDef, {
            attrelid: sys.def.firstPage,
            attname: col.name,
            atttypid: col.dataType,
            attnum: i + 1,
            attnotnull: !!col.isNotNull,
            attprimary: !!col.isPrimaryKey,
            attunique: !!col.isUnique,
            attref_table: col.references?.table || null,
            attref_col: col.references?.column || null,
            attref_on_delete: col.references?.onDelete || null,
            attref_on_update: col.references?.onUpdate || null,
            attdef: col.defaultVal ? JSON.stringify(col.defaultVal) : null,
            atttypmod: -1,
            attisdropped: false,
          });
        }
      }

      // Initial indexes in pg_index
      await this.insertRowIntoCatalog(this.pgIndexDef, {
        indexrelid: meta.nspIdx,
        indrelid: meta.nsp_f,
        indkey: [1], // oid
        indisprimary: true,
        indisunique: true,
      });
      await this.insertRowIntoCatalog(this.pgIndexDef, {
        indexrelid: meta.clsIdx,
        indrelid: meta.cls_f,
        indkey: [1], // oid
        indisprimary: true,
        indisunique: true,
      });

      await this.flush();
    } else {
      this.dbMeta = meta;
      this.refreshCatalogDefs();
    }
    this.currentDbName = dbName;
  }

  private refreshCatalogDefs() {
    this.pgNamespaceDef = {
      columns: StorageEngine.PG_NAMESPACE_COLS,
      firstPage: this.dbMeta.nsp_f,
      lastPage: this.dbMeta.nsp_l,
      sequence: 0,
      indexRootPage: this.dbMeta.nspIdx,
    };
    this.pgClassDef = {
      columns: StorageEngine.PG_CLASS_COLS,
      firstPage: this.dbMeta.cls_f,
      lastPage: this.dbMeta.cls_l,
      sequence: 0,
      indexRootPage: this.dbMeta.clsIdx,
    };
    this.pgAttributeDef = {
      columns: StorageEngine.PG_ATTRIBUTE_COLS,
      firstPage: this.dbMeta.att_f,
      lastPage: this.dbMeta.att_l,
      sequence: 0,
      indexRootPage: 0,
    };
    this.pgDescriptionDef = {
      columns: StorageEngine.PG_DESCRIPTION_COLS,
      firstPage: this.dbMeta.dsc_f,
      lastPage: this.dbMeta.dsc_l,
      sequence: 0,
      indexRootPage: 0,
    };
    this.pgAttrdefDef = {
      columns: StorageEngine.PG_ATTRDEF_COLS,
      firstPage: this.dbMeta.ad_f,
      lastPage: this.dbMeta.ad_l,
      sequence: 0,
      indexRootPage: 0,
    };
    this.pgIndexDef = {
      columns: StorageEngine.PG_INDEX_COLS,
      firstPage: this.dbMeta.idx_f,
      lastPage: this.dbMeta.idx_l,
      sequence: 0,
      indexRootPage: 0,
    };
  }

  private async insertRowIntoCatalog(
    table: TableData,
    row: any,
  ): Promise<{ pageId: number; slotIdx: number }> {
    let pageId = table.lastPage;
    let buf = await this.pager.readPage(pageId);
    let page = new SlottedPage(buf);
    const rawRowData = this.serializeRow(table.columns, row);
    const rowData = await this.handleOverflow(rawRowData);
    let slotIdx = page.insertTuple(rowData);

    if (slotIdx === -1) {
      const newPageId = await this.pager.allocatePage();
      const newBuf = await this.pager.readPage(newPageId);
      const newPage = new SlottedPage(newBuf);
      slotIdx = newPage.insertTuple(rowData);
      await this.pager.writePage(newPageId, newPage.buf);
      page.nextPageId = newPageId;
      await this.pager.writePage(pageId, page.buf);
      table.lastPage = newPageId;
      pageId = newPageId;

      // Update cluster meta if lastPage changed (Sync Page 0)
      if (
        this.clusterCatalogDef &&
        table.firstPage === this.clusterCatalogDef.firstPage
      ) {
        const p0 = await this.pager.readPage(0);
        p0.writeUInt32LE(table.lastPage, 8);
        await this.pager.writePage(0, p0);
      } else if (this.dbMeta) {
        if (table.firstPage === this.dbMeta.nsp_f) {
          this.dbMeta.nsp_l = newPageId;
          this.pgNamespaceDef.lastPage = newPageId;
        } else if (table.firstPage === this.dbMeta.cls_f) {
          this.dbMeta.cls_l = newPageId;
          this.pgClassDef.lastPage = newPageId;
        } else if (table.firstPage === this.dbMeta.att_f) {
          this.dbMeta.att_l = newPageId;
          this.pgAttributeDef.lastPage = newPageId;
        } else if (table.firstPage === this.dbMeta.dsc_f) {
          this.dbMeta.dsc_l = newPageId;
          this.pgDescriptionDef.lastPage = newPageId;
        } else if (table.firstPage === this.dbMeta.ad_f) {
          this.dbMeta.ad_l = newPageId;
          this.pgAttrdefDef.lastPage = newPageId;
        } else if (table.firstPage === this.dbMeta.idx_f) {
          this.dbMeta.idx_l = newPageId;
          this.pgIndexDef.lastPage = newPageId;
        }

        await this.updateRowsInCatalog(
          this.clusterCatalogDef,
          async (r) => r.name === this.dbMeta.name,
          async (r) => {
            r.nsp_l = this.dbMeta.nsp_l;
            r.cls_l = this.dbMeta.cls_l;
            r.att_l = this.dbMeta.att_l;
            r.dsc_l = this.dbMeta.dsc_l;
            r.ad_l = this.dbMeta.ad_l;
            r.idx_l = this.dbMeta.idx_l;
          },
        );
      }
    } else {
      await this.pager.writePage(pageId, page.buf);
    }

    // Maintain Global Metadata Indexes (O(log N) lookup)
    if (
      this.clusterCatalogDef &&
      table.firstPage === this.clusterCatalogDef.firstPage
    ) {
      const btree = new BTree(
        this.pager,
        this.clusterCatalogDef.indexRootPage!,
      );
      const newRoot = await btree.insert(row.name, { pageId, slotIdx });
      if (newRoot !== this.clusterCatalogDef.indexRootPage) {
        this.clusterCatalogDef.indexRootPage = newRoot;
        const p0 = await this.pager.readPage(0);
        p0.writeUInt32LE(newRoot, 12);
        await this.pager.writePage(0, p0);
      }
    } else if (this.dbMeta) {
      if (table.firstPage === this.dbMeta.nsp_f) {
        const btree = new BTree(this.pager, this.dbMeta.nspIdx);
        const newRoot = await btree.insert(row.nspname, { pageId, slotIdx });
        if (newRoot !== this.dbMeta.nspIdx) {
          this.dbMeta.nspIdx = newRoot;
          table.indexRootPage = newRoot;
          await this.updateRowsInCatalog(
            this.clusterCatalogDef,
            async (r) => r.name === this.dbMeta.name,
            async (r) => {
              r.nspIdx = newRoot;
            },
          );
        }
      } else if (this.dbMeta && table.firstPage === this.dbMeta.cls_f) {
        const btree = new BTree(this.pager, this.dbMeta.clsIdx);
        const key = `${row.relnamespace}:${row.relname}`;
        const newRoot = await btree.insert(key, { pageId, slotIdx });
        if (newRoot !== this.dbMeta.clsIdx) {
          this.dbMeta.clsIdx = newRoot;
          table.indexRootPage = newRoot;
          await this.updateRowsInCatalog(
            this.clusterCatalogDef,
            async (r) => r.name === this.dbMeta.name,
            async (r) => {
              r.clsIdx = newRoot;
            },
          );
        }
      }
    }
    return { pageId, slotIdx };
  }

  public getFullTableName(name: string): string {
    return name.includes(".") ? name : `public.${name}`;
  }

  public async createIndex(
    indexName: string,
    tableName: string,
    columns: string[],
    unique: boolean,
    ifNotExists: boolean,
  ): Promise<void> {
    const fullName = this.getFullTableName(tableName);
    const table = await this.getTableAsync(fullName);
    if (!table) throw new Error(`Table ${fullName} does not exist`);

    const idxFullName = this.getFullTableName(indexName);
    const parts = idxFullName.split(".");
    const schema = parts[0]!;
    const relname = parts[1]!;

    const nspOid = await this.getSchemaOid(schema);
    if (!nspOid) throw new Error(`Schema ${schema} does not exist`);

    let existing = false;
    for await (const row of this.scanCatalog(this.pgClassDef)) {
      if (row.relnamespace === nspOid && row.relname === relname) {
        existing = true;
        break;
      }
    }

    if (existing) {
      if (ifNotExists) return;
      throw new Error(`Relation ${idxFullName} already exists`);
    }

    const attNums = [];
    for (const col of columns) {
      let attrIdx = table.columns.findIndex((c) => c.name === col);
      if (attrIdx === -1) {
        const cleanCol = col.replace(/^"|"$/g, "").trim().toLowerCase();
        attrIdx = table.columns.findIndex(
          (c) => c.name.replace(/^"|"$/g, "").trim().toLowerCase() === cleanCol,
        );
      }
      if (attrIdx === -1) {
        const available = table.columns.map((c) => c.name).join(", ");
        throw new Error(
          `Column '${col}' does not exist in table ${fullName}. Available columns: [${available}]`,
        );
      }
      attNums.push(attrIdx + 1);
    }

    const rootPage = await this.pager.allocatePage();
    const rootBuf = Buffer.alloc(PAGE_SIZE);
    rootBuf.writeUInt8(1, 0);
    rootBuf.writeUInt16LE(0, 1);
    rootBuf.writeUInt32LE(0xffffffff, 3);
    await this.pager.writePage(rootPage, rootBuf);

    await this.insertRowIntoCatalog(this.pgClassDef, {
      oid: rootPage,
      relname: relname,
      relnamespace: nspOid,
      relfirstpage: rootPage,
      rellastpage: rootPage,
      relindexroot: rootPage,
      relsequence: 0,
      relkind: "i",
    });

    await this.insertRowIntoCatalog(this.pgIndexDef, {
      indexrelid: rootPage,
      indrelid: table.firstPage,
      indkey: attNums,
      indisprimary: false,
      indisunique: unique,
    });
  }

  public async createSchema(
    name: string,
    ifNotExists: boolean = false,
  ): Promise<void> {
    const schemas = await this.getSchemas();
    if (schemas.includes(name)) {
      if (ifNotExists) return;
      throw new Error(`Storage Error: Schema '${name}' already exists.`);
    }
    const maxOid = await this.getMaxOid("pg_namespace");
    await this.insertRowIntoCatalog(this.pgNamespaceDef, {
      oid: maxOid + 1,
      nspname: name,
    });
    StorageEngine.schemaCache.delete(`${this.filepath}:${this.currentDbName}`);
  }

  private async getSchemas(): Promise<string[]> {
    const key = `${this.filepath}:${this.currentDbName}`;
    let schemas = StorageEngine.schemaCache.get(key);
    if (schemas) return schemas;
    schemas = [];
    for await (const row of this.scanCatalog(this.pgNamespaceDef)) {
      schemas.push(row.nspname);
    }
    StorageEngine.schemaCache.set(key, schemas);
    return schemas;
  }

  private async getMaxOid(catalog: string): Promise<number> {
    let max = 0;
    const def =
      catalog === "pg_namespace" ? this.pgNamespaceDef : this.pgClassDef;
    for await (const row of this.scanCatalog(def)) {
      if (row.oid > max) max = row.oid;
    }
    return max;
  }

  public async dropSchema(
    name: string,
    ifExists: boolean = false,
    cascade: boolean = false,
  ): Promise<void> {
    const schemas = await this.getSchemas();
    if (!schemas.includes(name)) {
      if (ifExists) return;
      throw new Error(`Storage Error: Schema '${name}' does not exist.`);
    }
    if (["public", "pg_catalog", "information_schema"].includes(name)) {
      throw new Error(`Storage Error: Cannot drop system schema '${name}'.`);
    }

    const nspOid = await this.getSchemaOid(name);
    let hasTables = false;
    for await (const rel of this.scanCatalog(this.pgClassDef)) {
      if (rel.relnamespace === nspOid) {
        if (!cascade)
          throw new Error(`Storage Error: Schema '${name}' is not empty.`);
        hasTables = true;
        await this.dropTable(name + "." + rel.relname);
      }
    }

    await this.deleteRowsInCatalog(
      this.pgNamespaceDef,
      async (r) => r.nspname === name,
    );
    if (this.dbMeta) {
      const btree = new BTree(this.pager, this.dbMeta.nspIdx);
      await btree.delete(name);
    }
    StorageEngine.schemaCache.delete(`${this.filepath}:${this.currentDbName}`);
  }

  private async getSchemaOid(name: string): Promise<number | null> {
    for await (const row of this.scanCatalog(this.pgNamespaceDef)) {
      if (row.nspname === name) return row.oid;
    }
    return null;
  }

  public async renameTable(oldName: string, newName: string): Promise<void> {
    const fullOldName = this.getFullTableName(oldName);
    const fullNewName = this.getFullTableName(newName);

    const oldTable = await this.getTableAsync(fullOldName);
    if (!oldTable) throw new Error(`Table ${fullOldName} does not exist`);

    const partsOld = fullOldName.split(".");
    const oldSchema = partsOld[0]!;
    const oldRelName = partsOld[1]!;
    const oldNspOid = await this.getSchemaOid(oldSchema);

    const parts = fullNewName.split(".");
    const newSchema = parts[0]!;
    const newRelName = parts[1]!;
    const newNspOid = await this.getSchemaOid(newSchema);
    if (!newNspOid) throw new Error(`Schema ${newSchema} does not exist`);

    let location: { pageId: number; slotIdx: number } | null = null;
    await this.updateRowsInCatalog(
      this.pgClassDef,
      async (r) => r.oid === oldTable.firstPage,
      async (r, loc) => {
        r.relname = newRelName;
        r.relnamespace = newNspOid;
        if (loc) location = loc;
      },
    );

    if (location) {
      const btree = new BTree(this.pager, this.dbMeta.clsIdx);
      await btree.delete(`${oldNspOid}:${oldRelName}`);
      const newRoot = await btree.insert(
        `${newNspOid}:${newRelName}`,
        location,
      );
      if (newRoot !== this.dbMeta.clsIdx) {
        this.dbMeta.clsIdx = newRoot;
        this.pgClassDef.indexRootPage = newRoot;
        await this.updateRowsInCatalog(
          this.clusterCatalogDef,
          async (r) => r.name === this.dbMeta.name,
          async (r) => {
            r.clsIdx = newRoot;
          },
        );
      }
    }

    StorageEngine.tableCache.delete(`${this.filepath}:${this.currentDbName}:${fullOldName}`);
    StorageEngine.tableCache.delete(`${this.filepath}:${this.currentDbName}:${fullNewName}`);
  }

  public async dropIndex(
    indexName: string,
    ifExists: boolean = false,
    cascade: boolean = false,
  ): Promise<void> {
    const idxFullName = this.getFullTableName(indexName);
    const parts = idxFullName.split(".");
    const schema = parts[0]!;
    const relname = parts[1]!;

    const nspOid = await this.getSchemaOid(schema);
    if (!nspOid) {
      if (ifExists) return;
      throw new Error(`Schema ${schema} does not exist`);
    }

    let indexOid = null;
    for await (const row of this.scanCatalog(this.pgClassDef)) {
      if (
        row.relnamespace === nspOid &&
        row.relname === relname &&
        row.relkind === "i"
      ) {
        indexOid = row.oid;
        break;
      }
    }

    if (!indexOid) {
      if (ifExists) return;
      throw new Error(`Index ${idxFullName} does not exist`);
    }

    if (indexOid && indexOid !== 0 && indexOid !== 0xffffffff) {
      await this.pager.freePage(indexOid);
    }

    await this.deleteRowsInCatalog(
      this.pgClassDef,
      async (r) => r.oid === indexOid,
    );
    await this.deleteRowsInCatalog(
      this.pgIndexDef,
      async (r) => r.indexrelid === indexOid,
    );
  }

  public async dropTable(
    name: string,
    ifExists: boolean = false,
    cascade: boolean = false,
  ): Promise<void> {
    const fullName = this.getFullTableName(name);
    const table = await this.getTableAsync(fullName);
    if (!table) {
      if (ifExists) return;
      throw new Error(`Table ${fullName} not found`);
    }

    const referencing = await this.getReferencingColumnsInternal(fullName);
    if (referencing.length > 0 && !cascade) {
      const dependentTables = Array.from(new Set(referencing.map(r => r.childTable))).join(', ');
      const errorDetails = [
        `Foreign Key Violation (Drop Table): cannot drop table "${fullName}" because other objects depend on it.`,
        `- Target Table: "${fullName}"`,
        `- Dependent Tables: ${dependentTables}`,
        `Reason: You must use CASCADE to drop the table and automatically drop the dependent foreign key constraints.`
      ].join('\n');
      throw new Error(errorDetails);
    }

    // Drop referencing constraints if cascade is true
    if (cascade) {
      for (const ref of referencing) {
        const childTable = await this.getTableAsync(ref.childTable);
        if (childTable) {
          const col = childTable.columns.find(
            (c) => c.name === ref.childColumn,
          );
          if (col && col.references) {
            col.references = undefined;
            await this.updateTableSchema(ref.childTable, childTable);
            await this.updateRowsInCatalog(
              this.pgAttributeDef,
              async (r) =>
                r.attrelid === childTable.firstPage &&
                r.attname === ref.childColumn,
              async (r) => {
                r.attref_table = null;
                r.attref_col = null;
                r.attref_on_delete = null;
                r.attref_on_update = null;
              },
            );
          }
        }
      }
    }

    // Free data pages and overflow pages
    let pId = table.firstPage;
    while (pId !== 0xffffffff && pId !== 0) {
      const buf = await this.pager.readPage(pId);
      const page = new SlottedPage(buf);
      for (const t of page.getTuples()) {
        if (this.isOverflow(t.data)) {
          await this.freeOverflowPages(t.data);
        }
      }
      const nextId = page.nextPageId;
      await this.pager.freePage(pId);
      pId = nextId;
    }

    // Free B-Tree pages for PK if any
    if (table.indexRootPage && table.indexRootPage !== 0 && table.indexRootPage !== 0xffffffff) {
      await this.pager.freePage(table.indexRootPage);
    }

    await this.deleteRowsInCatalog(
      this.pgClassDef,
      async (r) => r.oid === table.firstPage,
    );
    await this.deleteRowsInCatalog(
      this.pgAttributeDef,
      async (r) => r.attrelid === table.firstPage,
    );
    if (this.dbMeta) {
      const btree = new BTree(this.pager, this.dbMeta.clsIdx);
      const parts = fullName.split(".");
      const nspOid = await this.getSchemaOid(parts[0]!);
      await btree.delete(`${nspOid}:${parts[1]!}`);
    }
    StorageEngine.tableCache.delete(`${this.filepath}:${this.currentDbName}:${fullName}`);
    StorageEngine.pkIndexes.delete(`${this.filepath}:${this.currentDbName}:${fullName}`);
  }

  public getTable(name: string): TableData {
    // Synchronous access required by executor for some logic
    const fullName = this.getFullTableName(name);
    const key = `${this.filepath}:${this.currentDbName}:${fullName}`;
    const cached = StorageEngine.tableCache.get(key);
    if (cached) return cached;
    throw new Error(
      `Metadata error: Table definition for ${fullName} not in cache. Ensure it was fetched first.`,
    );
  }

  public async getTableAsync(name: string): Promise<TableData | null> {
    let fullName = name.includes(".") ? name : `public.${name}`;
    const key = `${this.filepath}:${this.currentDbName}:${fullName}`;
    const cached = StorageEngine.tableCache.get(key);
    if (cached) return cached;

    const loadTable = async (schema: string, table: string) => {
      const nspOid = await this.getSchemaOid(schema);
      if (!nspOid) return null;

      // Global Catalog Index Lookup: O(log N) vs O(N) scan
      const btree = new BTree(this.pager, this.dbMeta.clsIdx);
      const loc = await btree.get(`${nspOid}:${table}`);

      if (!loc) return null;
      const buf = await this.pager.readPage(loc.pageId);
      const page = new SlottedPage(buf);
      let relRow = null;
      const t = page.getTuple(loc.slotIdx);
      if (t) {
        const resolved = this.isOverflow(t.data)
          ? await this.resolveOverflow(t.data)
          : t.data;
        relRow = this.deserializeRow(StorageEngine.PG_CLASS_COLS, resolved);
      }

      if (!relRow || relRow.relname !== table || relRow.relnamespace !== nspOid)
        return null;

      const attrs = [];
      for await (const attr of this.scanCatalog(this.pgAttributeDef)) {
        if (attr.attrelid === relRow.oid) {
          attrs.push(attr);
        }
      }
      attrs.sort((a, b) => a.attnum - b.attnum);

      let pkColumn: string | null = null;
      const uniqueColumns: string[] = [];

      const columns: ColumnDef[] = attrs.map((attr) => {
        if (attr.attprimary) pkColumn = attr.attname;
        if (attr.attunique) uniqueColumns.push(attr.attname);

        return {
          name: attr.attname,
          dataType: attr.atttypid,
          isPrimaryKey: !!attr.attprimary,
          isUnique: !!attr.attunique,
          isNotNull: !!attr.attnotnull,
          references: attr.attref_table
            ? {
                table: attr.attref_table,
                column: attr.attref_col,
                onDelete: attr.attref_on_delete || undefined,
                onUpdate: attr.attref_on_update || undefined,
              }
            : undefined,
          defaultVal: attr.attdef
            ? (() => {
                try {
                  const p = JSON.parse(attr.attdef);
                  return p?.__generated__ ? undefined : p;
                } catch {
                  return undefined;
                }
              })()
            : undefined,
          generatedExpr: attr.attdef
            ? (() => {
                try {
                  const p = JSON.parse(attr.attdef);
                  return p?.__generated__ ? p.expr : undefined;
                } catch {
                  return undefined;
                }
              })()
            : undefined,
        };
      });

      const fullName = `${schema}.${table}`;
      const referencingColumns =
        await this.getReferencingColumnsInternal(fullName);

      const data: TableData = {
        columns,
        firstPage: relRow.relfirstpage,
        lastPage: relRow.rellastpage,
        sequence: relRow.relsequence,
        indexRootPage:
          relRow.relindexroot === 0xffffffff ? 0 : relRow.relindexroot,
        pkColumn,
        uniqueColumns,
        referencingColumns,
      };

      if (this.dbMeta) {
        if (relRow.oid === this.dbMeta.nsp_f) {
          data.lastPage = this.dbMeta.nsp_l;
          data.indexRootPage = this.dbMeta.nspIdx;
        } else if (relRow.oid === this.dbMeta.cls_f) {
          data.lastPage = this.dbMeta.cls_l;
          data.indexRootPage = this.dbMeta.clsIdx;
        } else if (relRow.oid === this.dbMeta.att_f) {
          data.lastPage = this.dbMeta.att_l;
        } else if (relRow.oid === this.dbMeta.dsc_f) {
          data.lastPage = this.dbMeta.dsc_l;
        } else if (relRow.oid === this.dbMeta.ad_f) {
          data.lastPage = this.dbMeta.ad_l;
        } else if (relRow.oid === this.dbMeta.idx_f) {
          data.lastPage = this.dbMeta.idx_l;
        }
      } else if (
        this.clusterCatalogDef &&
        relRow.oid === this.clusterCatalogDef.firstPage
      ) {
        data.lastPage = this.clusterCatalogDef.lastPage;
        data.indexRootPage = this.clusterCatalogDef.indexRootPage;
      }

      StorageEngine.tableCache.set(`${this.filepath}:${this.currentDbName}:${fullName}`, data);
      return data;
    };

    const parts = fullName.split(".");
    let res = await loadTable(parts[0]!, parts[1]!);
    if (!res && !name.includes(".")) {
      res = await loadTable("pg_catalog", name);
    }
    return res;
  }

  public async createTable(
    name: string,
    columns: ColumnDef[],
    ifNotExists: boolean = false,
  ): Promise<void> {
    const fullName = this.getFullTableName(name);
    const existing = await this.getTableAsync(fullName);
    if (existing) {
      if (ifNotExists) return;
      throw new Error(`Table ${fullName} already exists`);
    }

    const parts = fullName.split(".");
    const schema = parts[0]!;
    const tableName = parts[1]!;
    const nspOid = await this.getSchemaOid(schema);
    if (!nspOid) throw new Error(`Schema ${schema} does not exist`);

    const firstPage = await this.pager.allocatePage();
    await this.insertRowIntoCatalog(this.pgClassDef, {
      oid: firstPage,
      relname: tableName,
      relnamespace: nspOid,
      relfirstpage: firstPage,
      rellastpage: firstPage,
      relindexroot: 0,
      relsequence: 0,
      relkind: "r",
    });

    const pkAttNums: number[] = [];
    const uniqueAttNums: Map<string, number[]> = new Map();

    for (let i = 0; i < columns.length; i++) {
      const col = columns[i]!;
      if (col.references) {
        this.invalidateTableCache(col.references.table);
      }
      if (col.isPrimaryKey) pkAttNums.push(i + 1);
      if (col.isUnique && !col.isPrimaryKey) {
        uniqueAttNums.set(`${tableName}_${col.name}_key`, [i + 1]);
      }

      await this.insertRowIntoCatalog(this.pgAttributeDef, {
        attrelid: firstPage,
        attname: col.name,
        atttypid: col.dataType,
        attnum: i + 1,
        attnotnull: !!col.isNotNull,
        attprimary: !!col.isPrimaryKey,
        attunique: !!col.isUnique,
        attref_table: col.references?.table || null,
        attref_col: col.references?.column || null,
        attref_on_delete: col.references?.onDelete || null,
        attref_on_update: col.references?.onUpdate || null,
        attdef: col.defaultVal
          ? JSON.stringify(col.defaultVal)
          : col.generatedExpr
            ? JSON.stringify({ __generated__: true, expr: col.generatedExpr })
            : null,
        atttypmod: -1,
        attisdropped: false,
      });
      if (col.defaultVal || col.generatedExpr) {
        await this.insertRowIntoCatalog(this.pgAttrdefDef, {
          adrelid: firstPage,
          adnum: i + 1,
          adbin: col.generatedExpr
            ? JSON.stringify({ __generated__: true, expr: col.generatedExpr })
            : JSON.stringify(col.defaultVal),
        });
      }
    }

    // Register PK/Unique in pg_index if exists
    if (pkAttNums.length > 0) {
      const rootPage = await this.pager.allocatePage();
      const rootBuf = Buffer.alloc(PAGE_SIZE);
      rootBuf.writeUInt8(1, 0);
      rootBuf.writeUInt16LE(0, 1);
      rootBuf.writeUInt32LE(0xffffffff, 3);
      await this.pager.writePage(rootPage, rootBuf);

      await this.insertRowIntoCatalog(this.pgIndexDef, {
        indexrelid: rootPage,
        indrelid: firstPage,
        indkey: pkAttNums,
        indisprimary: true,
        indisunique: true,
      });

      await this.updateRowsInCatalog(
        this.pgClassDef,
        async (r) => r.oid === firstPage,
        async (r) => {
          r.relindexroot = rootPage;
        },
      );
    }

    for (const attNums of uniqueAttNums.values()) {
      const rootPage = await this.pager.allocatePage();
      const rootBuf = Buffer.alloc(PAGE_SIZE);
      rootBuf.writeUInt8(1, 0);
      rootBuf.writeUInt16LE(0, 1);
      rootBuf.writeUInt32LE(0xffffffff, 3);
      await this.pager.writePage(rootPage, rootBuf);

      await this.insertRowIntoCatalog(this.pgIndexDef, {
        indexrelid: rootPage,
        indrelid: firstPage,
        indkey: attNums,
        indisprimary: false,
        indisunique: true,
      });
    }

    await this.getTableAsync(fullName); // Populate cache
  }

  public invalidateTableCache(name: string) {
    const fullName = this.getFullTableName(name);
    StorageEngine.tableCache.delete(`${this.filepath}:${this.currentDbName}:${fullName}`);
  }

  public async updateTableSchema(name: string, tableData: TableData) {
    const info = await this.getTableAsync(name);
    const isCatalog =
      tableData.firstPage === this.dbMeta.nsp_f ||
      tableData.firstPage === this.dbMeta.cls_f ||
      tableData.firstPage === this.dbMeta.att_f ||
      tableData.firstPage === this.dbMeta.dsc_f ||
      tableData.firstPage === this.dbMeta.ad_f;
    const fullName =
      info && isCatalog
        ? `pg_catalog.${name.split(".").pop()}`
        : this.getFullTableName(name);

    await this.updateRowsInCatalog(
      this.pgClassDef,
      async (r) => r.oid === tableData.firstPage,
      async (r) => {
        r.rellastpage = tableData.lastPage;
        r.relindexroot = tableData.indexRootPage || 0;
        r.relsequence = tableData.sequence;
      },
    );
    StorageEngine.tableCache.set(`${this.filepath}:${this.currentDbName}:${fullName}`, tableData);
  }

  private async *scanCatalog(def: TableData): AsyncIterableIterator<any> {
    let pageId = def.firstPage;
    while (pageId !== 0xffffffff && pageId !== 0) {
      const buf = await this.pager.readPage(pageId);
      const page = new SlottedPage(buf);
      for (const tuple of page.getTuples()) {
        const resolved = this.isOverflow(tuple.data)
          ? await this.resolveOverflow(tuple.data)
          : tuple.data;
        yield this.deserializeRow(def.columns, resolved);
      }
      pageId = page.nextPageId;
    }
  }

  private async deleteRowsInCatalog(
    def: TableData,
    filter: (r: any) => Promise<boolean>,
  ) {
    let pageId = def.firstPage;
    while (pageId !== 0xffffffff && pageId !== 0) {
      const buf = await this.pager.readPage(pageId);
      const page = new SlottedPage(buf);
      let mod = false;
      for (const tuple of page.getTuples()) {
        const resolved = this.isOverflow(tuple.data)
          ? await this.resolveOverflow(tuple.data)
          : tuple.data;
        if (await filter(this.deserializeRow(def.columns, resolved))) {
          if (this.isOverflow(tuple.data)) {
            await this.freeOverflowPages(tuple.data);
          }
          page.deleteTuple(tuple.slotIdx);
          mod = true;
        }
      }
      if (mod) await this.pager.writePage(pageId, page.buf);
      pageId = page.nextPageId;
    }
  }

  private async updateRowsInCatalog(
    def: TableData,
    filter: (r: any) => Promise<boolean>,
    update: (
      r: any,
      loc?: { pageId: number; slotIdx: number },
    ) => Promise<void>,
  ) {
    const pagesToVisit = [];
    let pId = def.firstPage;
    while (pId !== 0xffffffff && pId !== 0) {
      pagesToVisit.push(pId);
      const buf = await this.pager.readPage(pId);
      pId = new SlottedPage(buf).nextPageId;
    }

    for (const pageId of pagesToVisit) {
      const buf = await this.pager.readPage(pageId);
      const page = new SlottedPage(buf);
      let mod = false;
      for (const tuple of page.getTuples()) {
        const resolved = this.isOverflow(tuple.data)
          ? await this.resolveOverflow(tuple.data)
          : tuple.data;
        const row = this.deserializeRow(def.columns, resolved);
        if (await filter(row)) {
          const locObj = { pageId, slotIdx: tuple.slotIdx };
          await update(row, locObj);
          const rawRowData = this.serializeRow(def.columns, row);
          const rowData = await this.handleOverflow(rawRowData);
          
          const oldTupleData = Buffer.allocUnsafe(tuple.data.length);
          tuple.data.copy(oldTupleData);

          if (!page.updateTuple(tuple.slotIdx, rowData)) {
            if (this.isOverflow(oldTupleData)) {
              await this.freeOverflowPages(oldTupleData);
            }
            page.deleteTuple(tuple.slotIdx);
            const newLoc = await this.insertRowIntoCatalog(def, row);
            locObj.pageId = newLoc.pageId;
            locObj.slotIdx = newLoc.slotIdx;
          } else {
            if (this.isOverflow(oldTupleData)) {
              await this.freeOverflowPages(oldTupleData);
            }
          }
          mod = true;
        }
      }
      if (mod) await this.pager.writePage(pageId, page.buf);
    }
  }

  public createTempTable(name: string, rows: any[]): void {
    this.tempTables.set(name, rows);
  }
  public dropTempTable(name: string): void {
    this.tempTables.delete(name);
  }

  private static readonly NUMERIC_TYPES = new Set([
    "SERIAL",
    "NUMBER",
    "INT",
    "INTEGER",
    "SMALLINT",
    "BIGINT",
    "DECIMAL",
    "NUMERIC",
    "REAL",
    "DOUBLE PRECISION",
    "DOUBLE",
    "PRECISION",
    "SMALLSERIAL",
    "BIGSERIAL",
    "MONEY",
    "OID",
    "REGCLASS",
    "REGTYPE",
    "INT2",
    "INT4",
    "INT8",
    "FLOAT4",
    "FLOAT8",
  ]);

  private static numericTypeCache = new Map<string, boolean>();

  private isNumericType(dt: string): boolean {
    if (!dt) return false;
    let cached = StorageEngine.numericTypeCache.get(dt);
    if (cached !== undefined) return cached;

    let t = dt.toUpperCase();
    const idx = t.indexOf("(");
    if (idx !== -1) t = t.substring(0, idx);
    const result = StorageEngine.NUMERIC_TYPES.has(t.trim());
    StorageEngine.numericTypeCache.set(dt, result);
    return result;
  }

  private async handleOverflow(data: Buffer): Promise<Buffer> {
    if (data.length <= 4000) {
      // Return a copy to prevent corruption if the source buffer is reused
      const copy = Buffer.allocUnsafe(data.length);
      data.copy(copy);
      return copy;
    }

    let currDataOffset = 0;
    let firstPageId = -1;
    let prevPageId = -1;

    while (currDataOffset < data.length) {
      const pageId = await this.pager.allocatePage();
      if (firstPageId === -1) firstPageId = pageId;

      if (prevPageId !== -1) {
        const prevBuf = await this.pager.readPage(prevPageId);
        prevBuf.writeUInt32LE(pageId, 0);
        await this.pager.writePage(prevPageId, prevBuf);
      }

      const chunkLen = Math.min(data.length - currDataOffset, PAGE_SIZE - 4);
      const buf = Buffer.alloc(PAGE_SIZE);
      buf.writeUInt32LE(0xffffffff, 0);
      data.copy(buf, 4, currDataOffset, currDataOffset + chunkLen);
      await this.pager.writePage(pageId, buf);

      currDataOffset += chunkLen;
      prevPageId = pageId;
    }

    const ptrBuf = Buffer.alloc(10);
    ptrBuf.writeUInt16LE(data.readUInt16LE(0) | 0x8000, 0);
    ptrBuf.writeUInt32LE(firstPageId, 2);
    ptrBuf.writeUInt32LE(data.length, 6);
    return ptrBuf;
  }

  private isOverflow(data: Buffer): boolean {
    return data.length === 10 && (data.readUInt16LE(0) & 0x8000) !== 0;
  }

  private async freeOverflowPages(data: Buffer): Promise<void> {
    if (!this.isOverflow(data)) return;
    const firstPageId = data.readUInt32LE(2);
    let currPageId = firstPageId;
    while (currPageId !== 0xffffffff && currPageId !== 0) {
      const buf = await this.pager.readPage(currPageId);
      const nextId = buf.readUInt32LE(0);
      await this.pager.freePage(currPageId);
      currPageId = nextId;
    }
  }

  private async resolveOverflow(data: Buffer): Promise<Buffer> {
    const firstPageId = data.readUInt32LE(2);
    const totalLen = data.readUInt32LE(6);
    const fullData = Buffer.allocUnsafe(totalLen);
    let currPageId = firstPageId;
    let offset = 0;
    while (currPageId !== 0xffffffff && currPageId !== 0) {
      const buf = await this.pager.readPage(currPageId);
      const nextId = buf.readUInt32LE(0);
      const chunkLen = Math.min(totalLen - offset, PAGE_SIZE - 4);
      buf.copy(fullData, offset, 4, 4 + chunkLen);
      offset += chunkLen;
      currPageId = nextId;
    }
    return fullData;
  }

  private serializeRow(columns: ColumnDef[], row: any): Buffer {
    const colLen = columns.length;
    const nullBitmapLen = (colLen + 7) >> 3;

    let payloadSize = 0;
    const cache = new Array(colLen);

    for (let i = 0; i < colLen; i++) {
      const col = columns[i];
      const val = row[col!.name];
      if (val === null || val === undefined) continue;

      let isNum = col!._isNumeric;
      let isBool = col!._isBool;
      if (isNum === undefined) {
        const dt = col!.dataType.toUpperCase();
        col!._isNumeric = isNum = this.isNumericType(dt);
        col!._isBool = isBool = dt.startsWith("BOOL");
        col!._isJson = dt.includes("JSON") || dt.endsWith("[]");
      }

      if (isNum) {
        payloadSize += 8;
        cache[i] = 1;
      } else if (isBool) {
        payloadSize += 1;
        cache[i] = 2;
      } else {
        let str;
        if (val instanceof Date) {
          str = val.toISOString();
        } else {
          str = typeof val === "object" ? JSON.stringify(val) : String(val);
        }
        const byteLen = Buffer.byteLength(str);
        payloadSize += 4 + byteLen;
        cache[i] = str;
      }
    }

    const totalSize = 2 + nullBitmapLen + payloadSize;
    // Do not use a static scratch buffer to avoid concurrency corruption
    const target = Buffer.allocUnsafe(totalSize);

    target.writeUInt16LE(colLen, 0);
    const nullBitmapOffset = 2;
    target.fill(0, nullBitmapOffset, nullBitmapOffset + nullBitmapLen);

    let offset = nullBitmapOffset + nullBitmapLen;
    for (let i = 0; i < colLen; i++) {
      const col = columns[i];
      const typeInfo = cache[i];

      if (typeInfo === undefined) {
        (target as any)[nullBitmapOffset + (i >> 3)] |= 1 << (i & 7);
        continue;
      }

      if (typeInfo === 1) {
        // Numeric
        target.writeDoubleLE(Number(row[col!.name]), offset);
        offset += 8;
      } else if (typeInfo === 2) {
        // Boolean
        target[offset] = row[col!.name] ? 1 : 0;
        offset += 1;
      } else {
        // String / JSON
        const bytesWritten = target.write(
          typeInfo,
          offset + 4,
          target.length - (offset + 4),
          "utf-8",
        );
        target.writeUInt32LE(bytesWritten, offset);
        offset += 4 + bytesWritten;
      }
    }

    // Return a subarray (view) of the scratch buffer.
    // This is safe because SlottedPage.insertTuple/updateTuple performs a Buffer.copy immediately.
    return target.subarray(0, totalSize);
  }

  private deserializeRow(columns: ColumnDef[], buf: Buffer): any {
    if (!buf || buf.length < 2) return {};

    const storedColCount = buf.readUInt16LE(0);
    const nullBitmapLen = (storedColCount + 7) >> 3;
    const row: any = {};

    let offset = 2 + nullBitmapLen;
    const maxIdx = Math.min(storedColCount, columns.length);

    for (let i = 0; i < maxIdx; i++) {
      const col = columns[i];
      // Fast bitwise null check
      if ((buf[2 + (i >> 3)] & (1 << (i & 7))) !== 0) {
        row[col!.name] = null;
        continue;
      }

      let isNum = col!._isNumeric;
      let isBool = col!._isBool;
      let isJson = col!._isJson;
      if (isNum === undefined) {
        const dt = col!.dataType.toUpperCase();
        col!._isNumeric = isNum = this.isNumericType(dt);
        col!._isBool = isBool = dt.startsWith("BOOL");
        col!._isJson = isJson = dt.includes("JSON") || dt.endsWith("[]");
      }

      if (isNum) {
        row[col!.name] = buf.readDoubleLE(offset);
        offset += 8;
      } else if (isBool) {
        row[col!.name] = buf[offset] === 1;
        offset += 1;
      } else {
        const len = buf.readUInt32LE(offset);
        offset += 4;
        const str = buf.toString("utf-8", offset, offset + len);
        offset += len;

        // Optimization: Lazy check for JSON structure
        if (col!._isJson && (str[0] === "{" || str[0] === "[")) {
          try {
            row[col!.name] = JSON.parse(str);
          } catch {
            row[col!.name] = str;
          }
        } else {
          row[col!.name] = str;
        }
      }
    }

    for (let i = maxIdx; i < columns.length; i++) {
      if (columns[i] && columns[i]?.name) {
        row[columns[i]!.name] = null;
      }
    }

    return row;
  }

  public async truncateTable(
    name: string,
    cascade: boolean = false,
    restartIdentity: boolean = false,
    visited: Set<string> = new Set(),
    tablesInStmt: Set<string> = new Set(),
    ignoreFkChecks: boolean = false,
  ) {
    const fullName = this.getFullTableName(name);
    if (visited.has(fullName)) return;
    visited.add(fullName);

    const table = await this.getTableAsync(fullName);
    if (!table) throw new Error(`Table ${fullName} not found`);

    if (!ignoreFkChecks) {
      const referencing = await this.getReferencingColumnsInternal(fullName);
      if (cascade) {
        for (const ref of referencing) {
          await this.truncateTable(
            ref.childTable,
            true,
            restartIdentity,
            visited,
            tablesInStmt,
            false
          );
        }
      } else {
        for (const ref of referencing) {
          if (!tablesInStmt.has(ref.childTable) && !visited.has(ref.childTable)) {
            const errorDetails = [
              `Foreign Key Violation (Truncate): Cannot truncate table "${fullName}".`,
              `- Target Table: "${fullName}" (Parent)`,
              `- Dependent Table: "${ref.childTable}" (Child)`,
              `- Dependent Column: "${ref.childColumn}"`,
              `Reason: The table is referenced by a foreign key constraint from table "${ref.childTable}". You must either truncate both tables simultaneously or use CASCADE.`
            ].join('\n');
            throw new Error(errorDetails);
          }
        }
      }
    }

    // Free old data pages and overflow pages
    let pId = table.firstPage;
    const pagesToFree: number[] = [];
    while (pId !== 0xffffffff && pId !== 0) {
      const buf = await this.pager.readPage(pId);
      const page = new SlottedPage(buf);
      for (const t of page.getTuples()) {
        if (this.isOverflow(t.data)) {
          await this.freeOverflowPages(t.data);
        }
      }
      if (pId !== table.firstPage) pagesToFree.push(pId);
      pId = page.nextPageId;
    }

    for (const freeId of pagesToFree) {
      await this.pager.freePage(freeId);
    }

    const firstPageBuf = Buffer.alloc(PAGE_SIZE);
    firstPageBuf.writeUInt32LE(0xffffffff, 0);
    firstPageBuf.writeUInt16LE(0, 4);
    firstPageBuf.writeUInt16LE(PAGE_SIZE, 6);
    await this.pager.writePage(table.firstPage, firstPageBuf);

    table.lastPage = table.firstPage;
    if (restartIdentity) table.sequence = 0;

    await this.updateTableSchema(fullName, table);

    if (
      table.indexRootPage &&
      table.indexRootPage !== 0 &&
      table.indexRootPage !== 0xffffffff
    ) {
      const rootBuf = Buffer.alloc(PAGE_SIZE);
      rootBuf.writeUInt8(1, 0);
      rootBuf.writeUInt16LE(0, 1);
      rootBuf.writeUInt32LE(0xffffffff, 3);
      await this.pager.writePage(table.indexRootPage, rootBuf);
      StorageEngine.pkIndexes.delete(`${this.filepath}:${this.currentDbName}:${fullName}`);
    }
  }

  public async getPKColumn(name: string): Promise<string | null> {
    const table = await this.getTableAsync(name);
    return table?.pkColumn || null;
  }

  /**
   * Internal method to scan catalogs for referencing columns without triggering recursion via getTableAsync.
   */
  private async getReferencingColumnsInternal(targetTableName: string) {
    const targetTableParts = targetTableName.split(".");
    const targetBaseName = targetTableParts.pop()!;

    const refs = [];

    // Map OIDs to schema names and table names to avoid nested scans in the loop
    const nspMap = new Map<number, string>();
    for await (const nsp of this.scanCatalog(this.pgNamespaceDef))
      nspMap.set(nsp.oid, nsp.nspname);

    const clsMap = new Map<number, { name: string; nsp: string }>();
    for await (const cls of this.scanCatalog(this.pgClassDef)) {
      clsMap.set(cls.oid, {
        name: cls.relname,
        nsp: nspMap.get(cls.relnamespace) || "public",
      });
    }

    for await (const attr of this.scanCatalog(this.pgAttributeDef)) {
      if (
        attr.attref_table === targetBaseName ||
        attr.attref_table === targetTableName
      ) {
        const clsInfo = clsMap.get(attr.attrelid);
        if (!clsInfo) continue;

        refs.push({
          childTable: `${clsInfo.nsp}.${clsInfo.name}`,
          childColumn: attr.attname,
          parentColumn: attr.attref_col,
          onDelete: attr.attref_on_delete || "RESTRICT",
          onUpdate: attr.attref_on_update || "RESTRICT",
        });
      }
    }
    return refs;
  }

  /**
   * Returns cached referencing columns from Relcache.
   */
  public async getReferencingColumns(targetTableName: string) {
    const table = await this.getTableAsync(targetTableName);
    return table?.referencingColumns || [];
  }

  public async buildIndexes(name: string) {
    const table = await this.getTableAsync(name);
    if (!table) return;
    const fullName =
      table.firstPage >= 1 && table.firstPage <= 4
        ? `pg_catalog.${name.split(".").pop()}`
        : this.getFullTableName(name);
    const pkCol = table.columns.find((c) => c.isPrimaryKey);
    if (!pkCol) return;

    if (!table.indexRootPage || table.indexRootPage === 0) {
      table.indexRootPage = await this.pager.allocatePage();
      const rootBuf = Buffer.alloc(PAGE_SIZE);
      rootBuf.writeUInt8(1, 0);
      rootBuf.writeUInt16LE(0, 1);
      rootBuf.writeUInt32LE(0xffffffff, 3);
      await this.pager.writePage(table.indexRootPage, rootBuf);
      await this.updateTableSchema(fullName, table);

      const btree = new BTree(this.pager, table.indexRootPage);

      let pageId = table.firstPage;
      while (pageId !== 0xffffffff && pageId !== 0) {
        const buf = await this.pager.readPage(pageId);
        const page = new SlottedPage(buf);
        for (const tuple of page.getTuples()) {
          const resolved = this.isOverflow(tuple.data)
            ? await this.resolveOverflow(tuple.data)
            : tuple.data;
          const row = this.deserializeRow(table.columns, resolved);
          const rootId = await btree.insert(row[pkCol.name], {
            pageId,
            slotIdx: tuple.slotIdx,
          });
          if (rootId !== table.indexRootPage) {
            table.indexRootPage = rootId;
            await this.updateTableSchema(fullName, table);
          }
        }
        pageId = page.nextPageId;
      }
      StorageEngine.pkIndexes.set(`${this.filepath}:${this.currentDbName}:${fullName}`, btree);
    } else {
      StorageEngine.pkIndexes.set(`${this.filepath}:${this.currentDbName}:${fullName}`, new BTree(this.pager, table.indexRootPage));
    }
  }

  public async getRowByPK(name: string, pkValue: any): Promise<any | null> {
    const table = await this.getTableAsync(name);
    if (!table) return null;
    const fullName =
      table.firstPage! >= 1 && table.firstPage! <= 4
        ? `pg_catalog.${name.split(".").pop()}`
        : this.getFullTableName(name);

    // Ensure index is loaded/built for the requested table
    const cacheKey = `${this.filepath}:${this.currentDbName}:${fullName}`;
    if (!StorageEngine.pkIndexes.has(cacheKey)) {
      await this.buildIndexes(fullName);
    }

    const index = StorageEngine.pkIndexes.get(cacheKey);
    if (!index) return null;
    const loc = await index.get(pkValue);
    if (!loc) return null;

    const buf = await this.pager.readPage(loc.pageId);
    const page = new SlottedPage(buf);
    const tuple = page.getTuple(loc.slotIdx);
    if (tuple) {
      const resolved = this.isOverflow(tuple.data)
        ? await this.resolveOverflow(tuple.data)
        : tuple.data;
      return this.deserializeRow(table.columns, resolved);
    }
    return null;
  }

  public async *scanRows(name: string): AsyncIterableIterator<any> {
    let temp = this.tempTables.get(name);
    if (temp !== undefined) {
      for (let i = 0; i < temp.length; i++) yield temp[i];
      return;
    }
    const tableInfo = await this.getTableAsync(name);
    let fullName = name.includes(".") ? name : `public.${name}`;
    if (tableInfo && !name.includes(".")) {
      if (StorageEngine.tableCache.has(`${this.filepath}:${this.currentDbName}:pg_catalog.${name}`))
        fullName = `pg_catalog.${name}`;
    }

    temp = this.tempTables.get(fullName);
    if (temp !== undefined) {
      for (let i = 0; i < temp.length; i++) yield temp[i];
      return;
    }

    const isCatalog = (n: string) =>
      fullName === `pg_catalog.${n}` ||
      (fullName === `public.${n}` && !name.includes("."));

    // Virtual or Persistent Catalog Mapping
    if (isCatalog("pg_namespace")) {
      yield* this.scanCatalog(this.pgNamespaceDef);
      return;
    }
    if (isCatalog("pg_class")) {
      yield* this.scanCatalog(this.pgClassDef);
      return;
    }
    if (isCatalog("pg_attribute")) {
      yield* this.scanCatalog(this.pgAttributeDef);
      return;
    }
    if (isCatalog("pg_description")) {
      yield* this.scanCatalog(this.pgDescriptionDef);
      return;
    }
    if (isCatalog("pg_attrdef")) {
      yield* this.scanCatalog(this.pgAttrdefDef);
      return;
    }
    if (isCatalog("pg_index")) {
      yield* this.scanCatalog(this.pgIndexDef);
      return;
    }

    if (fullName === "information_schema.schemata") {
      for await (const s of this.scanCatalog(this.pgNamespaceDef)) {
        yield {
          catalog_name: "litepostgres",
          schema_name: s.nspname,
          schema_owner: "postgres",
        };
      }
      return;
    }

    if (isCatalog("pg_constraint")) {
      for await (const rel of this.scanCatalog(this.pgClassDef)) {
        const attrs = [];
        for await (const a of this.scanCatalog(this.pgAttributeDef)) {
          if (a.attrelid === rel.oid) attrs.push(a);
        }

        // 1. Primary Key (Aggregated for Composite Keys)
        const pkAttrs = attrs
          .filter((a) => a.attprimary)
          .sort((a, b) => a.attnum - b.attnum);
        if (pkAttrs.length > 0) {
          yield {
            conname: rel.relname + "_pkey",
            contype: "p",
            conrelid: rel.oid,
            connamespace: rel.relnamespace,
            conkey: pkAttrs.map((a) => a.attnum),
            confrelid: 0,
            confkey: null,
          };
        }

        // 2. Unique & Foreign Keys
        for (const attr of attrs) {
          if (attr.attunique && !attr.attprimary) {
            yield {
              conname: rel.relname + "_" + attr.attname + "_key",
              contype: "u",
              conrelid: rel.oid,
              connamespace: rel.relnamespace,
              conkey: [attr.attnum],
              confrelid: 0,
              confkey: null,
            };
          }
          if (attr.attref_table) {
            const refTable = await this.getTableAsync(attr.attref_table);
            let confkey = null;
            if (refTable) {
              const refAttrIdx = refTable.columns.findIndex(
                (c) => c.name === attr.attref_col,
              );
              if (refAttrIdx !== -1) confkey = [refAttrIdx + 1];
            }
            yield {
              conname: rel.relname + "_" + attr.attname + "_fkey",
              contype: "f",
              conrelid: rel.oid,
              connamespace: rel.relnamespace,
              conkey: [attr.attnum],
              confrelid: refTable ? refTable.firstPage : 0,
              confkey,
            };
          }
        }
      }
      return;
    }

    if (fullName === "information_schema.tables") {
      const nspMap = new Map();
      for await (const n of this.scanCatalog(this.pgNamespaceDef))
        nspMap.set(n.oid, n.nspname);
      for await (const r of this.scanCatalog(this.pgClassDef)) {
        const schema = nspMap.get(r.relnamespace);
        if (schema === "pg_catalog" || schema === "information_schema")
          continue;
        yield {
          table_schema: schema,
          table_name: r.relname,
          table_type: "BASE TABLE",
        };
      }
      return;
    }

    if (fullName === "information_schema.referential_constraints") {
      const nspMap = new Map();
      for await (const n of this.scanCatalog(this.pgNamespaceDef))
        nspMap.set(n.oid, n.nspname);
      const relMap = new Map();
      for await (const r of this.scanCatalog(this.pgClassDef))
        relMap.set(r.oid, r);

      for await (const attr of this.scanCatalog(this.pgAttributeDef)) {
        if (!attr.attref_table) continue;
        const rel = relMap.get(attr.attrelid);
        if (!rel) continue;
        const schema = nspMap.get(rel.relnamespace);
        if (schema === "pg_catalog" || schema === "information_schema")
          continue;

        yield {
          constraint_catalog: "litepostgres",
          constraint_schema: schema,
          constraint_name: `${rel.relname}_${attr.attname}_fkey`,
          unique_constraint_catalog: "litepostgres",
          unique_constraint_schema: "public",
          unique_constraint_name: `${attr.attref_table}_pkey`,
          match_option: "NONE",
          update_rule: attr.attref_on_update || "NO ACTION",
          delete_rule: attr.attref_on_delete || "NO ACTION",
        };
      }
      return;
    }

    if (fullName === "information_schema.columns") {
      const nspMap = new Map();
      for await (const n of this.scanCatalog(this.pgNamespaceDef))
        nspMap.set(n.oid, n.nspname);
      const relMap = new Map();
      for await (const r of this.scanCatalog(this.pgClassDef))
        relMap.set(r.oid, r);

      for await (const attr of this.scanCatalog(this.pgAttributeDef)) {
        const rel = relMap.get(attr.attrelid);
        if (!rel) continue;
        const schema = nspMap.get(rel.relnamespace);
        if (schema === "pg_catalog" || schema === "information_schema")
          continue;

        let comment = null;
        for await (const desc of this.scanCatalog(this.pgDescriptionDef)) {
          if (
            Number(desc.objoid) === Number(rel.oid) &&
            Number(desc.objsubid) === Number(attr.attnum)
          ) {
            comment = desc.description;
            break;
          }
        }

        yield {
          table_schema: schema,
          table_name: rel.relname,
          column_name: attr.attname,
          ordinal_position: attr.attnum,
          data_type: attr.atttypid,
          is_nullable: attr.attnotnull ? "NO" : "YES",
          column_default: attr.attdef,
          column_comment: comment,
        };
      }
      return;
    }

    const table = await this.getTableAsync(fullName);
    if (!table) throw new Error(`Table ${fullName} not found`);

    let pageId = table.firstPage;
    const columns = table.columns;
    while (pageId !== 0xffffffff && pageId !== 0) {
      const buf = await this.pager.readPage(pageId!);
      const page = new SlottedPage(buf);
      for (const tuple of page.getTuples()) {
        const resolved = this.isOverflow(tuple.data)
          ? await this.resolveOverflow(tuple.data)
          : tuple.data;
        yield this.deserializeRow(columns, resolved);
      }
      pageId = page.nextPageId;
    }
  }

  public async insertRow(name: string, row: any) {
    const fullName = this.getFullTableName(name);
    const table = await this.getTableAsync(fullName);
    if (!table) throw new Error(`Table ${fullName} not found`);

    if (
      this.dbMeta &&
      (table.firstPage === this.dbMeta.nsp_f ||
        table.firstPage === this.dbMeta.cls_f ||
        table.firstPage === this.dbMeta.att_f ||
        table.firstPage === this.dbMeta.dsc_f ||
        table.firstPage === this.dbMeta.ad_f ||
        table.firstPage === this.dbMeta.idx_f)
    ) {
      await this.insertRowIntoCatalog(table, row);
      return;
    }

    let pageId = table.lastPage!;
    let buf = await this.pager.readPage(pageId);
    let page = new SlottedPage(buf);
    const rawRowData = this.serializeRow(table.columns, row);
    const rowData = await this.handleOverflow(rawRowData);
    let slotIdx = page.insertTuple(rowData);

    if (slotIdx === -1) {
      const newPageId = await this.pager.allocatePage();
      const newBuf = await this.pager.readPage(newPageId);
      const newPage = new SlottedPage(newBuf);
      slotIdx = newPage.insertTuple(rowData);
      await this.pager.writePage(newPageId, newPage.buf);
      page.nextPageId = newPageId;
      await this.pager.writePage(pageId, page.buf);
      table.lastPage = newPageId;
      await this.updateTableSchema(fullName, table);
      pageId = newPageId;
    } else {
      await this.pager.writePage(pageId, page.buf);
    }

    const pkCol = table.columns.find((c) => c.isPrimaryKey);
    if (pkCol && row[pkCol.name] !== undefined) {
      const cacheKey = `${this.filepath}:${this.currentDbName}:${fullName}`;
      if (!StorageEngine.pkIndexes.has(cacheKey)) await this.buildIndexes(fullName);
      const btree = StorageEngine.pkIndexes.get(cacheKey)!;
      const newRoot = await btree.insert(row[pkCol.name], { pageId, slotIdx });
      if (newRoot !== table.indexRootPage) {
        table.indexRootPage = newRoot;
        await this.updateTableSchema(fullName, table);
      }
    }
  }

  public async updateRows(
    name: string,
    filterFn: (row: any) => Promise<boolean>,
    updateFn: (row: any) => Promise<void>,
  ): Promise<number> {
    const fullName = this.getFullTableName(name);
    const table = await this.getTableAsync(fullName);
    if (!table) throw new Error(`Table ${fullName} not found`);
    const pkCol = table.columns.find((c) => c.isPrimaryKey);

    // Thu thập danh sách các page ban đầu để tránh dính "Halloween Problem"
    // khi một Row bị phình to và relocate xuống lastPage.
    const pagesToVisit = [];
    let pId = table.firstPage;
    while (pId !== 0xffffffff && pId !== 0) {
      pagesToVisit.push(pId);
      const buf = await this.pager.readPage(pId);
      pId = new SlottedPage(buf).nextPageId;
    }

    let updated = 0;
    for (const pageId of pagesToVisit) {
      const buf = await this.pager.readPage(pageId);
      const page = new SlottedPage(buf);
      let pageModified = false;

      for (const tuple of page.getTuples()) {
        const resolved = this.isOverflow(tuple.data)
          ? await this.resolveOverflow(tuple.data)
          : tuple.data;
        const row = this.deserializeRow(table.columns, resolved);
        if (await filterFn(row)) {
          const oldRow = { ...row };
          await updateFn(row);
          const rawData = this.serializeRow(table.columns, row);
          const newData = await this.handleOverflow(rawData);

          const oldTupleData = Buffer.allocUnsafe(tuple.data.length);
          tuple.data.copy(oldTupleData);

          if (!page.updateTuple(tuple.slotIdx, newData)) {
            if (this.isOverflow(oldTupleData)) {
              await this.freeOverflowPages(oldTupleData);
            }
            page.deleteTuple(tuple.slotIdx);
            if (pkCol && StorageEngine.pkIndexes.has(`${this.filepath}:${this.currentDbName}:${fullName}`)) {
              await StorageEngine.pkIndexes.get(`${this.filepath}:${this.currentDbName}:${fullName}`)!.delete(oldRow[pkCol.name]);
            }
            await this.insertRow(name, row);
          } else {
            if (this.isOverflow(oldTupleData)) {
              await this.freeOverflowPages(oldTupleData);
            }
            if (pkCol && oldRow[pkCol.name] !== row[pkCol.name]) {
              // PK value changed but row still fits in page, update index
              if (StorageEngine.pkIndexes.has(`${this.filepath}:${this.currentDbName}:${fullName}`)) {
                const btree = StorageEngine.pkIndexes.get(`${this.filepath}:${this.currentDbName}:${fullName}`)!;
                await btree.delete(oldRow[pkCol.name]);
                const newRoot = await btree.insert(row[pkCol.name], {
                  pageId: pageId!,
                  slotIdx: tuple.slotIdx,
                });
                if (newRoot !== table.indexRootPage) {
                  table.indexRootPage = newRoot;
                  await this.updateTableSchema(fullName, table);
                }
              }
            }
          }

          pageModified = true;
          updated++;
        }
      }

      if (pageModified) await this.pager.writePage(pageId!, page.buf);
    }
    return updated;
  }

  public async updateRowByPK(
    name: string,
    pkValue: any,
    filterFn: (row: any) => Promise<boolean>,
    updateFn: (row: any) => Promise<void>,
  ): Promise<number> {
    const fullName = this.getFullTableName(name);
    const table = await this.getTableAsync(fullName);
    if (!table) throw new Error(`Table ${fullName} not found`);
    const pkCol = table.columns.find((c) => c.isPrimaryKey);
    if (!pkCol) return 0;

    const cacheKey = `${this.filepath}:${this.currentDbName}:${fullName}`;
    if (!StorageEngine.pkIndexes.has(cacheKey)) {
      await this.buildIndexes(fullName);
    }
    const index = StorageEngine.pkIndexes.get(cacheKey);
    if (!index) return 0;

    const loc = await index.get(pkValue);
    if (!loc) return 0;

    const pageId = loc.pageId;
    const buf = await this.pager.readPage(pageId);
    const page = new SlottedPage(buf);
    const tuple = page.getTuple(loc.slotIdx);
    if (!tuple) return 0;

    const resolved = this.isOverflow(tuple.data) ? await this.resolveOverflow(tuple.data) : tuple.data;
    const row = this.deserializeRow(table.columns, resolved);
    if (!(await filterFn(row))) return 0;

    const oldRow = { ...row };
    
    await updateFn(row);
    
    const rawData = this.serializeRow(table.columns, row);
    const newData = await this.handleOverflow(rawData);
    
    const oldTupleData = Buffer.allocUnsafe(tuple.data.length);
    tuple.data.copy(oldTupleData);

    if (!page.updateTuple(loc.slotIdx, newData)) {
      if (this.isOverflow(oldTupleData)) {
        await this.freeOverflowPages(oldTupleData);
      }
      page.deleteTuple(loc.slotIdx);
      await index.delete(oldRow[pkCol.name]);
      await this.insertRow(name, row);
    } else {
      if (this.isOverflow(oldTupleData)) {
        await this.freeOverflowPages(oldTupleData);
      }
      if (oldRow[pkCol.name] !== row[pkCol.name]) {
        await index.delete(oldRow[pkCol.name]);
        const newRoot = await index.insert(row[pkCol.name], {
          pageId,
          slotIdx: loc.slotIdx,
        });
        if (newRoot !== table.indexRootPage) {
          table.indexRootPage = newRoot;
          await this.updateTableSchema(fullName, table);
        }
      }
    }

    await this.pager.writePage(pageId, page.buf);
    return 1;
  }

  public async deleteRowByPK(
    name: string,
    pkValue: any,
    filterFn: (row: any) => Promise<boolean>
  ): Promise<number> {
    const fullName = this.getFullTableName(name);
    const table = await this.getTableAsync(fullName);
    if (!table) throw new Error(`Table ${fullName} not found`);
    const pkCol = table.columns.find((c) => c.isPrimaryKey);
    if (!pkCol) return 0;

    const cacheKey = `${this.filepath}:${this.currentDbName}:${fullName}`;
    if (!StorageEngine.pkIndexes.has(cacheKey)) {
      await this.buildIndexes(fullName);
    }
    const index = StorageEngine.pkIndexes.get(cacheKey);
    if (!index) return 0;

    const loc = await index.get(pkValue);
    if (!loc) return 0;

    const pageId = loc.pageId;
    const buf = await this.pager.readPage(pageId);
    const page = new SlottedPage(buf);
    const tuple = page.getTuple(loc.slotIdx);
    if (!tuple) return 0;

    const resolved = this.isOverflow(tuple.data) ? await this.resolveOverflow(tuple.data) : tuple.data;
    const row = this.deserializeRow(table.columns, resolved);

    if (await filterFn(row)) {
      if (this.isOverflow(tuple.data)) {
        await this.freeOverflowPages(tuple.data);
      }
      page.deleteTuple(loc.slotIdx);
      await index.delete(pkValue);
      await this.pager.writePage(pageId, page.buf);
      return 1;
    }
    return 0;
  }

  public async deleteRows(
    name: string,
    filterFn: (row: any) => Promise<boolean>,
  ): Promise<number> {
    const fullName = this.getFullTableName(name);
    const table = await this.getTableAsync(fullName);
    if (!table) throw new Error(`Table ${fullName} not found`);
    const pkCol = table.columns.find((c) => c.isPrimaryKey);
    let pageId = table.firstPage;
    let deleted = 0;

    while (pageId !== 0xffffffff && pageId !== 0) {
      const buf = await this.pager.readPage(pageId!);
      const page = new SlottedPage(buf);
      let pageModified = false;

      for (const tuple of page.getTuples()) {
        const resolved = this.isOverflow(tuple.data)
          ? await this.resolveOverflow(tuple.data)
          : tuple.data;
        const row = this.deserializeRow(table.columns, resolved);
        if (await filterFn(row)) {
          if (this.isOverflow(tuple.data)) {
            await this.freeOverflowPages(tuple.data);
          }
          page.deleteTuple(tuple.slotIdx);
          if (pkCol && StorageEngine.pkIndexes.has(`${this.filepath}:${this.currentDbName}:${fullName}`))
            await StorageEngine.pkIndexes.get(`${this.filepath}:${this.currentDbName}:${fullName}`)!.delete(row[pkCol.name]);
          pageModified = true;
          deleted++;
        }
      }

      if (pageModified) await this.pager.writePage(pageId!, page.buf);
      pageId = page.nextPageId;
    }
    return deleted;
  }

  public begin(): void {
    if (this.inTransaction) return;
    this.inTransaction = true;
    if (this.dbMeta) {
      this.txDbMetaBackup = JSON.stringify(this.dbMeta);
    }
    if (this.clusterCatalogDef) {
      this.txClusterMetaBackup = JSON.stringify(this.clusterCatalogDef);
    }
  }
  public async commit(): Promise<void> {
    this.inTransaction = false;
    this.txDbMetaBackup = null;
    this.txClusterMetaBackup = null;
    await this.flush();
  }
  public isInTransaction(): boolean {
    return this.inTransaction;
  }

  public async rollback(): Promise<void> {
    if (this.inTransaction) {
      await this.pager.clearDirty();
      StorageEngine.tableCache.clear();
      StorageEngine.schemaCache.clear();
      StorageEngine.pkIndexes.clear();
      this.inTransaction = false;

      if (this.txDbMetaBackup) {
        this.dbMeta = JSON.parse(this.txDbMetaBackup);
        this.refreshCatalogDefs();
        this.txDbMetaBackup = null;
        StorageEngine.dbMetaCache.set(
          `${this.filepath}:${this.dbMeta.name}`,
          this.dbMeta,
        );
      }
      if (this.txClusterMetaBackup) {
        this.clusterCatalogDef = JSON.parse(this.txClusterMetaBackup);
        this.txClusterMetaBackup = null;
      }
    }
  }

  public async rollbackStatement(): Promise<void> {
    await this.pager.clearDirty();
    StorageEngine.tableCache.clear();
    StorageEngine.schemaCache.clear();
    StorageEngine.pkIndexes.clear();
    this.currentDbName = undefined;
    if (this.dbMeta) {
      StorageEngine.dbMetaCache.delete(`${this.filepath}:${this.dbMeta.name}`);
    }
    this.clusterInitialized = false;
  }

  public async flush(): Promise<void> {
    if (!this.inTransaction) await this.pager.flush();
  }

  public async close(): Promise<void> {
    await this.pager.close();
  }

  public async destroy(): Promise<void> {
    await this.pager.destroy();
  }
}
