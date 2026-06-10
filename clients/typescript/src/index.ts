// warden-3yd
// Async client for the Warden daemon's control plane.
//
// Distinct from any worker SDK: this is a client of the *control* socket
// (WARDEN_CTRL_SOCKET) — the same protocol wardenctl speaks — used to create
// beams and spawn/message supervised workers. Wire-compatible with the Python
// warden.Client.
import net from "node:net";
import os from "node:os";
import path from "node:path";

const DEFAULT_SOCKET = "~/.warden/ctrl.sock";
const RETRY_INITIAL_MS = 50;
const RETRY_CAP_MS = 2000;

export const VALID_RESTART = ["permanent", "transient", "temporary"] as const;
export type RestartPolicy = (typeof VALID_RESTART)[number];

// warden-3yd
// WardenError and WardenUnavailable are intentionally independent Error
// subclasses (mirrors the Python SDK): WardenError means the daemon returned
// ok=false; WardenUnavailable means an explicit connect timeout elapsed.
export class WardenError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "WardenError";
  }
}

export class WardenUnavailable extends Error {
  constructor(message: string) {
    super(message);
    this.name = "WardenUnavailable";
  }
}

export function encodeFrame(obj: unknown): Buffer {
  const body = Buffer.from(JSON.stringify(obj), "utf8");
  const header = Buffer.allocUnsafe(4);
  header.writeUInt32BE(body.length, 0);
  return Buffer.concat([header, body]);
}

export function decodeBody(body: Buffer): unknown {
  return JSON.parse(body.toString("utf8"));
}

function expandHome(p: string): string {
  if (p === "~") return os.homedir();
  // ~user form intentionally unsupported (Unix daemon socket paths only)
  if (p.startsWith("~/")) return path.join(os.homedir(), p.slice(2));
  return p;
}

export function resolvePath(p?: string): string {
  const raw = p ?? process.env.WARDEN_CTRL_SOCKET ?? DEFAULT_SOCKET;
  return expandHome(raw);
}

// warden-3yd
export type Opener = () => Promise<net.Socket>;

export interface RetryClock {
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
}

const defaultSleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export async function connectWithRetry(
  open: Opener,
  opts: { timeoutMs?: number } & RetryClock = {},
): Promise<net.Socket> {
  const sleep = opts.sleep ?? defaultSleep;
  const now = opts.now ?? Date.now;
  const deadline = opts.timeoutMs == null ? null : now() + opts.timeoutMs;
  let delay = RETRY_INITIAL_MS;
  for (;;) {
    try {
      return await open();
    } catch (err) {
      const code = (err as NodeJS.ErrnoException).code;
      if (code !== "ENOENT" && code !== "ECONNREFUSED") throw err;
      if (deadline != null && now() >= deadline) {
        throw new WardenUnavailable(
          `warden daemon not reachable within ${opts.timeoutMs}ms`,
        );
      }
      await sleep(delay);
      delay = Math.min(delay * 2, RETRY_CAP_MS);
    }
  }
}

// warden-3yd
// Open a Unix-domain stream socket; rejects with the ENOENT/ECONNREFUSED that
// connectWithRetry treats as "daemon not up yet". The returned socket carries a
// no-op 'error' backstop so a stray post-connect error can't crash the process
// before a caller attaches its own handler.
export function openUnixSocket(sockPath: string): Promise<net.Socket> {
  return new Promise((resolve, reject) => {
    const sock = net.createConnection({ path: sockPath });
    const onConnect = () => {
      sock.off("error", onError);
      sock.on("error", () => {}); // backstop; per-request reader adds the real handler
      resolve(sock);
    };
    const onError = (err: Error) => {
      sock.off("connect", onConnect);
      reject(err);
    };
    sock.once("connect", onConnect);
    sock.once("error", onError);
  });
}

// warden-3yd
// Read exactly one length-prefixed JSON frame from a client socket, accumulating
// chunks until the full frame arrives. Rejects if the socket closes/errs first.
function readOneFrame(sock: net.Socket): Promise<any> {
  return new Promise((resolve, reject) => {
    let buf = Buffer.alloc(0);
    let need = -1;
    const onData = (chunk: Buffer) => {
      buf = Buffer.concat([buf, chunk]);
      if (need < 0 && buf.length >= 4) need = buf.readUInt32BE(0);
      if (need >= 0 && buf.length >= 4 + need) {
        cleanup();
        try {
          resolve(decodeBody(buf.subarray(4, 4 + need)));
        } catch (e) {
          reject(e);
        }
      }
    };
    const onErr = (e: Error) => {
      cleanup();
      reject(e);
    };
    const onClose = () => {
      cleanup();
      reject(new Error("warden: connection closed before a full response frame"));
    };
    const cleanup = () => {
      sock.off("data", onData);
      sock.off("error", onErr);
      sock.off("close", onClose);
    };
    sock.on("data", onData);
    sock.on("error", onErr);
    sock.on("close", onClose);
  });
}

export interface ConnectOptions {
  path?: string;
  /** Max seconds to wait for the daemon (undefined = forever). */
  timeout?: number;
  /** Test seam: override how a socket is opened. */
  _open?: Opener;
}

// warden-3yd
export class Warden {
  #path: string;
  #opener: Opener;
  #sock: net.Socket | null = null;
  #connected = false;
  #counter = 0;
  // promise-chain mutex: each request waits for the previous to settle.
  #tail: Promise<unknown> = Promise.resolve();

  private constructor(sockPath: string, opener: Opener) {
    this.#path = sockPath;
    this.#opener = opener;
  }

  static async connect(opts: ConnectOptions = {}): Promise<Warden> {
    const sockPath = resolvePath(opts.path);
    const opener = opts._open ?? (() => openUnixSocket(sockPath));
    const c = new Warden(sockPath, opener);
    await c.#ensureConnected(opts.timeout);
    return c;
  }

  async #ensureConnected(timeoutSec?: number): Promise<void> {
    if (this.#connected && this.#sock) return;
    if (this.#sock) {
      this.#sock.destroy();
      this.#sock = null;
    }
    const timeoutMs = timeoutSec == null ? undefined : timeoutSec * 1000;
    this.#sock = await connectWithRetry(this.#opener, { timeoutMs });
    this.#connected = true;
  }

  #markDisconnected(): void {
    this.#connected = false;
    if (this.#sock) {
      this.#sock.destroy();
      this.#sock = null;
    }
  }

  #writeAll(sock: net.Socket, frame: Buffer): Promise<void> {
    return new Promise((resolve, reject) => {
      sock.write(frame, (err) => (err ? reject(err) : resolve()));
    });
  }

  // Serialize: chain onto #tail so only one request runs at a time, regardless
  // of whether the previous one resolved or rejected.
  #runExclusive<T>(fn: () => Promise<T>): Promise<T> {
    const result = this.#tail.then(fn, fn);
    this.#tail = result.then(
      () => {},
      () => {},
    );
    return result;
  }

  /** @internal - exposed for tests; verb methods are the public surface. */
  request(action: string, payload: unknown): Promise<any> {
    return this.#runExclusive(async () => {
      await this.#ensureConnected();
      const sock = this.#sock;
      if (sock == null) {
        // close() raced us between connect and first use
        throw new Error(`warden: connection unavailable for '${action}'`);
      }
      this.#counter += 1;
      const reqId = String(this.#counter);
      const frame = encodeFrame({ req_id: reqId, action, payload });
      let resp: any;
      try {
        await this.#writeAll(sock, frame);
        resp = await readOneFrame(sock);
      } catch (err) {
        this.#markDisconnected();
        throw new Error(
          `warden connection lost during '${action}' (${this.#path}): ${(err as Error).message}`,
        );
      }
      // one-shot protocol: the daemon closes the socket after this response.
      this.#markDisconnected();
      // Fix 2: guard against a non-object response frame (e.g. a bare JSON
      // null/number) so we raise a clean WardenError instead of a TypeError.
      if (resp == null || typeof resp !== "object") {
        throw new WardenError(`malformed response frame: ${JSON.stringify(resp)}`);
      }
      if (resp.req_id !== reqId) {
        throw new WardenError(
          `req_id mismatch: sent ${reqId}, got ${JSON.stringify(resp.req_id)}`,
        );
      }
      if (!resp.ok) {
        throw new WardenError(resp.error ?? "unknown error");
      }
      return resp.payload;
    });
  }

  // warden-3yd
  #field<T>(result: any, key: string, action: string): T {
    if (result == null || typeof result !== "object" || !(key in result)) {
      throw new WardenError(
        `${action}: malformed response, missing '${key}': ${JSON.stringify(result)}`,
      );
    }
    return result[key] as T;
  }

  async beamCreate(beam?: number): Promise<number> {
    const payload = beam == null ? {} : { beam };
    const result = await this.request("beam.create", payload);
    return this.#field<number>(result, "beam_id", "beam.create");
  }

  async procSpawn(
    cmd: string | string[],
    opts: { beam?: number; parent?: string; restart?: RestartPolicy } = {},
  ): Promise<string> {
    const { beam, parent, restart } = opts;
    if (restart != null && !VALID_RESTART.includes(restart)) {
      throw new TypeError(
        `invalid restart policy ${JSON.stringify(restart)}; ` +
          `expected one of ${JSON.stringify(VALID_RESTART)}`,
      );
    }
    const cmdArr = typeof cmd === "string" ? [cmd] : [...cmd];
    const payload: Record<string, unknown> = { cmd: cmdArr };
    if (beam != null) payload.beam = beam;
    if (parent != null) payload.parent = parent;
    if (restart != null) payload.restart = restart;
    const result = await this.request("proc.spawn", payload);
    return this.#field<string>(result, "pid", "proc.spawn");
  }

  async procSend(pid: string, msgType: string, body: unknown): Promise<void> {
    await this.request("proc.send", { pid, type: msgType, body });
  }

  async procCall(
    pid: string,
    msgType: string,
    body: unknown,
    opts: { timeoutMs?: number } = {},
  ): Promise<{ type: string; body: unknown }> {
    const timeout_ms = opts.timeoutMs ?? 5000;
    return (await this.request("proc.call", {
      pid,
      type: msgType,
      body,
      timeout_ms,
    })) as { type: string; body: unknown };
  }

  async procList(beam?: number): Promise<any[]> {
    const payload = beam == null ? {} : { beam };
    const result = await this.request("proc.list", payload);
    return this.#field<any[]>(result, "processes", "proc.list");
  }

  async close(): Promise<void> {
    this.#markDisconnected();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}
