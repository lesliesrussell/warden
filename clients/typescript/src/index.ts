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
