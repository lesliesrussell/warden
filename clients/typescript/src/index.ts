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
