// warden-3yd
import test from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";
import { spawn, type ChildProcess } from "node:child_process";
import { Warden, WardenUnavailable } from "../src/index.ts";

const REPO_ROOT = path.resolve(import.meta.dirname, "..", "..", "..");
const DAEMON = path.join(REPO_ROOT, "zig-out", "bin", "warden");
const WORKER = path.join(REPO_ROOT, "examples", "live_demo", "math_worker.py");

function daemonAvailable(): boolean {
  try {
    fs.accessSync(DAEMON, fs.constants.X_OK);
    fs.accessSync(WORKER);
    return true;
  } catch {
    return false;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitForSocket(p: string, timeoutMs = 5000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (fs.existsSync(p)) return true;
    await sleep(20);
  }
  return false;
}

type Daemon = { proc: ChildProcess; sock: string; dir: string };

function startDaemon(): Daemon {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "warden-tsi-"));
  // socket in a subdir the daemon creates itself (avoids warden-w6n)
  const sock = path.join(dir, "sockets", "ctrl.sock");
  const logDir = path.join(dir, "logs");
  fs.mkdirSync(logDir, { recursive: true });
  const proc = spawn(DAEMON, {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      WARDEN_CTRL_SOCKET: sock,
      WARDEN_BEAM_ID: "1",
      WARDEN_LOG_DIR: logDir,
    },
    stdio: "ignore",
  });
  return { proc, sock, dir };
}

async function stopDaemon(d: Daemon): Promise<void> {
  if (d.proc.exitCode === null && d.proc.signalCode === null) {
    d.proc.kill("SIGTERM");
    await new Promise<void>((resolve) => {
      const t = setTimeout(() => {
        d.proc.kill("SIGKILL");
        resolve();
      }, 5000);
      d.proc.once("exit", () => {
        clearTimeout(t);
        resolve();
      });
    });
  }
  fs.rmSync(d.dir, { recursive: true, force: true });
}

test(
  "integration: spawn → call → list round-trip",
  { skip: daemonAvailable() ? false : "zig-out/bin/warden or worker absent" },
  async () => {
    const d = startDaemon();
    try {
      assert.ok(await waitForSocket(d.sock), "daemon never bound the socket");
      const c = await Warden.connect({ path: d.sock, timeout: 10 });
      try {
        const beam = await c.beamCreate(1); // primary beam (warden-95s)
        assert.equal(typeof beam, "number");

        const pid = await c.procSpawn(["python3", WORKER], {
          beam,
          restart: "permanent",
        });
        assert.match(pid, /^\d+\/\d+$/);

        const reply = await c.procCall(pid, "req.fib", 10, { timeoutMs: 5000 });
        assert.equal(reply.body, 55); // fib(10)

        const procs = await c.procList(beam);
        // proc.list rows carry split beam/pid ints (warden-36j)
        assert.ok(procs.some((p) => `${p.beam}/${p.pid}` === pid));
      } finally {
        await c.close();
      }
    } finally {
      await stopDaemon(d);
    }
  },
);

test(
  "integration: retry-connect blocks until the daemon comes up",
  { skip: daemonAvailable() ? false : "zig-out/bin/warden absent" },
  async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "warden-tsi2-"));
    const sock = path.join(dir, "sockets", "ctrl.sock");
    let started: Daemon | null = null;
    try {
      const connectP = Warden.connect({ path: sock, timeout: 10 });
      await sleep(300);
      let settled = false;
      void connectP.then(
        () => (settled = true),
        () => (settled = true),
      );
      await sleep(0);
      assert.equal(settled, false, "connect should still be blocking");

      const logDir = path.join(dir, "logs");
      fs.mkdirSync(logDir, { recursive: true });
      const proc = spawn(DAEMON, {
        cwd: REPO_ROOT,
        env: {
          ...process.env,
          WARDEN_CTRL_SOCKET: sock,
          WARDEN_BEAM_ID: "1",
          WARDEN_LOG_DIR: logDir,
        },
        stdio: "ignore",
      });
      started = { proc, sock, dir };

      const c = await connectP;
      try {
        assert.equal(typeof (await c.beamCreate(1)), "number");
      } finally {
        await c.close();
      }
    } finally {
      if (started) await stopDaemon(started);
      else fs.rmSync(dir, { recursive: true, force: true });
    }
  },
);

test("integration: connect timeout raises WardenUnavailable when no daemon", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "warden-tsi3-"));
  const sock = path.join(dir, "ctrl.sock");
  try {
    await assert.rejects(
      Warden.connect({ path: sock, timeout: 0.5 }),
      WardenUnavailable,
    );
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
