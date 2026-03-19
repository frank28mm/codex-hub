#!/usr/bin/env node
"use strict";

const http = require("node:http");
const { spawn } = require("node:child_process");
const path = require("node:path");

const APP_ROOT = __dirname;
const RENDERER_URL = process.env.WORKSPACE_HUB_RENDERER_URL || "http://127.0.0.1:3310";
const RENDERER_START_TIMEOUT_MS = 90_000;
const POLL_INTERVAL_MS = 1_000;

function log(message) {
  process.stdout.write(`[workspace] ${message}\n`);
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function checkRendererReady(url) {
  return new Promise((resolve) => {
    const request = http.get(url, (response) => {
      response.resume();
      resolve(response.statusCode && response.statusCode >= 200 && response.statusCode < 500);
    });
    request.on("error", () => resolve(false));
    request.setTimeout(2_000, () => {
      request.destroy();
      resolve(false);
    });
  });
}

async function waitForRenderer(url, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await checkRendererReady(url)) {
      return true;
    }
    await wait(POLL_INTERVAL_MS);
  }
  return false;
}

function spawnChild(command, args, env = {}) {
  const child = spawn(command, args, {
    cwd: APP_ROOT,
    env: { ...process.env, ...env },
    stdio: "inherit",
  });
  return child;
}

async function main() {
  log("starting Next.js renderer");
  const renderer = spawnChild("npm", ["run", "renderer:dev"]);
  let shuttingDown = false;
  let electron = null;

  const shutdown = (code = 0) => {
    if (shuttingDown) return;
    shuttingDown = true;
    for (const child of [electron, renderer]) {
      if (child && !child.killed) {
        try {
          child.kill("SIGTERM");
        } catch (_error) {
          // ignore
        }
      }
    }
    process.exit(code);
  };

  process.on("SIGINT", () => shutdown(0));
  process.on("SIGTERM", () => shutdown(0));

  renderer.on("exit", (code) => {
    if (!shuttingDown) {
      log(`renderer exited early with code ${code ?? 0}`);
      shutdown(code ?? 1);
    }
  });

  const ready = await waitForRenderer(RENDERER_URL, RENDERER_START_TIMEOUT_MS);
  if (!ready) {
    log(`renderer did not become ready at ${RENDERER_URL} within ${RENDERER_START_TIMEOUT_MS / 1000}s`);
    shutdown(1);
    return;
  }

  log(`renderer ready at ${RENDERER_URL}`);
  log("starting Electron shell");
  electron = spawnChild(
    "npm",
    ["run", "dev"],
    {
      WORKSPACE_HUB_RENDERER_MODE: "next-dev",
      WORKSPACE_HUB_RENDERER_URL: RENDERER_URL,
    },
  );

  electron.on("exit", (code) => {
    if (!shuttingDown) {
      log(`electron exited with code ${code ?? 0}`);
      shutdown(code ?? 0);
    }
  });
}

main().catch((error) => {
  process.stderr.write(`[workspace] failed: ${String(error?.message || error)}\n`);
  process.exit(1);
});
