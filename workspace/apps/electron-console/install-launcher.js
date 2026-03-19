#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const APP_ROOT = __dirname;
const LAUNCHER_NAME = "Codex Hub 工作台";
const DEFAULT_INSTALL_DIR = path.join(os.homedir(), "Applications");
const APP_BUNDLE_PATH = path.join(DEFAULT_INSTALL_DIR, `${LAUNCHER_NAME}.app`);
const COMMAND = String(process.argv[2] || "install").trim() || "install";

function ensureDir(targetPath) {
  fs.mkdirSync(targetPath, { recursive: true });
}

function shellEscape(value) {
  return `'${String(value).replace(/'/g, `'\"'\"'`)}'`;
}

function buildAppleScript() {
  const workspaceCommand = `cd ${shellEscape(APP_ROOT)} && npm run workspace`;
  return `
on run
  tell application "Terminal"
    activate
    do script ${JSON.stringify(workspaceCommand)}
  end tell
end run
`.trim();
}

function main() {
  if (COMMAND === "status") {
    console.log(
      JSON.stringify(
        {
          ok: true,
          installed: fs.existsSync(APP_BUNDLE_PATH),
          launcher_name: LAUNCHER_NAME,
          launcher_path: APP_BUNDLE_PATH,
        },
        null,
        2,
      ),
    );
    return;
  }

  if (COMMAND === "uninstall") {
    if (fs.existsSync(APP_BUNDLE_PATH)) {
      fs.rmSync(APP_BUNDLE_PATH, { recursive: true, force: true });
    }
    console.log(
      JSON.stringify(
        {
          ok: true,
          installed: false,
          launcher_name: LAUNCHER_NAME,
          launcher_path: APP_BUNDLE_PATH,
        },
        null,
        2,
      ),
    );
    return;
  }

  ensureDir(DEFAULT_INSTALL_DIR);
  const compile = spawnSync(
    "/usr/bin/osacompile",
    ["-o", APP_BUNDLE_PATH, "-e", buildAppleScript()],
    { cwd: APP_ROOT, encoding: "utf8" },
  );

  if (compile.status !== 0) {
    console.error(
      JSON.stringify(
        {
          ok: false,
          error: "osacompile failed",
          stdout: compile.stdout,
          stderr: compile.stderr,
        },
        null,
        2,
      ),
    );
    process.exit(1);
  }

  const plistPath = path.join(APP_BUNDLE_PATH, "Contents", "Info.plist");
  if (fs.existsSync(plistPath)) {
    const plist = fs.readFileSync(plistPath, "utf8");
    const normalized = plist.includes("LSUIElement")
      ? plist
      : plist.replace(
          "</dict>",
          "  <key>LSUIElement</key>\n  <false/>\n</dict>",
        );
    fs.writeFileSync(plistPath, normalized, "utf8");
  }

  console.log(
    JSON.stringify(
      {
        ok: true,
        installed: true,
        launcher_name: LAUNCHER_NAME,
        launcher_path: APP_BUNDLE_PATH,
        hint: "可将生成的 .app 拖到 Dock，作为 Codex Hub 中文工作台的图标快捷入口。",
      },
      null,
      2,
    ),
  );
}

main();
