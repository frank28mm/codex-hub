const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const APP_ROOT = __dirname;

function fail(message, details = {}) {
  console.error(JSON.stringify({ ok: false, error: message, ...details }, null, 2));
  process.exit(1);
}

function read(file) {
  return fs.readFileSync(path.join(APP_ROOT, file), "utf8");
}

function ensureFile(file) {
  const target = path.join(APP_ROOT, file);
  if (!fs.existsSync(target)) {
    fail(`missing required file: ${file}`);
  }
  return target;
}

function ensureIncludes(text, needle, source) {
  if (!text.includes(needle)) {
    fail(`missing expected content in ${source}`, { expected: needle });
  }
}

function checkSyntax(file) {
  const target = ensureFile(file);
  const result = spawnSync("node", ["--check", target], {
    cwd: APP_ROOT,
    encoding: "utf8",
  });
  if (result.status !== 0) {
    fail(`syntax check failed for ${file}`, {
      stdout: result.stdout,
      stderr: result.stderr,
    });
  }
}

function main() {
  const packageJsonPath = ensureFile("package.json");
  const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));

  if (
    !packageJson.scripts?.start ||
    !packageJson.scripts?.dev ||
    !packageJson.scripts?.smoke ||
    !packageJson.scripts?.["launcher:install"] ||
    !packageJson.scripts?.["launcher:status"] ||
    !packageJson.scripts?.["launcher:uninstall"] ||
    !packageJson.scripts?.["bridge:daemon"] ||
    !packageJson.scripts?.["bridge:install"] ||
    !packageJson.scripts?.["bridge:status"]
  ) {
    fail("package.json is missing required scripts", {
      scripts: packageJson.scripts || {},
    });
  }

  const mainJs = read("main.js");
  const bridgeHostJs = read("bridge-host.js");
  const bridgeServiceJs = read("coco-bridge-service.js");
  const installLauncherJs = read("install-launcher.js");
  const preloadJs = read("preload.js");
  const rendererJs = read("renderer.js");
  const indexHtml = read("index.html");
  const readme = read("README.md");
  ensureFile("next.config.mjs");
  ensureFile("install-launcher.js");
  ensureFile("renderer/app/layout.js");
  ensureFile("renderer/app/page.js");
  ensureFile("renderer/app/globals.css");

  checkSyntax("main.js");
  checkSyntax("bridge-host.js");
  checkSyntax("coco-bridge-service.js");
  checkSyntax("install-launcher.js");
  checkSyntax("preload.js");
  checkSyntax("renderer.js");
  checkSyntax("smoke-check.js");

  ensureIncludes(mainJs, "local_broker.py", "main.js");
  ensureIncludes(mainJs, '["codex exec", "codex resume", "codex app"]', "main.js");
  ensureIncludes(mainJs, 'ipcMain.handle("broker:panel"', "main.js");
  ensureIncludes(mainJs, 'ipcMain.handle("broker:command-center"', "main.js");
  ensureIncludes(mainJs, 'const executionProfile =', "main.js");
  ensureIncludes(mainJs, '"electron-full-access"', "main.js");
  ensureIncludes(mainJs, 'ipcMain.handle("broker:bridge-status"', "main.js");
  ensureIncludes(mainJs, 'ipcMain.handle("broker:bridge-settings"', "main.js");
  ensureIncludes(mainJs, 'ipcMain.handle("broker:bridge-conversations"', "main.js");
  ensureIncludes(mainJs, 'ipcMain.handle("broker:bridge-messages"', "main.js");
  ensureIncludes(mainJs, 'ipcMain.handle("bridge:connect"', "main.js");
  ensureIncludes(mainJs, 'ipcMain.handle("bridge:disconnect"', "main.js");
  ensureIncludes(mainJs, 'ipcMain.handle("bridge:reconnect"', "main.js");
  ensureIncludes(mainJs, 'ipcMain.handle("service:coco-status"', "main.js");
  ensureIncludes(mainJs, 'ipcMain.handle("service:coco-install"', "main.js");
  ensureIncludes(mainJs, 'ipcMain.handle("service:coco-restart"', "main.js");
  ensureIncludes(mainJs, 'ipcMain.handle("service:coco-verify"', "main.js");
  ensureIncludes(mainJs, 'ipcMain.handle("launcher:status"', "main.js");
  ensureIncludes(mainJs, 'ipcMain.handle("launcher:install"', "main.js");
  ensureIncludes(mainJs, 'ipcMain.handle("launcher:uninstall"', "main.js");
  ensureIncludes(mainJs, 'createBridgeHost', "main.js");

  ensureIncludes(bridgeHostJs, "createBridgeHost", "bridge-host.js");
  ensureIncludes(bridgeHostJs, 'brokerCall("bridge-connection"', "bridge-host.js");
  ensureIncludes(bridgeHostJs, "connect()", "bridge-host.js");
  ensureIncludes(bridgeHostJs, "reconnect()", "bridge-host.js");
  ensureIncludes(bridgeHostJs, "heartbeat_at", "bridge-host.js");

  ensureIncludes(bridgeServiceJs, 'com.codexhub.coco-feishu-bridge', "coco-bridge-service.js");
  ensureIncludes(bridgeServiceJs, "/usr/bin/caffeinate", "coco-bridge-service.js");
  ensureIncludes(bridgeServiceJs, "install-launchagent", "coco-bridge-service.js");
  ensureIncludes(bridgeServiceJs, "restart-launchagent", "coco-bridge-service.js");
  ensureIncludes(bridgeServiceJs, "verify-persistence", "coco-bridge-service.js");
  ensureIncludes(bridgeServiceJs, "run-daemon", "coco-bridge-service.js");
  ensureIncludes(bridgeServiceJs, "bridge_reconnect", "coco-bridge-service.js");
  ensureIncludes(installLauncherJs, "Codex Hub 工作台", "install-launcher.js");
  ensureIncludes(installLauncherJs, "osacompile", "install-launcher.js");
  ensureIncludes(installLauncherJs, "launcher_path", "install-launcher.js");

  ensureIncludes(preloadJs, "getMetadata", "preload.js");
  ensureIncludes(preloadJs, "getPanel", "preload.js");
  ensureIncludes(preloadJs, "getBridgeStatus", "preload.js");
  ensureIncludes(preloadJs, "updateBridgeSettings", "preload.js");
  ensureIncludes(preloadJs, "getBridgeConversations", "preload.js");
  ensureIncludes(preloadJs, "getBridgeMessages", "preload.js");
  ensureIncludes(preloadJs, "connectBridge", "preload.js");
  ensureIncludes(preloadJs, "disconnectBridge", "preload.js");
  ensureIncludes(preloadJs, "reconnectBridge", "preload.js");
  ensureIncludes(preloadJs, "getCoCoServiceStatus", "preload.js");
  ensureIncludes(preloadJs, "installCoCoService", "preload.js");
  ensureIncludes(preloadJs, "restartCoCoService", "preload.js");
  ensureIncludes(preloadJs, "verifyCoCoServicePersistence", "preload.js");
  ensureIncludes(preloadJs, "getLauncherStatus", "preload.js");
  ensureIncludes(preloadJs, "installLauncher", "preload.js");
  ensureIncludes(preloadJs, "uninstallLauncher", "preload.js");
  ensureIncludes(preloadJs, "runCommandCenter", "preload.js");

  [
    "panel-overview",
    "panel-command",
    "panel-projects",
    "panel-review",
    "panel-coordination",
    "panel-health",
  ].forEach((panelId) => ensureIncludes(indexHtml, `id="${panelId}"`, "index.html"));

  [
    "panel-feedback",
    "overview-highlights",
    "command-hint",
    "command-readiness",
    "command-status",
    "projects-list",
    "project-detail",
    "command-history",
    "review-scope",
    "review-summary",
    "coordination-scope",
    "coordination-summary",
  ].forEach((elementId) => ensureIncludes(indexHtml, `id="${elementId}"`, "index.html"));

  [
    "Overview",
    "Command Center",
    "Projects",
    "Review Inbox",
    "Coordination Inbox",
    "Health / Alerts",
  ].forEach((label) => ensureIncludes(indexHtml, label, "index.html"));

  ensureIncludes(rendererJs, "workspaceHubAPI.getMetadata", "renderer.js");
  ensureIncludes(rendererJs, "workspaceHubAPI.getPanel", "renderer.js");
  ensureIncludes(rendererJs, "workspaceHubAPI.runCommandCenter", "renderer.js");
  ensureIncludes(rendererJs, "Shared Substrate", "renderer.js");
  ensureIncludes(rendererJs, "Use In Command Center", "renderer.js");
  ensureIncludes(rendererJs, "renderCommandReadiness", "renderer.js");
  ensureIncludes(rendererJs, "renderCommandHistory", "renderer.js");
  ensureIncludes(rendererJs, "renderWorkflowCards", "renderer.js");
  ensureIncludes(rendererJs, "data-focus-panel", "renderer.js");
  ensureIncludes(rendererJs, "renderWorkflowScope", "renderer.js");

  ensureIncludes(readme, "local_broker.py", "README.md");
  ensureIncludes(readme, "Next.js", "README.md");
  ensureIncludes(readme, "codex exec", "README.md");
  ensureIncludes(readme, "npm run dev", "README.md");
  ensureIncludes(readme, "npm run renderer:dev", "README.md");
  ensureIncludes(readme, "npm run launcher:install", "README.md");
  ensureIncludes(readme, "npm run smoke", "README.md");
  ensureIncludes(readme, "Recent Command Results", "README.md");
  ensureIncludes(readme, "Feishu bridge status", "README.md");
  const nextPage = read("renderer/app/page.js");
  ensureIncludes(nextPage, "Feishu", "renderer/app/page.js");
  ensureIncludes(nextPage, "会话", "renderer/app/page.js");
  ensureIncludes(nextPage, "时间线", "renderer/app/page.js");
  ensureIncludes(nextPage, "assistantCustomizationHint", "renderer/app/page.js");
  ensureIncludes(nextPage, "WORKSPACE_HUB_ASSISTANT_NAME", "renderer/app/page.js");
  ensureIncludes(nextPage, "工作台", "renderer/app/page.js");
  ensureIncludes(nextPage, "workspace-rail", "renderer/app/page.js");
  ensureIncludes(nextPage, "rail-brand", "renderer/app/page.js");
  ensureIncludes(nextPage, "飞书来源线程", "renderer/app/page.js");
  ensureIncludes(nextPage, "桌面对话和飞书来源线程在同一个侧栏里显示", "renderer/app/page.js");
  ensureIncludes(nextPage, "原生 Codex", "renderer/app/page.js");
  ensureIncludes(nextPage, "完全访问", "renderer/app/page.js");
  ensureIncludes(nextPage, "像聊天一样直接输入任务", "renderer/app/page.js");
  ensureIncludes(nextPage, "RailIcon", "renderer/app/page.js");
  ensureIncludes(nextPage, "getBridgeConversations", "renderer/app/page.js");
  ensureIncludes(nextPage, "getBridgeMessages", "renderer/app/page.js");

  console.log(
    JSON.stringify(
      {
        ok: true,
        checked_files: [
          "package.json",
          "main.js",
          "preload.js",
          "next.config.mjs",
          "renderer.js",
          "index.html",
          "README.md",
        ],
        validated_panels: [
          "overview",
          "command",
          "projects",
          "review",
          "coordination",
          "health",
        ],
      },
      null,
      2,
    ),
  );
}

main();
