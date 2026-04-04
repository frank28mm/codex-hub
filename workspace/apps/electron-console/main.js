const fs = require("node:fs");
const { app, BrowserWindow, ipcMain, shell } = require("electron");
const path = require("node:path");
const { spawn } = require("node:child_process");
const {
  assistantCustomizationHint,
  assistantName,
  assistantPrivateThreadLabel,
  assistantServiceLabel,
} = require("../../assistant-branding");
const { createBridgeHost } = require("./bridge-host");

const APP_ROOT = __dirname;
const CONSOLE_WORKSPACE_ROOT = path.resolve(APP_ROOT, "..", "..");
const SHARED_WORKSPACE_ROOT = resolveSharedWorkspaceRoot();
const SHARED_RUNTIME_ROOT = resolveSharedRuntimeRoot();
const NODE_EXECUTABLE = resolveNodeExecutable();
const BROKER_PATH = path.join(SHARED_WORKSPACE_ROOT, "ops", "local_broker.py");
const RENDERER_ROOT = path.join(APP_ROOT, "renderer");
const ELECTRON_SINGLE_INSTANCE = app.requestSingleInstanceLock();
const bridgeHost = createBridgeHost({
  appRoot: APP_ROOT,
  workspaceRoot: CONSOLE_WORKSPACE_ROOT,
  runBroker,
});

function resolveSharedWorkspaceRoot() {
  const envOverride = process.env.WORKSPACE_HUB_SHARED_ROOT || process.env.WORKSPACE_HUB_ROOT;
  if (envOverride) {
    const overrideRoot = path.resolve(envOverride);
    if (fs.existsSync(path.join(overrideRoot, "ops", "local_broker.py"))) {
      return overrideRoot;
    }
  }

  const localBrokerPath = path.join(CONSOLE_WORKSPACE_ROOT, "ops", "local_broker.py");
  if (fs.existsSync(localBrokerPath)) {
    return CONSOLE_WORKSPACE_ROOT;
  }

  const worktreeParent = path.dirname(CONSOLE_WORKSPACE_ROOT);
  if (path.basename(worktreeParent) === "workspace-hub-worktrees") {
    const siblingCoreRoot = path.join(worktreeParent, "core-v1-0-3-to-v1-0-5");
    if (fs.existsSync(path.join(siblingCoreRoot, "ops", "local_broker.py"))) {
      return siblingCoreRoot;
    }
    const siblingMainlineRoot = path.join(path.dirname(worktreeParent), "workspace-hub");
    if (fs.existsSync(path.join(siblingMainlineRoot, "ops", "local_broker.py"))) {
      return siblingMainlineRoot;
    }
  }

  return CONSOLE_WORKSPACE_ROOT;
}

function resolveSharedRuntimeRoot() {
  const explicit = process.env.WORKSPACE_HUB_RUNTIME_ROOT;
  if (explicit) {
    return path.resolve(explicit);
  }

  const worktreeParent = path.dirname(CONSOLE_WORKSPACE_ROOT);
  if (path.basename(worktreeParent) === "workspace-hub-worktrees") {
    const siblingMainRoot = path.join(path.dirname(worktreeParent), "workspace-hub");
    const siblingMainRuntime = path.join(siblingMainRoot, "runtime");
    if (fs.existsSync(siblingMainRuntime)) {
      return siblingMainRuntime;
    }
  }

  return path.join(SHARED_WORKSPACE_ROOT, "runtime");
}

function resolveNodeExecutable() {
  const candidates = [
    process.env.WORKSPACE_HUB_NODE_PATH,
    process.env.npm_node_execpath,
    "/opt/homebrew/bin/node",
    "/opt/homebrew/Cellar/node/25.1.0/bin/node",
    "/usr/local/bin/node",
  ].filter(Boolean);

  for (const candidate of candidates) {
    if (path.isAbsolute(candidate) && fs.existsSync(candidate)) {
      return candidate;
    }
  }

  return "node";
}

function createWindow() {
  const window = new BrowserWindow({
    width: 1440,
    height: 960,
    minWidth: 1180,
    minHeight: 760,
    autoHideMenuBar: true,
    backgroundColor: "#07111f",
    webPreferences: {
      preload: path.join(APP_ROOT, "preload.js"),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });
  const rendererUrl = resolveRendererUrl();
  if (rendererUrl) {
    window.loadURL(rendererUrl);
  } else {
    window.loadFile(path.join(APP_ROOT, "index.html"));
  }
  return window;
}

function resolveRendererUrl() {
  const explicit = process.env.WORKSPACE_HUB_RENDERER_URL || process.env.NEXT_DEV_SERVER_URL;
  if (explicit) {
    return explicit;
  }
  if (process.env.WORKSPACE_HUB_RENDERER_MODE === "next-dev") {
    return "http://127.0.0.1:3310";
  }
  return "";
}

function runProcess(command, args, options = {}) {
  return new Promise((resolve) => {
    const child = spawn(command, args, {
      cwd: options.cwd || CONSOLE_WORKSPACE_ROOT,
      env: { ...process.env, ...(options.env || {}) },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("close", (code) => {
      resolve({ ok: code === 0, code, stdout, stderr });
    });
    child.on("error", (error) => {
      resolve({ ok: false, code: -1, stdout, stderr: error.message });
    });
  });
}

async function runBroker(args) {
  const brokerExists = await shellCommandExists("python3");
  if (!brokerExists) {
    return {
      ok: false,
      unavailable: true,
      reason: "python3 not found",
      command: ["python3", BROKER_PATH, ...args],
    };
  }
  const brokerPresent = await pathExists(BROKER_PATH);
  if (!brokerPresent) {
    return {
      ok: false,
      unavailable: true,
      reason: "local_broker.py not found yet",
      command: ["python3", BROKER_PATH, ...args],
    };
  }
  return runProcess("python3", [BROKER_PATH, ...args], {
    cwd: SHARED_WORKSPACE_ROOT,
    env: {
      WORKSPACE_HUB_ROOT: SHARED_WORKSPACE_ROOT,
      WORKSPACE_HUB_RUNTIME_ROOT: SHARED_RUNTIME_ROOT,
    },
  });
}

async function runCoCoService(command) {
  const servicePath = path.join(APP_ROOT, "coco-bridge-service.js");
  const response = await runProcess(NODE_EXECUTABLE, [servicePath, command], {
    cwd: APP_ROOT,
    env: {
      WORKSPACE_HUB_SHARED_ROOT: SHARED_WORKSPACE_ROOT,
      WORKSPACE_HUB_ROOT: SHARED_WORKSPACE_ROOT,
      WORKSPACE_HUB_RUNTIME_ROOT: SHARED_RUNTIME_ROOT,
    },
  });
  if (!response.ok) {
    return {
      ok: false,
      stderr: response.stderr || "",
      stdout: response.stdout || "",
      command: [NODE_EXECUTABLE, servicePath, command],
    };
  }
  return {
    ok: true,
    stdout: response.stdout,
    data: safeJsonParse(response.stdout),
  };
}

async function runLauncher(command) {
  const launcherPath = path.join(APP_ROOT, "install-launcher.js");
  const response = await runProcess(NODE_EXECUTABLE, [launcherPath, command], {
    cwd: APP_ROOT,
    env: {
      WORKSPACE_HUB_SHARED_ROOT: SHARED_WORKSPACE_ROOT,
      WORKSPACE_HUB_ROOT: SHARED_WORKSPACE_ROOT,
      WORKSPACE_HUB_RUNTIME_ROOT: SHARED_RUNTIME_ROOT,
    },
  });
  if (!response.ok) {
    return {
      ok: false,
      stderr: response.stderr || "",
      stdout: response.stdout || "",
      command: [NODE_EXECUTABLE, launcherPath, command],
    };
  }
  return {
    ok: true,
    stdout: response.stdout,
    data: safeJsonParse(response.stdout),
  };
}

function pathExists(targetPath) {
  return Promise.resolve(fs.existsSync(targetPath));
}

async function shellCommandExists(command) {
  const result = await runProcess("bash", ["-lc", `command -v ${command}`], { cwd: CONSOLE_WORKSPACE_ROOT });
  return result.ok;
}

function safeJsonParse(text) {
  try {
    return JSON.parse(text);
  } catch (_error) {
    return null;
  }
}

function fallbackPanel(panel) {
  const shared = {
    broker_status: "pending",
    note: "Shared broker unavailable. This panel is showing placeholder data until the broker can be reached.",
  };
  if (panel === "overview") {
    return {
      ...shared,
      cards: [
        { label: "Active Projects", value: "7" },
        { label: "Pending Reviews", value: "0" },
        { label: "Open Coordination", value: "0" },
        { label: "Health Alerts", value: "0" },
      ],
    };
  }
  if (panel === "projects") {
    return {
      ...shared,
      rows: [
        { project_name: "Codex Obsidian记忆与行动系统", status: "active", priority: "high", next_action: "Wire local broker" },
      ],
    };
  }
  if (panel === "review") {
    return { ...shared, rows: [] };
  }
  if (panel === "coordination") {
    return { ...shared, rows: [] };
  }
  return { ...shared, alerts: [] };
}

ipcMain.handle("app:metadata", async () => {
  const codexExists = await shellCommandExists("codex");
  const brokerPresent = await pathExists(BROKER_PATH);
  const modelResponse = await runBroker(["codex-models"]);
  const codexModelSettings = modelResponse.ok ? safeJsonParse(modelResponse.stdout) : null;
  return {
    app_name: "workspace-hub-electron-console",
    assistant_name: assistantName(),
    assistant_private_thread_label: assistantPrivateThreadLabel(),
    assistant_service_label: assistantServiceLabel(),
    assistant_customization_hint: assistantCustomizationHint(),
    workspace_root: CONSOLE_WORKSPACE_ROOT,
    broker_workspace_root: SHARED_WORKSPACE_ROOT,
    broker_path: BROKER_PATH,
    renderer_root: RENDERER_ROOT,
    renderer_mode: resolveRendererUrl() ? "next-dev" : "legacy-html",
    codex_available: codexExists,
    broker_available: brokerPresent,
    broker_mode: SHARED_WORKSPACE_ROOT === CONSOLE_WORKSPACE_ROOT ? "local" : "shared-mainline",
    codex_commands: ["codex exec", "codex resume", "codex app"],
    codex_model_settings: codexModelSettings,
  };
});

ipcMain.handle("broker:codex-models", async (_event, payload) => {
  const args = ["codex-models"];
  if (payload?.settings && typeof payload.settings === "object") {
    args.push("--settings-json", JSON.stringify(payload.settings));
  }
  const response = await runBroker(args);
  if (!response.ok) {
    return {
      ok: false,
      stderr: response.stderr || response.reason || "",
      unavailable: Boolean(response.unavailable),
      command: response.command || ["python3", BROKER_PATH, ...args],
    };
  }
  return {
    ok: true,
    stdout: response.stdout,
    data: safeJsonParse(response.stdout),
  };
});

ipcMain.handle("broker:panel", async (_event, request) => {
  const panel = typeof request === "string" ? request : request?.panelName || request?.name;
  const projectName = typeof request === "object" && request ? String(request.projectName || "").trim() : "";
  const args = ["panel", "--name", String(panel || "")];
  if (projectName) {
    args.push("--project-name", projectName);
  }
  const response = await runBroker(args);
  if (!response.ok) {
    return {
      ok: false,
      data: fallbackPanel(String(panel)),
      stderr: response.stderr || response.reason || "",
      unavailable: Boolean(response.unavailable),
      command: response.command || ["python3", BROKER_PATH, ...args],
    };
  }
  return {
    ok: true,
    data: safeJsonParse(response.stdout) || fallbackPanel(String(panel)),
    stdout: response.stdout,
  };
});

ipcMain.handle("broker:bridge-status", async (_event, payload) => {
  const bridge = String(payload?.bridge || "feishu").trim() || "feishu";
  if (bridge === "feishu") {
    try {
      return await bridgeHost.getStatus();
    } catch (error) {
      return {
        ok: false,
        error: String(error?.message || error || "bridge_status_failed"),
      };
    }
  }
  const args = ["bridge-status", "--bridge", bridge];
  const response = await runBroker(args);
  if (!response.ok) {
    return {
      ok: false,
      stderr: response.stderr || response.reason || "",
      unavailable: Boolean(response.unavailable),
      command: response.command || ["python3", BROKER_PATH, ...args],
    };
  }
  return {
    ok: true,
    stdout: response.stdout,
    data: safeJsonParse(response.stdout),
  };
});

ipcMain.handle("broker:bridge-settings", async (_event, payload) => {
  const bridge = String(payload?.bridge || "feishu").trim() || "feishu";
  if (bridge === "feishu") {
    try {
      if (payload?.settings) {
        return await bridgeHost.updateSettings(payload.settings);
      }
      return await bridgeHost.getSettings();
    } catch (error) {
      return {
        ok: false,
        error: String(error?.message || error || "bridge_settings_failed"),
      };
    }
  }
  const args = ["bridge-settings", "--bridge", bridge];
  if (payload?.settings) {
    args.push("--settings-json", JSON.stringify(payload.settings));
  }
  const response = await runBroker(args);
  if (!response.ok) {
    return {
      ok: false,
      stderr: response.stderr || response.reason || "",
      unavailable: Boolean(response.unavailable),
      command: response.command || ["python3", BROKER_PATH, ...args],
    };
  }
  return {
    ok: true,
    stdout: response.stdout,
    data: safeJsonParse(response.stdout),
  };
});

ipcMain.handle("broker:bridge-conversations", async (_event, payload) => {
  const bridge = String(payload?.bridge || "feishu").trim() || "feishu";
  const limit = Number(payload?.limit || 50);
  const args = ["bridge-conversations", "--bridge", bridge, "--limit", String(limit)];
  const response = await runBroker(args);
  if (!response.ok) {
    return {
      ok: false,
      stderr: response.stderr || response.reason || "",
      unavailable: Boolean(response.unavailable),
      command: response.command || ["python3", BROKER_PATH, ...args],
    };
  }
  return {
    ok: true,
    stdout: response.stdout,
    data: safeJsonParse(response.stdout),
  };
});

ipcMain.handle("broker:user-profile", async (_event, payload) => {
  const args = ["user-profile"];
  if (payload?.profile) {
    let profileJson = "";
    try {
      profileJson = JSON.stringify(payload.profile);
    } catch (error) {
      return {
        ok: false,
        error: `invalid profile payload: ${String(error?.message || error)}`,
      };
    }
    args.push("--profile-json", profileJson);
  }
  const response = await runBroker(args);
  if (!response.ok) {
    return {
      ok: false,
      stderr: response.stderr || response.reason || "",
      unavailable: Boolean(response.unavailable),
      command: response.command || ["python3", BROKER_PATH, ...args],
    };
  }
  return {
    ok: true,
    stdout: response.stdout,
    data: safeJsonParse(response.stdout),
  };
});

ipcMain.handle("broker:material-suggest", async (_event, payload) => {
  const projectName = String(payload?.projectName || payload?.project_name || "").trim();
  const prompt = String(payload?.prompt || "").trim();
  if (!projectName) {
    return { ok: false, error: "missing project name" };
  }
  const args = ["material-suggest", "--project-name", projectName];
  if (prompt) {
    args.push("--prompt", prompt);
  }
  const response = await runBroker(args);
  if (!response.ok) {
    return {
      ok: false,
      stderr: response.stderr || response.reason || "",
      unavailable: Boolean(response.unavailable),
      command: response.command || ["python3", BROKER_PATH, ...args],
    };
  }
  return {
    ok: true,
    stdout: response.stdout,
    data: safeJsonParse(response.stdout),
  };
});

ipcMain.handle("broker:bridge-messages", async (_event, payload) => {
  const bridge = String(payload?.bridge || "feishu").trim() || "feishu";
  const chatRef = String(payload?.chatRef || payload?.chat_ref || "").trim();
  const limit = Number(payload?.limit || 100);
  const args = ["bridge-messages", "--bridge", bridge, "--limit", String(limit)];
  if (chatRef) {
    args.push("--chat-ref", chatRef);
  }
  const response = await runBroker(args);
  if (!response.ok) {
    return {
      ok: false,
      stderr: response.stderr || response.reason || "",
      unavailable: Boolean(response.unavailable),
      command: response.command || ["python3", BROKER_PATH, ...args],
    };
  }
  return {
    ok: true,
    stdout: response.stdout,
    data: safeJsonParse(response.stdout),
  };
});

ipcMain.handle("bridge:connect", async (_event, payload) => {
  if (String(payload?.bridge || "feishu").trim() !== "feishu") {
    return { ok: false, error: "unsupported bridge" };
  }
  try {
    return await bridgeHost.connect();
  } catch (error) {
    return { ok: false, error: String(error?.message || error || "bridge_connect_failed") };
  }
});

ipcMain.handle("bridge:disconnect", async (_event, payload) => {
  if (String(payload?.bridge || "feishu").trim() !== "feishu") {
    return { ok: false, error: "unsupported bridge" };
  }
  try {
    return await bridgeHost.disconnect();
  } catch (error) {
    return { ok: false, error: String(error?.message || error || "bridge_disconnect_failed") };
  }
});

ipcMain.handle("bridge:reconnect", async (_event, payload) => {
  if (String(payload?.bridge || "feishu").trim() !== "feishu") {
    return { ok: false, error: "unsupported bridge" };
  }
  try {
    return await bridgeHost.reconnect();
  } catch (error) {
    return { ok: false, error: String(error?.message || error || "bridge_reconnect_failed") };
  }
});

ipcMain.handle("service:coco-status", async () => runCoCoService("status"));

ipcMain.handle("service:coco-install", async () => runCoCoService("install-launchagent"));

ipcMain.handle("service:coco-restart", async () => runCoCoService("restart-launchagent"));

ipcMain.handle("service:coco-uninstall", async () => runCoCoService("uninstall-launchagent"));

ipcMain.handle("service:coco-verify", async () => runCoCoService("verify-persistence"));

ipcMain.handle("launcher:status", async () => runLauncher("status"));

ipcMain.handle("launcher:install", async () => runLauncher("install"));

ipcMain.handle("launcher:uninstall", async () => runLauncher("uninstall"));

ipcMain.handle("broker:command-center", async (_event, payload) => {
  const action = String(payload?.action || "");
  if (!action) {
    return { ok: false, error: "missing action" };
  }
  const args = ["command-center", "--action", action];
  if (action === "codex-exec" || action === "codex-resume") {
    const requestedProfile = String(payload?.execution_profile || "").trim();
    const accessMode = String(payload?.access_mode || "").trim();
    const executionProfile =
      requestedProfile || (accessMode === "full" ? "electron-full-access" : "electron");
    args.push("--execution-profile", executionProfile);
  }
  const projectName = String(payload?.project_name || "").trim();
  const sessionId = String(payload?.session_id || "").trim();
  const prompt = String(payload?.prompt || "").trim();
  const model = String(payload?.model || "").trim();
  const reasoningEffort = String(payload?.reasoning_effort || "").trim();
  const source = String(payload?.source || "").trim();
  const chatRef = String(payload?.chat_ref || "").trim();
  const threadName = String(payload?.thread_name || "").trim();
  const threadLabel = String(payload?.thread_label || "").trim();
  const sourceMessageId = String(payload?.source_message_id || "").trim();
  if (projectName) args.push("--project-name", projectName);
  if (sessionId) args.push("--session-id", sessionId);
  if (prompt) args.push("--prompt", prompt);
  if (model) args.push("--model", model);
  if (reasoningEffort) args.push("--reasoning-effort", reasoningEffort);
  if (source) args.push("--source", source);
  if (chatRef) args.push("--chat-ref", chatRef);
  if (threadName) args.push("--thread-name", threadName);
  if (threadLabel) args.push("--thread-label", threadLabel);
  if (sourceMessageId) args.push("--source-message-id", sourceMessageId);

  const response = await runBroker(args);
  if (!response.ok) {
    if (action === "open-codex-app" && response.unavailable) {
      const fallback = await runProcess("codex", ["app", SHARED_WORKSPACE_ROOT], {
        cwd: SHARED_WORKSPACE_ROOT,
      });
      return {
        ok: fallback.ok,
        stdout: fallback.stdout,
        stderr: fallback.stderr,
        fallback: true,
        data: {
          broker_action: "open_codex_app_fallback",
          command: ["codex", "app", SHARED_WORKSPACE_ROOT],
          returncode: fallback.code,
          stdout: fallback.stdout,
          stderr: fallback.stderr,
        },
      };
    }
    return {
      ok: false,
      stderr: response.stderr || response.reason || "",
      unavailable: Boolean(response.unavailable),
      command: response.command || ["python3", BROKER_PATH, ...args],
    };
  }
  return {
    ok: true,
    stdout: response.stdout,
    data: safeJsonParse(response.stdout),
  };
});

ipcMain.handle("app:open-path", async (_event, targetPath) => {
  if (!targetPath) {
    return { ok: false, error: "missing path" };
  }
  await shell.openPath(String(targetPath));
  return { ok: true };
});

if (!ELECTRON_SINGLE_INSTANCE) {
  app.quit();
} else {
  app.on("second-instance", () => {
    const existingWindow = BrowserWindow.getAllWindows()[0];
    if (!existingWindow) return;
    if (existingWindow.isMinimized()) {
      existingWindow.restore();
    }
    existingWindow.focus();
  });

  app.whenReady().then(() => {
    createWindow();
    app.on("activate", () => {
      if (BrowserWindow.getAllWindows().length === 0) {
        createWindow();
      }
    });
  });
}

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") {
    app.quit();
  }
});
