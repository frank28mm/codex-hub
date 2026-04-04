"use strict";

const fs = require("node:fs");
const path = require("node:path");

function resolveFeishuBridgeRoot({ appRoot, workspaceRoot }) {
  const explicit = process.env.WORKSPACE_HUB_FEISHU_BRIDGE_ROOT;
  if (explicit) {
    return path.resolve(explicit);
  }
  const workspaceBridgeV2Root = path.join(workspaceRoot, "bridge", "feishu");
  if (fs.existsSync(path.join(workspaceBridgeV2Root, "index.js"))) {
    return workspaceBridgeV2Root;
  }
  const workspaceBridgeRoot = path.join(workspaceRoot, "bridge");
  if (fs.existsSync(path.join(workspaceBridgeRoot, "feishu_long_connection_service.js"))) {
    return workspaceBridgeRoot;
  }
  const worktreeParent = path.dirname(workspaceRoot);
  if (path.basename(worktreeParent) === "workspace-hub-worktrees") {
    const siblingFeishuV2 = path.join(worktreeParent, "feishu-bridge", "bridge", "feishu");
    if (fs.existsSync(path.join(siblingFeishuV2, "index.js"))) {
      return siblingFeishuV2;
    }
    const siblingFeishuBridge = path.join(worktreeParent, "feishu-bridge", "bridge");
    if (fs.existsSync(path.join(siblingFeishuBridge, "feishu_long_connection_service.js"))) {
      return siblingFeishuBridge;
    }
    const siblingMainBridgeV2 = path.join(path.dirname(worktreeParent), "workspace-hub", "bridge", "feishu");
    if (fs.existsSync(path.join(siblingMainBridgeV2, "index.js"))) {
      return siblingMainBridgeV2;
    }
    const siblingMainBridge = path.join(path.dirname(worktreeParent), "workspace-hub", "bridge");
    if (fs.existsSync(path.join(siblingMainBridge, "feishu_long_connection_service.js"))) {
      return siblingMainBridge;
    }
  }
  return path.join(appRoot, "bridge");
}

function resolveFeishuServiceModule({ appRoot, workspaceRoot }) {
  const bridgeRoot = resolveFeishuBridgeRoot({ appRoot, workspaceRoot });
  return {
    bridgeRoot,
    modulePath: fs.existsSync(path.join(bridgeRoot, "index.js"))
      ? path.join(bridgeRoot, "index.js")
      : path.join(bridgeRoot, "feishu_long_connection_service.js"),
  };
}

function safeJsonParse(text) {
  try {
    return JSON.parse(text);
  } catch (_error) {
    return null;
  }
}

function parseIsoTimestamp(value) {
  const parsed = Date.parse(String(value || "").trim());
  return Number.isNaN(parsed) ? Number.NaN : parsed;
}

function preferLatestIso(currentValue, nextValue) {
  const currentText = String(currentValue || "").trim();
  const nextText = String(nextValue || "").trim();
  if (!currentText) return nextText;
  if (!nextText) return currentText;
  const currentTs = parseIsoTimestamp(currentText);
  const nextTs = parseIsoTimestamp(nextText);
  if (!Number.isFinite(currentTs)) return nextText;
  if (!Number.isFinite(nextTs)) return currentText;
  return nextTs >= currentTs ? nextText : currentText;
}

function deriveLiveFreshness(statusPayload = {}) {
  const connectionStatus = String(statusPayload.connection_status || "").trim();
  if (connectionStatus !== "connected") {
    return {
      stale: false,
      event_stalled: false,
    };
  }
  const now = Date.now();
  const staleAfterSeconds = Math.max(0, Number(statusPayload.stale_after_seconds || 90));
  const eventIdleAfterSeconds = Math.max(0, Number(statusPayload.event_idle_after_seconds || 0));
  const heartbeatAt = parseIsoTimestamp(statusPayload.heartbeat_at || statusPayload.updated_at);
  const eventCandidates = [
    parseIsoTimestamp(statusPayload.last_event_at),
    parseIsoTimestamp(statusPayload.connected_at),
    parseIsoTimestamp(statusPayload.updated_at),
  ].filter((value) => Number.isFinite(value));
  const eventAt = eventCandidates.length ? Math.max(...eventCandidates) : Number.NaN;
  const stale =
    Number.isFinite(heartbeatAt) && staleAfterSeconds > 0
      ? (now - heartbeatAt) / 1000 > staleAfterSeconds
      : false;
  const eventStalled =
    Number.isFinite(eventAt) && eventIdleAfterSeconds > 0
      ? (now - eventAt) / 1000 > eventIdleAfterSeconds
      : false;
  return {
    stale: stale || eventStalled,
    event_stalled: eventStalled,
  };
}

function parseEnvFile(text) {
  const env = {};
  for (const raw of String(text || "").split(/\r?\n/)) {
    const line = raw.trim();
    if (!line || line.startsWith("#")) continue;
    const eq = line.indexOf("=");
    if (eq <= 0) continue;
    const key = line.slice(0, eq).trim();
    let value = line.slice(eq + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    env[key] = value;
  }
  return env;
}

function loadBootstrapEnv({ workspaceRoot }) {
  const explicit = process.env.WORKSPACE_HUB_FEISHU_ENV;
  const candidates = [
    explicit,
    path.join(workspaceRoot, "ops", "feishu_bridge.env.local"),
    path.join(workspaceRoot, ".env.feishu.local"),
  ].filter(Boolean);
  for (const filePath of candidates) {
    if (fs.existsSync(filePath)) {
      return {
        source: filePath,
        values: parseEnvFile(fs.readFileSync(filePath, "utf8")),
      };
    }
  }
  return { source: "", values: {} };
}

function buildBootstrapSettings({ workspaceRoot }) {
  const fileEnv = loadBootstrapEnv({ workspaceRoot });
  const values = { ...fileEnv.values };
  for (const key of [
    "FEISHU_APP_ID",
    "FEISHU_APP_SECRET",
    "FEISHU_DOMAIN",
    "FEISHU_ALLOWED_USERS",
    "FEISHU_GROUP_POLICY",
    "FEISHU_REQUIRE_MENTION",
  ]) {
    if (process.env[key]) {
      values[key] = process.env[key];
    }
  }
  const allowedUsers = String(values.FEISHU_ALLOWED_USERS || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  const settings = {
    app_id: String(values.FEISHU_APP_ID || "").trim(),
    app_secret: String(values.FEISHU_APP_SECRET || "").trim(),
    domain: String(values.FEISHU_DOMAIN || "feishu").trim() || "feishu",
    allowed_users: allowedUsers,
    group_policy: String(values.FEISHU_GROUP_POLICY || "mentions_only").trim() || "mentions_only",
    require_mention: String(values.FEISHU_REQUIRE_MENTION || "true").trim().toLowerCase() !== "false",
  };
  return {
    source: fileEnv.source || "process.env",
    hasCredentials: Boolean(settings.app_id && settings.app_secret),
    settings,
  };
}

function createBridgeHost({ appRoot, workspaceRoot, runBroker, logger = console, hostMode = "electron" }) {
  const moduleRef = resolveFeishuServiceModule({ appRoot, workspaceRoot });
  let service = null;
  let bootstrapSource = "";
  let lastBridgeStatus = {
    bridge: "feishu",
    connection_status: "disconnected",
    host_mode: hostMode,
    transport: "sdk_websocket_plus_rest",
    last_error: "",
    last_event_at: "",
    connected_at: "",
    recent_message_count: 0,
    recent_reply_count: 0,
    last_message_preview: "",
    last_sender_ref: "",
    heartbeat_at: "",
    stale_after_seconds: 90,
    event_idle_after_seconds: 300,
    updated_at: "",
    settings_summary: {},
  };

  async function brokerCall(command, payload = {}) {
    const args = [command];
    if (command === "bridge-settings" && payload.settings) {
      args.push("--bridge", "feishu");
      args.push("--settings-json", JSON.stringify(payload.settings));
    }
    if (command === "bridge-connection" && payload.connection) {
      args.push("--bridge", "feishu");
      args.push("--connection-json", JSON.stringify(payload.connection));
    }
    if (command === "bridge-status" || command === "bridge-settings" || command === "bridge-connection") {
      if (!args.includes("--bridge")) {
        args.push("--bridge", "feishu");
      }
    }
    if (command === "bridge-chat-binding") {
      args.push("--bridge", "feishu");
      args.push("--chat-ref", String(payload.chat_ref || ""));
      if (payload.binding_json) {
        args.push("--binding-json", JSON.stringify(payload.binding_json));
      }
    }
    if (command === "bridge-bindings") {
      args.push("--bridge", "feishu");
      if (payload.limit) {
        args.push("--limit", String(payload.limit));
      }
    }
    if (command === "bridge-execution-lease") {
      args.push("--bridge", "feishu");
      args.push("--conversation-key", String(payload.conversation_key || payload.conversationKey || ""));
      if (payload.lease_json) {
        args.push("--lease-json", JSON.stringify(payload.lease_json));
      }
    }
    if (command === "bridge-execution-leases") {
      args.push("--bridge", "feishu");
      if (payload.limit) {
        args.push("--limit", String(payload.limit));
      }
    }
    if (command === "approval-token") {
      if (payload.token) {
        args.push("--token", String(payload.token));
      }
      if (payload.token_json) {
        args.push("--token-json", JSON.stringify(payload.token_json));
      }
    }
    if (command === "approval-tokens") {
      if (payload.status) args.push("--status", String(payload.status));
      if (payload.scope) args.push("--scope", String(payload.scope));
      if (payload.limit) args.push("--limit", String(payload.limit));
    }
    if (command === "panel") {
      args.push("--name", String(payload.name || ""));
      if (payload.project_name) args.push("--project-name", String(payload.project_name));
    }
    if (command === "projects" || command === "review-inbox" || command === "coordination-inbox") {
      if (payload.project_name) args.push("--project-name", String(payload.project_name));
    }
    if (command === "material-suggest") {
      args.push("--project-name", String(payload.project_name || payload.projectName || ""));
      if (payload.prompt) args.push("--prompt", String(payload.prompt));
    }
    if (command === "codex-exec") {
      args.length = 0;
      args.push("command-center", "--action", "codex-exec", "--prompt", String(payload.prompt || ""));
      if (payload.project_name) args.push("--project-name", String(payload.project_name));
      if (payload.session_id) args.push("--session-id", String(payload.session_id));
      if (payload.execution_profile) args.push("--execution-profile", String(payload.execution_profile));
      if (payload.approval_token) args.push("--approval-token", String(payload.approval_token));
      if (payload.source) args.push("--source", String(payload.source));
      if (payload.chat_ref) args.push("--chat-ref", String(payload.chat_ref));
      if (payload.thread_name) args.push("--thread-name", String(payload.thread_name));
      if (payload.thread_label) args.push("--thread-label", String(payload.thread_label));
      if (payload.source_message_id) args.push("--source-message-id", String(payload.source_message_id));
    }
    if (command === "codex-resume") {
      args.length = 0;
      args.push("command-center", "--action", "codex-resume", "--session-id", String(payload.session_id || ""));
      if (payload.prompt) args.push("--prompt", String(payload.prompt));
      if (payload.project_name) args.push("--project-name", String(payload.project_name));
      if (payload.execution_profile) args.push("--execution-profile", String(payload.execution_profile));
      if (payload.approval_token) args.push("--approval-token", String(payload.approval_token));
      if (payload.source) args.push("--source", String(payload.source));
      if (payload.chat_ref) args.push("--chat-ref", String(payload.chat_ref));
      if (payload.thread_name) args.push("--thread-name", String(payload.thread_name));
      if (payload.thread_label) args.push("--thread-label", String(payload.thread_label));
      if (payload.source_message_id) args.push("--source-message-id", String(payload.source_message_id));
    }
    if (command === "feishu-op") {
      args.length = 0;
      args.push(
        "feishu-op",
        "--domain",
        String(payload.domain || ""),
        "--action",
        String(payload.action || ""),
        "--payload-json",
        JSON.stringify(payload.payload || {}),
      );
    }
    if (command === "feishu-callback-executor") {
      args.length = 0;
      args.push(
        "feishu-callback-executor",
        "--action",
        String(payload.action || ""),
        "--payload-json",
        payload.payload_json !== undefined
          ? String(payload.payload_json)
          : JSON.stringify(payload.payload || {}),
      );
    }
    if (command === "record-bridge-message") {
      args.length = 0;
      args.push(
        "record-bridge-message",
        "--bridge",
        "feishu",
        "--direction",
        String(payload.direction || "inbound"),
        "--message-id",
        String(payload.message_id || ""),
        "--status",
        String(payload.status || "received"),
      );
      if (payload.project_name) args.push("--project-name", String(payload.project_name));
      if (payload.session_id) args.push("--session-id", String(payload.session_id));
      if (payload.payload) args.push("--payload", JSON.stringify(payload.payload));
    }
    const response = await runBroker(args);
    if (!response.ok) {
      throw new Error(response.stderr || response.reason || `broker call failed: ${command}`);
    }
    return safeJsonParse(response.stdout) || response.data || {};
  }

  async function saveBridgeStatus(statusPayload) {
    const mergedStatus = {
      ...lastBridgeStatus,
      ...statusPayload,
      bridge: "feishu",
      updated_at: new Date().toISOString(),
    };
    lastBridgeStatus = {
      ...mergedStatus,
      connected_at: preferLatestIso(lastBridgeStatus.connected_at, mergedStatus.connected_at),
      last_event_at: preferLatestIso(lastBridgeStatus.last_event_at, mergedStatus.last_event_at),
      heartbeat_at: preferLatestIso(lastBridgeStatus.heartbeat_at, mergedStatus.heartbeat_at),
      recent_message_count: Math.max(
        Number(lastBridgeStatus.recent_message_count || 0),
        Number(mergedStatus.recent_message_count || 0),
      ),
      recent_reply_count: Math.max(
        Number(lastBridgeStatus.recent_reply_count || 0),
        Number(mergedStatus.recent_reply_count || 0),
      ),
    };
    await brokerCall("bridge-connection", {
      connection: {
        status: lastBridgeStatus.connection_status,
        host_mode: lastBridgeStatus.host_mode || hostMode,
        transport: lastBridgeStatus.transport || "sdk_websocket_plus_rest",
        last_error: lastBridgeStatus.last_error || "",
        last_event_at: lastBridgeStatus.last_event_at || "",
        metadata: {
          settings_summary: lastBridgeStatus.settings_summary || {},
          connected_at: lastBridgeStatus.connected_at || "",
          recent_message_count: Number(lastBridgeStatus.recent_message_count || 0),
          recent_reply_count: Number(lastBridgeStatus.recent_reply_count || 0),
          last_message_preview: lastBridgeStatus.last_message_preview || "",
          last_sender_ref: lastBridgeStatus.last_sender_ref || "",
          heartbeat_at: lastBridgeStatus.heartbeat_at || "",
          stale_after_seconds: Number(lastBridgeStatus.stale_after_seconds || 90),
          event_idle_after_seconds: Number(lastBridgeStatus.event_idle_after_seconds || 300),
          backfill_degraded: Boolean(lastBridgeStatus.backfill_degraded),
          backfill_degraded_count: Number(lastBridgeStatus.backfill_degraded_count || 0),
          last_backfill_error: lastBridgeStatus.last_backfill_error || "",
          last_backfill_error_at: lastBridgeStatus.last_backfill_error_at || "",
          bridge_root: moduleRef.bridgeRoot,
          module_path: moduleRef.modulePath,
        },
      },
    });
    return lastBridgeStatus;
  }

  async function saveBridgeSettings(settingsPayload) {
    return brokerCall("bridge-settings", { settings: settingsPayload });
  }

  async function saveBridgeExecutionLease(leasePayload) {
    return brokerCall("bridge-execution-lease", {
      conversation_key:
        leasePayload.conversation_key || leasePayload.conversationKey || "",
      lease_json: leasePayload,
    });
  }

  async function fetchBridgeExecutionLease({ conversationKey = "" } = {}) {
    return brokerCall("bridge-execution-lease", {
      conversation_key: conversationKey,
    });
  }

  async function ensureService() {
    if (service) return service;
    if (!fs.existsSync(moduleRef.modulePath)) {
      throw new Error(`Feishu bridge service not found: ${moduleRef.modulePath}`);
    }
    const factoryModule = require(moduleRef.modulePath);
    if (!factoryModule || typeof factoryModule.createFeishuLongConnectionService !== "function") {
      throw new Error("Feishu bridge module missing createFeishuLongConnectionService export");
    }
    const settingsPayload = await brokerCall("bridge-settings");
    const brokerSettings = settingsPayload.settings || {};
    const bootstrap = buildBootstrapSettings({ workspaceRoot });
    let effectiveSettings = brokerSettings;
    if (!brokerSettings.app_id && !brokerSettings.app_secret && bootstrap.hasCredentials) {
      bootstrapSource = bootstrap.source;
      await brokerCall("bridge-settings", { settings: bootstrap.settings });
      effectiveSettings = bootstrap.settings;
    }
    service = factoryModule.createFeishuLongConnectionService({
      brokerClient: {
        call: async (command, payload) => brokerCall(command, payload),
      },
      runtimeState: {
        saveBridgeStatus,
        saveBridgeSettings,
        saveBridgeExecutionLease,
        fetchBridgeExecutionLease,
      },
      logger,
    });
    await service.loadSettings(effectiveSettings);
    return service;
  }

  async function getStatus() {
    const brokerStatus = await brokerCall("bridge-status");
    const liveServiceStatus = service && typeof service.getStatus === "function" ? service.getStatus() : {};
    lastBridgeStatus = {
      ...lastBridgeStatus,
      ...brokerStatus,
      ...liveServiceStatus,
    };
    if (liveServiceStatus && Object.keys(liveServiceStatus).length) {
      lastBridgeStatus = {
        ...lastBridgeStatus,
        ...deriveLiveFreshness(lastBridgeStatus),
      };
    }
    if (bootstrapSource && !lastBridgeStatus.bootstrap_source) {
      lastBridgeStatus.bootstrap_source = bootstrapSource;
    }
    return {
      ok: true,
      data: lastBridgeStatus,
      bridge_root: moduleRef.bridgeRoot,
      module_path: moduleRef.modulePath,
    };
  }

  async function getSettings() {
    const brokerSettings = await brokerCall("bridge-settings");
    return {
      ok: true,
      data: brokerSettings,
      bridge_root: moduleRef.bridgeRoot,
      module_path: moduleRef.modulePath,
    };
  }

  async function updateSettings(settings) {
    const serviceInstance = await ensureService();
    await serviceInstance.loadSettings(settings);
    const payload = await brokerCall("bridge-settings", { settings });
    return {
      ok: true,
      data: payload,
      bridge_root: moduleRef.bridgeRoot,
      module_path: moduleRef.modulePath,
    };
  }

  async function connect() {
    const serviceInstance = await ensureService();
    const result = await serviceInstance.connect();
    const currentStatus = await getStatus();
    return { ok: Boolean(result.ok), data: currentStatus.data, bridge_root: moduleRef.bridgeRoot };
  }

  async function disconnect() {
    if (!service) {
      await saveBridgeStatus({ connection_status: "disconnected", last_error: "" });
      return getStatus();
    }
    const result = await service.disconnect();
    const currentStatus = await getStatus();
    return { ok: Boolean(result.ok), data: currentStatus.data, bridge_root: moduleRef.bridgeRoot };
  }

  async function reconnect() {
    const serviceInstance = await ensureService();
    const result = await serviceInstance.reconnect();
    const currentStatus = await getStatus();
    return { ok: Boolean(result.ok), data: currentStatus.data, bridge_root: moduleRef.bridgeRoot };
  }

  async function sendMessage({ chatRef = "", openId = "", text = "", phase = "report" } = {}) {
    const serviceInstance = await ensureService();
    const result = await serviceInstance.sendMessage({
      chatId: String(chatRef || "").trim(),
      openId: String(openId || "").trim(),
      text,
      phase,
    });
    const nextStatus = await getStatus();
    return {
      ok: Boolean(result?.ok),
      data: nextStatus.data,
      result: result || {},
      bridge_root: moduleRef.bridgeRoot,
    };
  }

  return {
    getStatus,
    getSettings,
    updateSettings,
    connect,
    disconnect,
    reconnect,
    sendMessage,
  };
}

module.exports = {
  createBridgeHost,
  resolveFeishuBridgeRoot,
  resolveFeishuServiceModule,
};
