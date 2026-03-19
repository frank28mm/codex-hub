#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawn, spawnSync } = require("node:child_process");
const { createBridgeHost } = require("./bridge-host");

const AGENT_NAME = "com.codexhub.coco-feishu-bridge";
const APP_ROOT = __dirname;
const CONSOLE_WORKSPACE_ROOT = path.resolve(APP_ROOT, "..", "..");
const SHARED_WORKSPACE_ROOT = resolveSharedWorkspaceRoot();
const SHARED_RUNTIME_ROOT = resolveSharedRuntimeRoot();
const FEISHU_BRIDGE_ROOT = resolveFeishuBridgeRoot();
const LOG_DIR = path.join(SHARED_WORKSPACE_ROOT, "logs");
const LOG_STDOUT = path.join(LOG_DIR, "coco-feishu-bridge.log");
const LOG_STDERR = path.join(LOG_DIR, "coco-feishu-bridge.err.log");
const LAUNCH_AGENT_PLIST = path.join(os.homedir(), "Library", "LaunchAgents", `${AGENT_NAME}.plist`);
const SERVICE_STATE_PATH = path.join(SHARED_RUNTIME_ROOT, "coco-service-state.json");
const HEARTBEAT_CHECK_INTERVAL_MS = 12_000;
const ACK_STALLED_AFTER_SECONDS = 75;

function resolveSharedWorkspaceRoot() {
  const envOverride = process.env.WORKSPACE_HUB_SHARED_ROOT || process.env.WORKSPACE_HUB_ROOT;
  if (envOverride) {
    const overrideRoot = path.resolve(envOverride);
    if (fs.existsSync(path.join(overrideRoot, "ops", "local_broker.py"))) {
      return overrideRoot;
    }
  }
  const worktreeParent = path.dirname(CONSOLE_WORKSPACE_ROOT);
  if (path.basename(worktreeParent) === "workspace-hub-worktrees") {
    const siblingCoreRoot = path.join(worktreeParent, "core-v1-0-3-to-v1-0-5");
    if (fs.existsSync(path.join(siblingCoreRoot, "ops", "local_broker.py"))) {
      return siblingCoreRoot;
    }
    const siblingMainRoot = path.join(path.dirname(worktreeParent), "workspace-hub");
    if (fs.existsSync(path.join(siblingMainRoot, "ops", "local_broker.py"))) {
      return siblingMainRoot;
    }
  }
  return CONSOLE_WORKSPACE_ROOT;
}

function resolveFeishuBridgeRoot() {
  const explicit = process.env.WORKSPACE_HUB_FEISHU_BRIDGE_ROOT;
  if (explicit) {
    return path.resolve(explicit);
  }
  const sharedBridgeV2Root = path.join(SHARED_WORKSPACE_ROOT, "bridge", "feishu");
  if (fs.existsSync(path.join(sharedBridgeV2Root, "index.js"))) {
    return sharedBridgeV2Root;
  }
  const sharedBridgeRoot = path.join(SHARED_WORKSPACE_ROOT, "bridge");
  if (fs.existsSync(path.join(sharedBridgeRoot, "feishu_long_connection_service.js"))) {
    return sharedBridgeRoot;
  }
  const worktreeParent = path.dirname(CONSOLE_WORKSPACE_ROOT);
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
  return path.join(APP_ROOT, "bridge");
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

function ensureDir(targetPath) {
  fs.mkdirSync(targetPath, { recursive: true });
}

function readServiceState() {
  try {
    if (!fs.existsSync(SERVICE_STATE_PATH)) {
      return {};
    }
    const payload = JSON.parse(fs.readFileSync(SERVICE_STATE_PATH, "utf8"));
    return payload && typeof payload === "object" ? payload : {};
  } catch (_error) {
    return {};
  }
}

function writeServiceState(nextState) {
  ensureDir(path.dirname(SERVICE_STATE_PATH));
  const payload = decorateServiceState({
    ...nextState,
    updated_at: new Date().toISOString(),
  });
  fs.writeFileSync(SERVICE_STATE_PATH, JSON.stringify(payload, null, 2));
  return payload;
}

function mergeServiceState(patch) {
  return writeServiceState({
    ...readServiceState(),
    ...patch,
  });
}

function parseTimestamp(value) {
  if (!value) return Number.NaN;
  const parsed = Date.parse(String(value));
  return Number.isNaN(parsed) ? Number.NaN : parsed;
}

function deriveAckStalledState(data = {}) {
  const executionState = String(data.last_execution_state || "").trim();
  const pendingAckAt = parseTimestamp(data.pending_ack_at);
  if (!Number.isFinite(pendingAckAt) || executionState !== "running") {
    return {
      ack_pending: false,
      ack_pending_age_seconds: 0,
      ack_stalled: false,
    };
  }
  const ageSeconds = Math.max(0, Math.round((Date.now() - pendingAckAt) / 1000));
  return {
    ack_pending: true,
    ack_pending_age_seconds: ageSeconds,
    ack_stalled: ageSeconds >= ACK_STALLED_AFTER_SECONDS,
  };
}

function preferCurrentValue(currentValue, previousValue) {
  const normalizedCurrent = String(currentValue || "").trim();
  if (normalizedCurrent) {
    return normalizedCurrent;
  }
  return String(previousValue || "").trim();
}

function preferCurrentNumber(currentValue, previousValue) {
  if (Number.isFinite(currentValue) && currentValue > 0) {
    return Number(currentValue);
  }
  if (Number.isFinite(previousValue) && previousValue > 0) {
    return Number(previousValue);
  }
  return 0;
}

function summarizeThreadSnapshot(state = {}) {
  const active = Number(state.active_threads || 0);
  const running = Number(state.running_threads || 0);
  const approvals = Number(state.approval_pending_threads || 0);
  const attention = Number(state.attention_threads || 0);
  const admins = Number(state.workspace_admin_threads || 0);
  const parts = [`活跃 ${active}`];
  if (running > 0) parts.push(`运行中 ${running}`);
  if (approvals > 0) parts.push(`待授权 ${approvals}`);
  if (attention > 0) parts.push(`需处理 ${attention}`);
  if (admins > 0) parts.push(`管理线程 ${admins}`);
  return parts.join(" · ");
}

function summarizeRecovery(state = {}) {
  if (!state.last_recovery_at) {
    return "";
  }
  const mismatchCount = Array.isArray(state.last_recovery_mismatches)
    ? state.last_recovery_mismatches.length
    : 0;
  const duration = state.last_recovery_duration_ms ? `${state.last_recovery_duration_ms} ms` : "耗时未记录";
  if (state.last_recovery_ok) {
    return mismatchCount > 0
      ? `最近自动恢复完成，但仍发现 ${mismatchCount} 处线程不一致（${duration}）`
      : `最近自动恢复成功，已核对 ${Number(state.last_recovery_compared_threads || 0)} 条线程（${duration}）`;
  }
  return `最近自动恢复失败：${String(state.last_recovery_reason || state.last_recovery_error || "原因未记录")}（${duration}）`;
}

function summarizePersistence(state = {}) {
  if (!state.last_persistence_check_at) {
    return "";
  }
  const mismatchCount = Array.isArray(state.last_persistence_mismatches)
    ? state.last_persistence_mismatches.length
    : 0;
  const duration = state.last_persistence_duration_ms ? `${state.last_persistence_duration_ms} ms` : "耗时未记录";
  if (state.last_persistence_ok) {
    return `最近持久化校验通过，已比对 ${Number(state.last_persistence_compared_threads || 0)} 条线程（${duration}）`;
  }
  return mismatchCount > 0
    ? `最近持久化校验发现 ${mismatchCount} 处线程不一致（${duration}）`
    : `最近持久化校验未通过（${duration}）`;
}

function summarizeLatestAnomaly(state = {}) {
  if (!state.last_unhealthy_at) {
    return "";
  }
  const reason = String(state.last_unhealthy_reason || "异常原因未记录");
  return `最近异常：${reason}`;
}

function buildHealthSummary(state = {}) {
  const connection = String(state.last_bridge_connection_status || "");
  const healthProbe = String(state.last_health_probe_status || "");
  const threadSummary = summarizeThreadSnapshot(state);
  if (!healthProbe) {
    return {
      health_summary_status: "unknown",
      health_summary_label: "健康探针尚未完成",
      health_summary_detail: "CoCo 服务刚启动或还未完成第一轮健康探针。",
      health_next_action: "先刷新服务状态，确认 heartbeat、最近事件和线程快照已经生成。",
    };
  }
  if (Boolean(state.ack_stalled)) {
    return {
      health_summary_status: "warning",
      health_summary_label: "确认已发出，但结果仍未送达",
      health_summary_detail: `${threadSummary} · 最近送达阶段 ${String(state.last_bridge_delivery_phase || "未记录")} · 等待 ${Number(state.last_bridge_pending_ack_age_seconds || 0)} 秒`,
      health_next_action: "优先检查最近送达阶段、运行中线程和 stdout/stderr 日志，确认是哪条线程已经 ack 但没有继续产出结果。",
    };
  }
  if (healthProbe !== "healthy") {
    const reason = String(state.last_unhealthy_reason || connection || "unknown");
    const labelByReason = {
      event_stalled: "事件流暂时停滞",
      stale: "桥接心跳过期",
      disconnected: "桥接未连接",
      ack_stalled: "确认等待超时",
    };
    return {
      health_summary_status: "warning",
      health_summary_label: labelByReason[reason] || "服务状态需要关注",
      health_summary_detail: `${threadSummary} · 连续异常 ${Number(state.consecutive_unhealthy_checks || 0)} 次`,
      health_next_action:
        reason === "event_stalled" || reason === "stale"
          ? "等待或触发 CoCo 重连，然后确认最近恢复结果和线程复核是否通过。"
          : "先检查最近异常原因、最近恢复结果与服务日志，再决定是否重启 CoCo 服务。",
    };
  }
  if (state.last_persistence_check_at && !state.last_persistence_ok) {
    return {
      health_summary_status: "warning",
      health_summary_label: "持久化校验需要关注",
      health_summary_detail: `${threadSummary} · ${summarizePersistence(state)}`,
      health_next_action: "先执行一次线程持久化校验，确认 binding/session 是否一致，再继续信任历史线程。",
    };
  }
  if (Number(state.attention_threads || 0) > 0) {
    return {
      health_summary_status: "healthy",
      health_summary_label: "服务健康，但仍有需处理线程",
      health_summary_detail: threadSummary,
      health_next_action: "优先处理 attention 队列，收敛延迟、失败或待跟进的线程。",
    };
  }
  if (Number(state.running_threads || 0) > 0) {
    return {
      health_summary_status: "healthy",
      health_summary_label: "服务健康，存在运行中线程",
      health_summary_detail: threadSummary,
      health_next_action: "关注运行中线程直到它们产出最终汇报。",
    };
  }
  return {
    health_summary_status: "healthy",
    health_summary_label: "服务健康",
    health_summary_detail: threadSummary,
    health_next_action: "当前可以继续把 CoCo 当成工作区远程协作入口使用。",
  };
}

function decorateServiceState(state = {}) {
  const recoverySummary = summarizeRecovery(state);
  const persistenceSummary = summarizePersistence(state);
  const latestAnomalySummary = summarizeLatestAnomaly(state);
  const threadSnapshotSummary = summarizeThreadSnapshot(state);
  const healthSummary = buildHealthSummary(state);
  return {
    ...state,
    thread_snapshot_summary: threadSnapshotSummary,
    latest_anomaly_summary: latestAnomalySummary,
    last_recovery_summary: recoverySummary,
    last_persistence_summary: persistenceSummary,
    ...healthSummary,
  };
}

function summarizeThreadRows(rows) {
  const allRows = Array.isArray(rows) ? rows.filter((row) => row && typeof row === "object") : [];
  const liveRows = allRows.filter((row) => !row.stale_thread);
  const workspaceAdminRows = liveRows.filter(
    (row) => String(row.chat_type || "").trim() === "p2p" && !String(row.project_name || "").trim(),
  );
  const approvalPendingRows = liveRows.filter((row) => Boolean(row.approval_pending));
  const attentionRows = liveRows.filter((row) => Boolean(row.needs_attention));
  const runningRows = liveRows.filter((row) => {
    const executionState = String(row.execution_state || "").trim();
    return executionState === "running" || Boolean(row.awaiting_report) || Boolean(row.ack_pending);
  });
  const unboundRows = liveRows.filter(
    (row) => !String(row.project_name || "").trim() && !(String(row.chat_type || "").trim() === "p2p"),
  );
  const recentRows = [...allRows].sort(
    (left, right) => parseTimestamp(right.last_message_at || right.updated_at) - parseTimestamp(left.last_message_at || left.updated_at),
  );
  const latestRow = recentRows[0] || null;
  const latestAttentionRow = [...attentionRows].sort(
    (left, right) => parseTimestamp(right.last_message_at || right.updated_at) - parseTimestamp(left.last_message_at || left.updated_at),
  )[0] || null;
  return {
    last_thread_snapshot_at: new Date().toISOString(),
    total_threads: allRows.length,
    active_threads: liveRows.length,
    archived_threads: allRows.length - liveRows.length,
    workspace_admin_threads: workspaceAdminRows.length,
    unbound_threads: unboundRows.length,
    approval_pending_threads: approvalPendingRows.length,
    attention_threads: attentionRows.length,
    running_threads: runningRows.length,
    last_thread_message_at: String(latestRow?.last_message_at || latestRow?.updated_at || ""),
    last_thread_label: String(latestRow?.thread_label || latestRow?.binding_label || latestRow?.chat_ref || ""),
    last_attention_thread_at: String(latestAttentionRow?.last_message_at || latestAttentionRow?.updated_at || ""),
    last_attention_thread_label: String(
      latestAttentionRow?.thread_label || latestAttentionRow?.binding_label || latestAttentionRow?.chat_ref || "",
    ),
  };
}

function shell(command, args) {
  return spawnSync(command, args, {
    cwd: SHARED_WORKSPACE_ROOT,
    encoding: "utf8",
  });
}

function runLaunchctl(...parts) {
  return shell("launchctl", parts);
}

function plistEscape(value) {
  return String(value).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

function plistValue(value, indent = "    ") {
  if (typeof value === "boolean") {
    return `${indent}<${String(value).toLowerCase()}/>`;
  }
  if (Array.isArray(value)) {
    return [
      `${indent}<array>`,
      ...value.map((item) => plistValue(item, `${indent}  `)),
      `${indent}</array>`,
    ].join("\n");
  }
  if (value && typeof value === "object") {
    return [
      `${indent}<dict>`,
      ...Object.entries(value).flatMap(([key, item]) => [
        `${indent}  <key>${plistEscape(key)}</key>`,
        plistValue(item, `${indent}  `),
      ]),
      `${indent}</dict>`,
    ].join("\n");
  }
  return `${indent}<string>${plistEscape(value)}</string>`;
}

function plistDumps(payload) {
  return [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">',
    '<plist version="1.0">',
    "  <dict>",
    ...Object.entries(payload).flatMap(([key, value]) => [
      `    <key>${plistEscape(key)}</key>`,
      plistValue(value, "    "),
    ]),
    "  </dict>",
    "</plist>",
    "",
  ].join("\n");
}

function launchAgentPayload(feishuSettings = {}) {
  const allowedUsers = Array.isArray(feishuSettings.allowed_users) ? feishuSettings.allowed_users : [];
  const basePathSegments = [
    path.dirname(process.execPath),
    "/opt/homebrew/bin",
    "/usr/local/bin",
    "/usr/bin",
    "/bin",
    "/usr/sbin",
    "/sbin",
  ];
  const mergedPath = Array.from(
    new Set(
      [...basePathSegments, ...String(process.env.PATH || "").split(":").filter(Boolean)].filter(Boolean),
    ),
  ).join(":");
  return {
    Label: AGENT_NAME,
    ProgramArguments: [
      "/usr/bin/caffeinate",
      "-is",
      process.execPath,
      path.join(APP_ROOT, "coco-bridge-service.js"),
      "run-daemon",
    ],
    RunAtLoad: true,
    KeepAlive: true,
    WorkingDirectory: APP_ROOT,
    StandardOutPath: LOG_STDOUT,
    StandardErrorPath: LOG_STDERR,
    EnvironmentVariables: {
      WORKSPACE_HUB_SHARED_ROOT: SHARED_WORKSPACE_ROOT,
      WORKSPACE_HUB_ROOT: SHARED_WORKSPACE_ROOT,
      WORKSPACE_HUB_RUNTIME_ROOT: SHARED_RUNTIME_ROOT,
      WORKSPACE_HUB_FEISHU_BRIDGE_ROOT: FEISHU_BRIDGE_ROOT,
      FEISHU_APP_ID: String(feishuSettings.app_id || "").trim(),
      FEISHU_APP_SECRET: String(feishuSettings.app_secret || "").trim(),
      FEISHU_DOMAIN: String(feishuSettings.domain || "feishu").trim() || "feishu",
      FEISHU_ALLOWED_USERS: allowedUsers.join(","),
      FEISHU_GROUP_POLICY: String(feishuSettings.group_policy || "mentions_only").trim() || "mentions_only",
      FEISHU_REQUIRE_MENTION: String(
        feishuSettings.require_mention == null ? true : Boolean(feishuSettings.require_mention),
      ),
      PATH: mergedPath,
      PYTHONUNBUFFERED: "1",
      NODE_ENV: "production",
    },
  };
}

async function runBroker(args) {
  return new Promise((resolve) => {
    const child = spawn("python3", [path.join(SHARED_WORKSPACE_ROOT, "ops", "local_broker.py"), ...args], {
      cwd: SHARED_WORKSPACE_ROOT,
      env: {
        ...process.env,
        WORKSPACE_HUB_ROOT: SHARED_WORKSPACE_ROOT,
        WORKSPACE_HUB_SHARED_ROOT: SHARED_WORKSPACE_ROOT,
        WORKSPACE_HUB_RUNTIME_ROOT: SHARED_RUNTIME_ROOT,
      },
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
    child.on("close", (code) => resolve({ ok: code === 0, code, stdout, stderr }));
    child.on("error", (error) => resolve({ ok: false, code: -1, stdout, stderr: String(error?.message || error) }));
  });
}

async function installLaunchAgent() {
  const settingsResult = shell("python3", [path.join(SHARED_WORKSPACE_ROOT, "ops", "local_broker.py"), "bridge-settings", "--bridge", "feishu"]);
  const settingsPayload = settingsResult.status === 0 ? JSON.parse(settingsResult.stdout || "{}") : {};
  const feishuSettings = settingsPayload.settings || {};
  ensureDir(path.dirname(LAUNCH_AGENT_PLIST));
  ensureDir(LOG_DIR);
  fs.writeFileSync(LAUNCH_AGENT_PLIST, plistDumps(launchAgentPayload(feishuSettings)), "utf8");
  const domain = `gui/${process.getuid()}`;
  runLaunchctl("bootout", domain, LAUNCH_AGENT_PLIST);
  const bootstrap = runLaunchctl("bootstrap", domain, LAUNCH_AGENT_PLIST);
  if (bootstrap.status !== 0) {
    throw new Error(bootstrap.stderr || "launchctl bootstrap failed");
  }
  const kickstart = runLaunchctl("kickstart", "-k", `${domain}/${AGENT_NAME}`);
  if (kickstart.status !== 0) {
    throw new Error(kickstart.stderr || "launchctl kickstart failed");
  }
  const serviceState = mergeServiceState({
    last_service_action: "install",
    last_service_action_at: new Date().toISOString(),
  });
  return {
    installed: true,
    loaded: true,
    plist: LAUNCH_AGENT_PLIST,
    logs: { stdout: LOG_STDOUT, stderr: LOG_STDERR },
    service_state: serviceState,
  };
}

async function uninstallLaunchAgent() {
  const domain = `gui/${process.getuid()}`;
  runLaunchctl("bootout", domain, LAUNCH_AGENT_PLIST);
  if (fs.existsSync(LAUNCH_AGENT_PLIST)) {
    fs.unlinkSync(LAUNCH_AGENT_PLIST);
  }
  const serviceState = mergeServiceState({
    last_service_action: "uninstall",
    last_service_action_at: new Date().toISOString(),
  });
  return {
    installed: false,
    loaded: false,
    plist: LAUNCH_AGENT_PLIST,
    service_state: serviceState,
  };
}

async function restartLaunchAgent() {
  return installLaunchAgent();
}

async function status() {
  const domain = `gui/${process.getuid()}/${AGENT_NAME}`;
  const loaded = runLaunchctl("print", domain).status === 0;
  let bridgeStatus = {};
  const response = await runBroker(["bridge-status", "--bridge", "feishu"]);
  if (response.ok) {
    try {
      bridgeStatus = JSON.parse(response.stdout);
    } catch (_error) {
      bridgeStatus = {};
    }
  }
  bridgeStatus = {
    ...bridgeStatus,
    ...deriveAckStalledState(bridgeStatus),
  };
  const threadPayload = response.ok
    ? await runBrokerJson(["bridge-conversations", "--bridge", "feishu", "--limit", "50"]).catch(() => ({ rows: [] }))
    : { rows: [] };
  const threadSummary = summarizeThreadRows(threadPayload.rows);
  return {
    installed: fs.existsSync(LAUNCH_AGENT_PLIST),
    loaded,
    plist: LAUNCH_AGENT_PLIST,
    logs: { stdout: LOG_STDOUT, stderr: LOG_STDERR },
    bridge_status: bridgeStatus,
    service_state: decorateServiceState({
      ...readServiceState(),
      ...threadSummary,
    }),
  };
}

async function runDaemon() {
  const bridgeHost = createBridgeHost({
    appRoot: APP_ROOT,
    workspaceRoot: CONSOLE_WORKSPACE_ROOT,
    runBroker,
    logger: console,
    hostMode: "launchagent",
  });
  let reconnecting = false;
  let consecutiveUnhealthyChecks = 0;

  function summarizeBridgeHealth(data, threadSummary = {}) {
    const ackState = deriveAckStalledState(data);
    const previousState = readServiceState();
    const metadata = data && typeof data.metadata === "object" ? data.metadata : {};
    return {
      last_health_probe_at: new Date().toISOString(),
      last_bridge_connection_status: String(data.connection_status || ""),
      last_bridge_heartbeat_at: preferCurrentValue(data.heartbeat_at, previousState.last_bridge_heartbeat_at),
      last_bridge_event_at: preferCurrentValue(data.last_event_at, previousState.last_bridge_event_at),
      last_bridge_delivery_at: preferCurrentValue(data.last_delivery_at, previousState.last_bridge_delivery_at),
      last_bridge_delivery_phase: preferCurrentValue(data.last_delivery_phase, previousState.last_bridge_delivery_phase || "report"),
      last_bridge_pending_ack_at: String(data.pending_ack_at || "").trim(),
      last_bridge_message_preview: preferCurrentValue(metadata.last_message_preview, previousState.last_bridge_message_preview),
      last_bridge_sender_ref: preferCurrentValue(metadata.last_sender_ref, previousState.last_bridge_sender_ref),
      last_bridge_recent_message_count: preferCurrentNumber(metadata.recent_message_count, previousState.last_bridge_recent_message_count),
      last_bridge_recent_reply_count: preferCurrentNumber(metadata.recent_reply_count, previousState.last_bridge_recent_reply_count),
      last_bridge_pending_ack_age_seconds: ackState.ack_pending_age_seconds,
      ack_pending: ackState.ack_pending,
      ack_stalled: ackState.ack_stalled,
      ...threadSummary,
    };
  }

  function markHealthy(data, threadSummary = {}) {
    consecutiveUnhealthyChecks = 0;
    mergeServiceState({
      ...summarizeBridgeHealth(data, threadSummary),
      last_health_probe_status: "healthy",
      last_healthy_at: new Date().toISOString(),
      last_unhealthy_at: "",
      last_unhealthy_reason: "",
      consecutive_unhealthy_checks: 0,
    });
  }

  function markUnhealthy(data, threadSummary = {}) {
    const previousState = readServiceState();
    consecutiveUnhealthyChecks += 1;
    const ackState = deriveAckStalledState(data);
    const reason = data.event_stalled
        ? "event_stalled"
        : data.stale
          ? "stale"
          : (data.connection_status || "disconnected");
    const isNewReason = previousState.last_health_probe_status !== "unhealthy" || previousState.last_unhealthy_reason !== reason;
    const nextPatch = {
      ...summarizeBridgeHealth(data, threadSummary),
      last_health_probe_status: "unhealthy",
      last_unhealthy_at: new Date().toISOString(),
      last_unhealthy_reason: reason,
      consecutive_unhealthy_checks: consecutiveUnhealthyChecks,
      total_unhealthy_checks: Number(previousState.total_unhealthy_checks || 0) + 1,
    };
    if (reason === "event_stalled" && isNewReason) {
      nextPatch.last_event_stalled_at = new Date().toISOString();
      nextPatch.total_event_stalled_count = Number(previousState.total_event_stalled_count || 0) + 1;
    }
    if (reason === "ack_stalled" && isNewReason) {
      nextPatch.last_ack_stalled_at = new Date().toISOString();
      nextPatch.total_ack_stalled_count = Number(previousState.total_ack_stalled_count || 0) + 1;
    }
    mergeServiceState(nextPatch);
    return reason;
  }

  async function ensureConnected() {
    const current = await bridgeHost.getStatus();
    const data = current?.data || {};
    const derivedData = {
      ...data,
      ...deriveAckStalledState(data),
    };
    const threadPayload = await runBrokerJson(["bridge-conversations", "--bridge", "feishu", "--limit", "50"]).catch(() => ({ rows: [] }));
    const threadSummary = summarizeThreadRows(threadPayload.rows);
    if (derivedData.connection_status === "connected" && !derivedData.stale && !derivedData.event_stalled) {
      markHealthy(derivedData, threadSummary);
      return;
    }
    if (reconnecting) {
      markUnhealthy(derivedData, threadSummary);
      return;
    }
    if (!derivedData.settings_summary?.has_app_credentials) {
      markUnhealthy(derivedData, threadSummary);
      console.log(JSON.stringify({ ok: false, phase: "bridge_waiting_for_credentials", data: derivedData }, null, 2));
      return;
    }
    reconnecting = true;
    const recoveryReason = markUnhealthy(derivedData, threadSummary);
    const reconnectStartedAt = Date.now();
    const previousState = readServiceState();
    mergeServiceState({
      last_reconnect_attempt_at: new Date().toISOString(),
      last_reconnect_attempt_reason: recoveryReason,
      last_reconnect_attempt_ok: false,
      total_reconnect_attempts: Number(previousState.total_reconnect_attempts || 0) + 1,
    });
    const beforeThreads = snapshotThreads(threadPayload.rows);
    try {
      const result = await bridgeHost.reconnect();
      await sleep(750);
      const afterStatus = await bridgeHost.getStatus();
      const afterData = {
        ...(afterStatus?.data || {}),
        ...deriveAckStalledState(afterStatus?.data || {}),
      };
      const afterPayload = await runBrokerJson(["bridge-conversations", "--bridge", "feishu", "--limit", "24"]).catch(() => ({ rows: [] }));
      const afterThreads = snapshotThreads(afterPayload.rows);
      const afterThreadSummary = summarizeThreadRows(afterPayload.rows);
      const mismatches = compareThreadSnapshots(beforeThreads, afterThreads);
      const recoveryOk = Boolean(result?.ok) && !afterData.stale && !afterData.event_stalled && mismatches.length === 0;
      if (recoveryOk && Object.keys(afterData).length) {
        markHealthy(afterData, afterThreadSummary);
      }
      const serviceState = mergeServiceState({
        ...afterThreadSummary,
        last_reconnect_attempt_ok: recoveryOk,
        last_recovery_at: new Date().toISOString(),
        last_recovery_reason: recoveryReason,
        last_recovery_ok: recoveryOk,
        last_recovery_duration_ms: Date.now() - reconnectStartedAt,
        last_recovery_compared_threads: beforeThreads.length,
        last_recovery_verified_threads: afterThreads.length,
        last_recovery_mismatches: mismatches,
        last_recovery_error: "",
        total_reconnect_successes: recoveryOk
          ? Number(readServiceState().total_reconnect_successes || 0) + 1
          : Number(readServiceState().total_reconnect_successes || 0),
      });
      if (recoveryOk) {
        try {
          await sleep(1500);
          const followupAudit = await auditThreadSnapshot("post_reconnect_followup", afterThreads, 24);
          mergeServiceState({
            last_recovery_followup_at: new Date().toISOString(),
            last_recovery_followup_ok: followupAudit.mismatches.length === 0,
            last_recovery_followup_mismatches: followupAudit.mismatches,
            last_recovery_followup_error: "",
          });
        } catch (error) {
          mergeServiceState({
            last_recovery_followup_at: new Date().toISOString(),
            last_recovery_followup_ok: false,
            last_recovery_followup_mismatches: [{ reason: "audit_failed" }],
            last_recovery_followup_error: String(error?.message || error || "thread_audit_failed"),
          });
        }
      }
      console.log(
        JSON.stringify(
          {
            ok: recoveryOk,
            phase: "bridge_reconnect",
            data: afterData || result?.data || {},
            service_state: serviceState,
          },
          null,
          2,
        ),
      );
    } catch (error) {
      const serviceState = mergeServiceState({
        last_reconnect_attempt_ok: false,
        last_recovery_at: new Date().toISOString(),
        last_recovery_reason: recoveryReason,
        last_recovery_ok: false,
        last_recovery_duration_ms: Date.now() - reconnectStartedAt,
        last_recovery_error: String(error?.message || error || "bridge_reconnect_failed"),
      });
      console.log(
        JSON.stringify(
          {
            ok: false,
            phase: "bridge_reconnect",
            error: String(error?.message || error || "bridge_reconnect_failed"),
            service_state: serviceState,
          },
          null,
          2,
        ),
      );
    } finally {
      reconnecting = false;
    }
  }

  const initial = await bridgeHost.connect();
  console.log(JSON.stringify({ ok: Boolean(initial?.ok), phase: "bridge_connect", data: initial?.data || {} }, null, 2));
  const initialPayload = await runBrokerJson(["bridge-conversations", "--bridge", "feishu", "--limit", "50"]).catch(() => ({ rows: [] }));
  const initialThreadSummary = summarizeThreadRows(initialPayload.rows);
  if (initial?.data?.connection_status === "connected" && !initial?.data?.stale && !initial?.data?.event_stalled) {
    markHealthy(initial.data, initialThreadSummary);
  }

  const timer = setInterval(() => {
    void ensureConnected();
  }, HEARTBEAT_CHECK_INTERVAL_MS);

  const shutdown = async () => {
    clearInterval(timer);
    try {
      await bridgeHost.disconnect();
    } finally {
      process.exit(0);
    }
  };

  process.on("SIGINT", () => void shutdown());
  process.on("SIGTERM", () => void shutdown());
}

function parseFlag(flagName) {
  const index = process.argv.indexOf(flagName);
  if (index === -1) {
    return "";
  }
  return String(process.argv[index + 1] || "").trim();
}

function parseIntFlag(flagName, defaultValue) {
  const raw = parseFlag(flagName);
  if (!raw) {
    return defaultValue;
  }
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : defaultValue;
}

function safeJsonParse(text) {
  try {
    return JSON.parse(text);
  } catch (_error) {
    return null;
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runBrokerJson(args) {
  const response = await runBroker(args);
  if (!response.ok) {
    throw new Error(response.stderr || response.stdout || `broker command failed: ${args.join(" ")}`);
  }
  const payload = safeJsonParse(response.stdout);
  if (!payload || typeof payload !== "object") {
    throw new Error(`broker command returned invalid JSON: ${args.join(" ")}`);
  }
  return payload;
}

function snapshotThreads(rows) {
  return (Array.isArray(rows) ? rows : []).map((row) => ({
    chat_ref: String(row.chat_ref || "").trim(),
    binding_scope: String(row.binding_scope || "").trim(),
    project_name: String(row.project_name || "").trim(),
    topic_name: String(row.topic_name || "").trim(),
    session_id: String(row.session_id || "").trim(),
    binding_label: String(row.binding_label || "").trim(),
    thread_label: String(row.thread_label || "").trim(),
    execution_state: String(row.execution_state || "").trim(),
    approval_pending: Boolean(row.approval_pending),
    reporting_status: String(row.reporting_status || "").trim(),
  })).filter((row) => row.chat_ref);
}

async function waitForHealthyBridge(timeoutMs) {
  const startedAt = Date.now();
  let lastStatus = await status();
  while (Date.now() - startedAt < timeoutMs) {
    const bridge = lastStatus.bridge_status || {};
    if (lastStatus.loaded && bridge.connection_status === "connected" && !bridge.stale && !bridge.event_stalled) {
      return {
        ok: true,
        waited_ms: Date.now() - startedAt,
        status: lastStatus,
      };
    }
    await sleep(500);
    lastStatus = await status();
  }
  return {
    ok: false,
    waited_ms: Date.now() - startedAt,
    status: lastStatus,
  };
}

function compareThreadSnapshots(beforeThreads, afterThreads) {
  const afterByChat = new Map(afterThreads.map((row) => [row.chat_ref, row]));
  const mismatches = [];
  for (const before of beforeThreads) {
    const after = afterByChat.get(before.chat_ref);
    if (!after) {
      mismatches.push({
        chat_ref: before.chat_ref,
        reason: "missing_after_restart",
      });
      continue;
    }
    for (const field of [
      "binding_scope",
      "project_name",
      "topic_name",
      "session_id",
      "binding_label",
      "thread_label",
    ]) {
      if (before[field] !== after[field]) {
        mismatches.push({
          chat_ref: before.chat_ref,
          reason: "field_changed",
          field,
          before: before[field],
          after: after[field],
        });
      }
    }
  }
  return mismatches;
}

async function auditThreadSnapshot(reason, baselineThreads = null, limit = 24) {
  const payload = await runBrokerJson(["bridge-conversations", "--bridge", "feishu", "--limit", String(limit)]);
  const currentThreads = snapshotThreads(payload.rows);
  const mismatches = Array.isArray(baselineThreads) ? compareThreadSnapshots(baselineThreads, currentThreads) : [];
  const threadSummary = summarizeThreadRows(payload.rows);
  const serviceState = mergeServiceState({
    ...threadSummary,
    last_thread_audit_at: new Date().toISOString(),
    last_thread_audit_reason: reason,
    last_thread_audit_ok: mismatches.length === 0,
    last_thread_audit_compared_threads: Array.isArray(baselineThreads) ? baselineThreads.length : currentThreads.length,
    last_thread_audit_verified_threads: currentThreads.length,
    last_thread_audit_mismatches: mismatches,
    last_thread_audit_error: "",
  });
  return {
    rows: payload.rows,
    currentThreads,
    mismatches,
    threadSummary,
    serviceState,
  };
}

async function verifyPersistenceCommand() {
  const limit = parseIntFlag("--limit", 12);
  const timeoutMs = parseIntFlag("--timeout-ms", 15000);
  const startedAt = Date.now();
  const beforeStatus = await status();
  const beforePayload = await runBrokerJson(["bridge-conversations", "--bridge", "feishu", "--limit", String(limit)]);
  const beforeThreads = snapshotThreads(beforePayload.rows);
  const restartResult = await restartLaunchAgent();
  const waitResult = await waitForHealthyBridge(timeoutMs);
  const afterPayload = await runBrokerJson(["bridge-conversations", "--bridge", "feishu", "--limit", String(limit)]);
  const afterThreads = snapshotThreads(afterPayload.rows);
  const mismatches = compareThreadSnapshots(beforeThreads, afterThreads);
  const audit = await auditThreadSnapshot("manual_verify_persistence", afterThreads, limit);
  const serviceState = mergeServiceState({
    last_persistence_check_at: new Date().toISOString(),
    last_persistence_ok: Boolean(waitResult.ok) && mismatches.length === 0,
    last_persistence_duration_ms: Date.now() - startedAt,
    last_persistence_compared_threads: beforeThreads.length,
    last_persistence_mismatches: mismatches,
    last_recovery_followup_at: audit.serviceState?.last_thread_audit_at || "",
    last_recovery_followup_ok: audit.mismatches.length === 0,
    last_recovery_followup_mismatches: audit.mismatches,
  });
  return {
    ok: Boolean(waitResult.ok) && mismatches.length === 0,
    compared_threads: beforeThreads.length,
    timeout_ms: timeoutMs,
    restart: restartResult,
    wait: waitResult,
    mismatches,
    service_state: serviceState,
    before_status: beforeStatus.bridge_status || {},
    after_status: waitResult.status?.bridge_status || {},
    before_threads: beforeThreads,
    after_threads: afterThreads,
    audit,
  };
}

async function sendMessageCommand() {
  const chatRef = parseFlag("--chat-ref");
  const openId = parseFlag("--open-id");
  const phase = parseFlag("--phase") || "report";
  const text = parseFlag("--text");
  if (!text) {
    throw new Error("send-message requires --text");
  }
  if (!chatRef && !openId) {
    throw new Error("send-message requires --chat-ref or --open-id");
  }
  const bridgeHost = createBridgeHost({
    appRoot: APP_ROOT,
    workspaceRoot: CONSOLE_WORKSPACE_ROOT,
    runBroker,
    logger: console,
    hostMode: "launchagent",
  });
  try {
    const result = await bridgeHost.sendMessage({
      chatRef,
      openId,
      text,
      phase,
    });
    console.log(JSON.stringify(result, null, 2));
  } finally {
    try {
      await bridgeHost.disconnect();
    } catch (_error) {
      // best-effort shutdown for the one-shot sender path
    }
  }
}

async function main() {
  const command = process.argv[2] || "status";
  if (command === "install-launchagent") {
    console.log(JSON.stringify(await installLaunchAgent(), null, 2));
    return;
  }
  if (command === "uninstall-launchagent") {
    console.log(JSON.stringify(await uninstallLaunchAgent(), null, 2));
    return;
  }
  if (command === "restart-launchagent") {
    console.log(JSON.stringify(await restartLaunchAgent(), null, 2));
    return;
  }
  if (command === "status") {
    console.log(JSON.stringify(await status(), null, 2));
    return;
  }
  if (command === "run-daemon") {
    await runDaemon();
    return;
  }
  if (command === "send-message") {
    await sendMessageCommand();
    return;
  }
  if (command === "verify-persistence") {
    console.log(JSON.stringify(await verifyPersistenceCommand(), null, 2));
    return;
  }
  throw new Error(`unsupported command: ${command}`);
}

main().catch((error) => {
  console.error(JSON.stringify({ ok: false, error: String(error?.message || error) }, null, 2));
  process.exit(1);
});
