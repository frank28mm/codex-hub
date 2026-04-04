"use strict";

const { execFile, spawn } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const readline = require("node:readline");
const { promisify } = require("node:util");

const LOG_TAG = "[feishu/gateway]";
const FALLBACK_TOAST = {
  toast: { type: "info", content: "已收到，正在处理..." },
};
const execFileAsync = promisify(execFile);

function parseJsonSafe(value) {
  const text = String(value || "").trim();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch (_error) {
    return {};
  }
}

function escapeProcessMatchPattern(value) {
  return String(value || "").replace(/[|\\{}()[\]^$+*?.]/g, "\\$&");
}

function subscribeLockPathForApp(appId) {
  const normalized = String(appId || "").trim();
  if (!normalized) {
    return "";
  }
  return path.join(os.homedir(), ".lark-cli", "locks", `subscribe_${normalized}.lock`);
}

function normalizeCliEventEnvelope(payload) {
  const header = payload && typeof payload === "object" ? payload.header || {} : {};
  const event = payload && typeof payload === "object" ? payload.event || payload : {};
  const eventType = String(header.event_type || payload?.type || "").trim();
  if (!event || typeof event !== "object") {
    return { event_type: eventType };
  }
  return {
    ...event,
    event_type: eventType || String(event.event_type || "").trim(),
  };
}

function shouldUseCliMessageTransport() {
  const raw = String(process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT || "auto").trim().toLowerCase();
  return raw !== "0" && raw !== "false" && raw !== "off" && raw !== "legacy";
}

function shouldUseCliEventTransport() {
  const raw = String(process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT || "auto").trim().toLowerCase();
  return raw !== "0" && raw !== "false" && raw !== "off" && raw !== "legacy";
}

function cliEventTypesForHandlers({ messageHandler, cardActionHandler }) {
  const eventTypes = [];
  if (messageHandler) {
    eventTypes.push("im.message.receive_v1");
  }
  if (cardActionHandler) {
    // Official lark-cli event transport is read-only NDJSON today. Keep card actions on SDK
    // until the CLI exposes a response channel for callback acknowledgements.
  }
  return eventTypes;
}

async function sendMessageViaLarkCli({ receiveIdType = "", receiveId = "", msgType = "text", content = "" }) {
  if (!receiveIdType || !receiveId) {
    throw new Error("missing_reply_target");
  }
  const command = ["im", "+messages-send", "--as", "bot", "--msg-type", String(msgType || "text")];
  if (receiveIdType === "chat_id") {
    command.push("--chat-id", String(receiveId));
  } else if (receiveIdType === "open_id") {
    command.push("--user-id", String(receiveId));
  } else {
    throw new Error(`unsupported_receive_id_type:${receiveIdType}`);
  }
  const normalizedContent = typeof content === "string" ? content : JSON.stringify(content || {});
  if (String(msgType || "text") === "text") {
    const payload = parseJsonSafe(normalizedContent);
    const text = String(payload.text || "").trim();
    if (!text) {
      throw new Error("missing_text");
    }
    command.push("--text", text);
  } else {
    command.push("--content", normalizedContent);
  }
  const { stdout } = await execFileAsync("lark-cli", command, { maxBuffer: 8 * 1024 * 1024 });
  const payload = parseJsonSafe(stdout);
  const body = payload?.data && typeof payload.data === "object" ? payload.data : payload;
  const messageId = String(body?.message_id || body?.messageId || "").trim();
  return {
    data: {
      message_id: messageId,
    },
  };
}

async function listChatMessagesViaLarkCli({
  chatId = "",
  userId = "",
  pageSize = 50,
  identity = "bot",
  execFileAsyncImpl = execFileAsync,
} = {}) {
  const normalizedChatId = String(chatId || "").trim();
  const normalizedUserId = String(userId || "").trim();
  if (!normalizedChatId && !normalizedUserId) {
    throw new Error("missing_history_target");
  }
  const command = [
    "im",
    "+chat-messages-list",
    "--as",
    String(identity || "bot"),
    "--page-size",
    String(Math.max(1, Number(pageSize || 50) || 50)),
  ];
  if (normalizedChatId) {
    command.push("--chat-id", normalizedChatId);
  } else {
    command.push("--user-id", normalizedUserId);
  }
  const { stdout } = await execFileAsyncImpl("lark-cli", command, { maxBuffer: 8 * 1024 * 1024 });
  const payload = parseJsonSafe(stdout);
  const body = payload?.data && typeof payload.data === "object" ? payload.data : payload;
  if (Array.isArray(body?.messages)) {
    return body.messages;
  }
  return Array.isArray(body?.items) ? body.items : [];
}

function createCliMessageClient(baseClient, logger) {
  return {
    im: {
      v1: {
        message: {
          create: async ({ params = {}, data = {} } = {}) => {
            try {
              return await sendMessageViaLarkCli({
                receiveIdType: String(params.receive_id_type || "").trim(),
                receiveId: String(data.receive_id || "").trim(),
                msgType: String(data.msg_type || "text").trim() || "text",
                content: data.content,
              });
            } catch (error) {
              logger?.warn?.(LOG_TAG, "lark-cli send fallback to sdk", error);
              return baseClient.im.v1.message.create({ params, data });
            }
          },
        },
      },
    },
    cardkit: baseClient.cardkit,
    __transport: "lark_cli_im_plus_sdk_cardkit",
  };
}

class FeishuGateway {
  constructor({
    sdk,
    settings,
    logger = console,
    spawnProcess = spawn,
    readlineModule = readline,
    execFileAsyncImpl = execFileAsync,
    waitImpl = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  }) {
    this.sdk = sdk;
    this.settings = settings;
    this.logger = logger;
    this.spawnProcess = spawnProcess;
    this.readlineModule = readlineModule;
    this.execFileAsync = execFileAsyncImpl;
    this.wait = waitImpl;
    this.client = null;
    this.baseClient = null;
    this.wsClient = null;
    this.eventDispatcher = null;
    this.messageHandler = null;
    this.cardActionHandler = null;
    this.cliEventProcess = null;
    this.cliEventLines = null;
    this.transport = "sdk_websocket_plus_rest";
    this.running = false;
    this.recentMessageIds = new Map();
    this.cliEventExitHandler = null;
    this.cliEventStopping = false;
  }

  getRestClient() {
    return this.client;
  }

  getEventDispatcher() {
    return this.eventDispatcher;
  }

  registerMessageHandler(handler) {
    this.messageHandler = handler;
  }

  registerCardActionHandler(handler) {
    this.cardActionHandler = handler;
  }

  registerCliEventExitHandler(handler) {
    this.cliEventExitHandler = typeof handler === "function" ? handler : null;
  }

  _extractMessageId(event) {
    if (!event || typeof event !== "object") {
      return "";
    }
    const direct = String(event.message_id || "").trim();
    if (direct) {
      return direct;
    }
    const nested = event.message;
    if (nested && typeof nested === "object") {
      return String(nested.message_id || nested.messageId || "").trim();
    }
    return "";
  }

  async dispatchMessageEvent(event) {
    if (!this.messageHandler) {
      return;
    }
    const messageId = this._extractMessageId(event);
    if (messageId) {
      const now = Date.now();
      const seenAt = Number(this.recentMessageIds.get(messageId) || 0);
      if (seenAt && now - seenAt < 60_000) {
        return;
      }
      this.recentMessageIds.set(messageId, now);
      if (this.recentMessageIds.size > 256) {
        const cutoff = now - 60_000;
        for (const [key, value] of this.recentMessageIds.entries()) {
          if (value < cutoff) {
            this.recentMessageIds.delete(key);
          }
        }
      }
    }
    await this.messageHandler(event);
  }

  async cleanupCompetingCliEventSubscribers() {
    let stdout = "";
    const eventTypes = cliEventTypesForHandlers({
      messageHandler: this.messageHandler,
      cardActionHandler: this.cardActionHandler,
    });
    const pgrepPattern = escapeProcessMatchPattern(
      eventTypes.length
        ? "lark-cli event +subscribe --as bot --event-types"
        : "lark-cli event +subscribe --as bot",
    );
    try {
      ({ stdout } = await this.execFileAsync(
        "pgrep",
        ["-af", pgrepPattern],
        { maxBuffer: 1024 * 1024 },
      ));
    } catch (error) {
      if (Number(error?.code || 0) === 1) {
        stdout = "";
      } else {
        this.logger.warn?.(LOG_TAG, "failed to inspect existing lark-cli event subscribers", error);
        return;
      }
    }
    const candidatePids = String(stdout || "")
      .split(/\r?\n/)
      .map((line) => String(line || "").trim())
      .filter(Boolean)
      .map((line) => {
        const match = line.match(/^(\d+)\b/);
        return match ? Number(match[1]) : 0;
      })
      .filter((pid) => Number.isInteger(pid) && pid > 0);
    const subscribeLockPath = subscribeLockPathForApp(this.settings.app_id);
    if (!candidatePids.length) {
      if (subscribeLockPath && fs.existsSync(subscribeLockPath)) {
        try {
          fs.unlinkSync(subscribeLockPath);
          this.logger.warn?.(LOG_TAG, "removed stale lark-cli event subscribe lock", {
            lock_path: subscribeLockPath,
          });
        } catch (error) {
          this.logger.warn?.(LOG_TAG, "failed to remove stale lark-cli event subscribe lock", {
            lock_path: subscribeLockPath,
            error: String(error?.message || error || "unlink_failed"),
          });
        }
      }
      return;
    }
    const terminated = [];
    for (const pid of candidatePids) {
      try {
        await this.execFileAsync("kill", ["-TERM", String(pid)], { maxBuffer: 1024 * 1024 });
        terminated.push(pid);
      } catch (error) {
        this.logger.warn?.(LOG_TAG, "failed to terminate competing lark-cli event subscriber", {
          pid,
          error: String(error?.message || error || "kill_failed"),
        });
      }
    }
    if (terminated.length) {
      if (subscribeLockPath && fs.existsSync(subscribeLockPath)) {
        try {
          fs.unlinkSync(subscribeLockPath);
        } catch (error) {
          this.logger.warn?.(LOG_TAG, "failed to clear lark-cli event subscribe lock after termination", {
            lock_path: subscribeLockPath,
            error: String(error?.message || error || "unlink_failed"),
          });
        }
      }
      this.logger.warn?.(LOG_TAG, "terminated competing lark-cli event subscribers", { pids: terminated });
      await this.wait(250);
    }
  }

  async start() {
    if (this.running) return;
    const sdk = this.sdk;
    this.baseClient = new sdk.Client({
      appId: this.settings.app_id,
      appSecret: this.settings.app_secret,
      domain: this.settings.sdk_domain,
    });
    this.client = shouldUseCliMessageTransport()
      ? createCliMessageClient(this.baseClient, this.logger)
      : this.baseClient;
    this.eventDispatcher = {};
    if (shouldUseCliEventTransport() && (this.messageHandler || this.cardActionHandler)) {
      if (this.cardActionHandler) {
        this.logger.warn?.(
          LOG_TAG,
          "card callbacks disabled while lark-cli event transport owns ingress",
          { app_id: this.settings.app_id },
        );
      }
      await this.startCliEventStream();
      this.transport = shouldUseCliMessageTransport()
        ? "lark_cli_event_plus_cli_im"
        : "lark_cli_event_plus_sdk_rest";
    } else if (this.messageHandler) {
      this.wsClient = new sdk.WSClient({
        appId: this.settings.app_id,
        appSecret: this.settings.app_secret,
        domain: this.settings.sdk_domain,
        autoReconnect: true,
        loggerLevel: sdk.LoggerLevel?.info,
      });
      this.patchWsClientForCardCallbacks();
      const messageDispatcher = new sdk.EventDispatcher({}).register({
        ...(this.cardActionHandler
          ? { "card.action.trigger": async (event) => this.safeCardActionHandler(event) }
          : {}),
        "im.message.receive_v1": async (event) => {
          await this.dispatchMessageEvent(event);
        },
      });
      this.eventDispatcher = messageDispatcher;
      await this.wsClient.start({ eventDispatcher: this.eventDispatcher });
      this.transport = shouldUseCliMessageTransport()
        ? "sdk_websocket_plus_cli_im"
        : "sdk_websocket_plus_rest";
    } else if (this.cardActionHandler) {
      this.wsClient = new sdk.WSClient({
        appId: this.settings.app_id,
        appSecret: this.settings.app_secret,
        domain: this.settings.sdk_domain,
        autoReconnect: true,
        loggerLevel: sdk.LoggerLevel?.info,
      });
      this.patchWsClientForCardCallbacks();
      this.eventDispatcher = new sdk.EventDispatcher({}).register({
        "card.action.trigger": async (event) => this.safeCardActionHandler(event),
      });
      await this.wsClient.start({ eventDispatcher: this.eventDispatcher });
      this.transport = shouldUseCliMessageTransport()
        ? "sdk_callback_plus_cli_im"
        : "sdk_websocket_plus_rest";
    } else if (shouldUseCliMessageTransport()) {
      this.transport = "sdk_callback_plus_cli_im";
    }
    this.running = true;
    this.logger.info?.(LOG_TAG, "connected", this.transport);
  }

  async stop() {
    if (this.cliEventLines && typeof this.cliEventLines.close === "function") {
      this.cliEventLines.close();
    }
    if (this.cliEventProcess && !this.cliEventProcess.killed) {
      this.cliEventStopping = true;
      this.cliEventProcess.kill("SIGTERM");
    }
    this.cliEventLines = null;
    this.cliEventProcess = null;
    if (this.wsClient && typeof this.wsClient.close === "function") {
      await this.wsClient.close();
    }
    this.running = false;
    this.transport = "sdk_websocket_plus_rest";
    this.wsClient = null;
    this.eventDispatcher = null;
    this.client = null;
    this.baseClient = null;
  }

  isRunning() {
    return this.running;
  }

  patchWsClientForCardCallbacks() {
    const wsClientAny = this.wsClient;
    if (!wsClientAny || typeof wsClientAny.handleEventData !== "function") {
      return;
    }
    const original = wsClientAny.handleEventData.bind(wsClientAny);
    wsClientAny.handleEventData = (data) => {
      const msgType = data?.headers?.find?.((item) => item.key === "type")?.value;
      if (msgType === "card") {
        const patched = {
          ...data,
          headers: Array.isArray(data.headers)
            ? data.headers.map((item) =>
                item.key === "type" ? { ...item, value: "event" } : item,
              )
            : data.headers,
        };
        return original(patched);
      }
      return original(data);
    };
  }

  async startCliEventStream() {
    if (this.cliEventProcess) {
      return;
    }
    await this.cleanupCompetingCliEventSubscribers();
    const eventTypes = cliEventTypesForHandlers({
      messageHandler: this.messageHandler,
      cardActionHandler: this.cardActionHandler,
    });
    if (!eventTypes.length) {
      return;
    }
    this.cliEventStopping = false;
    const child = this.spawnProcess(
      "lark-cli",
      ["event", "+subscribe", "--as", "bot", "--event-types", eventTypes.join(","), "--quiet"],
      {
        stdio: ["ignore", "pipe", "pipe"],
      }
    );
    child.stderr?.on("data", (chunk) => {
      const text = String(chunk || "").trim();
      if (text) {
        this.logger.warn?.(LOG_TAG, "lark-cli event stderr", text);
      }
    });
    child.on("exit", (code, signal) => {
      const intentional = this.cliEventStopping;
      this.cliEventStopping = false;
      this.logger.warn?.(LOG_TAG, "lark-cli event stream exited", { code, signal });
      this.cliEventProcess = null;
      this.cliEventLines = null;
      if (!intentional && typeof this.cliEventExitHandler === "function") {
        void Promise.resolve(
          this.cliEventExitHandler({
            code,
            signal,
          }),
        ).catch((error) => {
          this.logger.error?.(LOG_TAG, "cli event exit handler failed", error);
        });
      }
    });
    const rl = this.readlineModule.createInterface({ input: child.stdout });
    rl.on("line", async (line) => {
      const text = String(line || "").trim();
      if (!text) return;
      try {
        const payload = parseJsonSafe(text);
        const event = normalizeCliEventEnvelope(payload);
        const eventType = String(event.event_type || "").trim();
        if (eventType === "im.message.receive_v1" && this.messageHandler) {
          await this.dispatchMessageEvent(event);
        }
      } catch (error) {
        this.logger.error?.(LOG_TAG, "failed to process lark-cli event", error);
      }
    });
    this.cliEventProcess = child;
    this.cliEventLines = rl;
  }

  getTransport() {
    return this.transport;
  }

  async safeCardActionHandler(event) {
    const handler = this.cardActionHandler;
    if (!handler) return FALLBACK_TOAST;
    try {
      const result = await Promise.race([
        handler(event),
        new Promise((resolve) => setTimeout(() => resolve(undefined), 2500)),
      ]);
      if (result && typeof result === "object") {
        return result;
      }
      return FALLBACK_TOAST;
    } catch (error) {
      this.logger.error?.(LOG_TAG, "card action failed", error);
      return FALLBACK_TOAST;
    }
  }
}

module.exports = {
  FeishuGateway,
  FALLBACK_TOAST,
  createCliMessageClient,
  listChatMessagesViaLarkCli,
  normalizeCliEventEnvelope,
  shouldUseCliEventTransport,
  shouldUseCliMessageTransport,
};
