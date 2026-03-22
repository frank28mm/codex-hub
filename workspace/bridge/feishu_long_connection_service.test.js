"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const ACK_TEXT_PATTERN =
  /^Frank，(?:好的，我先处理。|收到，我马上跟进。|知道了，我先看一下。|明白，我这就开始处理。|收到，我先帮你过一遍。)$/;
const {
  ACK_PENDING_EVENT_IDLE_GRACE_SECONDS,
  ACTIVE_EXECUTION_EVENT_IDLE_SECONDS,
  EVENT_IDLE_AFTER_SECONDS,
  EVENT_IDLE_AFTER_SECONDS_IDLE,
  classifyApprovalRequirement,
  createFeishuLongConnectionService,
  isHighRiskRequest,
  isLocalExtensionRequest,
  normalizeMessageEvent,
  normalizeSdkDomain,
  resolveExecutionProfileForMessage,
  sanitizeSettings,
  summarizeSettings,
  shouldAcceptMessage,
  shouldRunInBackground,
} = require("./feishu_long_connection_service");

function parseReplyText(payload) {
  const content = JSON.parse(payload.data.content);
  if (payload.data.msg_type === "post") {
    return content.zh_cn.content[0][0].text;
  }
  if (payload.data.msg_type === "interactive") {
    const fragments = [];
    const visit = (node) => {
      if (!node || typeof node !== "object") return;
      if (typeof node.content === "string" && ["markdown", "plain_text"].includes(String(node.tag || ""))) {
        fragments.push(node.content);
      }
      if (Array.isArray(node.elements)) {
        node.elements.forEach(visit);
      }
      if (Array.isArray(node.columns)) {
        node.columns.forEach(visit);
      }
      if (node.title && typeof node.title === "object") {
        visit(node.title);
      }
      if (node.body && typeof node.body === "object") {
        visit(node.body);
      }
      if (node.header && typeof node.header === "object") {
        visit(node.header);
      }
    };
    visit(content);
    return fragments.join("\n");
  }
  return content.text;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withTempProjectRegistry(entries, fn) {
  const previous = process.env.WORKSPACE_HUB_VAULT_ROOT;
  const vaultRoot = fs.mkdtempSync(path.join(os.tmpdir(), "codex-hub-feishu-vault-"));
  const registryPath = path.join(vaultRoot, "PROJECT_REGISTRY.md");
  fs.writeFileSync(
    registryPath,
    [
      "# PROJECT_REGISTRY",
      "",
      "<!-- PROJECT_REGISTRY_DATA_START -->",
      "```json",
      JSON.stringify(entries, null, 2),
      "```",
      "<!-- PROJECT_REGISTRY_DATA_END -->",
      "",
    ].join("\n"),
    "utf-8",
  );
  process.env.WORKSPACE_HUB_VAULT_ROOT = vaultRoot;
  try {
    return await fn();
  } finally {
    if (typeof previous === "string") {
      process.env.WORKSPACE_HUB_VAULT_ROOT = previous;
    } else {
      delete process.env.WORKSPACE_HUB_VAULT_ROOT;
    }
    fs.rmSync(vaultRoot, { recursive: true, force: true });
  }
}

async function testSanitizeAndSummarize() {
  const settings = sanitizeSettings({
    app_id: " cli_123 ",
    app_secret: "secret",
    domain: "feishu",
    allowed_users: ["ou_1", "", "ou_2"],
    group_policy: "mentions_only",
    require_mention: true,
  });
  assert.equal(settings.app_id, "cli_123");
  assert.deepEqual(settings.allowed_users, ["ou_1", "ou_2"]);
  const summary = summarizeSettings(settings);
  assert.equal(summary.has_app_credentials, true);
  assert.equal(summary.allowed_user_count, 2);
}

async function testNormalizeSdkDomain() {
  const sdk = { Domain: { Feishu: 0, Lark: 1 } };
  assert.equal(normalizeSdkDomain(sdk, "feishu"), 0);
  assert.equal(normalizeSdkDomain(sdk, "lark"), 1);
  assert.equal(normalizeSdkDomain(sdk, "https://open.feishu.cn"), "https://open.feishu.cn");
}

async function testNormalizeMessageEvent() {
  const normalized = normalizeMessageEvent({
    message: {
      message_id: "om_1",
      message_type: "text",
      content: JSON.stringify({ text: "hello world" }),
      chat_type: "group",
      create_time: "1710400000000",
      mentions: [{ open_id: "ou_bot" }],
    },
    sender: {
      sender_id: { open_id: "ou_sender" },
    },
  });
  assert.equal(normalized.message_id, "om_1");
  assert.equal(normalized.text, "hello world");
  assert.equal(normalized.open_id, "ou_sender");
  assert.equal(normalized.mentions.length, 1);
  assert.equal(normalized.text_mentions.length, 0);
  assert.equal(normalized.message_created_at, "2024-03-14T07:06:40.000Z");
}

async function testNormalizeMessageEventDetectsTextMentionAlias() {
  const normalized = normalizeMessageEvent({
    message: {
      message_id: "om_alias",
      message_type: "text",
      content: JSON.stringify({ text: "@_user_1 你在工作吗" }),
      chat_type: "group",
    },
    sender: {
      sender_id: { open_id: "ou_sender" },
    },
  });
  assert.equal(normalized.text, "@_user_1 你在工作吗");
  assert.equal(normalized.mentions.length, 0);
  assert.equal(normalized.text_mentions.length, 1);
}

async function testShouldAcceptMessage() {
  const settings = sanitizeSettings({
    app_id: "cli_123",
    app_secret: "secret",
    allowed_users: ["ou_sender"],
    require_mention: true,
    group_policy: "mentions_only",
  });
  const accepted = shouldAcceptMessage(settings, {
    text: "hi",
    chat_type: "group",
    mentions: [{ open_id: "ou_bot" }],
    open_id: "ou_sender",
  });
  assert.equal(accepted.ok, true);
  const acceptedTextAlias = shouldAcceptMessage(settings, {
    text: "@_user_1 hi",
    chat_type: "group",
    mentions: [],
    text_mentions: ["@_user_1"],
    open_id: "ou_sender",
  });
  assert.equal(acceptedTextAlias.ok, true);
  const blocked = shouldAcceptMessage(settings, {
    text: "hi",
    chat_type: "group",
    mentions: [],
    text_mentions: [],
    open_id: "ou_sender",
  });
  assert.equal(blocked.ok, false);
  assert.equal(blocked.reason, "mention_required");
}

async function testShouldRunInBackgroundSkipsStatusQuestions() {
  assert.equal(
    shouldRunInBackground({ text: "我们先确定当前修改过后，AI辅导的状态是什么样的？" }),
    false,
  );
  assert.equal(
    shouldRunInBackground({ text: "你了解一下项目" }),
    false,
  );
  assert.equal(
    shouldRunInBackground({ text: "请继续排查 AI 辅导为什么前面的记录看不到了" }),
    false,
  );
  assert.equal(
    shouldRunInBackground({ text: "请继续修复 AI 辅导前面的记录丢失问题" }),
    true,
  );
  assert.equal(
    shouldRunInBackground({ text: "工作区未提交的这两条是什么作用？还有必要留着吗？" }),
    false,
  );
  assert.equal(
    shouldRunInBackground({ text: "关键是这个测试文件可以作为长期的单元测试文件，如果是，那么就保留。如果只是一个临时测试文件，那就没有意义。" }),
    false,
  );
  assert.equal(
    shouldRunInBackground({ text: "请跑一轮测试并汇总失败原因" }),
    true,
  );
  assert.equal(
    shouldRunInBackground({ text: "你先查 SampleProj 的 Supabase 配置和 .env 现状，再告诉我现在用的是哪套" }),
    false,
  );
  assert.equal(
    shouldRunInBackground({ text: "自己先找一下当前项目里的 secret、token 和环境变量引用，再给我结论" }),
    false,
  );
  assert.equal(
    shouldRunInBackground({ text: "帮我在飞书里面新建一个飞书多维表格做测试，名字就叫 Test" }),
    false,
  );
  assert.equal(
    shouldRunInBackground({ text: "帮我定一个明天下午一点半的日程，地点在云锦路" }),
    false,
  );
  assert.equal(
    shouldRunInBackground({ text: "请把 investigate 这个 skill 安装到 ~/.codex/skills/" }),
    false,
  );
}

async function testExecutionProfileAndApprovalClassificationCoverFeishuLocalExtensions() {
  assert.equal(isLocalExtensionRequest("请把 investigate 这个 skill 安装到 ~/.codex/skills/"), true);
  assert.equal(
    resolveExecutionProfileForMessage({ text: "请把 investigate 这个 skill 安装到 ~/.codex/skills/" }),
    "feishu-local-extend",
  );
  assert.equal(
    resolveExecutionProfileForMessage({ text: "帮我在飞书里面新建一个飞书多维表格做测试，名字就叫 Test" }),
    "feishu-object-op",
  );
  assert.equal(resolveExecutionProfileForMessage({ text: "请总结当前状态" }), "feishu");

  const localSystem = classifyApprovalRequirement("请帮我把 launch agent 安装到 ~/Library/LaunchAgents");
  assert.equal(localSystem.required, true);
  assert.equal(localSystem.scope, "feishu_local_system_execution");

  const highRisk = classifyApprovalRequirement("请帮我 git push 当前分支");
  assert.equal(highRisk.required, true);
  assert.equal(highRisk.scope, "feishu_high_risk_execution");

  const skillInstall = classifyApprovalRequirement("请把 investigate 这个 skill 安装到 ~/.codex/skills/");
  assert.equal(skillInstall.required, false);
}

async function testSkillInstallRoutesWithLocalExtensionProfile() {
  const brokerCalls = [];
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload = {}) => {
        brokerCalls.push({ command, payload });
        if (command === "record-bridge-message") {
          return { ok: true, record: { created_at: "2026-03-20T01:00:00Z", updated_at: "2026-03-20T01:00:00Z" } };
        }
        if (command === "user-profile") {
          return { ok: true, profile: { preferred_name: "Frank", relationship: "workspace owner" } };
        }
        if (command === "bridge-chat-binding") {
          return { ok: true, binding: { project_name: "Codex Hub", binding_scope: "project" } };
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: "已完成 skill 安装。",
            stderr: "session id: 019ce000-0000-7000-8000-000000000779",
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });
  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
    group_policy: "all_messages",
    require_mention: false,
  });

  const result = await service.processMessageEvent({
    message: {
      message_id: "om_skill_install",
      message_type: "text",
      content: JSON.stringify({ text: "请把 investigate 这个 skill 安装到 ~/.codex/skills/" }),
      chat_type: "p2p",
      chat_id: "oc_direct",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(result.ok, true);
  const execCall = brokerCalls.find((item) => item.command === "codex-exec");
  assert.ok(execCall);
  assert.equal(execCall.payload.execution_profile, "feishu-local-extend");
  assert.equal(result.replyPreview, "Frank，已完成 skill 安装。");
}

async function testHighRiskRequestDetectsChineseGitHubPushPhrasing() {
  assert.equal(isHighRiskRequest("那你先把当前的改动推到GitHub 吧。GitHub Action 会自动部署的"), true);
  assert.equal(isHighRiskRequest("请帮我把这个提交到 GitHub"), true);
}

async function testHandleMessageEventAcceptsTextMentionAlias() {
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command) => {
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-14T02:00:00Z",
              updated_at: "2026-03-14T02:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "bridge-chat-binding") {
          return { ok: true, binding: { project_name: "Codex Hub" } };
        }
        if (command === "panel") {
          return { ok: true, cards: [{ label: "Bridge", value: "ok" }] };
        }
        if (command === "health") {
          return { ok: true, payload: { open_alert_count: 0, latest_report: "latest" } };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });
  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
    group_policy: "mentions_only",
    require_mention: true,
  });

  const result = await service.handleMessageEvent({
    message: {
      message_id: "alias-direct",
      message_type: "text",
      content: JSON.stringify({ text: "@_user_1 当前系统状态" }),
      chat_type: "group",
      chat_id: "chat-alias",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(result.ok, true);
  assert.equal(result.direct, true);
  assert.equal(result.replyPhase, "status");
  assert.match(result.replyPreview, /当前系统状态/);
}

async function testBindingDeclarationResetsSessionAndReportsBindingPhase() {
  const bindings = new Map([
    [
      "chat-bind",
      {
        chat_ref: "chat-bind",
        bridge: "feishu",
        binding_scope: "project",
        project_name: "Old Project",
        topic_name: "",
        session_id: "sess-old",
      },
    ],
  ]);
  const runtimeCalls = [];
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-14T02:00:00Z",
              updated_at: "2026-03-14T02:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "bridge-chat-binding") {
          const chatRef = String(payload.chat_ref || "");
          if (payload.binding_json) {
            const binding = { bridge: "feishu", chat_ref: chatRef, ...payload.binding_json };
            bindings.set(chatRef, binding);
            return { ok: true, binding };
          }
          return { ok: true, binding: bindings.get(chatRef) || null };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async (payload) => runtimeCalls.push(payload),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });

  const result = await service.handleMessageEvent({
    message: {
      message_id: "bind-1",
      message_type: "text",
      content: JSON.stringify({ text: "这个群只聊 Codex Hub" }),
      chat_type: "group",
      chat_id: "chat-bind",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(result.ok, true);
  assert.equal(result.direct, true);
  assert.equal(result.replyPhase, "binding_bound");
  assert.match(result.replyPreview, /Codex Hub/);
  assert.match(result.replyPreview, /绑定|锁定/);
  assert.equal(bindings.get("chat-bind").session_id, "");
  assert.equal(runtimeCalls.some((payload) => payload.last_binding_result === "bound"), true);
}

async function testBindingDeclarationRequiresExplicitProjectForNewChat() {
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-14T02:00:00Z",
              updated_at: "2026-03-14T02:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "bridge-chat-binding") {
          if (payload.binding_json) {
            throw new Error("unexpected binding write");
          }
          return { ok: true, binding: null };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });

  const result = await service.handleMessageEvent({
    message: {
      message_id: "bind-missing-project",
      message_type: "text",
      content: JSON.stringify({ text: "这个群只聊 前端" }),
      chat_type: "group",
      chat_id: "chat-unbound",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(result.ok, true);
  assert.equal(result.direct, true);
  assert.equal(result.replyPhase, "binding_error");
  assert.match(result.replyPreview, /没有在这句话里识别出正式项目名/);
}

async function testProjectReplyIncludesMaterialSuggestionsFromBroker() {
  const brokerCalls = [];
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        brokerCalls.push({ command, payload });
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-17T02:00:00Z",
              updated_at: "2026-03-17T02:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "bridge-chat-binding") {
          return {
            ok: true,
            binding: {
              bridge: "feishu",
              chat_ref: "chat-material",
              binding_scope: "project",
              project_name: "SampleProj",
              topic_name: "",
              session_id: "",
            },
          };
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: "这是来自 start-codex 的正常回答。",
            stderr: "session id: 019ce000-0000-7000-8000-00000000m001",
            returncode: 0,
          };
        }
        if (command === "material-suggest") {
          return {
            ok: true,
            project_name: "SampleProj",
            board_path: "/tmp/SampleProj-项目板.md",
            hotset_hits: [
              {
                path: "/tmp/SampleProj/guide.md",
                title: "Guide",
                source_group: "project-doc",
                route_group: "project-material",
                heading: "Guide",
                line_start: 1,
                line_end: 3,
                is_hotset: true,
                pin_reason: "hotset_path",
              },
            ],
            report_hits: [
              {
                path: "/tmp/reports/system-overview.md",
                title: "System Report",
                source_group: "report",
                route_group: "report",
                heading: "System Report",
                line_start: 1,
                line_end: 8,
                is_hotset: false,
                pin_reason: "",
              },
            ],
            deliverable_hits: [],
            material_hits: [],
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });

  const result = await service.handleMessageEvent({
    message: {
      message_id: "material-1",
      message_type: "text",
      content: JSON.stringify({ text: "请总结 SampleProj 当前状态，并告诉我先看哪些材料和报告" }),
      chat_type: "p2p",
      chat_id: "",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(result.ok, true);
  assert.match(result.replyPreview, /这是来自 start-codex 的正常回答/);
  assert.match(result.replyPreview, /补充入口/);
  assert.match(result.replyPreview, /优先材料：Guide/);
  assert.match(result.replyPreview, /最新报告：System Report/);
  assert.equal(brokerCalls.some((entry) => entry.command === "material-suggest"), true);
}

async function testServiceConnectAndRoute() {
  const runtimeCalls = [];
  const brokerCalls = [];
  const replies = [];
  const wsStarts = [];
  const seenInboundMessages = new Set();
  const bindings = new Map();
  const sdkLoader = () => ({
    Domain: { Feishu: 0, Lark: 1 },
    Client: class {
      constructor(config) {
        this.config = config;
        this.im = {
          v1: {
            message: {
              create: async (payload) => {
                replies.push(payload);
                return { ok: true };
              },
            },
          },
        };
      }
    },
    EventDispatcher: class {
      constructor() {
        this.handlers = {};
      }
      register(handlers) {
        this.handlers = handlers;
        return this;
      }
    },
    WSClient: class {
      constructor(config) {
        this.config = config;
      }
      async start(payload) {
        wsStarts.push(payload);
      }
      async close() {}
    },
    LoggerLevel: { info: "info" },
  });
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        brokerCalls.push({ command, payload });
        if (command === "record-bridge-message") {
          const duplicate = seenInboundMessages.has(payload.message_id);
          seenInboundMessages.add(payload.message_id);
          return {
            ok: true,
            record: {
              created_at: "2026-03-13T15:00:00Z",
              updated_at: duplicate ? "2026-03-13T15:01:00Z" : "2026-03-13T15:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "bridge-chat-binding") {
          const chatRef = String(payload.chat_ref || "");
          if (payload.binding_json) {
            const binding = {
              chat_ref: chatRef,
              bridge: "feishu",
              ...payload.binding_json,
            };
            bindings.set(chatRef, binding);
            return { ok: true, binding };
          }
          return { ok: true, binding: bindings.get(chatRef) || null };
        }
        if (command === "codex-resume") {
          return {
            ok: true,
            command,
            payload,
            stdout: "这是续接后的回答。",
            stderr: "session id: 019ce000-0000-7000-8000-000000000001",
          };
        }
        if (command === "panel") {
          return {
            ok: true,
            panel_name: payload.name,
            cards: [
              { label: "Active Projects", value: "3" },
              { label: "Pending Reviews", value: "1" },
            ],
          };
        }
        if (command === "health") {
          return {
            ok: true,
            payload: {
              open_alert_count: 0,
              latest_report: "/tmp/latest.md",
            },
          };
        }
        if (command === "projects") {
          return {
            ok: true,
            projects: [
              { project_name: "Codex Obsidian记忆与行动系统", status: "doing", next_action: "继续推进" },
            ],
          };
        }
        return {
          ok: true,
          command,
          payload,
          stdout: "这是第一轮回答。",
          stderr: "session id: 019ce000-0000-7000-8000-000000000001",
        };
      },
    },
    runtimeState: {
      saveBridgeStatus: async (payload) => runtimeCalls.push({ kind: "status", payload }),
      saveBridgeSettings: async (payload) => runtimeCalls.push({ kind: "settings", payload }),
    },
    sdkLoader,
    logger: { info() {}, warn() {}, error() {} },
  });
  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
    allowed_users: ["ou_sender"],
  });
  const connected = await service.connect();
  assert.equal(connected.ok, true);
  assert.equal(wsStarts.length, 1);
  assert.equal(wsStarts[0].eventDispatcher != null, true);
  assert.equal(Boolean(service.getStatus().connected_at), true);
  const result = await service.handleMessageEvent({
    message: {
      message_id: "om_2",
      message_type: "text",
      content: JSON.stringify({ text: "请总结当前状态" }),
      chat_type: "p2p",
      chat_id: "",
    },
    sender: {
      sender_id: { open_id: "ou_sender" },
    },
  });
  assert.equal(result.ok, true);
  assert.equal(
    brokerCalls.some((entry) => entry.command === "record-bridge-message"),
    true,
  );
  const execCall = brokerCalls.find((entry) => entry.command === "codex-exec");
  assert.ok(execCall);
  assert.equal(execCall.payload.execution_profile, "feishu");
  assert.equal(execCall.payload.thread_name, "CoCo 私聊");
  assert.equal(execCall.payload.thread_label, "CoCo 私聊");
  assert.equal(result.replyPreview, "Frank，这是第一轮回答。");
  assert.equal(runtimeCalls.some((item) => item.kind === "status"), true);
  const status = service.getStatus();
  assert.equal(status.sdk_loaded, true);
  assert.equal(status.dispatcher_ready, true);
  assert.equal(status.recent_message_count, 1);
  assert.equal(status.last_message_preview, "请总结当前状态");
  assert.equal(status.last_sender_ref, "ou_sender");
  const handler = wsStarts[0].eventDispatcher.handlers["im.message.receive_v1"];
  await handler({
    message: {
      message_id: "om_3",
      message_type: "text",
      content: JSON.stringify({ text: "请回复" }),
      chat_type: "p2p",
      chat_id: "",
    },
    sender: {
      sender_id: { open_id: "ou_sender" },
    },
  });
  assert.equal(replies.length, 1);
  assert.equal(replies[0].data.msg_type, "interactive");
  assert.equal(replies[0].params.receive_id_type, "open_id");
  assert.match(parseReplyText(replies[0]), /Frank，这是续接后的回答。/);
  const handledStatus = service.getStatus();
  assert.equal(handledStatus.recent_message_count, 2);
  assert.equal(handledStatus.recent_reply_count, 1);
  assert.equal(handledStatus.last_message_preview, "请回复");
  const resumeCall = brokerCalls.find((entry) => entry.command === "codex-resume");
  assert.ok(resumeCall);
  assert.equal(resumeCall.payload.execution_profile, "feishu");
  assert.equal(resumeCall.payload.thread_name, "CoCo 私聊");
  assert.equal(resumeCall.payload.thread_label, "CoCo 私聊");

  await handler({
    message: {
      message_id: "om_3",
      message_type: "text",
      content: JSON.stringify({ text: "请回复" }),
      chat_type: "p2p",
      chat_id: "",
    },
    sender: {
      sender_id: { open_id: "ou_sender" },
    },
  });
  assert.equal(replies.length, 1);
  assert.equal(service.getStatus().recent_message_count, 2);
  assert.equal(brokerCalls.filter((item) => item.command === "record-bridge-message").length >= 2, true);
}

async function testConnectPatchesWsClientForCardCallbacks() {
  const handledTypes = [];
  let wsInstance = null;
  const sdkLoader = () => ({
    Domain: { Feishu: 0, Lark: 1 },
    Client: class {
      constructor() {
        this.im = {
          v1: {
            message: {
              create: async () => ({ ok: true, data: { message_id: "om_reply" } }),
            },
          },
        };
      }
    },
    EventDispatcher: class {
      register(handlers) {
        this.handlers = handlers;
        return this;
      }
    },
    WSClient: class {
      constructor() {
        wsInstance = this;
      }
      handleEventData(data) {
        const typeHeader = Array.isArray(data?.headers)
          ? data.headers.find((header) => header?.key === "type")
          : null;
        handledTypes.push(typeHeader?.value || "");
      }
      async start() {}
      async close() {}
    },
    LoggerLevel: { info: "info" },
  });
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command) => {
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "bridge-chat-binding") {
          return { ok: true, binding: null };
        }
        return { ok: true };
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader,
    logger: { info() {}, warn() {}, error() {} },
  });
  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
  });
  await service.connect();
  assert.ok(wsInstance);
  wsInstance.handleEventData({
    headers: [
      { key: "type", value: "card" },
      { key: "event_type", value: "card.action.trigger" },
    ],
    event: { action: { value: { callback_data: "perm:allow:coco-test" } } },
  });
  assert.deepEqual(handledTypes, ["event"]);
}

async function testFastBackgroundTasksDoNotSendAck() {
  const statuses = [];
  const replies = [];
  const sdkLoader = () => ({
    Domain: { Feishu: 0, Lark: 1 },
    Client: class {
      constructor() {
        this.im = {
          v1: {
            message: {
              create: async (payload) => {
                replies.push(parseReplyText(payload));
                return { data: { message_id: `ack-${Date.now()}` }, ok: true };
              },
            },
          },
        };
      }
    },
    EventDispatcher: class {
      register(handlers) {
        this.handlers = handlers;
        return this;
      }
    },
    WSClient: class {
      async start() {}
      async close() {}
    },
    LoggerLevel: { info: "info" },
  });
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command) => {
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-14T00:00:00Z",
              updated_at: "2026-03-14T00:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "bridge-chat-binding") {
          return { ok: true, binding: null };
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: "全部完成。",
            stderr: "session id: ack-pending",
          };
        }
        return { ok: true };
      },
    },
    runtimeState: {
      saveBridgeStatus: async (payload) => statuses.push({ ...payload }),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader,
    logger: { info() {}, warn() {}, error() {} },
  });

  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
  });
  await service.connect();
  const result = await service.processMessageEvent({
    message: {
      message_id: "om_ack_idle",
      message_type: "text",
      content: JSON.stringify({ text: "执行一个任务" }),
      chat_type: "p2p",
      chat_id: "",
    },
    sender: {
      sender_id: { open_id: "ou_sender" },
    },
  });
  assert.equal(result.ok, false);
  assert.equal(result.reason, "background_started");
  await new Promise((resolve) => setTimeout(resolve, 0));
  await new Promise((resolve) => setTimeout(resolve, 0));
  const ackStatus = statuses.find((entry) => entry.last_execution_state === "running");
  assert.ok(ackStatus);
  assert.equal(ackStatus.pending_ack_at, "");
  assert.equal(ackStatus.event_idle_after_seconds, ACTIVE_EXECUTION_EVENT_IDLE_SECONDS);
  const finalStatus = [...statuses].reverse().find((entry) => entry.last_execution_state === "reported");
  assert.ok(finalStatus);
  assert.equal(finalStatus.event_idle_after_seconds, EVENT_IDLE_AFTER_SECONDS_IDLE);
  assert.equal(finalStatus.pending_ack_at, "");
  assert.equal(replies.length, 1);
  assert.match(replies[0], /Frank，全部完成。/);
}

async function testSameChatBackgroundTasksSerialize() {
  const brokerCalls = [];
  let releaseFirstExec = null;
  let activeExecCount = 0;
  let maxActiveExecCount = 0;
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-16T00:00:00Z",
              updated_at: "2026-03-16T00:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "bridge-chat-binding") {
          return { ok: true, binding: null };
        }
        if (command === "codex-exec") {
          brokerCalls.push(String(payload.prompt || ""));
          activeExecCount += 1;
          maxActiveExecCount = Math.max(maxActiveExecCount, activeExecCount);
          if (String(payload.prompt || "").includes("第一条")) {
            await new Promise((resolve) => {
              releaseFirstExec = () => {
                activeExecCount -= 1;
                resolve();
              };
            });
          } else {
            activeExecCount -= 1;
          }
          return {
            ok: true,
            stdout: `完成：${payload.prompt}`,
            stderr: "session id: 019ce000-0000-7000-8000-000000001111",
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });
  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
    group_policy: "all_messages",
    require_mention: false,
  });

  const firstPromise = service.processMessageEvent({
    message: {
      message_id: "queue-1",
      message_type: "text",
      content: JSON.stringify({ text: "请执行第一条任务并修复问题" }),
      chat_type: "group",
      chat_id: "chat-serialized",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });
  await delay(20);
  const secondPromise = service.processMessageEvent({
    message: {
      message_id: "queue-2",
      message_type: "text",
      content: JSON.stringify({ text: "请执行第二条任务并继续处理" }),
      chat_type: "group",
      chat_id: "chat-serialized",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  let secondResolved = false;
  secondPromise.then(() => {
    secondResolved = true;
  });

  await firstPromise;
  await delay(50);
  assert.deepEqual(brokerCalls, ["请执行第一条任务并修复问题"]);
  assert.equal(secondResolved, false);
  assert.equal(maxActiveExecCount, 1);
  assert.equal(typeof releaseFirstExec, "function");

  releaseFirstExec();
  await secondPromise;
  await delay(50);

  assert.deepEqual(brokerCalls, [
    "请执行第一条任务并修复问题",
    "请执行第二条任务并继续处理",
  ]);
  assert.equal(maxActiveExecCount, 1);
}

async function testDifferentChatsBackgroundTasksRunInParallel() {
  const brokerCalls = [];
  let releaseFirstExec = null;
  let activeExecCount = 0;
  let maxActiveExecCount = 0;
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-16T00:00:00Z",
              updated_at: "2026-03-16T00:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "bridge-chat-binding") {
          return { ok: true, binding: null };
        }
        if (command === "codex-exec") {
          brokerCalls.push(String(payload.prompt || ""));
          activeExecCount += 1;
          maxActiveExecCount = Math.max(maxActiveExecCount, activeExecCount);
          if (String(payload.prompt || "").includes("第一条")) {
            await new Promise((resolve) => {
              releaseFirstExec = () => {
                activeExecCount -= 1;
                resolve();
              };
            });
          } else {
            await delay(10);
            activeExecCount -= 1;
          }
          return {
            ok: true,
            stdout: `完成：${payload.prompt}`,
            stderr: "session id: 019ce000-0000-7000-8000-000000002222",
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });
  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
    group_policy: "all_messages",
    require_mention: false,
  });

  const firstPromise = service.processMessageEvent({
    message: {
      message_id: "parallel-1",
      message_type: "text",
      content: JSON.stringify({ text: "请执行第一条任务并修复问题" }),
      chat_type: "group",
      chat_id: "chat-parallel-1",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });
  await delay(20);
  const secondPromise = service.processMessageEvent({
    message: {
      message_id: "parallel-2",
      message_type: "text",
      content: JSON.stringify({ text: "请执行第二条任务并继续处理" }),
      chat_type: "group",
      chat_id: "chat-parallel-2",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  await delay(50);
  assert.deepEqual(brokerCalls, [
    "请执行第一条任务并修复问题",
    "请执行第二条任务并继续处理",
  ]);
  assert.equal(maxActiveExecCount >= 2, true);
  assert.equal(typeof releaseFirstExec, "function");

  releaseFirstExec();
  await Promise.all([firstPromise, secondPromise]);
}

async function testLongBackgroundTasksStaySilentUntilFinalReply() {
  const statuses = [];
  const replies = [];
  const originalSetTimeout = global.setTimeout;
  const originalClearTimeout = global.clearTimeout;
  global.setTimeout = ((fn, ms, ...args) => {
    if (ms === 30_000) {
      return originalSetTimeout(fn, 0, ...args);
    }
    if (ms === 60_000) {
      return originalSetTimeout(() => {}, 1_000, ...args);
    }
    return originalSetTimeout(fn, ms, ...args);
  });
  global.clearTimeout = ((timer) => originalClearTimeout(timer));
  try {
    const sdkLoader = () => ({
      Domain: { Feishu: 0, Lark: 1 },
      Client: class {
        constructor() {
          this.im = {
            v1: {
              message: {
                create: async (payload) => {
                  replies.push(parseReplyText(payload));
                  return { data: { message_id: `ack-${Date.now()}` }, ok: true };
                },
              },
            },
          };
        }
      },
      EventDispatcher: class {
        register(handlers) {
          this.handlers = handlers;
          return this;
        }
      },
      WSClient: class {
        async start() {}
        async close() {}
      },
      LoggerLevel: { info: "info" },
    });
    const service = createFeishuLongConnectionService({
      brokerClient: {
        call: async (command) => {
          if (command === "record-bridge-message") {
            return {
              ok: true,
              record: {
                created_at: "2026-03-14T00:00:00Z",
                updated_at: "2026-03-14T00:00:00Z",
              },
            };
          }
          if (command === "user-profile") {
            return {
              ok: true,
              profile: { preferred_name: "Frank", relationship: "workspace owner" },
            };
          }
          if (command === "bridge-chat-binding") {
            return { ok: true, binding: null };
          }
          if (command === "codex-exec") {
            await new Promise((resolve) => originalSetTimeout(resolve, 50));
            return {
              ok: true,
              stdout: "全部完成。",
              stderr: "session id: ack-delayed",
            };
          }
          return { ok: true };
        },
      },
      runtimeState: {
        saveBridgeStatus: async (payload) => statuses.push({ ...payload }),
        saveBridgeSettings: async () => ({}),
      },
      sdkLoader,
      logger: { info() {}, warn() {}, error() {} },
    });

    await service.loadSettings({
      app_id: "cli_123",
      app_secret: "secret",
    });
    await service.connect();
    const result = await service.processMessageEvent({
      message: {
        message_id: "om_ack_delayed",
        message_type: "text",
        content: JSON.stringify({ text: "执行一个任务" }),
        chat_type: "p2p",
        chat_id: "",
      },
      sender: {
        sender_id: { open_id: "ou_sender" },
      },
    });
    assert.equal(result.ok, false);
    assert.equal(result.reason, "background_started");
    await new Promise((resolve) => originalSetTimeout(resolve, 200));
    assert.equal(replies.length, 1);
    assert.match(replies[0], /Frank，全部完成。/);
    const runningStatus = statuses.find((entry) => entry.last_execution_state === "running");
    assert.ok(runningStatus);
    assert.equal(runningStatus.pending_ack_at, "");
  } finally {
    global.setTimeout = originalSetTimeout;
    global.clearTimeout = originalClearTimeout;
  }
}

async function testFeishuObjectOperationRunsDirectlyWithoutBackgroundAck() {
  const brokerCalls = [];
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        brokerCalls.push({ command, payload });
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-17T13:27:00Z",
              updated_at: "2026-03-17T13:27:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "bridge-chat-binding") {
          return { ok: true, binding: null };
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: "已创建飞书多维表格 Test。",
            stderr: "session id: 019ce000-0000-7000-8000-00000000f123",
            returncode: 0,
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });

  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
    group_policy: "all_messages",
    require_mention: false,
  });

  const result = await service.processMessageEvent({
    message: {
      message_id: "om_feishu_create_bitable",
      message_type: "text",
      content: JSON.stringify({ text: "帮我在飞书里面新建一个飞书多维表格做测试，名字就叫 Test" }),
      chat_type: "p2p",
      chat_id: "",
    },
    sender: {
      sender_id: { open_id: "ou_sender" },
    },
  });

  assert.equal(result.ok, true);
  assert.match(result.replyPreview, /已创建飞书多维表格 Test/);
  const execCall = brokerCalls.find((entry) => entry.command === "codex-exec");
  assert.ok(execCall);
  assert.equal(execCall.payload.execution_profile, "feishu");
}

async function testConnectRecoversPendingConversationWithRecoveryNotice() {
  const replies = [];
  const sdkLoader = () => ({
    Domain: { Feishu: 0, Lark: 1 },
    Client: class {
      constructor() {
        this.im = {
          v1: {
            message: {
              create: async (payload) => {
                replies.push(parseReplyText(payload));
                return { ok: true };
              },
            },
          },
        };
      }
    },
    EventDispatcher: class {
      constructor() {
        this.handlers = {};
      }
      register(handlers) {
        this.handlers = handlers;
        return this;
      }
    },
    WSClient: class {
      async start() {}
      async close() {}
    },
    LoggerLevel: { info: "info" },
  });

  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command) => {
        if (command === "bridge-conversations") {
          return {
            ok: true,
            rows: [
              {
                chat_ref: "oc_recover_me",
                project_name: "Codex Hub",
                topic_name: "",
                pending_request: true,
                ack_pending: false,
                awaiting_report: false,
                needs_attention: true,
                attention_reason: "response_delayed",
                last_user_request_age_seconds: 120,
              },
            ],
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        return { ok: true };
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader,
    logger: { info() {}, warn() {}, error() {} },
  });

  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
  });
  const connected = await service.connect();
  assert.equal(connected.ok, true);
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(replies.length, 1);
  assert.match(replies[0], /我刚恢复在线/);
  assert.match(replies[0], /请把你当前还需要我处理的最新指令再发一遍/);
}

async function testOnlyRuntimeQueriesStayDirect() {
  const brokerCalls = [];
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        brokerCalls.push({ command, payload });
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-14T01:00:00Z",
              updated_at: "2026-03-14T01:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "bridge-chat-binding") {
          if (payload.binding_json) {
            return {
              ok: true,
              binding: {
                chat_ref: payload.chat_ref,
                bridge: "feishu",
                ...payload.binding_json,
              },
            };
          }
          return { ok: true, binding: null };
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: "这是来自 start-codex 的正常回答。",
            stderr: "session id: routed-from-codex",
          };
        }
        if (command === "panel") {
          return {
            ok: true,
            cards: [{ label: "Active Projects", value: "5" }],
          };
        }
        if (command === "health") {
          return {
            ok: true,
            payload: { open_alert_count: 0, latest_report: "/tmp/health.md" },
          };
        }
        if (command === "projects") {
          return {
            ok: true,
            projects: [{ project_name: "Codex Obsidian记忆与行动系统", status: "doing", next_action: "继续推进" }],
          };
        }
        if (command === "bridge-conversations") {
          return {
            ok: true,
            rows: [
              {
                chat_ref: "ou_sender",
                thread_label: "Codex Hub",
                binding_label: "Codex Hub",
                execution_state: "running",
                last_user_request: "请帮我继续推进 Feishu 稳定化",
                last_report: "已完成授权链修复，正在继续做可靠性验证。",
                last_error: "",
                pending_approval_token: "coco-allow9",
                pending_approval_action: "git push origin codex/feishu-bridge",
              },
            ],
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });

  let result = await service.handleMessageEvent({
    message: {
      message_id: "om_help",
      message_type: "text",
      content: JSON.stringify({ text: "你能做什么" }),
      chat_type: "p2p",
      chat_id: "",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });
  assert.equal(result.ok, true);
  assert.equal(Boolean(result.direct), false);
  assert.equal(result.replyPhase, "reply");
  assert.match(result.replyPreview, /这是来自 start-codex 的正常回答/);

  result = await service.handleMessageEvent({
    message: {
      message_id: "om_status",
      message_type: "text",
      content: JSON.stringify({ text: "当前系统状态" }),
      chat_type: "p2p",
      chat_id: "",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });
  assert.equal(result.ok, true);
  assert.equal(result.direct, true);
  assert.match(result.replyPreview, /当前系统状态/);
  assert.equal(brokerCalls.some((item) => item.command === "panel"), true);
  assert.equal(brokerCalls.some((item) => item.command === "health"), true);

  result = await service.handleMessageEvent({
    message: {
      message_id: "om_projects",
      message_type: "text",
      content: JSON.stringify({ text: "当前项目状态" }),
      chat_type: "p2p",
      chat_id: "",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });
  assert.equal(result.ok, true);
  assert.equal(Boolean(result.direct), false);
  assert.equal(result.replyPhase, "reply");
  assert.match(result.replyPreview, /这是来自 start-codex 的正常回答/);

  result = await service.handleMessageEvent({
    message: {
      message_id: "om_auth",
      message_type: "text",
      content: JSON.stringify({ text: "当前权限边界是什么" }),
      chat_type: "p2p",
      chat_id: "",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });
  assert.equal(result.ok, true);
  assert.equal(Boolean(result.direct), false);
  assert.equal(result.replyPhase, "reply");
  assert.match(result.replyPreview, /这是来自 start-codex 的正常回答/);

  result = await service.handleMessageEvent({
    message: {
      message_id: "om_thread_status",
      message_type: "text",
      content: JSON.stringify({ text: "当前线程状态" }),
      chat_type: "p2p",
      chat_id: "",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });
  assert.equal(result.ok, true);
  assert.equal(result.direct, true);
  assert.match(result.replyPreview, /当前线程：Codex Hub/);
  assert.match(result.replyPreview, /执行状态：running/);
  assert.match(result.replyPreview, /最近请求：/);

  result = await service.handleMessageEvent({
    message: {
      message_id: "om_approval_status",
      message_type: "text",
      content: JSON.stringify({ text: "当前待授权项" }),
      chat_type: "p2p",
      chat_id: "",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });
  assert.equal(result.ok, true);
  assert.equal(result.direct, true);
  assert.match(result.replyPreview, /当前待授权 token：coco-allow9/);
  assert.match(result.replyPreview, /\/approve coco-allow9/);
}

async function testServiceCanSendManualReport() {
  const replies = [];
  const brokerCalls = [];
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        brokerCalls.push({ command, payload });
        if (command === "bridge-settings") {
          return { ok: true, settings: payload?.settings || {} };
        }
        if (command === "bridge-connection") {
          return { ok: true };
        }
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-14T04:00:00Z",
              updated_at: "2026-03-14T04:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => ({
      Domain: { Feishu: 0, Lark: 1 },
      Client: class {
        constructor() {
          this.im = {
            v1: {
              message: {
                create: async (payload) => {
                  replies.push(payload);
                  return { data: { message_id: "manual-report-1" } };
                },
              },
            },
          };
        }
      },
      EventDispatcher: class {
        register(handlers) {
          this.handlers = handlers;
          return this;
        }
      },
      WSClient: class {
        async start() {}
        async close() {}
      },
      LoggerLevel: { info: "info" },
    }),
    logger: { info() {}, warn() {}, error() {} },
  });

  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
    group_policy: "all_messages",
    require_mention: false,
  });
  await service.connect();

  const result = await service.sendMessage({
    chatId: "oc_codex_hub",
    text: "当前阶段已完成 Feishu 稳定化修复。",
    phase: "report",
  });
  assert.equal(result.ok, true);
  assert.equal(replies.length, 1);
  assert.equal(replies[0].params.receive_id_type, "chat_id");
  assert.equal(replies[0].data.msg_type, "interactive");
  assert.match(parseReplyText(replies[0]), /Frank，当前阶段已完成 Feishu 稳定化修复。/);
  const outbound = brokerCalls.find(
    (item) => item.command === "record-bridge-message" && item.payload.direction === "outbound",
  );
  assert.ok(outbound);
  assert.equal(outbound.payload.payload.phase, "report");
}

async function testLongManualReportMirrorsToDocAndKeepsCardReply() {
  const replies = [];
  const brokerCalls = [];
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        brokerCalls.push({ command, payload });
        if (command === "bridge-settings") {
          return { ok: true, settings: payload?.settings || {} };
        }
        if (command === "bridge-connection") {
          return { ok: true };
        }
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-22T09:00:00Z",
              updated_at: "2026-03-22T09:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "feishu-op") {
          assert.equal(payload.domain, "doc");
          assert.equal(payload.action, "create");
          return {
            ok: true,
            result: {
              document_id: "doc_456",
              url: "https://feishu.cn/docx/doc_456",
            },
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => ({
      Domain: { Feishu: 0, Lark: 1 },
      Client: class {
        constructor() {
          this.im = {
            v1: {
              message: {
                create: async (payload) => {
                  replies.push(payload);
                  return { data: { message_id: "manual-report-long-1" } };
                },
              },
            },
          };
        }
      },
      EventDispatcher: class {
        register(handlers) {
          this.handlers = handlers;
          return this;
        }
      },
      WSClient: class {
        async start() {}
        async close() {}
      },
      LoggerLevel: { info: "info" },
    }),
    logger: { info() {}, warn() {}, error() {} },
  });

  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
    group_policy: "all_messages",
    require_mention: false,
  });
  await service.connect();

  const longReport = `结论：这一轮 Feishu UI 升级已进入验收。\n\n${"细节说明。".repeat(420)}`;
  const result = await service.sendMessage({
    chatId: "oc_codex_hub",
    text: longReport,
    phase: "report",
  });
  assert.equal(result.ok, true);
  assert.equal(replies.length, 1);
  assert.equal(replies[0].data.msg_type, "interactive");
  const rendered = parseReplyText(replies[0]);
  assert.match(rendered, /CoCo 汇报摘要/);
  assert.match(rendered, /结论：这一轮 Feishu UI 升级已进入验收/);
  const docCall = brokerCalls.find((item) => item.command === "feishu-op");
  assert.ok(docCall);
  const outbound = brokerCalls.find(
    (item) => item.command === "record-bridge-message" && item.payload.direction === "outbound",
  );
  assert.ok(outbound);
  assert.match(JSON.stringify(outbound.payload.payload), /doc_456/);
}

async function testStatusMessageUsesMetricDigestCard() {
  const replies = [];
  const brokerCalls = [];
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        brokerCalls.push({ command, payload });
        if (command === "bridge-settings") {
          return { ok: true, settings: payload?.settings || {} };
        }
        if (command === "bridge-connection") {
          return { ok: true };
        }
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-22T09:10:00Z",
              updated_at: "2026-03-22T09:10:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => ({
      Domain: { Feishu: 0, Lark: 1 },
      Client: class {
        constructor() {
          this.im = {
            v1: {
              message: {
                create: async (payload) => {
                  replies.push(payload);
                  return { data: { message_id: "status-card-1" } };
                },
              },
            },
          };
        }
      },
      EventDispatcher: class {
        register(handlers) {
          this.handlers = handlers;
          return this;
        }
      },
      WSClient: class {
        async start() {}
        async close() {}
      },
      LoggerLevel: { info: "info" },
    }),
    logger: { info() {}, warn() {}, error() {} },
  });
  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
    group_policy: "all_messages",
    require_mention: false,
  });
  await service.connect();

  const result = await service.sendMessage({
    chatId: "oc_codex_hub",
    text: "总任务数: 45\nDoing: 7\nBlocked: 2",
    phase: "status",
  });
  assert.equal(result.ok, true);
  assert.equal(replies.length, 1);
  assert.equal(replies[0].data.msg_type, "interactive");
  const rendered = parseReplyText(replies[0]);
  assert.match(rendered, /CoCo 状态摘要/);
  assert.match(rendered, /总任务数/);
  assert.match(rendered, /Doing/);
  assert.match(rendered, /Blocked/);
}

async function testDelayedReplyNotice() {
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command) => {
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-14T01:00:00Z",
              updated_at: "2026-03-14T01:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "panel") {
          return {
            ok: true,
            cards: [{ label: "Active Projects", value: "5" }],
          };
        }
        if (command === "health") {
          return {
            ok: true,
            payload: { open_alert_count: 0, latest_report: "/tmp/health.md" },
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });

  const oldCreateTime = String(Date.now() - 120_000);
  const result = await service.handleMessageEvent({
    message: {
      message_id: "om_delayed",
      message_type: "text",
      content: JSON.stringify({ text: "当前系统状态" }),
      chat_type: "p2p",
      chat_id: "",
      create_time: oldCreateTime,
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });
  assert.equal(result.ok, true);
  assert.match(result.replyPreview, /说明：你这条消息是延迟补回的/);
  assert.match(result.replyPreview, /我现在已经在线/);
}

async function testChatBindingDeclarationRoutesWithProjectContext() {
  const brokerCalls = [];
  const bindings = new Map();
  await withTempProjectRegistry(
    [
      {
        project_name: "SampleProj",
        aliases: ["Sample", "SampleProj"],
        path: "03_semantic/projects/SampleProj.md",
        status: "active",
        summary_note: "Sample product project",
      },
    ],
    async () => {
      const service = createFeishuLongConnectionService({
        brokerClient: {
          call: async (command, payload) => {
            brokerCalls.push({ command, payload });
            if (command === "record-bridge-message") {
              return {
                ok: true,
                record: {
                  created_at: "2026-03-14T01:00:00Z",
                  updated_at: "2026-03-14T01:00:00Z",
                },
              };
            }
            if (command === "user-profile") {
              return {
                ok: true,
                profile: { preferred_name: "Frank", relationship: "workspace owner" },
              };
            }
            if (command === "bridge-chat-binding") {
              const chatRef = String(payload.chat_ref || "");
              if (payload.binding_json) {
                const binding = {
                  chat_ref: chatRef,
                  bridge: "feishu",
                  ...payload.binding_json,
                };
                bindings.set(chatRef, binding);
                return { ok: true, binding };
              }
              return { ok: true, binding: bindings.get(chatRef) || null };
            }
            return {
              ok: true,
              stdout: "这是第一轮回答。",
              stderr: "session id: binding-session-1",
            };
          },
        },
        runtimeState: {
          saveBridgeStatus: async () => ({}),
          saveBridgeSettings: async () => ({}),
        },
        sdkLoader: () => null,
        logger: { info() {}, warn() {}, error() {} },
      });
      await service.loadSettings({
        app_id: "cli_123",
        app_secret: "secret",
        group_policy: "all_messages",
        require_mention: false,
      });

      const bindingResult = await service.handleMessageEvent({
        message: {
          message_id: "bind-msg",
          message_type: "text",
          content: JSON.stringify({ text: "哦不是，我说的是 SampleProj。在这个聊天群里面，我们只聊 SampleProj @_user_1" }),
          chat_type: "group",
          chat_id: "chat-bind",
        },
        sender: { sender_id: { open_id: "ou_sender" } },
      });
      assert.equal(bindingResult.ok, true);
      assert.equal(bindingResult.direct, true);
      assert.match(bindingResult.replyPreview, /已将本群绑定到项目 `SampleProj`/);

      const binding = await service.getBindingForConversationKey("chat-bind");
      assert.equal(binding?.project_name, "SampleProj");
      assert.equal(binding?.topic_name, "");

      const followupResult = await service.handleMessageEvent({
        message: {
          message_id: "bind-msg-2",
          message_type: "text",
          content: JSON.stringify({ text: "聊聊这个项目的进展" }),
          chat_type: "group",
          chat_id: "chat-bind",
        },
        sender: { sender_id: { open_id: "ou_sender" } },
      });
      assert.equal(followupResult.ok, true);
      const execCall = brokerCalls.find((item) => item.command === "codex-exec");
      assert.ok(execCall);
      assert.equal(execCall.payload.project_name, "SampleProj");
      assert.equal(execCall.payload.topic_name, undefined);
      assert.equal(execCall.payload.thread_label, "SampleProj");
      assert.equal(execCall.payload.no_auto_resume, true);

      await service.handleMessageEvent({
        message: {
          message_id: "bind-msg-3",
          message_type: "text",
          content: JSON.stringify({ text: "继续推进" }),
          chat_type: "group",
          chat_id: "chat-bind",
        },
        sender: { sender_id: { open_id: "ou_sender" } },
      });
      const resumeCall = brokerCalls.find((item) => item.command === "codex-resume");
      assert.ok(resumeCall);
      assert.equal(resumeCall.payload.session_id, "binding-session-1");

      await service.handleMessageEvent({
        message: {
          message_id: "bind-msg-4",
          message_type: "text",
          content: JSON.stringify({ text: "继续说 SampleProj 这个项目的后续安排" }),
          chat_type: "group",
          chat_id: "chat-bind",
        },
        sender: { sender_id: { open_id: "ou_sender" } },
      });
      const resumeCalls = brokerCalls.filter((item) => item.command === "codex-resume");
      assert.equal(resumeCalls.length, 2);
      assert.equal(resumeCalls[1].payload.session_id, "binding-session-1");
    },
  );
}

async function testChatBindingDeclarationHandlesNaturalProjectPhrase() {
  const bindings = new Map();
  await withTempProjectRegistry(
    [
      {
        project_name: "示例交付",
        aliases: ["示例交付", "示例交付项目"],
        path: "03_semantic/projects/示例交付.md",
        status: "active",
        summary_note: "Sample delivery project",
      },
    ],
    async () => {
      const service = createFeishuLongConnectionService({
        brokerClient: {
          call: async (command, payload) => {
            if (command === "record-bridge-message") {
              return {
                ok: true,
                record: {
                  created_at: "2026-03-14T02:30:00Z",
                  updated_at: "2026-03-14T02:30:00Z",
                },
              };
            }
            if (command === "user-profile") {
              return {
                ok: true,
                profile: { preferred_name: "Frank", relationship: "workspace owner" },
              };
            }
            if (command === "bridge-chat-binding") {
              const chatRef = String(payload.chat_ref || "");
              if (payload.binding_json) {
                const binding = {
                  chat_ref: chatRef,
                  bridge: "feishu",
                  ...payload.binding_json,
                };
                bindings.set(chatRef, binding);
                return { ok: true, binding };
              }
              return { ok: true, binding: bindings.get(chatRef) || null };
            }
            return {
              ok: true,
              stdout: "已绑定。",
              stderr: "session id: binding-session-natural",
            };
          },
        },
        runtimeState: {
          saveBridgeStatus: async () => ({}),
          saveBridgeSettings: async () => ({}),
        },
        sdkLoader: () => null,
        logger: { info() {}, warn() {}, error() {} },
      });
      await service.loadSettings({
        app_id: "cli_123",
        app_secret: "secret",
        group_policy: "all_messages",
        require_mention: false,
      });

      const bindingResult = await service.handleMessageEvent({
        message: {
          message_id: "bind-natural",
          message_type: "text",
          content: JSON.stringify({
            text: "@_user_1 这个群组只有你和我。你可以叫我吉祥或者Frank 。在这里，我们只聊示例交付的项目",
          }),
          chat_type: "group",
          chat_id: "chat-natural",
        },
        sender: { sender_id: { open_id: "ou_sender" } },
      });
      assert.equal(bindingResult.ok, true);
      assert.equal(bindingResult.direct, true);
      assert.match(bindingResult.replyPreview, /已将本群绑定到项目 `示例交付`/);

      const binding = await service.getBindingForConversationKey("chat-natural");
      assert.equal(binding?.project_name, "示例交付");
      assert.equal(binding?.topic_name, "");
    },
  );
}

async function testChatBindingDeclarationCapturesTopicHint() {
  const brokerCalls = [];
  const bindings = new Map();
  await withTempProjectRegistry(
    [
      {
        project_name: "示例交付",
        aliases: ["示例交付"],
        path: "03_semantic/projects/示例交付.md",
        status: "active",
        summary_note: "Sample delivery project",
      },
    ],
    async () => {
      const service = createFeishuLongConnectionService({
        brokerClient: {
          call: async (command, payload) => {
            brokerCalls.push({ command, payload });
            if (command === "record-bridge-message") {
              return {
                ok: true,
                record: {
                  created_at: "2026-03-14T02:00:00Z",
                  updated_at: "2026-03-14T02:00:00Z",
                },
              };
            }
            if (command === "user-profile") {
              return {
                ok: true,
                profile: { preferred_name: "Frank", relationship: "workspace owner" },
              };
            }
            if (command === "bridge-chat-binding") {
              const chatRef = String(payload.chat_ref || "");
              if (payload.binding_json) {
                const binding = {
                  chat_ref: chatRef,
                  bridge: "feishu",
                  ...payload.binding_json,
                };
                bindings.set(chatRef, binding);
                return { ok: true, binding };
              }
              return { ok: true, binding: bindings.get(chatRef) || null };
            }
            return {
              ok: true,
              stdout: "这是第一轮回答。",
              stderr: "session id: binding-session-2",
            };
          },
        },
        runtimeState: {
          saveBridgeStatus: async () => ({}),
          saveBridgeSettings: async () => ({}),
        },
        sdkLoader: () => null,
        logger: { info() {}, warn() {}, error() {} },
      });
      await service.loadSettings({
        app_id: "cli_123",
        app_secret: "secret",
        group_policy: "all_messages",
        require_mention: false,
      });

      const bindingResult = await service.handleMessageEvent({
        message: {
          message_id: "topic-bind-msg",
          message_type: "text",
          content: JSON.stringify({ text: "这个群只聊 示例交付 展馆线" }),
          chat_type: "group",
          chat_id: "chat-topic",
        },
        sender: { sender_id: { open_id: "ou_sender" } },
      });
      assert.equal(bindingResult.ok, true);
      assert.equal(bindingResult.direct, true);
      assert.match(bindingResult.replyPreview, /话题 `展馆线`/);

      const binding = await service.getBindingForConversationKey("chat-topic");
      assert.equal(binding?.project_name, "示例交付");
      assert.equal(binding?.topic_name, "展馆线");

      const followup = await service.handleMessageEvent({
        message: {
          message_id: "topic-followup",
          message_type: "text",
          content: JSON.stringify({ text: "继续" }),
          chat_type: "group",
          chat_id: "chat-topic",
        },
        sender: { sender_id: { open_id: "ou_sender" } },
      });
      assert.equal(followup.ok, true);
      const execCall = brokerCalls.find((item) => item.command === "codex-exec");
      assert.ok(execCall);
      assert.equal(execCall.payload.project_name, "示例交付");
      assert.equal(execCall.payload.topic_name, "展馆线");
      assert.equal(execCall.payload.thread_label, "示例交付 / 展馆线");
      assert.equal(execCall.payload.no_auto_resume, true);
    },
  );
}

async function testChatBindingDeclarationReportsBrokerValidationFailure() {
  const bindings = new Map();
  await withTempProjectRegistry(
    [
      {
        project_name: "示例交付",
        aliases: ["示例交付"],
        path: "03_semantic/projects/示例交付.md",
        status: "active",
        summary_note: "Sample delivery project",
      },
    ],
    async () => {
      const service = createFeishuLongConnectionService({
        brokerClient: {
          call: async (command, payload) => {
            if (command === "record-bridge-message") {
              return {
                ok: true,
                record: {
                  created_at: "2026-03-14T02:00:00Z",
                  updated_at: "2026-03-14T02:00:00Z",
                },
              };
            }
            if (command === "user-profile") {
              return {
                ok: true,
                profile: { preferred_name: "Frank", relationship: "workspace owner" },
              };
            }
            if (command === "bridge-chat-binding") {
              const chatRef = String(payload.chat_ref || "");
              if (payload.binding_json) {
                return {
                  ok: false,
                  error: "unknown topic_name `bad-topic` for project `示例交付`",
                  available_topics: ["展馆线"],
                };
              }
              return { ok: true, binding: bindings.get(chatRef) || null };
            }
            throw new Error(`unexpected command ${command}`);
          },
        },
        runtimeState: {
          saveBridgeStatus: async () => ({}),
          saveBridgeSettings: async () => ({}),
        },
        sdkLoader: () => null,
        logger: { info() {}, warn() {}, error() {} },
      });
      await service.loadSettings({
        app_id: "cli_123",
        app_secret: "secret",
        group_policy: "all_messages",
        require_mention: false,
      });

      const result = await service.handleMessageEvent({
        message: {
          message_id: "topic-bind-fail",
          message_type: "text",
          content: JSON.stringify({ text: "这个群只聊 示例交付 bad-topic" }),
          chat_type: "group",
          chat_id: "chat-topic-fail",
        },
        sender: { sender_id: { open_id: "ou_sender" } },
      });

      assert.equal(result.ok, true);
      assert.equal(result.direct, true);
      assert.match(result.replyPreview, /绑定没有成功/);
      assert.match(result.replyPreview, /展馆线/);
      assert.equal(await service.getBindingForConversationKey("chat-topic-fail"), null);
    },
  );
}

async function testPausedProjectReturnsPauseSummary() {
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command) => {
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-14T03:00:00Z",
              updated_at: "2026-03-14T03:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "bridge-chat-binding") {
          return {
            ok: true,
            binding: {
              chat_ref: "chat-paused",
              bridge: "feishu",
              binding_scope: "project",
              project_name: "SampleProj",
              topic_name: "",
              session_id: "",
              metadata: {},
            },
          };
        }
        if (command === "codex-exec") {
          return {
            ok: false,
            result_status: "suppressed",
            reason: "project_paused",
            error_type: "project_paused",
            pause: {
              summary: "项目 `SampleProj` 当前已暂停执行，请先恢复后再发起任务。",
            },
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });
  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
  });

  const result = await service.handleMessageEvent({
    message: {
      message_id: "paused-msg",
      message_type: "text",
      content: JSON.stringify({ text: "请继续执行最新任务" }),
      chat_type: "p2p",
      chat_id: "chat-paused",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(result.ok, true);
  assert.match(result.replyPreview, /SampleProj/);
  assert.match(result.replyPreview, /暂停执行/);
}

async function testUnboundGroupPromptsForBinding() {
  const brokerCalls = [];
  const replies = [];
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        brokerCalls.push({ command, payload });
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-14T03:00:00Z",
              updated_at: "2026-03-14T03:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "bridge-chat-binding") {
          return { ok: true, binding: null };
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: "测试文件已创建。",
            stderr: "session id: 019ce000-0000-7000-8000-000000000888",
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => ({
      Domain: { Feishu: 0, Lark: 1 },
      Client: class {
        constructor() {
          this.im = {
            v1: {
              message: {
                create: async (payload) => {
                  replies.push(parseReplyText(payload));
                  return { ok: true };
                },
              },
            },
          };
        }
      },
      EventDispatcher: class {
        register() {
          return this;
        }
      },
      WSClient: class {
        async start() {}
        async close() {}
      },
      LoggerLevel: { info: "info" },
    }),
    logger: { info() {}, warn() {}, error() {} },
  });
  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
    group_policy: "all_messages",
    require_mention: false,
  });
  await service.connect();

  const result = await service.processMessageEvent({
    message: {
      message_id: "unbind-group",
      message_type: "text",
      content: JSON.stringify({ text: "帮我创建一个测试文件" }),
      chat_type: "group",
      chat_id: "chat-unbound",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(result.ok, false);
  assert.equal(result.reason, "background_started");
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.ok(brokerCalls.some((item) => item.command === "codex-exec"));
  assert.equal(replies.some((item) => /Frank，测试文件已创建。/.test(item)), true);
}

async function testUnboundGroupAutoRoutesByProjectAliasAndPersistsContext() {
  const bindings = new Map();
  const brokerCalls = [];
  await withTempProjectRegistry(
    [
      {
        project_name: "SampleProj",
        aliases: ["Sample", "SampleProj"],
        path: "03_semantic/projects/SampleProj.md",
        status: "active",
        summary_note: "Sample education project",
      },
    ],
    async () => {
      const service = createFeishuLongConnectionService({
        brokerClient: {
          call: async (command, payload) => {
            brokerCalls.push({ command, payload });
            if (command === "record-bridge-message") {
              return {
                ok: true,
                record: {
                  created_at: "2026-03-16T04:00:00Z",
                  updated_at: "2026-03-16T04:00:00Z",
                },
              };
            }
            if (command === "user-profile") {
              return {
                ok: true,
                profile: { preferred_name: "Frank", relationship: "workspace owner" },
              };
            }
            if (command === "bridge-chat-binding") {
              const chatRef = String(payload.chat_ref || "");
              if (payload.binding_json) {
                const binding = { bridge: "feishu", chat_ref: chatRef, ...payload.binding_json };
                bindings.set(chatRef, binding);
                return { ok: true, binding };
              }
              return { ok: true, binding: bindings.get(chatRef) || null };
            }
            if (command === "codex-exec") {
              return {
                ok: true,
                stdout: "已切到 SampleProj 上下文并完成摘要。",
                stderr: "session id: 019ce000-0000-7000-8000-000000000999",
              };
            }
            throw new Error(`unexpected command ${command}`);
          },
        },
        runtimeState: {
          saveBridgeStatus: async () => ({}),
          saveBridgeSettings: async () => ({}),
        },
        sdkLoader: () => null,
        logger: { info() {}, warn() {}, error() {} },
      });
      await service.loadSettings({
        app_id: "cli_123",
        app_secret: "secret",
        group_policy: "all_messages",
        require_mention: false,
      });

      const result = await service.processMessageEvent({
        message: {
          message_id: "route-tint-1",
          message_type: "text",
          content: JSON.stringify({ text: "继续看一下 SampleProj 项目当前的情况，然后汇报。" }),
          chat_type: "group",
          chat_id: "chat-soft-route",
        },
        sender: { sender_id: { open_id: "ou_sender" } },
      });

      assert.equal(result.ok, true);
      const execCall = brokerCalls.find((item) => item.command === "codex-exec");
      assert.ok(execCall);
      assert.equal(execCall.payload.project_name, "SampleProj");
      const binding = bindings.get("chat-soft-route");
      assert.equal(binding?.project_name, "SampleProj");
      assert.equal(binding?.session_id, "019ce000-0000-7000-8000-000000000999");
      assert.equal(binding?.metadata?.last_route_source, "message_project_alias");
    },
  );
}

async function testServiceBlocksWithoutSdk() {
  const service = createFeishuLongConnectionService({
    brokerClient: { call: async () => ({ ok: true }) },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });
  await service.loadSettings({ app_id: "cli_123", app_secret: "secret" });
  const result = await service.connect();
  assert.equal(result.ok, false);
  assert.equal(result.reason, "sdk_unavailable");
}

async function testLongRepliesAreSplitIntoMultipleMessages() {
  const replies = [];
  const wsStarts = [];
  const sdkLoader = () => ({
    Domain: { Feishu: 0, Lark: 1 },
    Client: class {
      constructor() {
        this.im = {
          v1: {
            message: {
              create: async (payload) => {
                replies.push(parseReplyText(payload));
                return { ok: true };
              },
            },
          },
        };
      }
    },
    EventDispatcher: class {
      constructor() {
        this.handlers = {};
      }
      register(handlers) {
        this.handlers = handlers;
        return this;
      }
    },
    WSClient: class {
      async start(payload) {
        wsStarts.push(payload);
      }
      async close() {}
    },
    LoggerLevel: { info: "info" },
  });
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command) => {
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-14T01:00:00Z",
              updated_at: "2026-03-14T01:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        return {
          ok: true,
          stdout: `${"长回复。".repeat(900)}\n结尾`,
          stderr: "session id: 019ce000-0000-7000-8000-000000000099",
        };
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader,
    logger: { info() {}, warn() {}, error() {} },
  });
  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
    group_policy: "all_messages",
    require_mention: false,
  });
  const connected = await service.connect();
  assert.equal(connected.ok, true);
  const handler = wsStarts[0].eventDispatcher.handlers["im.message.receive_v1"];
  await handler({
    message: {
      message_id: "om_long_reply",
      message_type: "text",
      content: JSON.stringify({ text: "请给我完整长回复" }),
      chat_type: "p2p",
      chat_id: "",
    },
    sender: {
      sender_id: { open_id: "ou_sender" },
    },
  });
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.ok(replies.length >= 1);
  assert.doesNotMatch(replies[0], /如需细节，我再展开。/);
  assert.match(replies[0], /长回复。/);
}

async function testFeishuReplyCompactsLinksAndPaths() {
  const replies = [];
  const wsStarts = [];
  const sdkLoader = () => ({
    Domain: { Feishu: 0, Lark: 1 },
    Client: class {
      constructor() {
        this.im = {
          v1: {
            message: {
              create: async (payload) => {
                replies.push(parseReplyText(payload));
                return { ok: true };
              },
            },
          },
        };
      }
    },
    EventDispatcher: class {
      constructor() {
        this.handlers = {};
      }
      register(handlers) {
        this.handlers = handlers;
        return this;
      }
    },
    WSClient: class {
      async start(payload) {
        wsStarts.push(payload);
      }
      async close() {}
    },
    LoggerLevel: { info: "info" },
  });
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command) => {
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-14T01:00:00Z",
              updated_at: "2026-03-14T01:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        return {
          ok: true,
          stdout:
            "请查看 [详细报告](reports/system/feishu-bridge-runtime-plan.md)\n" +
            "完整日志在 https://example.com/report/12345\n" +
            "问题点位于 workspace/bridge/feishu_long_connection_service.js",
          stderr: "session id: 019ce000-0000-7000-8000-000000000555",
        };
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader,
    logger: { info() {}, warn() {}, error() {} },
  });
  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
    group_policy: "all_messages",
    require_mention: false,
  });
  const connected = await service.connect();
  assert.equal(connected.ok, true);
  const handler = wsStarts[0].eventDispatcher.handlers["im.message.receive_v1"];
  await handler({
    message: {
      message_id: "om_compact",
      message_type: "text",
      content: JSON.stringify({ text: "把报告发我" }),
      chat_type: "p2p",
      chat_id: "",
    },
    sender: {
      sender_id: { open_id: "ou_sender" },
    },
  });
  await new Promise((resolve) => setTimeout(resolve, 50));
  const finalReply = replies[replies.length - 1];
  assert.match(finalReply, /详细报告/);
  assert.match(finalReply, /https:\/\/example\.com\/report\/12345/);
  assert.match(finalReply, /workspace\/bridge\/feishu_long_connection_service\.js/);
}

async function testHighRiskRequestReturnsApprovalTokenPrompt() {
  const brokerCalls = [];
  const approvalItems = new Map();
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        brokerCalls.push({ command, payload });
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-14T01:00:00Z",
              updated_at: "2026-03-14T01:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "bridge-chat-binding") {
          if (payload.binding_json) {
            return { ok: true, binding: { chat_ref: payload.chat_ref, ...payload.binding_json } };
          }
          return { ok: true, binding: { chat_ref: payload.chat_ref, project_name: "Codex Hub", binding_scope: "project" } };
        }
        if (command === "approval-token") {
          if (payload.token_json) {
            const item = { token: payload.token, ...payload.token_json };
            approvalItems.set(payload.token, item);
            return { ok: true, item };
          }
          return { ok: true, item: approvalItems.get(payload.token) || { token: payload.token, status: "" } };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });

  const result = await service.handleMessageEvent({
    message: {
      message_id: "om_high_risk",
      message_type: "text",
      content: JSON.stringify({ text: "请帮我 git push 当前分支" }),
      chat_type: "p2p",
      chat_id: "oc_direct",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(result.ok, true);
  assert.equal(result.direct, true);
  assert.match(result.replyPreview, /(高风险远程或不可逆动作|需要授权后才能继续)/);
  assert.match(result.replyPreview, /\/approve coco-/);
  const saved = brokerCalls.find((item) => item.command === "approval-token" && item.payload.token_json);
  assert.ok(saved);
  assert.equal(saved.payload.token_json.scope, "feishu_high_risk_execution");
  assert.equal(saved.payload.token_json.status, "pending");
  assert.equal(saved.payload.token_json.metadata.requested_text, "请帮我 git push 当前分支");
}

async function testHighRiskRequestEventHandlerSendsInteractiveApprovalCard() {
  const sentMessages = [];
  const approvalItems = new Map();
  const wsStarts = [];
  const sdkLoader = () => ({
    Domain: { Feishu: 0, Lark: 1 },
    Client: class {
      constructor() {
        this.im = {
          v1: {
            message: {
              create: async (payload) => {
                sentMessages.push(payload);
                return { data: { message_id: "msg-card-1" }, ok: true };
              },
            },
          },
        };
      }
    },
    EventDispatcher: class {
      register(handlers) {
        this.handlers = handlers;
        return this;
      }
    },
    WSClient: class {
      async start(payload) {
        wsStarts.push(payload);
      }
      async close() {}
    },
    LoggerLevel: { info: "info" },
  });
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-16T01:00:00Z",
              updated_at: "2026-03-16T01:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "bridge-chat-binding") {
          return { ok: true, binding: { chat_ref: payload.chat_ref, project_name: "Codex Hub", binding_scope: "project" } };
        }
        if (command === "approval-token") {
          if (payload.token_json) {
            const existing = approvalItems.get(payload.token) || { token: payload.token };
            const item = { ...existing, ...payload.token_json };
            approvalItems.set(payload.token, item);
            return { ok: true, item };
          }
          return { ok: true, item: approvalItems.get(payload.token) || { token: payload.token, status: "" } };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader,
    logger: { info() {}, warn() {}, error() {} },
  });
  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
    group_policy: "all_messages",
    require_mention: false,
  });
  await service.connect();
  const handler = wsStarts[0].eventDispatcher.handlers["im.message.receive_v1"];
  await handler({
    message: {
      message_id: "om_card_prompt",
      message_type: "text",
      content: JSON.stringify({ text: "请帮我 git push 当前分支" }),
      chat_type: "p2p",
      chat_id: "oc_direct",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(sentMessages.length, 1);
  assert.equal(sentMessages[0].data.msg_type, "interactive");
  const card = JSON.parse(sentMessages[0].data.content);
  const markdownBlock = card.body.elements.find((item) => item.tag === "markdown");
  assert.match(markdownBlock.content, /\/approve coco-/);
  const buttonCallbacks = JSON.stringify(card);
  assert.match(buttonCallbacks, /perm:allow:coco-/);
  const [[token, tokenItem]] = approvalItems.entries();
  assert.ok(token.startsWith("coco-"));
  assert.equal(tokenItem.metadata.approval_delivery, "interactive_card");
  assert.equal(tokenItem.metadata.approval_message_id, "msg-card-1");
}

async function testLocalSystemApprovalPromptStoresScopedApprovedProfile() {
  const approvalItems = new Map();
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-20T01:00:00Z",
              updated_at: "2026-03-20T01:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "approval-token") {
          if (payload.token_json) {
            const item = { token: payload.token, ...payload.token_json };
            approvalItems.set(payload.token, item);
            return { ok: true, item };
          }
          return { ok: true, item: approvalItems.get(payload.token) || { token: payload.token, status: "" } };
        }
        if (command === "bridge-chat-binding") {
          return { ok: true, binding: { project_name: "Codex Hub", binding_scope: "project" } };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });
  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
    group_policy: "all_messages",
    require_mention: false,
  });

  const result = await service.processMessageEvent({
    message: {
      message_id: "om_local_system_request",
      message_type: "text",
      content: JSON.stringify({ text: "请帮我把 launch agent 安装到 ~/Library/LaunchAgents" }),
      chat_type: "p2p",
      chat_id: "oc_direct",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(result.ok, true);
  const [[token, tokenItem]] = approvalItems.entries();
  assert.ok(token.startsWith("coco-"));
  assert.equal(tokenItem.scope, "feishu_local_system_execution");
  assert.equal(tokenItem.metadata.approved_execution_profile, "feishu-local-system-approved");
}

async function testApproveCommandExecutesWithApprovedProfile() {
  const replies = [];
  const approvalItems = new Map([
    [
      "coco-allow1",
      {
        token: "coco-allow1",
        scope: "feishu_high_risk_execution",
        status: "pending",
        project_name: "Codex Hub",
        session_id: "",
        expires_at: "2036-03-20T00:00:00Z",
        metadata: {
          requested_text: "请帮我 git push 当前分支",
        },
      },
    ],
  ]);
  const brokerCalls = [];
  const sdkLoader = () => ({
    Domain: { Feishu: 0, Lark: 1 },
    Client: class {
      constructor() {
        this.im = {
          v1: {
            message: {
              create: async (payload) => {
                replies.push(parseReplyText(payload));
                return { ok: true };
              },
            },
          },
        };
      }
    },
    EventDispatcher: class {
      register(handlers) {
        this.handlers = handlers;
        return this;
      }
    },
    WSClient: class {
      async start() {}
      async close() {}
    },
    LoggerLevel: { info: "info" },
  });
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        brokerCalls.push({ command, payload });
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-14T01:00:00Z",
              updated_at: "2026-03-14T01:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "approval-token") {
          if (payload.token_json) {
            const existing = approvalItems.get(payload.token) || { token: payload.token };
            const item = { ...existing, ...payload.token_json };
            approvalItems.set(payload.token, item);
            return { ok: true, item };
          }
          return { ok: true, item: approvalItems.get(payload.token) || { token: payload.token, status: "" } };
        }
        if (command === "bridge-chat-binding") {
          return {
            ok: true,
            binding: {
              chat_ref: payload.chat_ref,
              project_name: "Codex Hub",
              binding_scope: "project",
            },
          };
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: "已完成 git push。",
            stderr: "session id: 019ce000-0000-7000-8000-000000000777",
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader,
    logger: { info() {}, warn() {}, error() {} },
  });
  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
    group_policy: "all_messages",
    require_mention: false,
  });
  await service.connect();

  const result = await service.processMessageEvent({
    message: {
      message_id: "om_approve",
      message_type: "text",
      content: JSON.stringify({ text: "/approve coco-allow1" }),
      chat_type: "p2p",
      chat_id: "oc_direct",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(result.ok, true);
  assert.match(replies[0], /已记录授权，开始执行。/);
  assert.match(replies[0], /当前线程：Codex Hub/);
  assert.equal(result.replyPreview, "Frank，已完成 git push。");
  const execCall = brokerCalls.find((item) => item.command === "codex-exec");
  assert.ok(execCall);
  assert.equal(execCall.payload.execution_profile, "feishu-approved");
  assert.equal(execCall.payload.approval_token, "coco-allow1");
  assert.equal(execCall.payload.prompt, "请帮我 git push 当前分支");
  assert.equal(approvalItems.get("coco-allow1").status, "approved");
}

async function testApproveCommandExecutesWithLocalSystemApprovedProfile() {
  const approvalItems = new Map([
    [
      "coco-local-system-1",
      {
        token: "coco-local-system-1",
        scope: "feishu_local_system_execution",
        status: "pending",
        project_name: "Codex Hub",
        session_id: "",
        expires_at: "2036-03-20T00:00:00Z",
        metadata: {
          requested_text: "请帮我把 launch agent 安装到 ~/Library/LaunchAgents",
        },
      },
    ],
  ]);
  const brokerCalls = [];
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        brokerCalls.push({ command, payload });
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-20T01:00:00Z",
              updated_at: "2026-03-20T01:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "approval-token") {
          if (payload.token_json) {
            const existing = approvalItems.get(payload.token) || { token: payload.token };
            const item = { ...existing, ...payload.token_json };
            approvalItems.set(payload.token, item);
            return { ok: true, item };
          }
          return { ok: true, item: approvalItems.get(payload.token) || { token: payload.token, status: "" } };
        }
        if (command === "bridge-chat-binding") {
          return {
            ok: true,
            binding: {
              chat_ref: payload.chat_ref,
              project_name: "Codex Hub",
              binding_scope: "project",
            },
          };
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: "已完成本地系统安装。",
            stderr: "session id: 019ce000-0000-7000-8000-000000000778",
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });
  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
    group_policy: "all_messages",
    require_mention: false,
  });

  const result = await service.processMessageEvent({
    message: {
      message_id: "om_approve_local_system",
      message_type: "text",
      content: JSON.stringify({ text: "/approve coco-local-system-1" }),
      chat_type: "p2p",
      chat_id: "oc_direct",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(result.ok, true);
  const execCall = brokerCalls.find((item) => item.command === "codex-exec");
  assert.ok(execCall);
  assert.equal(execCall.payload.execution_profile, "feishu-local-system-approved");
  assert.equal(execCall.payload.approval_token, "coco-local-system-1");
  assert.equal(execCall.payload.prompt, "请帮我把 launch agent 安装到 ~/Library/LaunchAgents");
  assert.equal(approvalItems.get("coco-local-system-1").status, "approved");
}

async function testCardActionTriggerApprovesViaInteractiveCard() {
  const replies = [];
  const approvalItems = new Map([
    [
      "coco-card-1",
      {
        token: "coco-card-1",
        scope: "feishu_high_risk_execution",
        status: "pending",
        project_name: "Codex Hub",
        session_id: "",
        expires_at: "2036-03-20T00:00:00Z",
        metadata: {
          requested_text: "请帮我 git push 当前分支",
          chat_id: "oc_direct",
          open_id: "ou_sender",
          approval_message_id: "msg-card-approve",
        },
      },
    ],
  ]);
  const brokerCalls = [];
  const sdkLoader = () => ({
    Domain: { Feishu: 0, Lark: 1 },
    Client: class {
      constructor() {
        this.im = {
          v1: {
            message: {
              create: async (payload) => {
                replies.push(parseReplyText(payload));
                return { ok: true };
              },
            },
          },
        };
      }
    },
    EventDispatcher: class {
      register(handlers) {
        this.handlers = handlers;
        return this;
      }
    },
    WSClient: class {
      async start() {}
      async close() {}
    },
    LoggerLevel: { info: "info" },
  });
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        brokerCalls.push({ command, payload });
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-16T01:10:00Z",
              updated_at: "2026-03-16T01:10:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "approval-token") {
          if (payload.token_json) {
            const existing = approvalItems.get(payload.token) || { token: payload.token };
            const item = { ...existing, ...payload.token_json };
            approvalItems.set(payload.token, item);
            return { ok: true, item };
          }
          return { ok: true, item: approvalItems.get(payload.token) || { token: payload.token, status: "" } };
        }
        if (command === "bridge-chat-binding") {
          return {
            ok: true,
            binding: {
              chat_ref: payload.chat_ref,
              project_name: "Codex Hub",
              binding_scope: "project",
            },
          };
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: "已完成 git push。",
            stderr: "session id: 019ce000-0000-7000-8000-000000000888",
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader,
    logger: { info() {}, warn() {}, error() {} },
  });
  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
    group_policy: "all_messages",
    require_mention: false,
  });
  await service.connect();
  const result = await service.handleCardActionEvent({
    action: {
      value: {
        callback_data: "perm:allow:coco-card-1",
      },
    },
    context: {
      open_chat_id: "oc_direct",
      open_message_id: "msg-card-approve",
    },
    operator: {
      open_id: "ou_sender",
    },
  });
  await new Promise((resolve) => setTimeout(resolve, 10));

  assert.equal(result.toast.content, "已批准，正在执行。");
  assert.equal(result.card?.type, "raw");
  assert.equal(result.card?.data?.header?.title?.content, "CoCo 授权状态");
  assert.equal(result.card?.data?.header?.template, "green");
  assert.equal(approvalItems.get("coco-card-1").status, "approved");
  const execCall = brokerCalls.find((item) => item.command === "codex-exec");
  assert.ok(execCall);
  assert.equal(execCall.payload.execution_profile, "feishu-approved");
  assert.equal(execCall.payload.approval_token, "coco-card-1");
  assert.equal(execCall.payload.prompt, "请帮我 git push 当前分支");
  assert.match(replies[0], /已记录授权，开始执行。/);
  assert.match(replies[0], /当前线程：Codex Hub/);
  assert.match(replies[1], /Frank，已完成 git push。/);
}

async function testCardActionFallsBackToEmbeddedChatContext() {
  const approvalItems = new Map([
    [
      "coco-card-2",
      {
        token: "coco-card-2",
        scope: "feishu_high_risk_execution",
        status: "pending",
        project_name: "Codex Hub",
        session_id: "",
        expires_at: "2036-03-20T00:00:00Z",
        metadata: {
          requested_text: "请帮我 git push 当前分支",
          chat_id: "oc_direct_fallback",
          open_id: "ou_sender",
          source_message_id: "om-source-fallback",
          approval_message_id: "msg-card-fallback",
        },
      },
    ],
  ]);
  const brokerCalls = [];
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        brokerCalls.push({ command, payload });
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-16T01:15:00Z",
              updated_at: "2026-03-16T01:15:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "approval-token") {
          if (payload.token_json) {
            const existing = approvalItems.get(payload.token) || { token: payload.token };
            const item = { ...existing, ...payload.token_json };
            approvalItems.set(payload.token, item);
            return { ok: true, item };
          }
          return { ok: true, item: approvalItems.get(payload.token) || { token: payload.token, status: "" } };
        }
        if (command === "bridge-chat-binding") {
          return {
            ok: true,
            binding: {
              chat_ref: payload.chat_ref,
              project_name: "Codex Hub",
              binding_scope: "project",
            },
          };
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: "已完成 git push。",
            stderr: "session id: 019ce000-0000-7000-8000-000000000999",
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });

  const result = await service.handleCardActionEvent({
    action: {
      value: {
        callback_data: "perm:allow:coco-card-2",
        chat_id: "oc_direct_fallback",
        message_id: "om-source-fallback",
      },
    },
    context: {
      open_chat_id: "oc_direct_fallback",
      open_message_id: "msg-card-fallback",
    },
    operator: {
      open_id: "ou_sender",
    },
  });
  await new Promise((resolve) => setTimeout(resolve, 10));

  assert.equal(result.toast.content, "已批准，正在执行。");
  assert.equal(result.card?.type, "raw");
  const execCall = brokerCalls.find((item) => item.command === "codex-exec");
  assert.ok(execCall);
  assert.equal(execCall.payload.execution_profile, "feishu-approved");
  assert.equal(execCall.payload.approval_token, "coco-card-2");
  assert.equal(approvalItems.get("coco-card-2").status, "approved");
}

async function testLegacyCardActionSourceMessageFallbackStillApproves() {
  const approvalItems = new Map([
    [
      "coco-card-legacy",
      {
        token: "coco-card-legacy",
        scope: "feishu_high_risk_execution",
        status: "pending",
        project_name: "Codex Hub",
        session_id: "",
        expires_at: "2036-03-20T00:00:00Z",
        metadata: {
          requested_text: "请帮我 git push 当前分支",
          chat_id: "oc_direct_legacy",
          open_id: "ou_sender",
          source_message_id: "om-source-legacy",
          approval_message_id: "msg-card-legacy",
        },
      },
    ],
  ]);
  const brokerCalls = [];
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        brokerCalls.push({ command, payload });
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-16T01:20:00Z",
              updated_at: "2026-03-16T01:20:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "approval-token") {
          if (payload.token_json) {
            const existing = approvalItems.get(payload.token) || { token: payload.token };
            const item = { ...existing, ...payload.token_json };
            approvalItems.set(payload.token, item);
            return { ok: true, item };
          }
          return { ok: true, item: approvalItems.get(payload.token) || { token: payload.token, status: "" } };
        }
        if (command === "bridge-chat-binding") {
          return {
            ok: true,
            binding: {
              chat_ref: payload.chat_ref,
              project_name: "Codex Hub",
              binding_scope: "project",
            },
          };
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: "已完成 git push。",
            stderr: "session id: 019ce000-0000-7000-8000-000000001111",
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });

  const result = await service.handleCardActionEvent({
    action: {
      value: {
        callback_data: "perm:allow:coco-card-legacy",
        chat_id: "oc_direct_legacy",
        message_id: "om-source-legacy",
      },
    },
    operator: {
      open_id: "ou_sender",
    },
  });
  await new Promise((resolve) => setTimeout(resolve, 10));

  assert.equal(result.toast.content, "已批准，正在执行。");
  const execCall = brokerCalls.find((item) => item.command === "codex-exec");
  assert.ok(execCall);
  assert.equal(execCall.payload.execution_profile, "feishu-approved");
  assert.equal(execCall.payload.approval_token, "coco-card-legacy");
  assert.equal(approvalItems.get("coco-card-legacy").status, "approved");
}

async function testApproveCommandRejectsWrongThread() {
  const approvalItems = new Map([
    [
      "coco-allow3",
      {
        token: "coco-allow3",
        scope: "feishu_high_risk_execution",
        status: "pending",
        project_name: "Codex Hub",
        session_id: "",
        expires_at: "2036-03-20T00:00:00Z",
        metadata: {
          requested_text: "请帮我 git push 当前分支",
          chat_id: "oc_direct_owner",
          open_id: "ou_owner",
        },
      },
    ],
  ]);
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-15T03:00:00Z",
              updated_at: "2026-03-15T03:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "approval-token") {
          if (payload.token_json) {
            const existing = approvalItems.get(payload.token) || { token: payload.token };
            const item = { ...existing, ...payload.token_json };
            approvalItems.set(payload.token, item);
            return { ok: true, item };
          }
          return { ok: true, item: approvalItems.get(payload.token) || { token: payload.token, status: "" } };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });

  const result = await service.processMessageEvent({
    message: {
      message_id: "om_approve_wrong_thread",
      message_type: "text",
      content: JSON.stringify({ text: "/approve coco-allow3" }),
      chat_type: "p2p",
      chat_id: "oc_direct_other",
    },
    sender: { sender_id: { open_id: "ou_other" } },
  });

  assert.equal(result.ok, true);
  assert.equal(result.direct, true);
  assert.equal(result.replyPhase, "approval_forbidden");
  assert.match(result.replyPreview, /不属于当前线程/);
  assert.equal(approvalItems.get("coco-allow3").status, "pending");
}

async function testApproveCommandRejectsExpiredToken() {
  const approvalItems = new Map([
    [
      "coco-expired1",
      {
        token: "coco-expired1",
        scope: "feishu_high_risk_execution",
        status: "pending",
        project_name: "Codex Hub",
        session_id: "",
        expires_at: "2020-03-20T00:00:00Z",
        metadata: {
          requested_text: "请帮我 git push 当前分支",
          chat_id: "oc_direct",
          open_id: "ou_sender",
        },
      },
    ],
  ]);
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-15T03:00:00Z",
              updated_at: "2026-03-15T03:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "approval-token") {
          if (payload.token_json) {
            const existing = approvalItems.get(payload.token) || { token: payload.token };
            const item = { ...existing, ...payload.token_json };
            approvalItems.set(payload.token, item);
            return { ok: true, item };
          }
          return { ok: true, item: approvalItems.get(payload.token) || { token: payload.token, status: "" } };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });

  const result = await service.processMessageEvent({
    message: {
      message_id: "om_approve_expired",
      message_type: "text",
      content: JSON.stringify({ text: "/approve coco-expired1" }),
      chat_type: "p2p",
      chat_id: "oc_direct",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(result.ok, true);
  assert.equal(result.direct, true);
  assert.equal(result.replyPhase, "approval_expired");
  assert.match(result.replyPreview, /已过期/);
  assert.equal(approvalItems.get("coco-expired1").status, "expired");
}

async function testApproveShorthandUsesPendingThreadToken() {
  const replies = [];
  const approvalItems = new Map([
    [
      "coco-allow2",
      {
        token: "coco-allow2",
        scope: "feishu_high_risk_execution",
        status: "pending",
        project_name: "Codex Hub",
        session_id: "",
        expires_at: "2036-03-20T00:00:00Z",
        metadata: {
          requested_text: "请帮我 git push electron 分支",
        },
      },
    ],
  ]);
  const brokerCalls = [];
  const sdkLoader = () => ({
    Domain: { Feishu: 0, Lark: 1 },
    Client: class {
      constructor() {
        this.im = {
          v1: {
            message: {
              create: async (payload) => {
                replies.push(parseReplyText(payload));
                return { ok: true };
              },
            },
          },
        };
      }
    },
    EventDispatcher: class {
      register(handlers) {
        this.handlers = handlers;
        return this;
      }
    },
    WSClient: class {
      async start() {}
      async close() {}
    },
    LoggerLevel: { info: "info" },
  });
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        brokerCalls.push({ command, payload });
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-15T03:00:00Z",
              updated_at: "2026-03-15T03:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return {
            ok: true,
            profile: { preferred_name: "Frank", relationship: "workspace owner" },
          };
        }
        if (command === "bridge-conversations") {
          return {
            ok: true,
            rows: [
              {
                chat_ref: "oc_direct",
                project_name: "Codex Hub",
                binding_label: "Codex Hub",
                thread_label: "Codex Hub",
                pending_approval_token: "coco-allow2",
                pending_approval_action: "git push electron 分支",
              },
            ],
          };
        }
        if (command === "approval-token") {
          if (payload.token_json) {
            const existing = approvalItems.get(payload.token) || { token: payload.token };
            const item = { ...existing, ...payload.token_json };
            approvalItems.set(payload.token, item);
            return { ok: true, item };
          }
          return { ok: true, item: approvalItems.get(payload.token) || { token: payload.token, status: "" } };
        }
        if (command === "bridge-chat-binding") {
          return {
            ok: true,
            binding: {
              chat_ref: payload.chat_ref,
              project_name: "Codex Hub",
              binding_scope: "project",
            },
          };
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: "已完成 electron 分支推送。",
            stderr: "session id: 019ce000-0000-7000-8000-000000000778",
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
    },
    sdkLoader,
    logger: { info() {}, warn() {}, error() {} },
  });
  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
    group_policy: "all_messages",
    require_mention: false,
  });
  await service.connect();

  const result = await service.processMessageEvent({
    message: {
      message_id: "om_approve_short",
      message_type: "text",
      content: JSON.stringify({ text: "批准" }),
      chat_type: "p2p",
      chat_id: "oc_direct",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(result.ok, true);
  assert.match(replies[0], /已记录授权，开始执行。/);
  assert.match(replies[0], /当前线程：Codex Hub/);
  assert.equal(result.replyPreview, "Frank，已完成 electron 分支推送。");
  const execCall = brokerCalls.find((item) => item.command === "codex-exec");
  assert.ok(execCall);
  assert.equal(execCall.payload.execution_profile, "feishu-approved");
  assert.equal(execCall.payload.approval_token, "coco-allow2");
  assert.equal(execCall.payload.prompt, "请帮我 git push electron 分支");
  assert.equal(approvalItems.get("coco-allow2").status, "approved");
}

async function main() {
  await testSanitizeAndSummarize();
  await testNormalizeSdkDomain();
  await testNormalizeMessageEvent();
  await testNormalizeMessageEventDetectsTextMentionAlias();
  await testShouldAcceptMessage();
  await testShouldRunInBackgroundSkipsStatusQuestions();
  await testExecutionProfileAndApprovalClassificationCoverFeishuLocalExtensions();
  await testHighRiskRequestDetectsChineseGitHubPushPhrasing();
  await testSkillInstallRoutesWithLocalExtensionProfile();
  await testHandleMessageEventAcceptsTextMentionAlias();
  await testBindingDeclarationResetsSessionAndReportsBindingPhase();
  await testBindingDeclarationRequiresExplicitProjectForNewChat();
  await testProjectReplyIncludesMaterialSuggestionsFromBroker();
  await testServiceConnectAndRoute();
  await testConnectPatchesWsClientForCardCallbacks();
  await testFastBackgroundTasksDoNotSendAck();
  await testSameChatBackgroundTasksSerialize();
  await testDifferentChatsBackgroundTasksRunInParallel();
  await testLongBackgroundTasksStaySilentUntilFinalReply();
  await testConnectRecoversPendingConversationWithRecoveryNotice();
  await testOnlyRuntimeQueriesStayDirect();
  await testServiceCanSendManualReport();
  await testLongManualReportMirrorsToDocAndKeepsCardReply();
  await testStatusMessageUsesMetricDigestCard();
  await testDelayedReplyNotice();
  await testChatBindingDeclarationRoutesWithProjectContext();
  await testChatBindingDeclarationHandlesNaturalProjectPhrase();
  await testChatBindingDeclarationCapturesTopicHint();
  await testChatBindingDeclarationReportsBrokerValidationFailure();
  await testPausedProjectReturnsPauseSummary();
  await testUnboundGroupPromptsForBinding();
  await testUnboundGroupAutoRoutesByProjectAliasAndPersistsContext();
  await testServiceBlocksWithoutSdk();
  await testLongRepliesAreSplitIntoMultipleMessages();
  await testFeishuReplyCompactsLinksAndPaths();
  await testHighRiskRequestReturnsApprovalTokenPrompt();
  await testHighRiskRequestEventHandlerSendsInteractiveApprovalCard();
  await testLocalSystemApprovalPromptStoresScopedApprovedProfile();
  await testApproveCommandExecutesWithApprovedProfile();
  await testApproveCommandExecutesWithLocalSystemApprovedProfile();
  await testCardActionTriggerApprovesViaInteractiveCard();
  await testCardActionFallsBackToEmbeddedChatContext();
  await testLegacyCardActionSourceMessageFallbackStillApproves();
  await testApproveCommandRejectsWrongThread();
  await testApproveCommandRejectsExpiredToken();
  await testApproveShorthandUsesPendingThreadToken();
  console.log("ok");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
