"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT = "legacy";
process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT = "legacy";
const TEST_VAULT_ROOT = fs.mkdtempSync(path.join(os.tmpdir(), "codex-hub-feishu-vault-"));
fs.mkdirSync(path.join(TEST_VAULT_ROOT, "01_working"), { recursive: true });
fs.writeFileSync(
  path.join(TEST_VAULT_ROOT, "PROJECT_REGISTRY.md"),
  [
    "# PROJECT_REGISTRY",
    "",
    "<!-- PROJECT_REGISTRY_DATA_START -->",
    "```json",
    JSON.stringify(
      [
        {
          project_name: "Codex Hub",
          aliases: ["CodexHub", "codex hub", "coco workspace"],
          path: "",
          status: "active",
          summary_note: "Public Codex Hub system workspace.",
        },
        {
          project_name: "SampleProj",
          aliases: ["sampleproj", "sample proj"],
          path: "",
          status: "active",
          summary_note: "Generic sample project for bridge tests.",
        },
        {
          project_name: "Example Workspace",
          aliases: ["example workspace", "example"],
          path: "",
          status: "active",
          summary_note: "Generic example workspace for bridge tests.",
        },
        {
          project_name: "Old Project",
          aliases: ["old project"],
          path: "",
          status: "active",
          summary_note: "Existing binding fixture project.",
        },
        {
          project_name: "Growth System",
          aliases: ["growth system"],
          path: "",
          status: "active",
          summary_note: "Generic growth workflow project.",
        },
      ],
      null,
      2,
    ),
    "```",
    "<!-- PROJECT_REGISTRY_DATA_END -->",
    "",
  ].join("\n"),
  "utf8",
);
process.env.WORKSPACE_HUB_VAULT_ROOT = TEST_VAULT_ROOT;
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
  summarizeErrorText,
  summarizeSettings,
  shouldAcceptMessage,
  shouldRunInBackground,
} = require("./feishu_long_connection_service");
const {
  normalizeMessageEvent: normalizeInboundMessageEvent,
} = require("./feishu/inbound");

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

async function testNormalizeMessageEventExtractsNestedPostContent() {
  const normalized = normalizeMessageEvent({
    message: {
      message_id: "om_post",
      message_type: "post",
      content: JSON.stringify({
        zh_cn: {
          content: [
            [{ tag: "text", text: "第一段" }],
            [{ tag: "text", text: "第二段" }],
          ],
        },
      }),
      chat_type: "group",
    },
    sender: {
      sender_id: { open_id: "ou_sender" },
    },
  });
  assert.equal(normalized.message_type, "post");
  assert.equal(normalized.text, "第一段\n第二段");
}

async function testNormalizeMessageEventIgnoresAttachmentMetadataInPostContent() {
  const normalized = normalizeMessageEvent({
    message: {
      message_id: "om_post_mixed",
      message_type: "post",
      content: JSON.stringify({
        zh_cn: {
          content: [
            [
              { tag: "text", text: "第一段" },
              { tag: "img", image_key: "img_v3_hidden" },
            ],
            [
              { tag: "file", file_key: "file_v3_hidden" },
              { tag: "text", text: "第二段" },
            ],
          ],
        },
      }),
      chat_type: "group",
    },
    sender: {
      sender_id: { open_id: "ou_sender" },
    },
  });
  assert.equal(normalized.text, "第一段\n第二段");
}

async function testNormalizeMessageEventSkipsAttachmentOnlyPayloads() {
  const imageMessage = normalizeMessageEvent({
    message: {
      message_id: "om_image",
      message_type: "image",
      content: JSON.stringify({ image_key: "img_v3_123456" }),
      chat_type: "group",
    },
    sender: {
      sender_id: { open_id: "ou_sender" },
    },
  });
  assert.equal(imageMessage.text, "");

  const fileMessage = normalizeMessageEvent({
    message: {
      message_id: "om_file",
      message_type: "file",
      content: JSON.stringify({ file_key: "file_v3_abcdef" }),
      chat_type: "group",
    },
    sender: {
      sender_id: { open_id: "ou_sender" },
    },
  });
  assert.equal(fileMessage.text, "");
}

async function testInboundNormalizeMessageEventSkipsAttachmentMetadata() {
  const imageMessage = normalizeInboundMessageEvent({
    message: {
      message_id: "om_inbound_image",
      message_type: "image",
      content: JSON.stringify({ image_key: "img_v3_hidden" }),
      chat_type: "group",
    },
    sender: {
      sender_id: { open_id: "ou_sender" },
    },
  });
  assert.equal(imageMessage.text, "");

  const postMessage = normalizeInboundMessageEvent({
    message: {
      message_id: "om_inbound_post",
      message_type: "post",
      content: JSON.stringify({
        zh_cn: {
          content: [
            [
              { tag: "text", text: "正文" },
              { tag: "img", image_key: "img_v3_hidden" },
            ],
          ],
        },
      }),
      chat_type: "group",
    },
    sender: {
      sender_id: { open_id: "ou_sender" },
    },
  });
  assert.equal(postMessage.text, "正文");
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

async function testAttachmentMessagesDoNotRouteIntoCodex() {
  const brokerCalls = [];
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command) => {
        brokerCalls.push(command);
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-28T13:00:00Z",
              updated_at: "2026-03-28T13:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return { ok: true, profile: { preferred_name: "Frank", relationship: "workspace owner" } };
        }
        if (command === "bridge-chat-binding") {
          return { ok: true, binding: null };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
      fetchBridgeExecutionLease: async () => ({
        lease: {
          bridge: "feishu",
          conversation_key: "chat-timeout",
          session_id: "sess-timeout",
          state: "running",
          started_at: "2026-03-24T00:00:00Z",
          last_progress_at: "2026-03-24T00:00:05Z",
          stale_after_seconds: 300,
          metadata: {
            source_message_id: "om_timeout_old",
          },
        },
      }),
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
      message_id: "om_attachment_only",
      message_type: "image",
      content: JSON.stringify({ image_key: "img_v3_123456" }),
      chat_type: "group",
      chat_id: "oc_attachment_only",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(result.ok, true);
  assert.equal(result.replyPhase, "error");
  assert.match(result.replyPreview, /empty_text/);
  assert.equal(brokerCalls.includes("codex-exec"), false);
  assert.equal(brokerCalls.includes("codex-resume"), false);
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

  const gitCommit = classifyApprovalRequirement("请帮我 git commit 当前改动");
  assert.equal(gitCommit.required, true);
  assert.equal(gitCommit.scope, "feishu_high_risk_execution");

  const commitBundling = classifyApprovalRequirement("请帮我把这三组提交面直接收口");
  assert.equal(commitBundling.required, true);
  assert.equal(commitBundling.scope, "feishu_high_risk_execution");

  const skillInstall = classifyApprovalRequirement("请把 investigate 这个 skill 安装到 ~/.codex/skills/");
  assert.equal(skillInstall.required, false);
}

async function testSummarizeErrorTextPrefersMeaningfulExceptionLine() {
  const text = [
    "Traceback (most recent call last):",
    '  File "/workspace/ops/local_broker.py", line 19, in <module>',
    "    import material_router",
    "During handling of the above exception, another exception occurred:",
    '  File "/workspace/ops/codex_context.py", line 11',
    "    from ops import codex_retrieval",
    "IndentationError: expected an indented block after 'try' statement on line 10",
  ].join("\n");
  assert.equal(
    summarizeErrorText(text),
    "IndentationError: expected an indented block after 'try' statement on line 10",
  );
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
  assert.equal(isHighRiskRequest("请帮我 git commit 当前改动"), true);
  assert.equal(isHighRiskRequest("请帮我把这三组提交面直接收口"), true);
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
  assert.match(result.replyPreview, /绑定到项目 `Codex Hub`/);
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

async function testSessionLaneMismatchForcesFreshExecPerChat() {
  const bindings = new Map([
    [
      "chat-group-lane",
      {
        bridge: "feishu",
        chat_ref: "chat-group-lane",
        binding_scope: "project",
        project_name: "Codex Hub",
        topic_name: "",
        session_id: "sess-shared",
        metadata: {
          session_lane: "oc_direct_coco",
        },
      },
    ],
  ]);
  const brokerCalls = [];
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload = {}) => {
        brokerCalls.push({ command, payload });
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-24T00:00:00Z",
              updated_at: "2026-03-24T00:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return { ok: true, profile: { preferred_name: "Frank", relationship: "workspace owner" } };
        }
        if (command === "bridge-chat-binding") {
          const chatRef = String(payload.chat_ref || "");
          if (payload.binding_json) {
            const next = { ...(bindings.get(chatRef) || {}), bridge: "feishu", chat_ref: chatRef, ...payload.binding_json };
            bindings.set(chatRef, next);
            return { ok: true, binding: next };
          }
          return { ok: true, binding: bindings.get(chatRef) || null };
        }
        if (command === "codex-resume") {
          throw new Error("unexpected resume");
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: "新的群聊会话已建立。",
            stderr: "session id: 019ce000-0000-7000-8000-00000000lane",
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

  const result = await service.handleMessageEvent({
    message: {
      message_id: "om_lane_group",
      message_type: "text",
      content: JSON.stringify({ text: "请回复一下当前进展" }),
      chat_type: "group",
      chat_id: "chat-group-lane",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(result.ok, true);
  assert.match(result.replyPreview, /新的群聊会话已建立/);
  assert.equal(brokerCalls.some((entry) => entry.command === "codex-resume"), false);
  assert.equal(brokerCalls.some((entry) => entry.command === "codex-exec"), true);
  assert.equal(bindings.get("chat-group-lane").metadata.session_lane, "chat-group-lane");
}

async function testTimedOutResumeClearsBindingAndAllowsFreshFollowup() {
  const liveProgressAt = new Date().toISOString();
  const bindings = new Map([
    [
      "chat-timeout",
      {
        bridge: "feishu",
        chat_ref: "chat-timeout",
        binding_scope: "project",
        project_name: "Codex Hub",
        topic_name: "",
        session_id: "sess-timeout",
        metadata: {
          session_lane: "chat-timeout",
        },
      },
    ],
  ]);
  const brokerCalls = [];
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload = {}) => {
        brokerCalls.push({ command, payload });
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-24T00:00:00Z",
              updated_at: "2026-03-24T00:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return { ok: true, profile: { preferred_name: "Frank", relationship: "workspace owner" } };
        }
        if (command === "bridge-chat-binding") {
          const chatRef = String(payload.chat_ref || "");
          if (payload.binding_json) {
            const next = { ...(bindings.get(chatRef) || {}), bridge: "feishu", chat_ref: chatRef, ...payload.binding_json };
            bindings.set(chatRef, next);
            return { ok: true, binding: next };
          }
          return { ok: true, binding: bindings.get(chatRef) || null };
        }
        if (command === "codex-resume") {
          return new Promise(() => {});
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: `新会话完成：${payload.prompt}`,
            stderr: "session id: 019ce000-0000-7000-8000-00000000fresh",
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
      fetchBridgeExecutionLease: async () => ({
        lease: {
          bridge: "feishu",
          conversation_key: "chat-timeout",
          session_id: "sess-timeout",
          state: "running",
          started_at: liveProgressAt,
          last_progress_at: liveProgressAt,
          stale_after_seconds: 300,
          metadata: {
            source_message_id: "om_timeout_old",
          },
        },
      }),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
    routeExecutionTimeoutMs: 10,
  });
  await service.loadSettings({
    app_id: "cli_123",
    app_secret: "secret",
    group_policy: "all_messages",
    require_mention: false,
  });

  const first = await service.handleMessageEvent({
    message: {
      message_id: "om_timeout_1",
      message_type: "text",
      content: JSON.stringify({ text: "@_user_1 你的意思是，做了一些，但没做完，是这样吗？" }),
      chat_type: "group",
      chat_id: "chat-timeout",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(first.ok, true);
  assert.equal(first.replyPhase, "error");
  assert.match(first.replyPreview, /长时间未返回/);
  assert.equal(bindings.get("chat-timeout").session_id, "");

  const second = await service.handleMessageEvent({
    message: {
      message_id: "om_timeout_2",
      message_type: "text",
      content: JSON.stringify({ text: "请继续回复这个问题" }),
      chat_type: "group",
      chat_id: "chat-timeout",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(second.ok, true);
  assert.match(second.replyPreview, /新会话完成/);
  assert.equal(brokerCalls.filter((entry) => entry.command === "codex-resume").length, 1);
  assert.equal(brokerCalls.filter((entry) => entry.command === "codex-exec").length, 1);
}

async function testProgressStalledRunningLeaseForcesFreshExecWithoutResume() {
  const bindings = new Map([
    [
      "chat-stalled",
      {
        bridge: "feishu",
        chat_ref: "chat-stalled",
        binding_scope: "project",
        project_name: "Codex Hub",
        topic_name: "",
        session_id: "sess-stalled",
        metadata: {
          session_lane: "chat-stalled",
        },
      },
    ],
  ]);
  const leaseWrites = [];
  const brokerCalls = [];
  const staleProgressAt = new Date(Date.now() - 10 * 60_000).toISOString();
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload = {}) => {
        brokerCalls.push({ command, payload });
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-28T13:00:00Z",
              updated_at: "2026-03-28T13:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return { ok: true, profile: { preferred_name: "Frank", relationship: "workspace owner" } };
        }
        if (command === "bridge-chat-binding") {
          const chatRef = String(payload.chat_ref || "");
          if (payload.binding_json) {
            const next = { ...(bindings.get(chatRef) || {}), bridge: "feishu", chat_ref: chatRef, ...payload.binding_json };
            bindings.set(chatRef, next);
            return { ok: true, binding: next };
          }
          return { ok: true, binding: bindings.get(chatRef) || null };
        }
        if (command === "codex-resume") {
          throw new Error("unexpected resume");
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: "已改走新会话。",
            stderr: "session id: 019ce000-0000-7000-8000-00000000fresh2",
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
      saveBridgeExecutionLease: async (payload) => {
        leaseWrites.push({ ...payload });
        return { lease: payload };
      },
      fetchBridgeExecutionLease: async () => ({
        lease: {
          bridge: "feishu",
          conversation_key: "chat-stalled",
          session_id: "sess-stalled",
          state: "running",
          started_at: staleProgressAt,
          last_progress_at: staleProgressAt,
          stale_after_seconds: 300,
          metadata: {
            source_message_id: "om_old_stalled",
          },
        },
      }),
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
      message_id: "om_stalled_followup",
      message_type: "text",
      content: JSON.stringify({ text: "请继续处理现在这条新消息" }),
      chat_type: "group",
      chat_id: "chat-stalled",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(result.ok, true);
  assert.match(result.replyPreview, /已改走新会话/);
  assert.equal(brokerCalls.some((entry) => entry.command === "codex-resume"), false);
  assert.equal(brokerCalls.some((entry) => entry.command === "codex-exec"), true);
  assert.equal(bindings.get("chat-stalled").session_id, "019ce000-0000-7000-8000-00000000fresh2");
  assert.equal(bindings.get("chat-stalled").metadata.last_route_error, "lease_progress_stalled");
  assert.equal(leaseWrites.some((item) => item.state === "failed" && item.last_error === "lease_progress_stalled"), true);
}

async function testMissingLeaseForcesFreshExecWithoutResume() {
  const bindings = new Map([
    [
      "chat-missing-lease",
      {
        bridge: "feishu",
        chat_ref: "chat-missing-lease",
        binding_scope: "project",
        project_name: "Codex Hub",
        topic_name: "",
        session_id: "sess-missing-lease",
        metadata: {
          session_lane: "chat-missing-lease",
        },
      },
    ],
  ]);
  const leaseWrites = [];
  const brokerCalls = [];
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload = {}) => {
        brokerCalls.push({ command, payload });
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-28T13:00:00Z",
              updated_at: "2026-03-28T13:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return { ok: true, profile: { preferred_name: "Frank", relationship: "workspace owner" } };
        }
        if (command === "bridge-chat-binding") {
          const chatRef = String(payload.chat_ref || "");
          if (payload.binding_json) {
            const next = { ...(bindings.get(chatRef) || {}), bridge: "feishu", chat_ref: chatRef, ...payload.binding_json };
            bindings.set(chatRef, next);
            return { ok: true, binding: next };
          }
          return { ok: true, binding: bindings.get(chatRef) || null };
        }
        if (command === "codex-resume") {
          throw new Error("unexpected resume");
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: "已改走新会话。",
            stderr: "session id: 019ce000-0000-7000-8000-00000000fresh3",
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
      saveBridgeExecutionLease: async (payload) => {
        leaseWrites.push({ ...payload });
        return { lease: payload };
      },
      fetchBridgeExecutionLease: async () => ({ lease: null }),
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
      message_id: "om_missing_lease_followup",
      message_type: "text",
      content: JSON.stringify({ text: "请继续处理这条新消息" }),
      chat_type: "group",
      chat_id: "chat-missing-lease",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(result.ok, true);
  assert.match(result.replyPreview, /已改走新会话/);
  assert.equal(brokerCalls.some((entry) => entry.command === "codex-resume"), false);
  assert.equal(brokerCalls.some((entry) => entry.command === "codex-exec"), true);
  assert.equal(bindings.get("chat-missing-lease").session_id, "019ce000-0000-7000-8000-00000000fresh3");
  assert.equal(bindings.get("chat-missing-lease").metadata.last_route_error, "lease_missing");
  assert.equal(leaseWrites.some((item) => item.state === "failed" && item.last_error === "lease_missing"), false);
}

async function testNonRunningLeaseForcesFreshExecWithoutResume() {
  const bindings = new Map([
    [
      "chat-reported-lease",
      {
        bridge: "feishu",
        chat_ref: "chat-reported-lease",
        binding_scope: "project",
        project_name: "Codex Hub",
        topic_name: "",
        session_id: "sess-reported-lease",
        metadata: {
          session_lane: "chat-reported-lease",
        },
      },
    ],
  ]);
  const leaseWrites = [];
  const brokerCalls = [];
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload = {}) => {
        brokerCalls.push({ command, payload });
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-28T13:00:00Z",
              updated_at: "2026-03-28T13:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return { ok: true, profile: { preferred_name: "Frank", relationship: "workspace owner" } };
        }
        if (command === "bridge-chat-binding") {
          const chatRef = String(payload.chat_ref || "");
          if (payload.binding_json) {
            const next = { ...(bindings.get(chatRef) || {}), bridge: "feishu", chat_ref: chatRef, ...payload.binding_json };
            bindings.set(chatRef, next);
            return { ok: true, binding: next };
          }
          return { ok: true, binding: bindings.get(chatRef) || null };
        }
        if (command === "codex-resume") {
          throw new Error("unexpected resume");
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: "已改走新会话。",
            stderr: "session id: 019ce000-0000-7000-8000-00000000fresh4",
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
      saveBridgeExecutionLease: async (payload) => {
        leaseWrites.push({ ...payload });
        return { lease: payload };
      },
      fetchBridgeExecutionLease: async () => ({
        lease: {
          bridge: "feishu",
          conversation_key: "chat-reported-lease",
          session_id: "sess-reported-lease",
          state: "reported",
          started_at: "2026-03-28T12:00:00Z",
          last_progress_at: "2026-03-28T12:05:00Z",
          stale_after_seconds: 300,
          metadata: {
            source_message_id: "om_reported_old",
          },
        },
      }),
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
      message_id: "om_reported_lease_followup",
      message_type: "text",
      content: JSON.stringify({ text: "请继续处理这条新消息" }),
      chat_type: "group",
      chat_id: "chat-reported-lease",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(result.ok, true);
  assert.match(result.replyPreview, /已改走新会话/);
  assert.equal(brokerCalls.some((entry) => entry.command === "codex-resume"), false);
  assert.equal(brokerCalls.some((entry) => entry.command === "codex-exec"), true);
  assert.equal(bindings.get("chat-reported-lease").session_id, "019ce000-0000-7000-8000-00000000fresh4");
  assert.equal(bindings.get("chat-reported-lease").metadata.last_route_error, "lease_not_running:reported");
  assert.equal(
    leaseWrites.some((item) => item.state === "failed" && item.last_error === "lease_not_running:reported"),
    false,
  );
}

async function testHandleMessageEventPrefersFinalizeLaunchReplyText() {
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command) => {
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-28T13:00:00Z",
              updated_at: "2026-03-28T13:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return { ok: true, profile: { preferred_name: "Frank", relationship: "workspace owner" } };
        }
        if (command === "bridge-chat-binding") {
          return { ok: true, binding: null };
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: "这是一段不该优先显示的原始 stdout。",
            stderr: "session id: finalize-pref-1",
            finalize_launch: {
              status: "completed",
              reply_text: "这是来自 finalize_launch 的正式回复。",
              summary_excerpt: "短摘要",
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
    group_policy: "all_messages",
    require_mention: false,
  });

  const result = await service.handleMessageEvent({
    message: {
      message_id: "om_finalize_reply",
      message_type: "text",
      content: JSON.stringify({ text: "请告诉我最终结论" }),
      chat_type: "p2p",
      chat_id: "",
    },
    sender: { sender_id: { open_id: "ou_finalize" } },
  });

  assert.equal(result.ok, true);
  assert.match(result.replyPreview, /这是来自 finalize_launch 的正式回复/);
  assert.equal(result.replyPreview.includes("原始 stdout"), false);
}

async function testNestedPostContentRoutesWithoutEmptyTextFailure() {
  const brokerCalls = [];
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload = {}) => {
        brokerCalls.push({ command, payload });
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-28T13:00:00Z",
              updated_at: "2026-03-28T13:00:00Z",
            },
          };
        }
        if (command === "user-profile") {
          return { ok: true, profile: { preferred_name: "Frank", relationship: "workspace owner" } };
        }
        if (command === "bridge-chat-binding") {
          return { ok: true, binding: null };
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: "富文本消息已正常进入执行链。",
            stderr: "session id: post-route-1",
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

  const result = await service.handleMessageEvent({
    message: {
      message_id: "om_post_route",
      message_type: "post",
      content: JSON.stringify({
        zh_cn: {
          content: [
            [{ tag: "text", text: "@_user_1 " }, { tag: "text", text: "请继续排查这个问题" }],
            [{ tag: "text", text: "补充说明也在这里" }],
          ],
        },
      }),
      chat_type: "group",
      chat_id: "oc_post_route",
    },
    sender: { sender_id: { open_id: "ou_post_route" } },
  });

  assert.equal(result.ok, true);
  assert.match(result.replyPreview, /富文本消息已正常进入执行链/);
  const inboundRecord = brokerCalls.find(
    (item) => item.command === "record-bridge-message" && item.payload.direction === "inbound",
  );
  assert.ok(inboundRecord);
  assert.equal(inboundRecord.payload.payload.message_type, "post");
  assert.equal(inboundRecord.payload.payload.text, "@_user_1\n请继续排查这个问题\n补充说明也在这里");
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
  assert.match(replies[0], /说明：我刚恢复在线/);
  assert.match(replies[0], /请把你当前还需要我处理的最新指令再发一遍/);
}

async function testReconnectNotifiesRecentActiveConversationAfterOutage() {
  const replies = [];
  const bridgeConversationRows = [];
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
          return { ok: true, rows: bridgeConversationRows.slice() };
        }
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-23T03:00:00Z",
              updated_at: "2026-03-23T03:00:00Z",
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
            binding: { project_name: "Codex Hub", binding_scope: "project" },
          };
        }
        if (command === "codex-exec") {
          return {
            ok: true,
            stdout: "先记录一条正常回复。",
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
  assert.equal((await service.connect()).ok, true);
  await service.handleMessageEvent({
    message: {
      message_id: "om_seed_recent",
      message_type: "text",
      content: JSON.stringify({ text: "请总结当前状态" }),
      chat_type: "p2p",
      chat_id: "oc_recent",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });
  const replyCountBeforeReconnect = replies.length;

  bridgeConversationRows.push({
    chat_ref: "oc_recent",
    chat_type: "group",
    project_name: "Codex Hub",
    topic_name: "",
    participant_count: 1,
    pending_request: true,
    awaiting_report: false,
    needs_attention: true,
    attention_reason: "response_delayed",
    last_user_request_age_seconds: 180,
    last_message_age_seconds: 180,
  });

  const originalNow = Date.now;
  Date.now = () => originalNow() + 5 * 60 * 1000;
  try {
    assert.equal((await service.reconnect()).ok, true);
  } finally {
    Date.now = originalNow;
  }
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(replies.length, replyCountBeforeReconnect + 1);
  assert.match(replies.at(-1), /说明：我刚恢复在线/);
  assert.match(replies.at(-1), /请把你当前还需要我处理的最新指令再发一遍/);
}

async function testReconnectSkipsRecentConversationWithoutOpenWork() {
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
                chat_ref: "oc_recent_idle",
                chat_type: "group",
                project_name: "Codex Hub",
                topic_name: "",
                participant_count: 1,
                pending_request: false,
                awaiting_report: false,
                ack_pending: false,
                needs_attention: false,
                last_user_request_age_seconds: 180,
                last_message_age_seconds: 180,
              },
            ],
          };
        }
        if (command === "bridge-status") {
          return {
            ok: true,
            connection_status: "disconnected",
            last_event_at: "2026-03-23T03:00:00Z",
            metadata: {
              connected_at: "2026-03-23T02:55:00Z",
              last_recovery_notice_at: "",
            },
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

  const originalNow = Date.now;
  Date.now = () => new Date("2026-03-23T03:10:00Z").getTime();
  try {
    assert.equal((await service.connect()).ok, true);
  } finally {
    Date.now = originalNow;
  }
  await delay(10);
  assert.equal(replies.length, 0);
}

async function testReconnectWhilePreviouslyConnectedSkipsRecentRecoveryNotice() {
  const replies = [];
  const bridgeConversationRows = [];
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
          return { ok: true, rows: bridgeConversationRows.slice() };
        }
        if (command === "bridge-status") {
          return {
            ok: true,
            connection_status: "connected",
            last_event_at: new Date(Date.now() - 5 * 60_000).toISOString(),
            metadata: {
              connected_at: new Date(Date.now() - 5 * 60_000).toISOString(),
            },
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
  assert.equal((await service.connect()).ok, true);
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(replies.length, 0);

  bridgeConversationRows.push({
    chat_ref: "oc_recent_connected",
    chat_type: "group",
    project_name: "Growth System",
    topic_name: "",
    participant_count: 1,
    pending_request: false,
    awaiting_report: false,
    needs_attention: false,
    last_user_request_age_seconds: 180,
    last_message_age_seconds: 180,
  });

  assert.equal((await service.connect()).ok, true);
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(replies.length, 0);
}

async function testConnectResetsEventClockAfterLongIdleGap() {
  const sdkLoader = () => ({
    Domain: { Feishu: 0, Lark: 1 },
    Client: class {
      constructor() {
        this.im = {
          v1: {
            message: {
              create: async () => ({ ok: true }),
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
        if (command === "bridge-status") {
          return {
            ok: true,
            connection_status: "connected",
            last_event_at: "2026-03-23T03:00:00Z",
            metadata: {
              connected_at: "2026-03-23T02:55:00Z",
            },
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

  const originalNow = Date.now;
  Date.now = () => new Date("2026-03-23T03:45:00Z").getTime();
  try {
    assert.equal((await service.connect()).ok, true);
  } finally {
    Date.now = originalNow;
  }

  const currentStatus = service.getStatus();
  assert.notEqual(currentStatus.connected_at, "2026-03-23T02:55:00Z");
  assert.equal(currentStatus.last_event_at, currentStatus.connected_at);
}

async function testColdConnectLoadsPersistedBridgeStatusForRecoveryNotice() {
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
        if (command === "bridge-status") {
          return {
            ok: true,
            last_event_at: "2026-03-23T03:00:00Z",
            metadata: {
              connected_at: "2026-03-23T02:55:00Z",
              last_recovery_notice_at: "",
              recent_message_count: 12,
              recent_reply_count: 10,
              last_message_preview: "preview",
              last_sender_ref: "ou_sender",
            },
          };
        }
        if (command === "bridge-conversations") {
          return {
            ok: true,
        rows: [
          {
            chat_ref: "oc_cold_recover",
            chat_type: "group",
            project_name: "Codex Hub",
            topic_name: "",
            participant_count: 1,
            pending_request: true,
            awaiting_report: false,
            ack_pending: false,
            needs_attention: true,
            attention_reason: "response_delayed",
            last_user_request_age_seconds: 180,
            last_message_age_seconds: 180,
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

  const originalNow = Date.now;
  Date.now = () => new Date("2026-03-23T03:10:00Z").getTime();
  try {
    assert.equal((await service.connect()).ok, true);
  } finally {
    Date.now = originalNow;
  }
  await delay(10);
  assert.equal(replies.length, 1);
  assert.match(replies[0], /说明：我刚恢复在线/);
}

async function testColdConnectSkipsRecentRecoveryNoticeWhenPersistedStatusWasConnected() {
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
        if (command === "bridge-status") {
          return {
            ok: true,
            connection_status: "connected",
            last_event_at: "2026-03-23T03:00:00Z",
            metadata: {
              connected_at: "2026-03-23T02:55:00Z",
              last_recovery_notice_at: "",
            },
          };
        }
        if (command === "bridge-conversations") {
          return {
            ok: true,
            rows: [
              {
                chat_ref: "oc_cold_connected",
                chat_type: "group",
                project_name: "Codex Hub",
                topic_name: "",
                participant_count: 1,
                pending_request: false,
                awaiting_report: false,
                needs_attention: false,
                last_user_request_age_seconds: 180,
                last_message_age_seconds: 180,
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

  const originalNow = Date.now;
  Date.now = () => new Date("2026-03-23T03:10:00Z").getTime();
  try {
    assert.equal((await service.connect()).ok, true);
  } finally {
    Date.now = originalNow;
  }
  await delay(10);
  assert.equal(replies.length, 0);
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

async function testTopLevelHandlerSendsFallbackReplyOnUnexpectedFailure() {
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
              created_at: "2026-03-23T03:00:00Z",
              updated_at: "2026-03-23T03:00:00Z",
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
            binding: { project_name: "Codex Hub", binding_scope: "project" },
          };
        }
        if (command === "codex-exec") {
          throw new Error("route exploded");
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
  assert.equal((await service.connect()).ok, true);
  const handler = wsStarts[0].eventDispatcher.handlers["im.message.receive_v1"];
  await handler({
    message: {
      message_id: "om_handler_fail",
      message_type: "text",
      content: JSON.stringify({ text: "为什么现在会这样？" }),
      chat_type: "p2p",
      chat_id: "oc_failure",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });
  await delay(10);
  assert.ok(replies.some((item) => /内部故障/.test(item)));
  assert.ok(replies.some((item) => /请把当前仍需处理的最新指令再发一遍/.test(item)));
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
        if (command === "feishu-callback-executor") {
          assert.equal(payload.action, "doc-create");
          const callbackPayload = JSON.parse(payload.payload_json);
          assert.equal(callbackPayload.target, "feishu:chat:oc_codex_hub");
          assert.equal(callbackPayload.title.includes("CoCo 汇报摘要"), true);
          return {
            ok: true,
            result: {
              ok: true,
              action: "doc-create",
              document: {
                document_id: "doc_456",
                url: "https://feishu.cn/docx/doc_456",
              },
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
  const docCall = brokerCalls.find((item) => item.command === "feishu-callback-executor");
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
  assert.doesNotMatch(result.replyPreview, /说明：你这条消息是延迟补回的/);
}

async function testChatBindingDeclarationRoutesWithProjectContext() {
  const brokerCalls = [];
  const bindings = new Map();
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
      content: JSON.stringify({ text: "哦不是，我说的是SampleProj。在这个聊天群里面，我们只聊SampleProj @_user_1" }),
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
}

async function testChatBindingDeclarationHandlesNaturalProjectPhrase() {
  const bindings = new Map();
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
        text: "@_user_1 这个群组只有你和我。你可以叫我 Alex 或 Casey。在这里，我们只聊 Example Workspace 的项目",
      }),
      chat_type: "group",
      chat_id: "chat-natural",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });
  assert.equal(bindingResult.ok, true);
  assert.equal(bindingResult.direct, true);
  assert.match(bindingResult.replyPreview, /已将本群绑定到项目 `Example Workspace`/);

  const binding = await service.getBindingForConversationKey("chat-natural");
  assert.equal(binding?.project_name, "Example Workspace");
  assert.equal(binding?.topic_name, "");
}

async function testChatBindingDeclarationCapturesTopicHint() {
  const brokerCalls = [];
  const bindings = new Map();
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
      content: JSON.stringify({ text: "这个群只聊 Example Workspace PhaseA" }),
      chat_type: "group",
      chat_id: "chat-topic",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });
  assert.equal(bindingResult.ok, true);
  assert.equal(bindingResult.direct, true);
  assert.match(bindingResult.replyPreview, /话题 `PhaseA`/);

  const binding = await service.getBindingForConversationKey("chat-topic");
  assert.equal(binding?.project_name, "Example Workspace");
  assert.equal(binding?.topic_name, "PhaseA");

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
  assert.equal(execCall.payload.project_name, "Example Workspace");
  assert.equal(execCall.payload.topic_name, "PhaseA");
  assert.equal(execCall.payload.thread_label, "Example Workspace / PhaseA");
  assert.equal(execCall.payload.no_auto_resume, true);
}

async function testChatBindingDeclarationReportsBrokerValidationFailure() {
  const bindings = new Map();
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
              error: "unknown topic_name `bad-topic` for project `Example Workspace`",
              available_topics: ["PhaseA"],
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
      content: JSON.stringify({ text: "这个群只聊 Example Workspace bad-topic" }),
      chat_type: "group",
      chat_id: "chat-topic-fail",
    },
    sender: { sender_id: { open_id: "ou_sender" } },
  });

  assert.equal(result.ok, true);
  assert.equal(result.direct, true);
  assert.match(result.replyPreview, /绑定没有成功/);
  assert.match(result.replyPreview, /PhaseA/);
  assert.equal(await service.getBindingForConversationKey("chat-topic-fail"), null);
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
            "请查看 [详细报告](/workspace/reports/system/coco-feishu-development-plan.md)\n" +
            "完整日志在 https://example.com/report/12345\n" +
            "问题点位于 /workspace/bridge/feishu_long_connection_service.js",
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
  assert.match(finalReply, /`详细报告`/);
  assert.match(finalReply, /https:\/\/example\.com\/report\/12345/);
  assert.match(finalReply, /\/workspace\/bridge\/feishu_long_connection_service\.js/);
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

async function testGitCommitRequestReturnsApprovalTokenPrompt() {
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
              created_at: "2026-03-28T01:00:00Z",
              updated_at: "2026-03-28T01:00:00Z",
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
      message_id: "om_git_commit",
      message_type: "text",
      content: JSON.stringify({ text: "请帮我把这三组提交面直接收口" }),
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
  assert.equal(saved.payload.token_json.metadata.requested_text, "请帮我把这三组提交面直接收口");
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

async function testCardActionTriggerResumesBackgroundJobDelivery() {
  const approvalItems = new Map([
    [
      "bgate-card-1",
      {
        token: "bgate-card-1",
        scope: "background_job_external_delivery",
        status: "pending",
        project_name: "Codex Hub",
        session_id: "bge-123",
        expires_at: "2036-03-20T00:00:00Z",
        metadata: {
          task_id: "WH-FS-12",
          job_id: "board-job.codex-hub.feishu-native-ai-followup",
          open_id: "ou_sender",
          approval_message_id: "msg-bg-card-approve",
        },
      },
    ],
  ]);
  const brokerCalls = [];
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
              created_at: "2026-03-25T07:10:00Z",
              updated_at: "2026-03-25T07:10:00Z",
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
        if (command === "feishu-callback-executor") {
          return {
            ok: true,
            result_status: "success",
            result: {
              ok: true,
              action: payload.action,
              route: "background-job",
              run_record: {
                run_id: "bge-20260325-999999-demo",
                delivery_status: "delivered",
              },
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
        callback_data: "perm:allow:bgate-card-1",
      },
    },
    context: {
      open_chat_id: "oc_direct",
      open_message_id: "msg-bg-card-approve",
    },
    operator: {
      open_id: "ou_sender",
    },
  });
  await new Promise((resolve) => setTimeout(resolve, 10));

  assert.equal(result.toast.content, "已批准，正在执行。");
  assert.equal(approvalItems.get("bgate-card-1").status, "approved");
  const backgroundJobCall = brokerCalls.find((item) => item.command === "feishu-callback-executor");
  assert.ok(backgroundJobCall);
  assert.equal(backgroundJobCall.payload.action, "approval-routed-action");
  const callbackPayload = JSON.parse(backgroundJobCall.payload.payload_json);
  assert.equal(callbackPayload.project_name, "Codex Hub");
  assert.equal(callbackPayload.task_id, "WH-FS-12");
  assert.equal(callbackPayload.approval_token, "bgate-card-1");
  assert.ok(!brokerCalls.find((item) => item.command === "codex-exec"));
  assert.match(replies[0], /已记录授权，开始执行。/);
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

async function testHandleMessageEventWritesExecutionLeaseForRunningRoute() {
  const leaseWrites = [];
  const service = createFeishuLongConnectionService({
    brokerClient: {
      call: async (command, payload) => {
        if (command === "record-bridge-message") {
          return {
            ok: true,
            record: {
              created_at: "2026-03-26T03:00:00Z",
              updated_at: "2026-03-26T03:00:00Z",
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
            stdout: "这是一次排查结论。",
            stderr: "session id: routed-lease-1",
          };
        }
        throw new Error(`unexpected command ${command}`);
      },
    },
    runtimeState: {
      saveBridgeStatus: async () => ({}),
      saveBridgeSettings: async () => ({}),
      saveBridgeExecutionLease: async (payload) => {
        leaseWrites.push({ ...payload });
        return { lease: payload };
      },
      fetchBridgeExecutionLease: async () => ({ lease: null }),
    },
    sdkLoader: () => null,
    logger: { info() {}, warn() {}, error() {} },
  });

  const result = await service.handleMessageEvent({
    message: {
      message_id: "lease-route-1",
      message_type: "text",
      content: JSON.stringify({ text: "请继续排查 AI 辅导为什么前面的记录看不到了" }),
      chat_type: "p2p",
      chat_id: "",
    },
    sender: { sender_id: { open_id: "ou_lease_sender" } },
  });

  assert.equal(result.ok, true);
  const runningLease = leaseWrites.find((item) => item.state === "running");
  assert.ok(runningLease);
  assert.equal(runningLease.conversation_key, "ou_lease_sender");
  assert.equal(runningLease.session_id, "routed-lease-1");
  assert.equal(runningLease.stale_after_seconds, 300);
}

async function testExecutionLeaseHeartbeatDoesNotInventProgress() {
  const leaseWrites = [];
  const capturedTimers = [];
  const originalSetInterval = global.setInterval;
  const originalClearInterval = global.clearInterval;
  global.setInterval = (callback, _interval) => {
    const handle = { callback, unref() {} };
    capturedTimers.push(handle);
    return handle;
  };
  global.clearInterval = () => {};
  try {
    const service = createFeishuLongConnectionService({
      brokerClient: {
        call: async (command, payload) => {
          if (command === "record-bridge-message") {
            return {
              ok: true,
              record: {
                created_at: "2026-03-26T03:00:00Z",
                updated_at: "2026-03-26T03:00:00Z",
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
              stdout: "这是一次排查结论。",
              stderr: "session id: routed-lease-heartbeat-1",
            };
          }
          throw new Error(`unexpected command ${command}`);
        },
      },
      runtimeState: {
        saveBridgeStatus: async () => ({}),
        saveBridgeSettings: async () => ({}),
        saveBridgeExecutionLease: async (payload) => {
          leaseWrites.push({ ...payload });
          return { lease: payload };
        },
        fetchBridgeExecutionLease: async () => ({ lease: null }),
      },
      sdkLoader: () => null,
      logger: { info() {}, warn() {}, error() {} },
    });

    const result = await service.handleMessageEvent({
      message: {
        message_id: "lease-route-heartbeat-1",
        message_type: "text",
        content: JSON.stringify({ text: "请继续排查这个飞书线程为什么没有后续结果" }),
        chat_type: "p2p",
        chat_id: "",
      },
      sender: { sender_id: { open_id: "ou_lease_heartbeat_sender" } },
    });

    assert.equal(result.ok, true);
    const runningWrites = leaseWrites.filter((item) => item.state === "running");
    assert.ok(runningWrites.length >= 1);
    const initialProgressAt = runningWrites[0].last_progress_at;
    assert.ok(initialProgressAt);
    assert.ok(capturedTimers.length >= 1);

    capturedTimers[0].callback();
    await new Promise((resolve) => setImmediate(resolve));
    await new Promise((resolve) => setImmediate(resolve));

    const refreshedRunningWrites = leaseWrites.filter((item) => item.state === "running");
    assert.ok(refreshedRunningWrites.length >= 2);
    const latestRunning = refreshedRunningWrites[refreshedRunningWrites.length - 1];
    assert.equal(latestRunning.last_progress_at, initialProgressAt);
  } finally {
    global.setInterval = originalSetInterval;
    global.clearInterval = originalClearInterval;
  }
}

async function main() {
  await testSanitizeAndSummarize();
  await testNormalizeSdkDomain();
  await testNormalizeMessageEvent();
  await testNormalizeMessageEventExtractsNestedPostContent();
  await testNormalizeMessageEventIgnoresAttachmentMetadataInPostContent();
  await testNormalizeMessageEventSkipsAttachmentOnlyPayloads();
  await testInboundNormalizeMessageEventSkipsAttachmentMetadata();
  await testNormalizeMessageEventDetectsTextMentionAlias();
  await testShouldAcceptMessage();
  await testAttachmentMessagesDoNotRouteIntoCodex();
  await testShouldRunInBackgroundSkipsStatusQuestions();
  await testExecutionProfileAndApprovalClassificationCoverFeishuLocalExtensions();
  await testSummarizeErrorTextPrefersMeaningfulExceptionLine();
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
  await testSessionLaneMismatchForcesFreshExecPerChat();
  await testTimedOutResumeClearsBindingAndAllowsFreshFollowup();
  await testProgressStalledRunningLeaseForcesFreshExecWithoutResume();
  await testMissingLeaseForcesFreshExecWithoutResume();
  await testNonRunningLeaseForcesFreshExecWithoutResume();
  await testHandleMessageEventPrefersFinalizeLaunchReplyText();
  await testNestedPostContentRoutesWithoutEmptyTextFailure();
  await testLongBackgroundTasksStaySilentUntilFinalReply();
  await testConnectRecoversPendingConversationWithRecoveryNotice();
  await testReconnectNotifiesRecentActiveConversationAfterOutage();
  await testReconnectSkipsRecentConversationWithoutOpenWork();
  await testReconnectWhilePreviouslyConnectedSkipsRecentRecoveryNotice();
  await testConnectResetsEventClockAfterLongIdleGap();
  await testColdConnectLoadsPersistedBridgeStatusForRecoveryNotice();
  await testColdConnectSkipsRecentRecoveryNoticeWhenPersistedStatusWasConnected();
  await testOnlyRuntimeQueriesStayDirect();
  await testTopLevelHandlerSendsFallbackReplyOnUnexpectedFailure();
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
  await testGitCommitRequestReturnsApprovalTokenPrompt();
  await testHighRiskRequestEventHandlerSendsInteractiveApprovalCard();
  await testLocalSystemApprovalPromptStoresScopedApprovedProfile();
  await testApproveCommandExecutesWithApprovedProfile();
  await testApproveCommandExecutesWithLocalSystemApprovedProfile();
  await testCardActionTriggerApprovesViaInteractiveCard();
  await testCardActionFallsBackToEmbeddedChatContext();
  await testLegacyCardActionSourceMessageFallbackStillApproves();
  await testCardActionTriggerResumesBackgroundJobDelivery();
  await testApproveCommandRejectsWrongThread();
  await testApproveCommandRejectsExpiredToken();
  await testApproveShorthandUsesPendingThreadToken();
  await testHandleMessageEventWritesExecutionLeaseForRunningRoute();
  await testExecutionLeaseHeartbeatDoesNotInventProgress();
  console.log("ok");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
