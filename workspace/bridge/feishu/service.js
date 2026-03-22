"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");
const { FeishuGateway } = require("./gateway");
const {
  buildMetricDigestCardPayload,
  buildReplyCardPayload,
  phaseCardMeta,
  sendPostMessage,
  sendTextMessage,
  sendInteractiveCardMessage,
} = require("./outbound");
const { createCardStreamController } = require("./card-controller");

const BRIDGE_NAME = "feishu";

const DEFAULT_SETTINGS = {
  app_id: "",
  app_secret: "",
  domain: "feishu",
  allowed_users: [],
  group_policy: "mentions_only",
  require_mention: true,
};
const TEXT_MENTION_PATTERNS = [
  /(?:^|[\s(（【\[])[@＠]_user_1(?=$|[\s,.!?:;，。！？：；)\]】）])/i,
  /(?:^|[\s(（【\[])[@＠]coco(?=$|[\s,.!?:;，。！？：；)\]】）])/i,
  /(?:^|[\s(（【\[])[@＠]可可(?=$|[\s,.!?:;，。！？：；)\]】）])/,
];
const HEARTBEAT_INTERVAL_MS = 30_000;
const STALE_AFTER_SECONDS = 90;
// "event idle" drives `event_stalled` detection downstream. Keep it low when actively handling a request,
// but much higher when idle to avoid false reconnect loops when nobody is talking to CoCo.
const EVENT_IDLE_AFTER_SECONDS = 5 * 60;
const EVENT_IDLE_AFTER_SECONDS_IDLE = 30 * 60;
const ACTIVE_EXECUTION_EVENT_IDLE_SECONDS = 30 * 60;
const ACK_PENDING_EVENT_IDLE_GRACE_SECONDS = 2 * 60;
const DELAYED_REPLY_NOTICE_SECONDS = 45;
const BACKGROUND_ACK_SECONDS = 30;
const BACKGROUND_FOLLOWUP_REPEAT_SECONDS = 60;
const MAX_BACKGROUND_FOLLOWUPS = 3;
const RECOVERY_SWEEP_MIN_AGE_SECONDS = 20;
const RECOVERY_SWEEP_MAX_AGE_SECONDS = 6 * 60 * 60;
const RECOVERY_SWEEP_LIMIT = 20;
const APPROVAL_TOKEN_TTL_SECONDS = 1800;
const HIGH_RISK_PATTERNS = [
  /\bgit\s+push\b/i,
  /\bgh\s+pr\b/i,
  /\bssh\b/i,
  /\bscp\b/i,
  /\brsync\b/i,
  /\bdeploy\b/i,
  /\bpublish\b/i,
  /推(?:到|送到|上)?\s*github/iu,
  /提交(?:到)?\s*github/iu,
  /发(?:到|上)?\s*github/iu,
  /发布生产/u,
  /线上部署/u,
  /合并\s*pr/iu,
  /\bmerge\s+pr\b/i,
];
const BACKGROUND_REQUEST_PATTERN =
  /(修复|实现|执行|运行|安装|配置|部署|生成|编写|更新|删除|新建|创建|排查|处理|完成|重构|整理|跑(?:一轮)?测试|运行测试|执行测试|测试一下|测一下|回归测试|提交代码|提交改动|提交到|推送到|推到|pull|push|ssh|deploy|发布|合并|merge\s+pr|commit|代码改动|apply[_ -]?patch)/iu;
const STATUS_QUERY_PATTERNS = [
  /状态是什?么样/iu,
  /当前.*状态/iu,
  /先确定.*状态/iu,
  /说说.*状态/iu,
  /先查.*(配置|凭据|密钥|token|secret|env|环境变量)/iu,
  /自己先找.*(配置|凭据|密钥|token|secret|env|环境变量)/iu,
  /看.*(\.env|配置|凭据|密钥|token|secret|环境变量)/iu,
  /为什么/iu,
  /怎么回事/iu,
  /解释(?:一下)?/iu,
  /说明(?:一下)?/iu,
  /了解一下(?:项目)?/iu,
  /什么作用/iu,
  /有(?:没有)?必要/iu,
  /值不值得/iu,
  /适合长期保留/iu,
];
const FEISHU_OBJECT_OPERATION_PATTERNS = [
  /(飞书|lark).*(多维表格|表格|文档|日历|日程|会议|视频会议|任务|消息|群聊|用户)/iu,
  /(多维表格|飞书表格|bitable)/iu,
  /(日程|日历|会议|视频会议)/iu,
  /飞书文档/iu,
  /飞书任务/iu,
  /在飞书里.*(新建|创建|添加|安排|预约|预定|发送|编辑|更新|管理)/iu,
];
const LOCAL_EXTENSION_PATTERNS = [
  /(?:^|[\s])skill(?:s)?(?:$|[\s,.!?:;，。！？：；])/iu,
  /Skill Creator/i,
  /安装.*(?:skill|skills|技能)/iu,
  /(?:skill|skills|技能).*(安装|创建|生成|注册|同步|写入|放到|复制到|更新)/iu,
  /~\/\.codex\/skills/iu,
  /\.codex\/skills/iu,
  /~\/\.codex\/agents/iu,
  /\.codex\/agents/iu,
];
const LOCAL_SYSTEM_APPROVAL_PATTERNS = [
  /~\/Library\/LaunchAgents/iu,
  /LaunchAgents/iu,
  /\blaunchctl\b/i,
  /\bbrew\s+services\b/i,
  /\bdefaults\s+write\b/i,
  /\bcrontab\b/i,
  /~\/\.(?:zshrc|bashrc|bash_profile|zprofile)/iu,
  /\/Applications\//iu,
  /安装到\s*Applications/iu,
  /\/usr\/local\/bin/iu,
  /\/opt\/homebrew\/bin/iu,
  /登录项/iu,
  /启动项/iu,
  /系统级安装/iu,
];
const SILENT_GATE_REASONS = new Set([
  "duplicate_message",
  "empty_text",
  "mention_required",
  "sender_not_allowed",
]);
const FEISHU_ACK_TEMPLATES = [
  "好的，我先处理。",
  "收到，我马上跟进。",
  "知道了，我先看一下。",
  "明白，我这就开始处理。",
  "收到，我先帮你过一遍。",
];
const APPROVAL_CARD_ACTION_PREFIX = "perm";
const APPROVAL_CARD_FALLBACK_HINT = "如果按钮不可用，也可以直接回复文本命令。";
const CARD_ACTION_TIMEOUT_MS = 2500;
const DOC_REPLY_CHAR_THRESHOLD = 1400;
const DOC_REPLY_LINE_THRESHOLD = 14;
const DOC_MIRROR_PHASES = new Set(["reply", "report", "final", "status", "thread_status"]);
const INTERACTIVE_REPLY_PHASES = new Set([
  "reply",
  "report",
  "final",
  "status",
  "thread_status",
  "approval_status",
  "approval_confirmed",
  "binding_prompt",
  "binding_bound",
  "binding_error",
  "error",
]);
const MATERIAL_HINT_PATTERN =
  /(状态|总结|梳理|看看|查看|继续|材料|报告|交付|文档|配置|资料|背景|入口|现状|进展|环境|环境变量|env|\.env|凭据|密钥|token|secret|supabase|ecs|服务器|github|阿里云|火山云|review|审查|审核|检查|查一下|查一查|了解|怎么做)/iu;
const MATERIAL_HINT_SKIP_PATTERN =
  /(修改|修复|实现|提交|删除|重构|生成|运行|跑测试|测试一下|push|ssh|deploy|发布|合并|commit|代码改动|apply[_ -]?patch)/iu;
const CARD_ACTION_FALLBACK_RESPONSE = {
  toast: {
    type: "info",
    content: "已收到，正在处理...",
  },
};

const PROJECT_REGISTRY_PATTERN =
  /<!-- PROJECT_REGISTRY_DATA_START -->\s*```json\s*([\s\S]*?)\s*```\s*<!-- PROJECT_REGISTRY_DATA_END -->/;

function guessProjectRegistryPath() {
  const candidates = [];
  const vaultFromEnv = process.env.WORKSPACE_HUB_VAULT_ROOT;
  if (vaultFromEnv) {
    candidates.push(path.join(vaultFromEnv, "PROJECT_REGISTRY.md"));
  }
  const workspaceRoot = process.env.WORKSPACE_HUB_ROOT;
  if (workspaceRoot) {
    candidates.push(
      path.resolve(
        workspaceRoot,
        "../memory/PROJECT_REGISTRY.md",
      ),
    );
  }
  candidates.push(
    path.resolve(__dirname, "../../../memory/PROJECT_REGISTRY.md"),
  );
  for (const candidate of candidates) {
    if (!candidate) {
      continue;
    }
    try {
      if (fs.existsSync(candidate)) {
        return candidate;
      }
    } catch (_error) {
      // ignore
    }
  }
  return candidates[candidates.length - 1];
}

function loadProjectRegistryEntries() {
  const registryPath = guessProjectRegistryPath();
  try {
    const fileText = fs.readFileSync(registryPath, "utf-8");
    const match = PROJECT_REGISTRY_PATTERN.exec(fileText);
    if (!match) {
      return [];
    }
    return JSON.parse(match[1]);
  } catch (_error) {
    return [];
  }
}

function determineDefaultProjectName(entries) {
  if (!Array.isArray(entries) || entries.length === 0) {
    return "Codex Hub";
  }
  const preferred = entries.find(
    (entry) => String(entry.project_name || "").trim() === "Codex Hub",
  );
  if (preferred && preferred.project_name) {
    return String(preferred.project_name).trim();
  }
  const firstEntryName = String(entries[0].project_name || "").trim();
  return firstEntryName || "Codex Hub";
}

function buildProjectAliasCandidates(entries) {
  const seen = new Set();
  const candidates = [];
  if (!Array.isArray(entries)) {
    return candidates;
  }
  for (const entry of entries) {
    const canonical = String(entry?.project_name || "").trim();
    if (!canonical) {
      continue;
    }
    const names = [canonical, ...(Array.isArray(entry.aliases) ? entry.aliases : [])];
    for (const rawName of names) {
      const trimmed = String(rawName || "").trim();
      if (!trimmed) {
        continue;
      }
      const normalized = trimmed.toLowerCase();
      if (seen.has(normalized)) {
        continue;
      }
      seen.add(normalized);
      candidates.push({ canonical, alias: trimmed, normalized });
    }
  }
  candidates.sort((a, b) => b.alias.length - a.alias.length);
  return candidates;
}

function getProjectRegistryEntries() {
  return loadProjectRegistryEntries();
}

function getProjectAliasCandidates() {
  return buildProjectAliasCandidates(
    getProjectRegistryEntries(),
  );
}

const BINDING_DECLARATION_PATTERNS = [
  /只聊\s*(.+)/i,
  /只讨论\s*(.+)/i,
  /只处理\s*(.+)/i,
  /以后(?:在这里|在这个群(?:组)?(?:里|里面)?|这个群(?:组)?(?:里|里面)?)?[^。？！\n]*?(?:只聊|只讨论|只处理)\s*(.+)/i,
];
const BINDING_TARGET_CLEANUP_PATTERN = /[。？！!?.,，]+$/u;
const BINDING_TARGET_MENTION_PATTERN = /@[_a-z0-9-]+/gi;
const BINDING_TOPIC_EMPTY_PATTERN = /^(?:的|项目|项目的|这个项目|相关|相关事项|的事情|事项|工作|任务)*$/u;

function extractBindingDeclaration(text) {
  const source = String(text || "").trim();
  if (!source) {
    return "";
  }
  for (const pattern of BINDING_DECLARATION_PATTERNS) {
    const match = source.match(pattern);
    if (match && match[1]) {
      return String(match[1]).trim();
    }
  }
  return "";
}

function tidyBindingTarget(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  return text
    .replace(BINDING_TARGET_MENTION_PATTERN, "")
    .replace(/\s+/g, " ")
    .replace(BINDING_TARGET_CLEANUP_PATTERN, "")
    .trim();
}

function matchProjectAlias(target) {
  const aliasCandidates = getProjectAliasCandidates();
  if (!target || !aliasCandidates.length) {
    return null;
  }
  const lowerTarget = target.toLowerCase();
  for (const candidate of aliasCandidates) {
    if (lowerTarget.includes(candidate.normalized)) {
      return candidate;
    }
  }
  return null;
}

function stripAliasFromTarget(target, alias) {
  if (!alias) {
    return target.trim();
  }
  const escaped = alias.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const replaced = target.replace(new RegExp(escaped, "i"), "");
  return replaced.replace(/\s+/g, " ").trim();
}

function normalizeTopicName(value) {
  const cleaned = tidyBindingTarget(value)
    .replace(/^(?:的|项目|这个项目|相关|相关事项|事项|工作|任务)+/u, "")
    .replace(/(?:的|项目|这个项目|相关|相关事项|事项|工作|任务)+$/u, "")
    .trim();
  if (!cleaned || BINDING_TOPIC_EMPTY_PATTERN.test(cleaned)) {
    return "";
  }
  return cleaned;
}

function getConversationKey(normalized) {
  if (!normalized) return "";
  return (
    normalized.chat_id ||
    normalized.open_id ||
    normalized.user_id ||
    normalized.message_id ||
    ""
  );
}

function buildBindingCandidate(normalized, existingBinding) {
  if (!normalized?.text) {
    return null;
  }
  const declaration = extractBindingDeclaration(normalized.text);
  if (!declaration) {
    return null;
  }
  const target = tidyBindingTarget(declaration);
  if (!target) {
    return null;
  }
  const aliasMatch = matchProjectAlias(target);
  const fallbackProject = existingBinding?.project_name || "";
  if (!aliasMatch && !fallbackProject) {
    return {
      error: "project_alias_required",
      declared_target: target,
    };
  }
  const projectName = aliasMatch?.canonical || fallbackProject;
  let topicName = aliasMatch
    ? stripAliasFromTarget(target, aliasMatch.alias)
    : target;
  topicName = normalizeTopicName(topicName);
  if (!topicName) {
    topicName = existingBinding?.topic_name || "";
  }
  if (!projectName) {
    return null;
  }
  return { project_name: projectName, topic_name: topicName };
}

function resolveMessageRouteContext(normalized, existingBinding) {
  const binding = existingBinding || null;
  const declaration = extractBindingDeclaration(normalized?.text || "");
  const declarationTarget = declaration ? tidyBindingTarget(declaration) : "";
  const rawText = tidyBindingTarget(normalized?.text || "");
  const aliasMatch = matchProjectAlias(declarationTarget || rawText);
  const projectName = String(aliasMatch?.canonical || binding?.project_name || "").trim();
  let topicName = "";
  if (aliasMatch && declarationTarget) {
    topicName = normalizeTopicName(stripAliasFromTarget(declarationTarget, aliasMatch.alias));
  }
  if (!topicName) {
    topicName = String(binding?.topic_name || "").trim();
  }
  const routeSource = aliasMatch
    ? declarationTarget
      ? "binding_declaration"
      : "message_project_alias"
    : projectName
      ? "thread_binding"
      : "workspace";
  return {
    project_name: projectName,
    topic_name: topicName,
    route_source: routeSource,
    alias_matched: aliasMatch?.alias || "",
  };
}

function resolveSourceThreadIdentity(normalized, routeContext, existingBinding, conversationKey) {
  const projectName = String(routeContext?.project_name || existingBinding?.project_name || "").trim();
  const topicName = String(routeContext?.topic_name || existingBinding?.topic_name || "").trim();
  let threadLabel = "";
  if (projectName && topicName) {
    threadLabel = `${projectName} / ${topicName}`;
  } else if (projectName) {
    threadLabel = projectName;
  } else if (String(normalized?.chat_type || "").trim() === "p2p") {
    threadLabel = "CoCo 私聊";
  } else {
    threadLabel = String(conversationKey || "").trim() || "Feishu 线程";
  }
  return {
    threadName: threadLabel,
    threadLabel,
  };
}

async function readPersistedBinding(brokerClient, conversationKey) {
  if (!conversationKey) {
    return null;
  }
  try {
    const payload = await brokerClient.call("bridge-chat-binding", {
      bridge: BRIDGE_NAME,
      chat_ref: conversationKey,
    });
    const binding = payload?.binding || payload || null;
    if (!binding || !binding.chat_ref) {
      return null;
    }
    return binding;
  } catch (_error) {
    return null;
  }
}

async function writePersistedBinding(brokerClient, conversationKey, binding) {
  if (!conversationKey || !binding) {
    return null;
  }
  const payload = await brokerClient.call("bridge-chat-binding", {
    bridge: BRIDGE_NAME,
    chat_ref: conversationKey,
    binding_json: {
      binding_scope: String(binding.binding_scope || (binding.topic_name ? "topic" : "project")).trim() || "project",
      project_name: String(binding.project_name || "").trim(),
      topic_name: String(binding.topic_name || "").trim(),
      session_id: String(binding.session_id || "").trim(),
      metadata: binding.metadata || {},
    },
  });
  if (payload?.ok === false) {
    const error = new Error(String(payload.error || payload.reason || "binding_rejected").trim() || "binding_rejected");
    error.brokerPayload = payload;
    throw error;
  }
  const persisted = payload?.binding || payload || null;
  if (!persisted || !persisted.chat_ref) {
    return null;
  }
  return persisted;
}

function sanitizeSettings(input = {}) {
  const allowedUsers = Array.isArray(input.allowed_users)
    ? input.allowed_users.map((value) => String(value || "").trim()).filter(Boolean)
    : [];
  return {
    app_id: String(input.app_id || "").trim(),
    app_secret: String(input.app_secret || "").trim(),
    domain: String(input.domain || DEFAULT_SETTINGS.domain).trim() || DEFAULT_SETTINGS.domain,
    allowed_users: allowedUsers,
    group_policy: String(input.group_policy || DEFAULT_SETTINGS.group_policy).trim() || DEFAULT_SETTINGS.group_policy,
    require_mention: input.require_mention == null ? DEFAULT_SETTINGS.require_mention : Boolean(input.require_mention),
  };
}

function summarizeSettings(settings) {
  return {
    domain: settings.domain,
    group_policy: settings.group_policy,
    require_mention: settings.require_mention,
    allowed_user_count: settings.allowed_users.length,
    has_app_credentials: Boolean(settings.app_id && settings.app_secret),
    configured_keys: Object.entries(settings)
      .filter(([key, value]) => key !== "app_secret" && !(Array.isArray(value) && !value.length) && value !== "")
      .map(([key]) => key)
      .sort(),
  };
}

function normalizeSdkDomain(sdk, domain) {
  const normalized = String(domain || "").trim().toLowerCase();
  if (normalized === "lark") {
    return sdk?.Domain?.Lark ?? domain;
  }
  if (!normalized || normalized === "feishu") {
    return sdk?.Domain?.Feishu ?? domain;
  }
  return domain;
}

function tryLoadFeishuSdk() {
  try {
    return require("@larksuiteoapi/node-sdk");
  } catch (_error) {
    return null;
  }
}

function normalizeMessageEvent(event) {
  const message = event?.message || {};
  const sender = event?.sender || {};
  const mentions = Array.isArray(message?.mentions) ? message.mentions : [];
  const content = safeParseContent(message?.content);
  const text = extractText(content);
  const messageCreatedAt = normalizeEventTimestamp(message?.create_time || message?.create_at || event?.create_time);
  return {
    event_type: event?.event_type || "im.message.receive_v1",
    message_id: String(message?.message_id || "").trim(),
    message_type: String(message?.message_type || "text").trim() || "text",
    chat_id: String(message?.chat_id || "").trim(),
    chat_type: String(message?.chat_type || "").trim(),
    open_id: String(sender?.sender_id?.open_id || sender?.open_id || "").trim(),
    user_id: String(sender?.sender_id?.user_id || sender?.user_id || "").trim(),
    text,
    mentions,
    text_mentions: TEXT_MENTION_PATTERNS.filter((pattern) => pattern.test(text)).map((pattern) => pattern.source),
    raw_content: content,
    message_created_at: messageCreatedAt,
  };
}

function normalizeCardActionEvent(event) {
  const actionValue = event?.action?.value || event?.action?.form_value || {};
  const callbackData = String(
    actionValue?.callback_data ||
      "",
  ).trim();
  const chatId = String(
    event?.context?.open_chat_id ||
      event?.event?.context?.open_chat_id ||
      actionValue?.chat_id ||
      actionValue?.open_chat_id ||
      event?.open_chat_id ||
      "",
  ).trim();
  const callbackMessageId = String(
    event?.context?.open_message_id ||
      event?.event?.context?.open_message_id ||
      actionValue?.message_id ||
      actionValue?.open_message_id ||
      event?.open_message_id ||
      "",
  ).trim();
  const operator = event?.operator || event?.event?.operator || {};
  const openId = String(operator?.open_id || event?.open_id || "").trim();
  const userId = String(operator?.user_id || event?.user_id || "").trim();
  const tokenIntent = parseApprovalCardAction(callbackData);
  const token = String(tokenIntent.token || "").trim();
  const syntheticMessageId = `card-action:${callbackMessageId || chatId || "unknown"}:${Date.now()}`;
  return {
    event_type: "card.action.trigger",
    message_id: syntheticMessageId,
    message_type: "card_action",
    callback_data: callbackData,
    callback_message_id: callbackMessageId,
    chat_id: chatId,
    chat_type: String(event?.context?.chat_type || event?.event?.context?.chat_type || "").trim(),
    open_id: openId,
    user_id: userId,
    text:
      tokenIntent.kind === "deny"
        ? `/deny ${token}`
        : token
          ? `/approve ${token}`
          : "",
    mentions: [],
    text_mentions: [],
    raw_content: event || {},
    message_created_at: new Date().toISOString(),
  };
}

function normalizeEventTimestamp(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  if (/^\d+$/.test(text)) {
    const numeric = Number(text);
    if (!Number.isFinite(numeric) || numeric <= 0) return "";
    const milliseconds = numeric > 1_000_000_000_000 ? numeric : numeric * 1000;
    return new Date(milliseconds).toISOString();
  }
  const parsed = Date.parse(text);
  if (Number.isNaN(parsed)) return "";
  return new Date(parsed).toISOString();
}

function safeParseContent(content) {
  if (typeof content !== "string") return content || {};
  const text = content.trim();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch (_error) {
    return { text };
  }
}

function extractText(content) {
  if (!content) return "";
  if (typeof content === "string") return content.trim();
  if (typeof content.text === "string") return content.text.trim();
  return "";
}

function trimReplyText(text, limit = 1500) {
  const normalized = String(text || "").trim();
  if (!normalized) return "";
  if (normalized.length <= limit) return normalized;
  return `${normalized.slice(0, limit - 1).trimEnd()}…`;
}

function chooseAckTemplate(seedText) {
  const source = String(seedText || "");
  if (!source) {
    return FEISHU_ACK_TEMPLATES[0];
  }
  let score = 0;
  for (let index = 0; index < source.length; index += 1) {
    score = (score + source.charCodeAt(index) * (index + 1)) % 10007;
  }
  return FEISHU_ACK_TEMPLATES[score % FEISHU_ACK_TEMPLATES.length];
}

function demoteMarkdownHeadings(text) {
  const MARK = "___FEISHU_CODE_BLOCK_";
  const codeBlocks = [];
  let body = String(text || "").replace(/```[\s\S]*?```/g, (block) => {
    return `${MARK}${codeBlocks.push(block) - 1}___`;
  });
  if (/^#{1,3}\s+/m.test(body)) {
    body = body.replace(/^#{2,6}\s+(.+)$/gm, "##### $1");
    body = body.replace(/^#\s+(.+)$/gm, "#### $1");
  }
  codeBlocks.forEach((block, index) => {
    body = body.replace(`${MARK}${index}___`, `\n\n${block}\n\n`);
  });
  return body;
}

function stripLocalMarkdownLinks(text) {
  return String(text || "").replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_full, label, target) => {
    const href = String(target || "").trim();
    if (href.startsWith("/Users/")) {
      return `\`${String(label || "").trim() || href}\``;
    }
    return `[${label}](${href})`;
  });
}

function normalizeFeishuMarkdown(text) {
  return demoteMarkdownHeadings(stripLocalMarkdownLinks(text))
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function splitReplyText(text, limit = 3200) {
  const normalized = String(text || "").trim();
  if (!normalized) return [];
  if (normalized.length <= limit) return [normalized];
  const parts = [];
  let remaining = normalized;
  while (remaining.length > limit) {
    const candidate = remaining.slice(0, limit);
    const splitIndex = Math.max(
      candidate.lastIndexOf("\n\n"),
      candidate.lastIndexOf("\n"),
      candidate.lastIndexOf("。"),
      candidate.lastIndexOf("！"),
      candidate.lastIndexOf("？"),
      candidate.lastIndexOf("，"),
      candidate.lastIndexOf(" "),
    );
    const boundary = splitIndex >= Math.floor(limit * 0.5) ? splitIndex + 1 : limit;
    const chunk = remaining.slice(0, boundary).trim();
    if (chunk) {
      parts.push(chunk);
    }
    remaining = remaining.slice(boundary).trim();
  }
  if (remaining) {
    parts.push(remaining);
  }
  if (parts.length <= 1) return parts;
  return parts.map((part, index) => `(${index + 1}/${parts.length}) ${part}`);
}

function summarizeErrorText(text) {
  const normalized = String(text || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .find((line) => !line.startsWith("OpenAI Codex") && !line.startsWith("--------") && !line.startsWith("tokens used"));
  return trimReplyText(normalized || "执行失败，但没有拿到可读的错误信息。", 500);
}

function extractSessionId(text) {
  const match = String(text || "").match(/session id:\s*([A-Za-z0-9-]+)/i);
  return match ? match[1] : "";
}

function summarizeBrokerFailure(payload, fallback = "执行失败") {
  const direct = String(payload?.pause?.summary || payload?.error || payload?.reason || "").trim();
  if (direct) {
    return trimReplyText(direct, 500);
  }
  return `${fallback}：${summarizeErrorText(payload?.stderr || payload?.stdout || "")}`;
}

function buildBindingFailureReply(payload) {
  if (String(payload?.error || "").trim() === "project_alias_required") {
    const declaredTarget = String(payload?.declared_target || "").trim();
    const lines = ["这个群的绑定没有成功。"];
    if (declaredTarget) {
      lines.push(`我没有在这句话里识别出正式项目名：\`${declaredTarget}\``);
    }
    lines.push("首次绑定时，请先用正式项目名或项目别名重新声明，例如：这个群只聊 SampleProj。");
    return lines.join("\n");
  }
  const lines = ["这个群的绑定没有成功。", summarizeBrokerFailure(payload, "绑定失败")];
  const availableTopics = Array.isArray(payload?.available_topics)
    ? payload.available_topics.map((item) => String(item || "").trim()).filter(Boolean)
    : [];
  if (availableTopics.length) {
    lines.push(`当前可用专题：${availableTopics.join(" / ")}`);
  }
  lines.push("请先用正式项目名或项目别名重新声明，例如：这个群只聊 SampleProj。");
  return lines.join("\n");
}

function renderReplyText(brokerPayload, fallbackText) {
  const stdout = String(brokerPayload?.stdout || "").trim();
  if (stdout) return stdout;
  if (brokerPayload?.reason === "project_paused" || brokerPayload?.error_type === "project_paused") {
    return summarizeBrokerFailure(brokerPayload, "当前项目已暂停执行");
  }
  if (
    brokerPayload?.ok === false ||
    brokerPayload?.result_status === "error" ||
    brokerPayload?.result_status === "suppressed" ||
    Number(brokerPayload?.returncode || 0) !== 0
  ) {
    return summarizeBrokerFailure(brokerPayload);
  }
  if (fallbackText) {
    return `我已收到：${trimReplyText(fallbackText, 300)}`;
  }
  return "我已收到消息，但这次没有拿到可返回的结果。";
}

function compactDisplayPath(pathValue) {
  const raw = String(pathValue || "").trim();
  if (!raw) {
    return "";
  }
  const normalized = raw.replace(/\\/g, "/");
  const parts = normalized.split("/").filter(Boolean);
  if (parts.length <= 4) {
    return parts.join("/");
  }
  return parts.slice(-4).join("/");
}

function formatMaterialHintItem(item) {
  const title = String(item?.title || item?.heading || "").trim() || path.basename(String(item?.path || ""));
  const compactPath = compactDisplayPath(item?.path);
  return compactPath ? `${title}（\`${compactPath}\`）` : title;
}

function buildMaterialHintBlock(materialPayload) {
  if (!materialPayload || typeof materialPayload !== "object") {
    return "";
  }
  const lines = [];
  const boardPath = compactDisplayPath(materialPayload.board_path);
  if (boardPath) {
    lines.push(`当前板面：\`${boardPath}\``);
  }
  const hotsetItem = Array.isArray(materialPayload.hotset_hits) ? materialPayload.hotset_hits[0] : null;
  const reportItem = Array.isArray(materialPayload.report_hits) ? materialPayload.report_hits[0] : null;
  const deliverableItem = Array.isArray(materialPayload.deliverable_hits) ? materialPayload.deliverable_hits[0] : null;
  const materialItem = Array.isArray(materialPayload.material_hits) ? materialPayload.material_hits[0] : null;
  if (hotsetItem) {
    lines.push(`优先材料：${formatMaterialHintItem(hotsetItem)}`);
  }
  if (reportItem) {
    lines.push(`最新报告：${formatMaterialHintItem(reportItem)}`);
  }
  if (deliverableItem) {
    lines.push(`待看交付：${formatMaterialHintItem(deliverableItem)}`);
  } else if (materialItem && (!hotsetItem || materialItem.path !== hotsetItem.path)) {
    lines.push(`相关材料：${formatMaterialHintItem(materialItem)}`);
  }
  if (!lines.length) {
    return "";
  }
  return `补充入口：\n${lines.map((line) => `- ${line}`).join("\n")}`;
}

function shouldAttachMaterialHints(normalized, routeContext, brokerPayload) {
  if (!routeContext?.project_name) {
    return false;
  }
  if (
    brokerPayload?.ok === false ||
    brokerPayload?.result_status === "error" ||
    brokerPayload?.result_status === "suppressed" ||
    Number(brokerPayload?.returncode || 0) !== 0
  ) {
    return false;
  }
  const text = String(normalized?.text || "").trim();
  if (!text) {
    return false;
  }
  if (isHighRiskRequest(text) || MATERIAL_HINT_SKIP_PATTERN.test(text)) {
    return false;
  }
  return MATERIAL_HINT_PATTERN.test(text) || /[?？]$/.test(text);
}

function buildDelayedReplyNotice(normalized, currentStatus) {
  const createdAtText = String(normalized?.message_created_at || "").trim();
  if (!createdAtText) return "";
  const createdAt = Date.parse(createdAtText);
  if (Number.isNaN(createdAt)) return "";
  const now = Date.now();
  const ageSeconds = Math.floor((now - createdAt) / 1000);
  if (ageSeconds < DELAYED_REPLY_NOTICE_SECONDS) {
    return "";
  }
  const connectedAt = Date.parse(String(currentStatus?.connected_at || "").trim());
  const reconnectedAfterMessage = Number.isFinite(connectedAt) && createdAt < connectedAt;
  const reason = reconnectedAfterMessage
    ? "之前因为我离线或刚完成重连，没能及时回复。"
    : "之前因为本地桥接恢复较慢，没能及时回复。";
  return `说明：你这条消息是延迟补回的，${reason}我现在已经在线，下面给你补上处理结果。`;
}

function buildBackgroundFollowupText(attempt = 0) {
  if (attempt <= 0) {
    return "进度更新：我还在处理这条任务，完成后会继续汇报。";
  }
  if (attempt === 1) {
    return "继续处理中：这条任务还在执行，我会在完成后第一时间汇报。";
  }
  return "状态同步：这条任务仍在处理中，我会继续跟进并在完成后补上结果。";
}

function withDeliveryNotice(text, notice) {
  const body = String(text || "").trim();
  const prefix = String(notice || "").trim();
  if (!prefix) return body;
  if (!body) return prefix;
  return `${prefix}\n\n${body}`;
}

function shapeFeishuReplyText(text, phase = "") {
  const normalized = String(text || "").trim();
  if (!normalized) {
    return "";
  }
  if (phase === "ack") {
    return trimReplyText(normalized, 80);
  }
  return normalizeFeishuMarkdown(normalized);
}

function summarizeReplyForCard(text, limit = 200) {
  const lines = String(text || "")
    .split(/\r?\n/)
    .map((line) => line.replace(/^[-*]\s*/, "").trim())
    .filter(Boolean);
  return trimReplyText(lines[0] || String(text || "").trim(), limit);
}

function extractReplyMetrics(text, limit = 4) {
  const metrics = [];
  for (const rawLine of String(text || "").split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line) continue;
    const match = line.match(/^(?:[-*]\s*)?([^:：]{1,40})[:：]\s*(.+)$/);
    if (!match) continue;
    metrics.push({
      label: trimReplyText(match[1], 20),
      value: trimReplyText(match[2], 40),
    });
    if (metrics.length >= limit) {
      break;
    }
  }
  return metrics;
}

function shouldUseInteractiveReply(phase = "") {
  return INTERACTIVE_REPLY_PHASES.has(String(phase || "").trim());
}

function shouldMirrorReplyToDoc(phase = "", text = "") {
  const normalized = String(text || "").trim();
  if (!normalized || !DOC_MIRROR_PHASES.has(String(phase || "").trim())) {
    return false;
  }
  if (normalized.length >= DOC_REPLY_CHAR_THRESHOLD) {
    return true;
  }
  const lines = normalized.split(/\r?\n/).filter((line) => line.trim()).length;
  return lines >= DOC_REPLY_LINE_THRESHOLD;
}

function buildReplyDocTitle(phase = "", text = "") {
  const meta = phaseCardMeta(phase);
  const summary = summarizeReplyForCard(text, 72).replace(/[`*#_[\]]/g, "").trim();
  if (summary) {
    return `${meta.title}｜${summary}`;
  }
  return meta.title;
}

async function createReplyDocIfNeeded(brokerClient, logger, { phase = "", text = "" } = {}) {
  if (!brokerClient || typeof brokerClient.call !== "function") {
    return null;
  }
  if (!shouldMirrorReplyToDoc(phase, text)) {
    return null;
  }
  try {
    const title = buildReplyDocTitle(phase, text);
    const payload = await brokerClient.call("feishu-op", {
      domain: "doc",
      action: "create",
      payload: {
        title,
        content: text,
      },
    });
    const result = payload?.result || payload;
    const documentId = String(result?.document_id || "").trim();
    const url = String(result?.url || "").trim();
    if (!documentId || !url) {
      return null;
    }
    return { documentId, url, title };
  } catch (error) {
    logger?.warn?.("feishu reply doc mirror skipped", error);
    return null;
  }
}

function buildInteractiveReplyCard(phase = "", text = "", docRef = null) {
  const normalizedPhase = String(phase || "").trim();
  if (["status", "thread_status", "approval_status"].includes(normalizedPhase)) {
    return buildMetricDigestCardPayload({
      title: phaseCardMeta(normalizedPhase).title,
      summary: summarizeReplyForCard(text, 200),
      metrics: extractReplyMetrics(text, 4),
      docUrl: String(docRef?.url || "").trim(),
      docTitle: String(docRef?.title || "").trim(),
    });
  }
  return buildReplyCardPayload({
    phase: normalizedPhase,
    text,
    docUrl: String(docRef?.url || "").trim(),
    docTitle: String(docRef?.title || "").trim(),
  });
}

function buildPostContent(text) {
  return JSON.stringify({
    zh_cn: {
      content: [[{ tag: "md", text: String(text || "").trim() }]],
    },
  });
}

function buildRecoverySweepReply(row) {
  const scope =
    row?.project_name && row?.topic_name
      ? `项目 \`${row.project_name}\` / 话题 \`${row.topic_name}\``
      : row?.project_name
        ? `项目 \`${row.project_name}\``
        : "当前工作线程";
  return [
    "说明：我刚恢复在线。",
    `我看到这个聊天线程之前还有一条没有及时收口的请求，目前仍绑定在${scope}。`,
    "为了避免把旧任务和新任务串在一起，请把你当前还需要我处理的最新指令再发一遍，我会直接继续执行。",
  ].join("\n");
}

function shouldRecoverConversation(row) {
  if (!row || !row.chat_ref) {
    return false;
  }
  if (!row.project_name) {
    return false;
  }
  if (!row.pending_request) {
    return false;
  }
  if (!(row.pending_request || row.awaiting_report || row.needs_attention)) {
    return false;
  }
  const reason = String(row.attention_reason || "").trim();
  if (reason && !["response_delayed", "progress_stalled"].includes(reason)) {
    return false;
  }
  const ageSeconds = Number(row.last_user_request_age_seconds || 0);
  if (!Number.isFinite(ageSeconds) || ageSeconds < RECOVERY_SWEEP_MIN_AGE_SECONDS) {
    return false;
  }
  if (ageSeconds > RECOVERY_SWEEP_MAX_AGE_SECONDS) {
    return false;
  }
  return true;
}

function formatAddressedReply(name, text) {
  const preferredName = String(name || "").trim();
  const body = String(text || "").trim();
  if (!preferredName || !body) {
    return body;
  }
  if (body.includes("\n")) {
    return `${preferredName}，\n${body}`;
  }
  return `${preferredName}，${body}`;
}

function patchWsClientForCardCallbacks(wsClientInstance) {
  const wsClientAny = wsClientInstance;
  if (!wsClientAny || typeof wsClientAny.handleEventData !== "function") {
    return;
  }
  const originalHandleEventData = wsClientAny.handleEventData.bind(wsClientAny);
  wsClientAny.handleEventData = (data) => {
    const headers = Array.isArray(data?.headers) ? data.headers : [];
    const typeIndex = headers.findIndex((header) => header?.key === "type");
    if (typeIndex >= 0 && headers[typeIndex]?.value === "card") {
      const patchedHeaders = headers.map((header, index) =>
        index === typeIndex ? { ...header, value: "event" } : header,
      );
      return originalHandleEventData({ ...data, headers: patchedHeaders });
    }
    return originalHandleEventData(data);
  };
}

function formatBulletLines(items, renderItem, emptyText) {
  if (!Array.isArray(items) || !items.length) {
    return emptyText;
  }
  return items.map((item) => `- ${renderItem(item)}`).join("\n");
}

function classifyDirectIntent(normalized) {
  const text = String(normalized?.text || "").trim();
  if (!text) {
    return { kind: "empty" };
  }
  const approvalCommand = matchApprovalCommand(text);
  if (approvalCommand.kind !== "none") {
    return approvalCommand;
  }
  if (/(待授权|待审批|待批准|授权token|授权令牌|approval token|pending approval)/i.test(text)) {
    return { kind: "approval_status" };
  }
  if (/(当前线程状态|线程状态|会话状态|这个群现在做到哪|这个聊天现在做到哪|当前做到哪|进度状态)/u.test(text)) {
    return { kind: "thread_status" };
  }
  if (/(系统状态|当前系统状态|健康状态|health|告警|alerts?)/i.test(text)) {
    return { kind: "system_status" };
  }
  return { kind: "none" };
}

function matchApprovalCommand(text) {
  const normalized = String(text || "").trim();
  const approveMatch = normalized.match(/^\/approve\s+([A-Za-z0-9_-]+)\s*$/i);
  if (approveMatch) {
    return { kind: "approve", token: approveMatch[1] };
  }
  if (/^\/approve\s*$/i.test(normalized) || /^(批准|同意|确认执行|执行吧|可以执行了)$/u.test(normalized)) {
    return { kind: "approve", token: "" };
  }
  const denyMatch = normalized.match(/^\/deny\s+([A-Za-z0-9_-]+)\s*$/i);
  if (denyMatch) {
    return { kind: "deny", token: denyMatch[1] };
  }
  if (/^\/deny\s*$/i.test(normalized) || /^(拒绝|不同意|取消|取消授权|先别执行)$/u.test(normalized)) {
    return { kind: "deny", token: "" };
  }
  return { kind: "none", token: "" };
}

function parseApprovalCardAction(callbackData) {
  const source = String(callbackData || "").trim();
  const parts = source.split(":");
  if (parts.length < 3 || parts[0] !== APPROVAL_CARD_ACTION_PREFIX) {
    return { kind: "none", token: "" };
  }
  const action = parts[1];
  const token = parts.slice(2).join(":").trim();
  if (!token) {
    return { kind: "none", token: "" };
  }
  if (action === "allow" || action === "allow_session") {
    return { kind: "approve", token, via: "card", mode: action };
  }
  if (action === "deny") {
    return { kind: "deny", token, via: "card", mode: action };
  }
  return { kind: "none", token: "" };
}

function isHighRiskRequest(text) {
  const source = String(text || "").trim();
  if (!source) return false;
  return HIGH_RISK_PATTERNS.some((pattern) => pattern.test(source));
}

function isLocalExtensionRequest(text) {
  const source = String(text || "").trim();
  if (!source) return false;
  return LOCAL_EXTENSION_PATTERNS.some((pattern) => pattern.test(source));
}

function classifyApprovalRequirement(text) {
  const source = String(text || "").trim();
  if (!source) {
    return { required: false, scope: "", promptLabel: "", statusLabel: "" };
  }
  if (isHighRiskRequest(source)) {
    return {
      required: true,
      scope: "feishu_high_risk_execution",
      promptLabel: "这条命令涉及高风险远程或不可逆动作，我先暂停执行。",
      statusLabel: "高风险远程或不可逆操作",
    };
  }
  if (LOCAL_SYSTEM_APPROVAL_PATTERNS.some((pattern) => pattern.test(source))) {
    return {
      required: true,
      scope: "feishu_local_system_execution",
      promptLabel: "这条命令需要更高的本地系统权限，我先暂停执行。",
      statusLabel: "本地系统级操作",
    };
  }
  return { required: false, scope: "", promptLabel: "", statusLabel: "" };
}

function resolveExecutionProfileForMessage(normalized, options = {}) {
  const explicit = String(options.executionProfile || "").trim();
  if (explicit) {
    return explicit;
  }
  const text = String(normalized?.text || "").trim();
  if (isLocalExtensionRequest(text)) {
    return "feishu-local-extend";
  }
  if (FEISHU_OBJECT_OPERATION_PATTERNS.some((pattern) => pattern.test(text))) {
    return "feishu-object-op";
  }
  return "feishu";
}

function approvedExecutionProfileForScope(scope) {
  const normalized = String(scope || "").trim();
  if (normalized === "feishu_local_system_execution") {
    return "feishu-local-system-approved";
  }
  return "feishu-approved";
}

function approvedExecutionProfileForItem(item) {
  const metadata = item && typeof item.metadata === "object" ? item.metadata : {};
  const profile = String(metadata.approved_execution_profile || "").trim();
  if (profile) {
    return profile;
  }
  return approvedExecutionProfileForScope(item?.scope || "");
}

function createApprovalToken() {
  return `coco-${crypto.randomUUID().split("-")[0]}`;
}

function approvalTokenExpiresAt() {
  return new Date(Date.now() + APPROVAL_TOKEN_TTL_SECONDS * 1000).toISOString();
}

function approvalTokenExpired(item) {
  const expiresAt = normalizeEventTimestamp(item?.expires_at || "");
  if (!expiresAt) return false;
  const parsed = Date.parse(expiresAt);
  return Number.isFinite(parsed) && parsed <= Date.now();
}

function approvalMismatchReason(item, normalized) {
  const metadata = item?.metadata || {};
  const tokenChatId = String(metadata.chat_id || "").trim();
  const currentChatId = String(normalized?.chat_id || "").trim();
  if (tokenChatId && currentChatId && tokenChatId !== currentChatId) {
    return "thread";
  }
  const tokenOpenId = String(metadata.open_id || "").trim();
  const currentOpenId = String(normalized?.open_id || "").trim();
  if (tokenOpenId && currentOpenId && tokenOpenId !== currentOpenId) {
    return "actor";
  }
  const tokenUserId = String(metadata.user_id || "").trim();
  const currentUserId = String(normalized?.user_id || "").trim();
  if (!tokenOpenId && tokenUserId && currentUserId && tokenUserId !== currentUserId) {
    return "actor";
  }
  return "";
}

function shouldRunInBackground(normalized) {
  const text = String(normalized?.text || "");
  if (!text) return false;
  if (classifyApprovalRequirement(text).required) {
    return false;
  }
  if (STATUS_QUERY_PATTERNS.some((pattern) => pattern.test(text))) {
    return false;
  }
  if (isLocalExtensionRequest(text)) {
    return false;
  }
  if (FEISHU_OBJECT_OPERATION_PATTERNS.some((pattern) => pattern.test(text))) {
    return false;
  }
  return BACKGROUND_REQUEST_PATTERN.test(text);
}

function shouldAcceptMessage(settings, normalized) {
  if (!normalized.text) {
    return { ok: false, reason: "empty_text" };
  }
  if (settings.allowed_users.length) {
    const senderRef = normalized.open_id || normalized.user_id;
    if (!settings.allowed_users.includes(senderRef)) {
      return { ok: false, reason: "sender_not_allowed" };
    }
  }
  if (normalized.chat_type === "group" && settings.group_policy === "mentions_only" && settings.require_mention) {
    const mentionCount = Number(normalized.mentions?.length || 0) + Number(normalized.text_mentions?.length || 0);
    if (!mentionCount) {
      return { ok: false, reason: "mention_required" };
    }
  }
  return { ok: true, reason: "" };
}

function createFeishuLongConnectionService({
  brokerClient,
  runtimeState,
  sdkLoader = tryLoadFeishuSdk,
  logger = console,
} = {}) {
  if (!brokerClient || typeof brokerClient.call !== "function") {
    throw new Error("brokerClient.call is required");
  }
  const runtime = runtimeState || {
    saveBridgeStatus: async () => ({}),
    saveBridgeSettings: async () => ({}),
  };
  let settings = { ...DEFAULT_SETTINGS };
  let sdk = null;
  let client = null;
  let wsClient = null;
  let eventDispatcher = null;
  let gateway = null;
  let cardStreamController = null;
  let heartbeatTimer = null;
  const processedMessageIds = new Map();
  const pendingFollowups = new Map();
  const conversationQueueTails = new Map();
  const activeStreamCards = new Map();
  let cachedUserProfile = null;
  let cachedUserProfileAt = 0;
  let status = {
    bridge: "feishu",
    host_mode: "electron",
    transport: "sdk_websocket_plus_rest",
    connection_status: "disconnected",
    last_error: "",
    last_event_at: "",
    connected_at: "",
    recent_message_count: 0,
    recent_reply_count: 0,
    last_message_preview: "",
    last_sender_ref: "",
    heartbeat_at: "",
    stale_after_seconds: STALE_AFTER_SECONDS,
    event_idle_after_seconds: EVENT_IDLE_AFTER_SECONDS_IDLE,
    last_delivery_at: "",
    last_delivery_phase: "",
    last_binding_result: "",
    last_binding_chat_ref: "",
    last_binding_project: "",
    last_binding_topic: "",
    last_execution_state: "",
    pending_ack_at: "",
  };

  async function persistStatus(nextStatus) {
    status = { ...status, ...nextStatus };
    await runtime.saveBridgeStatus(status);
    return status;
  }

  async function markExecutionStarted({ ackPending = false } = {}) {
    const nextStatus = {
      last_execution_state: "running",
      heartbeat_at: new Date().toISOString(),
      event_idle_after_seconds: ACTIVE_EXECUTION_EVENT_IDLE_SECONDS,
      pending_ack_at: ackPending ? status.pending_ack_at || new Date().toISOString() : "",
    };
    await persistStatus(nextStatus);
    return nextStatus;
  }

  function createExecutionControl() {
    let tracked = Promise.resolve();
    return {
      extend(promise) {
        tracked = Promise.resolve(promise).catch(() => {});
      },
      wait() {
        return tracked;
      },
    };
  }

  async function withConversationQueue(normalized, runner) {
    const conversationKey = getConversationKey(normalized);
    if (!conversationKey) {
      const control = createExecutionControl();
      const result = await runner(control);
      await control.wait();
      return result;
    }
    const previousTail = conversationQueueTails.get(conversationKey) || Promise.resolve();
    let releaseCurrent;
    const currentDone = new Promise((resolve) => {
      releaseCurrent = resolve;
    });
    const chainedTail = previousTail.catch(() => {}).then(() => currentDone);
    conversationQueueTails.set(conversationKey, chainedTail);
    await previousTail.catch(() => {});

    const control = createExecutionControl();
    const releaseQueue = () => {
      releaseCurrent();
      queueMicrotask(() => {
        if (conversationQueueTails.get(conversationKey) === chainedTail) {
          conversationQueueTails.delete(conversationKey);
        }
      });
    };

    try {
      const result = await runner(control);
      control
        .wait()
        .catch(() => {})
        .finally(releaseQueue);
      return result;
    } catch (error) {
      control
        .wait()
        .catch(() => {})
        .finally(releaseQueue);
      throw error;
    }
  }

  function clearHeartbeat() {
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
  }

  function startHeartbeat() {
    clearHeartbeat();
    heartbeatTimer = setInterval(() => {
      void persistStatus({
        heartbeat_at: new Date().toISOString(),
        stale_after_seconds: STALE_AFTER_SECONDS,
      });
    }, HEARTBEAT_INTERVAL_MS);
    if (heartbeatTimer && typeof heartbeatTimer.unref === "function") {
      heartbeatTimer.unref();
    }
  }

  async function loadSettings(nextSettings) {
    settings = sanitizeSettings(nextSettings || settings);
    await runtime.saveBridgeSettings(settings);
    return { settings, settings_summary: summarizeSettings(settings) };
  }

  function describeBinding(binding) {
    if (!binding) return "";
    const projectName = String(binding.project_name || "").trim();
    const topicName = String(binding.topic_name || "").trim();
    if (projectName && topicName) {
      return `${projectName} / ${topicName}`;
    }
    return projectName || "";
  }

  function describeRouteContext(binding, routeContext = null) {
    const routeProject = String(routeContext?.project_name || "").trim();
    const routeTopic = String(routeContext?.topic_name || "").trim();
    if (routeProject && routeTopic) {
      return `${routeProject} / ${routeTopic}`;
    }
    if (routeProject) {
      return routeProject;
    }
    return describeBinding(binding);
  }

  async function buildApprovalPromptReply(approval, binding, routeContext = null) {
    const threadLabel = describeRouteContext(binding, routeContext);
    const promptLabel =
      String(approval?.metadata?.approval_prompt_label || "").trim() || "这条命令需要授权后才能继续，我先暂停执行。";
    const lines = [promptLabel];
    if (threadLabel) {
      lines.push(`当前线程：${threadLabel}`);
    }
    lines.push(`如果确认执行，请回复：/approve ${approval.token}`);
    lines.push(`如果取消，请回复：/deny ${approval.token}`);
    return addressReply(lines.join("\n"));
  }

  function buildApprovalCardPayload(approval, binding) {
    const threadLabel = describeBinding(binding);
    const token = String(approval?.token || "").trim();
    const promptLabel =
      String(approval?.metadata?.approval_prompt_label || "").trim() || "这条命令需要授权后才能继续，我先暂停执行。";
    const lines = [promptLabel];
    if (threadLabel) {
      lines.push(`当前线程：${threadLabel}`);
    }
    lines.push("请直接点击下方按钮确认，或使用文本命令继续。");
    lines.push(`批准命令：/approve ${token}`);
    lines.push(`拒绝命令：/deny ${token}`);
    return JSON.stringify({
      schema: "2.0",
      config: {
        wide_screen_mode: true,
      },
      header: {
        title: {
          tag: "plain_text",
          content: "CoCo 授权确认",
        },
        template: "orange",
      },
      body: {
        elements: [
          {
            tag: "markdown",
            content: lines.join("\n"),
          },
          {
            tag: "column_set",
            flex_mode: "none",
            horizontal_align: "left",
            columns: [
              {
                tag: "column",
                width: "auto",
                elements: [
                  {
                    tag: "button",
                    text: { tag: "plain_text", content: "批准执行" },
                    type: "primary",
                    size: "medium",
                    value: {
                      callback_data: `${APPROVAL_CARD_ACTION_PREFIX}:allow:${token}`,
                      chat_id: String(approval?.metadata?.chat_id || "").trim(),
                      source_message_id: String(approval?.metadata?.source_message_id || "").trim(),
                    },
                  },
                ],
              },
              {
                tag: "column",
                width: "auto",
                elements: [
                  {
                    tag: "button",
                    text: { tag: "plain_text", content: "拒绝执行" },
                    type: "danger",
                    size: "medium",
                    value: {
                      callback_data: `${APPROVAL_CARD_ACTION_PREFIX}:deny:${token}`,
                      chat_id: String(approval?.metadata?.chat_id || "").trim(),
                      source_message_id: String(approval?.metadata?.source_message_id || "").trim(),
                    },
                  },
                ],
              },
            ],
          },
          {
            tag: "markdown",
            content: APPROVAL_CARD_FALLBACK_HINT,
            text_size: "notation",
          },
        ],
      },
    });
  }

  function buildApprovalCardResolutionPayload(item, binding, resolution) {
    const threadLabel = describeBinding(binding || item);
    const statusText = {
      approved: "已批准，正在执行。",
      denied: "已拒绝，本次不会执行。",
      expired: "授权已过期，请重新发起命令。",
      approved_again: "该授权已经批准，无需重复确认。",
      denied_again: "该授权已经拒绝，无需重复操作。",
    }[resolution] || "已处理。";
    const headerTemplate = {
      approved: "green",
      approved_again: "green",
      denied: "red",
      denied_again: "red",
      expired: "grey",
    }[resolution] || "wathet";
    const lines = [statusText];
    if (threadLabel) {
      lines.push(`当前线程：${threadLabel}`);
    }
    const requestedText = String(item?.metadata?.requested_text || "").trim();
    if (requestedText) {
      lines.push(`原始命令：${requestedText}`);
    }
    const actor =
      String(item?.metadata?.approved_by || item?.metadata?.denied_by || "").trim();
    const actionAt =
      String(item?.metadata?.approved_at || item?.metadata?.denied_at || item?.metadata?.expired_at || "").trim();
    if (actor || actionAt) {
      const extras = [];
      if (actor) extras.push(`操作人：${actor}`);
      if (actionAt) extras.push(`时间：${actionAt}`);
      lines.push(extras.join(" | "));
    }
    lines.push(APPROVAL_CARD_FALLBACK_HINT);
    return {
      toast: {
        type: resolution === "denied" || resolution === "denied_again" || resolution === "expired" ? "info" : "success",
        content: statusText,
      },
      card: {
        type: "raw",
        data: {
          schema: "2.0",
          config: {
            wide_screen_mode: true,
          },
          header: {
            title: {
              tag: "plain_text",
              content: "CoCo 授权状态",
            },
            template: headerTemplate,
          },
          body: {
            elements: [
              {
                tag: "markdown",
                content: lines.join("\n"),
              },
            ],
          },
        },
      },
    };
  }

  async function buildApprovalConfirmedReply(binding, routeContext = null) {
    const threadLabel = describeRouteContext(binding, routeContext);
    if (!threadLabel) {
      return addressReply("已记录授权，开始执行。");
    }
    return addressReply(`已记录授权，开始执行。\n当前线程：${threadLabel}`);
  }

  async function getUserProfile() {
    const now = Date.now();
    if (cachedUserProfile && now - cachedUserProfileAt < 60_000) {
      return cachedUserProfile;
    }
    try {
      const payload = await brokerClient.call("user-profile", {});
      const profile = payload?.profile || payload?.data?.profile || null;
      cachedUserProfile = profile || null;
      cachedUserProfileAt = now;
      return cachedUserProfile;
    } catch (_error) {
      return cachedUserProfile;
    }
  }

  async function addressReply(text) {
    const profile = await getUserProfile();
    const preferredName = String(profile?.preferred_name || "").trim();
    return formatAddressedReply(preferredName, text);
  }

  async function connect() {
    if (!settings.app_id || !settings.app_secret) {
      await persistStatus({ connection_status: "blocked", last_error: "missing_app_credentials" });
      return { ok: false, reason: "missing_app_credentials", status };
    }
    const previousStatus = { ...status };
    sdk = sdkLoader();
    if (!sdk || !sdk.Client || !sdk.WSClient || !sdk.EventDispatcher) {
      await persistStatus({ connection_status: "blocked", last_error: "sdk_unavailable" });
      return { ok: false, reason: "sdk_unavailable", status };
    }

    const sdkDomain = normalizeSdkDomain(sdk, settings.domain);

    gateway = new FeishuGateway({
      sdk,
      settings: {
        ...settings,
        sdk_domain: sdkDomain,
      },
      logger,
    });
    gateway.registerMessageHandler(async (event) => {
        try {
          const handled = await processMessageEvent(event);
          if (!handled.ok) {
            return;
          }
          if (handled.replyPayload?.kind === "approval_prompt") {
            await sendApprovalPrompt({
              normalized: handled.normalized,
              approval: handled.replyPayload.approval,
              binding: handled.replyPayload.binding,
              deliveryNotice: handled.replyPayload.deliveryNotice || "",
            });
            return;
          }
          await sendReply({
            chatId: handled.normalized.chat_id,
            openId: handled.normalized.open_id,
            text: handled.replyPreview,
            sourceMessageId: handled.normalized.message_id,
            phase: handled.replyPhase || (handled.direct ? "direct" : "reply"),
          });
        } catch (error) {
          await persistStatus({
            connection_status: "error",
            last_error: String(error?.message || error || "feishu_event_handler_failed"),
          });
        }
    });
    gateway.registerCardActionHandler(async (event) => {
      try {
        return await handleCardActionEvent(event);
      } catch (error) {
        await persistStatus({
          connection_status: "error",
          last_error: String(error?.message || error || "feishu_card_action_failed"),
        });
        return {
          toast: {
            type: "failed",
            content: "授权处理失败，请稍后重试。",
          },
        };
      }
    });
    await gateway.start();
    client = gateway.getRestClient();
    eventDispatcher = gateway.getEventDispatcher();
    wsClient = null;
    cardStreamController = client
      ? createCardStreamController(client, { throttleMs: 200, footer: { status: true, elapsed: true } })
      : null;
    await persistStatus({
      connection_status: "connected",
      last_error: "",
      host_mode: "electron",
      connected_at: new Date().toISOString(),
      heartbeat_at: new Date().toISOString(),
      stale_after_seconds: STALE_AFTER_SECONDS,
      event_idle_after_seconds: EVENT_IDLE_AFTER_SECONDS_IDLE,
      last_event_at: previousStatus.last_event_at || "",
      recent_message_count: Number(previousStatus.recent_message_count || 0),
      recent_reply_count: Number(previousStatus.recent_reply_count || 0),
      last_message_preview: previousStatus.last_message_preview || "",
      last_sender_ref: previousStatus.last_sender_ref || "",
      settings_summary: summarizeSettings(settings),
    });
    startHeartbeat();
    void recoverPendingConversations();
    return { ok: true, status };
  }

  async function disconnect() {
    clearHeartbeat();
    if (gateway) {
      await gateway.stop();
    }
    if (wsClient && typeof wsClient.close === "function") {
      await wsClient.close({ force: true });
    }
    gateway = null;
    wsClient = null;
    client = null;
    eventDispatcher = null;
    cardStreamController = null;
    await persistStatus({ connection_status: "disconnected", heartbeat_at: "" });
    return { ok: true, status };
  }

  async function reconnect() {
    await disconnect();
    return connect();
  }

  async function getPersistedBinding(conversationKey) {
    return readPersistedBinding(brokerClient, conversationKey);
  }

  async function persistBinding(conversationKey, nextBinding, existingBinding = null) {
    const previous = existingBinding || (await getPersistedBinding(conversationKey)) || {};
    return writePersistedBinding(brokerClient, conversationKey, {
      binding_scope: nextBinding.binding_scope || previous.binding_scope || (nextBinding.topic_name ? "topic" : "project"),
      project_name: nextBinding.project_name || previous.project_name || "",
      topic_name: nextBinding.topic_name != null ? nextBinding.topic_name : previous.topic_name || "",
      session_id: nextBinding.session_id != null ? nextBinding.session_id : previous.session_id || "",
      metadata: {
        ...(previous.metadata || {}),
        ...(nextBinding.metadata || {}),
      },
    });
  }

  async function fetchApprovalTokenRecord(token) {
    const payload = await brokerClient.call("approval-token", {
      token,
    });
    return payload?.item || payload?.data?.item || payload?.approval_token || payload || null;
  }

  async function saveApprovalTokenRecord(token, nextToken) {
    const payload = await brokerClient.call("approval-token", {
      token,
      token_json: nextToken,
    });
    return payload?.item || payload?.data?.item || payload?.approval_token || payload || null;
  }

  async function createPendingApproval(normalized, binding = null, routeContext = null, requirement = null) {
    const token = createApprovalToken();
    const currentBinding = binding || (await getPersistedBinding(getConversationKey(normalized)));
    const resolvedRoute = routeContext || resolveMessageRouteContext(normalized, currentBinding);
    const resolvedRequirement = requirement || classifyApprovalRequirement(normalized.text);
    const item = await saveApprovalTokenRecord(token, {
      scope: resolvedRequirement.scope || "feishu_high_risk_execution",
      status: "pending",
      project_name: String(resolvedRoute?.project_name || currentBinding?.project_name || "").trim(),
      session_id: String(currentBinding?.session_id || "").trim(),
      expires_at: approvalTokenExpiresAt(),
      metadata: {
        requested_text: normalized.text,
        chat_id: normalized.chat_id || "",
        chat_type: normalized.chat_type || "",
        open_id: normalized.open_id || "",
        user_id: normalized.user_id || "",
        source_message_id: normalized.message_id || "",
        topic_name: String(resolvedRoute?.topic_name || currentBinding?.topic_name || "").trim(),
        route_source: String(resolvedRoute?.route_source || "").trim(),
        approval_prompt_label: resolvedRequirement.promptLabel || "",
        approval_status_label: resolvedRequirement.statusLabel || "",
        approved_execution_profile: approvedExecutionProfileForScope(resolvedRequirement.scope || ""),
      },
    });
    return item;
  }

  async function resolveApprovalCommand(normalized, explicitIntent = null) {
    const intent = explicitIntent || matchApprovalCommand(normalized.text);
    if (intent.kind === "none") {
      return null;
    }
    let approvalToken = String(intent.token || "").trim();
    if (!approvalToken) {
      const conversation = await getConversationSnapshot(getConversationKey(normalized));
      approvalToken = String(conversation?.pending_approval_token || "").trim();
      if (!approvalToken) {
        return {
          ok: true,
          normalized,
          direct: true,
          replyPhase: "approval_missing",
          replyPreview: await addressReply("当前线程没有待授权项。若我识别到需要授权的动作，会先返回 token。"),
        };
      }
    }
    const item = await fetchApprovalTokenRecord(approvalToken);
    if (!item || !item.token || !item.status) {
      return {
        ok: true,
        normalized,
        direct: true,
        replyPhase: "approval_missing",
        replyPreview: await addressReply(`没有找到授权 token \`${approvalToken}\`。请先重新发起需要授权的命令。`),
      };
    }
    const metadata = { ...(item.metadata || {}) };
    const mismatchReason = approvalMismatchReason(item, normalized);
    if (mismatchReason === "thread") {
      return {
        ok: true,
        normalized,
        direct: true,
        replyPhase: "approval_forbidden",
        replyPreview: await addressReply("这个授权 token 不属于当前线程，请回到原来的聊天继续授权。"),
      };
    }
    if (mismatchReason === "actor") {
      return {
        ok: true,
        normalized,
        direct: true,
        replyPhase: "approval_forbidden",
        replyPreview: await addressReply("只有发起这条需要授权命令的用户才能在当前线程里批准或拒绝它。"),
      };
    }
    const approvalMessageId = String(metadata.approval_message_id || "").trim();
    const callbackMessageId = String(normalized.callback_message_id || "").trim();
    const sourceMessageId = String(metadata.source_message_id || "").trim();
    if (intent.via === "card" && approvalMessageId && callbackMessageId && approvalMessageId !== callbackMessageId) {
      const isLegacyApprovalCard = sourceMessageId && callbackMessageId === sourceMessageId;
      if (!isLegacyApprovalCard) {
        return {
          ok: true,
          normalized,
          direct: true,
          replyPhase: "approval_forbidden",
          replyPreview: await addressReply("这个授权按钮不属于当前审批卡片，请回到原始授权消息继续操作。"),
        };
      }
    }
    if (item.status === "expired" || (item.status === "pending" && approvalTokenExpired(item))) {
      if (item.status !== "expired") {
        await saveApprovalTokenRecord(approvalToken, {
          scope: item.scope,
          status: "expired",
          project_name: item.project_name || "",
          session_id: item.session_id || "",
          expires_at: item.expires_at || "",
          metadata: {
            ...metadata,
            expired_at: new Date().toISOString(),
          },
        });
      }
      return {
        ok: true,
        normalized,
        direct: true,
        replyPhase: "approval_expired",
        replyPreview: await addressReply(`授权 \`${approvalToken}\` 已过期，请重新发送需要授权的命令。`),
      };
    }
    if (intent.kind === "deny") {
      await saveApprovalTokenRecord(approvalToken, {
        scope: item.scope,
        status: "denied",
        project_name: item.project_name || "",
        session_id: item.session_id || "",
        expires_at: item.expires_at || "",
        metadata: {
          ...metadata,
          denied_by: normalized.open_id || normalized.user_id || "",
          denied_at: new Date().toISOString(),
        },
      });
      return {
        ok: true,
        normalized,
        direct: true,
        replyPhase: "approval_denied",
        replyPreview: await addressReply(`已拒绝授权 \`${approvalToken}\`，我不会执行这条需要授权的命令。`),
      };
    }
    if (item.status === "denied") {
      return {
        ok: true,
        normalized,
        direct: true,
        replyPhase: "approval_denied",
        replyPreview: await addressReply(`授权 \`${approvalToken}\` 已经被拒绝，当前不会执行。`),
      };
    }
    if (item.status === "approved") {
      return {
        ok: true,
        normalized,
        direct: true,
        replyPhase: "approval_already_recorded",
        replyPreview: await addressReply(`授权 \`${approvalToken}\` 已经记录为 approved，不需要重复确认。`),
      };
    }
    await saveApprovalTokenRecord(approvalToken, {
      scope: item.scope,
      status: "approved",
      project_name: item.project_name || "",
      session_id: item.session_id || "",
      expires_at: item.expires_at || "",
      metadata: {
        ...metadata,
        approval_mode: intent.mode || metadata.approval_mode || "text",
        approved_by: normalized.open_id || normalized.user_id || "",
        approved_at: new Date().toISOString(),
      },
    });
    const requestedText = String(metadata.requested_text || "").trim();
    if (!requestedText) {
      return {
        ok: true,
        normalized,
        direct: true,
        replyPhase: "approval_missing_request",
        replyPreview: await addressReply(`已记录授权 \`${approvalToken}\`，但没有找到原始命令文本，请重新发送命令。`),
      };
    }
    return {
      ok: true,
      normalized: {
        ...normalized,
        text: requestedText,
        source_approval_token: approvalToken,
      },
      approvalItem: item,
      approvalToken,
      executeApproved: true,
      replyPhase: "approval_confirmed",
    };
  }

  async function tryBindChat(normalized, existingBinding = null) {
    const conversationKey = getConversationKey(normalized);
    if (!conversationKey) {
      return null;
    }
    const currentBinding = existingBinding || (await getPersistedBinding(conversationKey));
    const binding = buildBindingCandidate(normalized, currentBinding);
    if (!binding) {
      return null;
    }
    if (binding.error) {
      return {
        ok: false,
        brokerPayload: {
          error: binding.error,
          declared_target: binding.declared_target || "",
        },
        error: binding.error,
      };
    }
    try {
      const persisted = await persistBinding(
        conversationKey,
        {
          binding_scope: binding.topic_name ? "topic" : "project",
          project_name: binding.project_name,
          topic_name: binding.topic_name || "",
          session_id: "",
          metadata: {
            binding_source: "chat_declaration",
            declared_text: normalized.text,
            chat_type: normalized.chat_type || "",
          },
        },
        currentBinding,
      );
      return {
        ok: true,
        binding: persisted || {
          chat_ref: conversationKey,
          binding_scope: binding.topic_name ? "topic" : "project",
          project_name: binding.project_name,
          topic_name: binding.topic_name || "",
          session_id: currentBinding?.session_id || "",
        },
      };
    } catch (error) {
      return {
        ok: false,
        brokerPayload: error?.brokerPayload || null,
        error: String(error?.message || error || "binding_rejected"),
      };
    }
  }

  async function getBindingForConversationKey(conversationKey) {
    return getPersistedBinding(conversationKey);
  }

  function bindingMatchesRouteContext(binding, routeContext) {
    const bindingSessionId = String(binding?.session_id || "").trim();
    const bindingProject = String(binding?.project_name || "").trim();
    const bindingTopic = String(binding?.topic_name || "").trim();
    const routeProject = String(routeContext?.project_name || "").trim();
    const routeTopic = String(routeContext?.topic_name || "").trim();
    if (!bindingSessionId) {
      return false;
    }
    if (!routeProject && !routeTopic) {
      return true;
    }
    if (!bindingProject) {
      return false;
    }
    return bindingProject === routeProject && bindingTopic === routeTopic;
  }

  async function routeMessage(normalized, options = {}) {
    const gate = shouldAcceptMessage(settings, normalized);
    if (!gate.ok) {
      if (!SILENT_GATE_REASONS.has(gate.reason)) {
        await persistStatus({ last_error: gate.reason });
      }
      return { ok: false, reason: gate.reason, normalized };
    }
    const conversationKey = getConversationKey(normalized);
    const binding = await getPersistedBinding(conversationKey);
    const routeContext = resolveMessageRouteContext(normalized, binding);
    const threadIdentity = resolveSourceThreadIdentity(
      normalized,
      routeContext,
      binding,
      conversationKey,
    );
    const activeSessionId = bindingMatchesRouteContext(binding, routeContext)
      ? String(binding?.session_id || "").trim()
      : "";
    const projectName = routeContext.project_name;
    const topicName = routeContext.topic_name;
    const executionProfile = resolveExecutionProfileForMessage(normalized, options);
    const selectedModel = String(options.model || "").trim();
    const selectedReasoningEffort = String(
      options.reasoningEffort || options.reasoning_effort || "",
    ).trim();
    const commonPayload = {
      prompt: normalized.text,
      project_name: projectName,
      execution_profile: executionProfile,
    };
    const approvalToken = String(normalized?.source_approval_token || "").trim();
    if (approvalToken) {
      commonPayload.approval_token = approvalToken;
    }
    if (selectedModel) {
      commonPayload.model = selectedModel;
    }
    if (selectedReasoningEffort) {
      commonPayload.reasoning_effort = selectedReasoningEffort;
    }
    if (topicName) {
      commonPayload.topic_name = topicName;
    }
    const brokerPayload = activeSessionId
      ? await brokerClient.call("codex-resume", {
          ...commonPayload,
          session_id: activeSessionId,
          source: "feishu",
          chat_ref: conversationKey,
          thread_name: threadIdentity.threadName,
          thread_label: threadIdentity.threadLabel,
          source_message_id: normalized.message_id,
        })
      : await brokerClient.call("codex-exec", {
          ...commonPayload,
          session_id: normalized.message_id,
          no_auto_resume: Boolean(conversationKey),
          source: "feishu",
          chat_ref: conversationKey,
          thread_name: threadIdentity.threadName,
          thread_label: threadIdentity.threadLabel,
          source_message_id: normalized.message_id,
        });
    const discoveredSessionId = extractSessionId(brokerPayload?.stderr || "") || extractSessionId(brokerPayload?.stdout || "");
    const nextSessionId = discoveredSessionId || activeSessionId;
    if (conversationKey && nextSessionId) {
      const nextProjectName = routeContext.project_name || binding?.project_name || "";
      const nextTopicName = routeContext.project_name
        ? routeContext.topic_name || ""
        : binding?.topic_name || "";
      await persistBinding(
        conversationKey,
        {
          binding_scope:
            nextProjectName
              ? nextTopicName
                ? "topic"
                : "project"
              : binding?.binding_scope || "chat",
          project_name: nextProjectName,
          topic_name: nextTopicName,
          session_id: nextSessionId,
          metadata: {
            last_message_id: normalized.message_id || "",
            last_sender_ref: normalized.open_id || normalized.user_id || "",
            last_route_source: routeContext.route_source || "",
          },
        },
        binding,
      );
    }
    await persistStatus({ last_error: "" });
    return {
      ok: true,
      normalized,
      brokerPayload,
      conversationKey,
      sessionId: nextSessionId,
      routeContext,
      binding,
    };
  }

  async function getConversationSnapshot(chatRef) {
    if (!chatRef) return null;
    const payload = await brokerClient.call("bridge-conversations", {
      bridge: BRIDGE_NAME,
      limit: 50,
    });
    const rows = Array.isArray(payload?.rows) ? payload.rows : [];
    return rows.find((row) => String(row?.chat_ref || "").trim() === chatRef) || null;
  }

  async function fetchMaterialSuggestions(routeContext, prompt) {
    const projectName = String(routeContext?.project_name || "").trim();
    if (!projectName) {
      return null;
    }
    try {
      const payload = await brokerClient.call("material-suggest", {
        project_name: projectName,
        prompt: String(prompt || "").trim(),
      });
      return payload?.ok === false ? null : payload;
    } catch (_error) {
      return null;
    }
  }

  async function buildBrokerReplyPreview(normalized, routed) {
    const baseReply = await addressReply(renderReplyText(routed?.brokerPayload, normalized?.text));
    if (!shouldAttachMaterialHints(normalized, routed?.routeContext, routed?.brokerPayload)) {
      return baseReply;
    }
    const materialPayload = await fetchMaterialSuggestions(routed?.routeContext, normalized?.text);
    const materialBlock = buildMaterialHintBlock(materialPayload);
    if (!materialBlock) {
      return baseReply;
    }
    return `${baseReply}\n\n${materialBlock}`;
  }

  async function directRoute(normalized) {
    const conversationKey = getConversationKey(normalized);
    const binding = await getPersistedBinding(conversationKey);
    const routeContext = resolveMessageRouteContext(normalized, binding);
    const scopedProjectName = String(routeContext.project_name || "").trim();
    const intent = classifyDirectIntent(normalized);
    if (intent.kind === "none" || intent.kind === "empty") {
      return { ok: false, reason: "no_direct_route", normalized };
    }
    if (intent.kind === "approval_status") {
      const conversation = await getConversationSnapshot(conversationKey);
      const token = String(conversation?.pending_approval_token || "").trim();
      const action = trimReplyText(String(conversation?.pending_approval_action || "").trim(), 180);
      if (!token) {
        return {
          ok: true,
          normalized,
          direct: true,
          replyPhase: "approval_status",
          replyPreview: await addressReply([
            "当前没有待授权项。",
            "如果我识别到需要授权的动作，会先回 token，再等你发 `/approve <token>`。",
          ].join("\n")),
        };
      }
      return {
        ok: true,
        normalized,
        direct: true,
        replyPhase: "approval_status",
        replyPreview: await addressReply([
          `当前待授权 token：${token}`,
          action ? `待执行动作：${action}` : "待执行动作：需要授权的操作",
          `如确认执行，请回复：/approve ${token}`,
        ].join("\n")),
      };
    }
    if (intent.kind === "thread_status") {
      const conversation = await getConversationSnapshot(conversationKey);
      if (!conversation) {
        return {
          ok: true,
          normalized,
          direct: true,
          replyPhase: "thread_status",
          replyPreview: await addressReply("我还没找到这条聊天的线程状态。你先发一条任务，我就能开始记录。"),
        };
      };
      const lines = [
        `当前线程：${conversation.thread_label || conversation.binding_label || conversation.chat_ref || "未命名线程"}`,
        `执行状态：${conversation.execution_state || "idle"}`,
      ];
      if (conversation.last_user_request) {
        lines.push(`最近请求：${trimReplyText(conversation.last_user_request, 160)}`);
      }
      if (conversation.last_report) {
        lines.push(`最近汇报：${trimReplyText(conversation.last_report, 160)}`);
      }
      if (conversation.pending_approval_token) {
        lines.push(`待授权：${conversation.pending_approval_token}`);
      }
      if (conversation.last_error) {
        lines.push(`最近异常：${summarizeErrorText(conversation.last_error)}`);
      }
      return {
        ok: true,
        normalized,
        direct: true,
        replyPhase: "thread_status",
        replyPreview: await addressReply(lines.join("\n")),
      };
    }
    if (intent.kind === "system_status") {
      const [overviewPayload, healthPayload] = await Promise.all([
        brokerClient.call("panel", { name: "overview" }),
        brokerClient.call("health", {}),
      ]);
      const cards = Array.isArray(overviewPayload?.cards) ? overviewPayload.cards : [];
      const health = healthPayload?.payload || {};
      const cardLines = formatBulletLines(
        cards,
        (item) => `${item.label}: ${item.value}`,
        "- 暂无 overview 卡片",
      );
      return {
        ok: true,
        normalized,
        direct: true,
        replyPhase: "status",
        replyPreview: await addressReply([
          "当前系统状态：",
          cardLines,
          `- open_alert_count: ${health.open_alert_count ?? 0}`,
          `- latest_report: ${health.latest_report || "未记录"}`,
        ].join("\n")),
      };
    }
    return { ok: false, reason: "no_direct_route", normalized };
  }

  async function shouldPromptForGroupBinding(normalized) {
    if (normalized.chat_type !== "group") {
      return { required: false, binding: null };
    }
    const conversationKey = getConversationKey(normalized);
    if (!conversationKey) {
      return { required: false, binding: null };
    }
    const binding = await getPersistedBinding(conversationKey);
    if (binding?.project_name) {
      return { required: false, binding };
    }
    return { required: false, binding };
  }

  async function buildGroupBindingPrompt() {
    return addressReply(
      [
        "如果你希望这个群默认聚焦某个项目，可以告诉我：",
        "- 这个群只聊 SampleProj",
        "- 这个群只聊 Codex Hub 前端",
        "- 这个群只聊 示例交付 展馆线",
        "不声明也可以，我会优先根据你当前消息里提到的项目来路由上下文。",
      ].join("\n"),
    );
  }

  async function recordInboundMessage(normalized) {
    if (!normalized.message_id || !brokerClient || typeof brokerClient.call !== "function") {
      return { duplicate: false };
    }
    try {
      const payload = await brokerClient.call("record-bridge-message", {
        direction: "inbound",
        message_id: normalized.message_id,
        status: "received",
        session_id: normalized.message_id,
        payload: {
          chat_id: normalized.chat_id,
          chat_type: normalized.chat_type,
          open_id: normalized.open_id,
          user_id: normalized.user_id,
          text: normalized.text,
        },
      });
      const record = payload?.record || payload || {};
      return {
        duplicate: Boolean(record.created_at && record.updated_at && record.created_at !== record.updated_at),
      };
    } catch (_error) {
      return { duplicate: false };
    }
  }

  async function prepareMessage(normalized) {
    if (normalized.message_id) {
      if (processedMessageIds.has(normalized.message_id)) {
        return { ok: false, reason: "duplicate_message", normalized };
      }
      const persisted = await recordInboundMessage(normalized);
      if (persisted.duplicate) {
        processedMessageIds.set(normalized.message_id, Date.now());
        return { ok: false, reason: "duplicate_message", normalized };
      }
      processedMessageIds.set(normalized.message_id, Date.now());
      if (processedMessageIds.size > 200) {
        const oldestKey = processedMessageIds.keys().next().value;
        if (oldestKey) processedMessageIds.delete(oldestKey);
      }
    }
    const lastEventTimestamp = Date.parse(status.last_event_at || status.connected_at || "") || 0;
    const offlineDuration = lastEventTimestamp ? Date.now() - lastEventTimestamp : 0;
    const wasOffline = lastEventTimestamp && offlineDuration > STALE_AFTER_SECONDS * 1000;
    if (wasOffline) {
      normalized.offline_notice = `我刚刚恢复在线，补发了 ${Math.round(offlineDuration / 60000)} 分钟前的消息。`;
    }
    await persistStatus({
      last_event_at: new Date().toISOString(),
      recent_message_count: Number(status.recent_message_count || 0) + 1,
      last_message_preview: normalized.text.slice(0, 120),
      last_sender_ref: normalized.open_id || normalized.user_id || "",
      heartbeat_at: new Date().toISOString(),
      event_idle_after_seconds: EVENT_IDLE_AFTER_SECONDS,
    });
    return { ok: true, normalized };
  }

  async function recordOutboundMessage({ messageId, text, chatId, openId, sourceMessageId = "", phase = "" }) {
    if (!messageId || !brokerClient || typeof brokerClient.call !== "function") {
      return;
    }
    try {
      await brokerClient.call("record-bridge-message", {
        direction: "outbound",
        message_id: messageId,
        status: "sent",
        session_id: sourceMessageId || messageId,
        payload: {
          chat_id: chatId || "",
          reply_target: chatId || openId || "",
          reply_target_type: chatId ? "chat_id" : openId ? "open_id" : "",
          text,
          source_message_id: sourceMessageId,
          phase,
        },
      });
    } catch (_error) {
      // keep reply success path non-fatal if telemetry persistence fails
    }
  }

  async function recoverPendingConversations() {
    try {
      const payload = await brokerClient.call("bridge-conversations", {
        bridge: BRIDGE_NAME,
        limit: RECOVERY_SWEEP_LIMIT,
      });
      const rows = Array.isArray(payload?.rows) ? payload.rows : [];
      for (const row of rows) {
        if (!shouldRecoverConversation(row)) {
          continue;
        }
        await sendReply({
          chatId: String(row.chat_ref || "").trim(),
          openId: "",
          text: await addressReply(buildRecoverySweepReply(row)),
          sourceMessageId: "",
          phase: "report",
        });
      }
    } catch (error) {
      logger.warn?.("feishu recovery sweep skipped", error);
    }
  }

  async function sendApprovalPrompt({ normalized, approval, binding, deliveryNotice = "" }) {
    const routeContext = {
      project_name: String(approval?.project_name || "").trim(),
      topic_name: String(approval?.metadata?.topic_name || "").trim(),
    };
    const replyText = withDeliveryNotice(await buildApprovalPromptReply(approval, binding, routeContext), deliveryNotice);
    const receiveId = normalized.chat_id || normalized.open_id;
    const receiveIdType = normalized.chat_id ? "chat_id" : normalized.open_id ? "open_id" : "";
    if (!client || !receiveId || !receiveIdType || typeof client.im?.v1?.message?.create !== "function") {
      return sendReply({
        chatId: normalized.chat_id,
        openId: normalized.open_id,
        text: replyText,
        sourceMessageId: normalized.message_id,
        phase: "approval_prompt",
      });
    }
    try {
      const response = await sendInteractiveCardMessage(client, {
        chatId: receiveIdType === "chat_id" ? receiveId : "",
        openId: receiveIdType === "open_id" ? receiveId : "",
        card: buildApprovalCardPayload(approval, binding),
      });
      const replyMessageId = String(
        response?.messageId || `${normalized.message_id || receiveId}:approval:${Date.now()}`
      );
      const metadata = { ...(approval?.metadata || {}) };
      await saveApprovalTokenRecord(approval.token, {
        scope: approval.scope,
        status: approval.status,
        project_name: approval.project_name || "",
        session_id: approval.session_id || "",
        expires_at: approval.expires_at || "",
        metadata: {
          ...metadata,
          approval_delivery: "interactive_card",
          approval_message_id: replyMessageId,
        },
      });
      await recordOutboundMessage({
        messageId: replyMessageId,
        text: replyText,
        chatId: normalized.chat_id,
        openId: normalized.open_id,
        sourceMessageId: normalized.message_id,
        phase: "approval_prompt",
      });
      await persistStatus({
        recent_reply_count: Number(status.recent_reply_count || 0) + 1,
        last_error: "",
        heartbeat_at: new Date().toISOString(),
        last_delivery_at: new Date().toISOString(),
        last_delivery_phase: "approval_prompt",
        last_execution_state: "approval_pending",
        event_idle_after_seconds: EVENT_IDLE_AFTER_SECONDS_IDLE,
        pending_ack_at: "",
      });
      return { ok: true, kind: "interactive_card" };
    } catch (_error) {
      return sendReply({
        chatId: normalized.chat_id,
        openId: normalized.open_id,
        text: replyText,
        sourceMessageId: normalized.message_id,
        phase: "approval_prompt",
      });
    }
  }

  async function sendReply({ chatId, openId, text, sourceMessageId = "", phase = "" }) {
    if (!client || !text) {
      await persistStatus({ last_error: "reply_unavailable" });
      return { ok: false, reason: "reply_unavailable" };
    }
    if (typeof client.im?.v1?.message?.create !== "function") {
      await persistStatus({ last_error: "rest_client_unavailable" });
      return { ok: false, reason: "rest_client_unavailable" };
    }
    const receiveId = chatId || openId;
    const receiveIdType = chatId ? "chat_id" : openId ? "open_id" : "";
    if (!receiveId || !receiveIdType) {
      await persistStatus({ last_error: "missing_reply_target" });
      return { ok: false, reason: "missing_reply_target" };
    }
    const shapedText = shapeFeishuReplyText(text, phase);
    if (!shapedText) {
      return { ok: false, reason: "empty_message" };
    }
    const docRef = await createReplyDocIfNeeded(brokerClient, logger, {
      phase,
      text: shapedText,
    });
    if (
      sourceMessageId &&
      activeStreamCards.has(sourceMessageId) &&
      cardStreamController &&
      ["progress", "final", "reply", "report", "error"].includes(phase)
    ) {
      const streamMessageId = activeStreamCards.get(sourceMessageId);
      const mergedText = shapedText;
      if (phase === "progress") {
        await cardStreamController.update(streamMessageId, mergedText);
        await recordOutboundMessage({
          messageId: `${streamMessageId}:progress:${Date.now()}`,
          text: mergedText,
          chatId,
          openId,
          sourceMessageId,
          phase,
        });
      } else if (["final", "reply", "report", "error"].includes(phase)) {
        await cardStreamController.finalize(
          streamMessageId,
          mergedText,
          phase === "error" ? "error" : "completed",
          {
            docUrl: String(docRef?.url || "").trim(),
            docTitle: String(docRef?.title || "").trim(),
          },
        );
        activeStreamCards.delete(sourceMessageId);
        await recordOutboundMessage({
          messageId: `${streamMessageId}:final:${Date.now()}`,
          text: [mergedText, docRef?.url ? `完整文档：${docRef.url}` : ""].filter(Boolean).join("\n\n"),
          chatId,
          openId,
          sourceMessageId,
          phase,
        });
      }
      const nextStatus = {
        recent_reply_count: Number(status.recent_reply_count || 0) + 1,
        last_error: "",
        heartbeat_at: new Date().toISOString(),
        last_delivery_at: new Date().toISOString(),
        last_delivery_phase: String(phase || "report"),
        event_idle_after_seconds:
          phase === "progress"
            ? ACTIVE_EXECUTION_EVENT_IDLE_SECONDS
            : EVENT_IDLE_AFTER_SECONDS_IDLE,
        pending_ack_at: phase === "progress" ? status.pending_ack_at || new Date().toISOString() : "",
      };
      if (phase === "error") {
        nextStatus.last_execution_state = "failed";
      } else if (["final", "reply", "report"].includes(phase)) {
        nextStatus.last_execution_state = "reported";
      } else {
        nextStatus.last_execution_state = "running";
      }
      await persistStatus(nextStatus);
      if (["final", "reply", "report", "error"].includes(phase)) {
        clearFollowup(sourceMessageId);
      }
      return { ok: true, part_count: 1, mode: "card_stream" };
    }
    if (shouldUseInteractiveReply(phase)) {
      const card = buildInteractiveReplyCard(phase, shapedText, docRef);
      const response = await sendInteractiveCardMessage(client, {
        chatId,
        openId,
        card,
      });
      const replyMessageId = String(
        response?.messageId || `${sourceMessageId || receiveId}:reply:${Date.now()}`
      );
      await recordOutboundMessage({
        messageId: replyMessageId,
        text: [shapedText, docRef?.url ? `完整文档：${docRef.url}` : ""].filter(Boolean).join("\n\n"),
        chatId,
        openId,
        sourceMessageId,
        phase,
      });
      const nextStatus = {
        recent_reply_count: Number(status.recent_reply_count || 0) + 1,
        last_error: "",
        heartbeat_at: new Date().toISOString(),
        last_delivery_at: new Date().toISOString(),
        last_delivery_phase: String(phase || "report"),
      };
      if (phase === "approval_confirmed") {
        nextStatus.last_execution_state = "running";
        nextStatus.event_idle_after_seconds = ACTIVE_EXECUTION_EVENT_IDLE_SECONDS;
        nextStatus.pending_ack_at = "";
      } else if (phase === "approval_prompt") {
        nextStatus.last_execution_state = "approval_pending";
        nextStatus.event_idle_after_seconds = EVENT_IDLE_AFTER_SECONDS_IDLE;
        nextStatus.pending_ack_at = "";
      } else if (phase === "error") {
        nextStatus.last_execution_state = "failed";
        nextStatus.event_idle_after_seconds = EVENT_IDLE_AFTER_SECONDS_IDLE;
        nextStatus.pending_ack_at = "";
      } else {
        nextStatus.last_execution_state = "reported";
        nextStatus.event_idle_after_seconds = EVENT_IDLE_AFTER_SECONDS_IDLE;
        nextStatus.pending_ack_at = "";
      }
      await persistStatus(nextStatus);
      if (sourceMessageId && ["final", "reply", "report", "error"].includes(phase)) {
        clearFollowup(sourceMessageId);
      }
      return { ok: true, part_count: 1, mode: "interactive_card" };
    }
    const parts = splitReplyText(shapedText);
    if (!parts.length) {
      return { ok: false, reason: "empty_message" };
    }
    const messageType = phase === "ack" ? "text" : "post";
    for (const part of parts) {
      const response =
        messageType === "text"
          ? await sendTextMessage(client, { chatId, openId, text: part })
          : await sendPostMessage(client, { chatId, openId, text: part });
      const replyMessageId = String(
        response?.messageId || `${sourceMessageId || receiveId}:reply:${Date.now()}`
      );
      await recordOutboundMessage({
        messageId: replyMessageId,
        text: part,
        chatId,
        openId,
        sourceMessageId,
        phase,
      });
    }
    const nextStatus = {
      recent_reply_count: Number(status.recent_reply_count || 0) + parts.length,
      last_error: "",
      heartbeat_at: new Date().toISOString(),
      last_delivery_at: new Date().toISOString(),
      last_delivery_phase: String(phase || "report"),
    };
    if (phase === "ack") {
      nextStatus.last_execution_state = "running";
      nextStatus.event_idle_after_seconds = ACTIVE_EXECUTION_EVENT_IDLE_SECONDS;
      nextStatus.pending_ack_at = new Date().toISOString();
    } else if (phase === "approval_confirmed") {
      nextStatus.last_execution_state = "running";
      nextStatus.event_idle_after_seconds = ACTIVE_EXECUTION_EVENT_IDLE_SECONDS;
      nextStatus.pending_ack_at = "";
    } else if (phase === "progress") {
      nextStatus.last_execution_state = "running";
      nextStatus.event_idle_after_seconds = ACTIVE_EXECUTION_EVENT_IDLE_SECONDS;
      nextStatus.pending_ack_at = status.pending_ack_at || new Date().toISOString();
    } else if (phase === "approval_prompt") {
      nextStatus.last_execution_state = "approval_pending";
      nextStatus.event_idle_after_seconds = EVENT_IDLE_AFTER_SECONDS_IDLE;
      nextStatus.pending_ack_at = "";
    } else if (phase === "error") {
      nextStatus.last_execution_state = "failed";
      nextStatus.event_idle_after_seconds = EVENT_IDLE_AFTER_SECONDS_IDLE;
      nextStatus.pending_ack_at = "";
    } else if (phase === "final" || phase === "reply" || phase === "report") {
      nextStatus.last_execution_state = "reported";
      nextStatus.event_idle_after_seconds = EVENT_IDLE_AFTER_SECONDS_IDLE;
      nextStatus.pending_ack_at = "";
    }
    await persistStatus(nextStatus);
    if (sourceMessageId && ["final", "reply", "report", "error"].includes(phase)) {
      clearFollowup(sourceMessageId);
    }
    return { ok: true, part_count: parts.length };
  }

  async function sendMessage({ chatId = "", openId = "", text = "", phase = "report" } = {}) {
    const trimmed = trimReplyText(text);
    if (!trimmed) {
      return { ok: false, reason: "empty_message" };
    }
    if (!client) {
      return { ok: false, reason: "bridge_not_connected" };
    }
    return sendReply({
      chatId: String(chatId || "").trim(),
      openId: String(openId || "").trim(),
      text: await addressReply(trimmed),
      sourceMessageId: "",
      phase,
    });
  }

  async function sendProcessingAck(normalized, deliveryNotice = "") {
    const ackText = chooseAckTemplate(normalized.message_id || normalized.text || normalized.chat_id);
    await sendReply({
      chatId: normalized.chat_id,
      openId: normalized.open_id,
      text: withDeliveryNotice(await addressReply(ackText), deliveryNotice),
      sourceMessageId: normalized.message_id,
      phase: "ack",
    });
  }

  async function ensureStreamCard(normalized, deliveryNotice = "") {
    if (!cardStreamController) return "";
    const sourceMessageId = String(normalized?.message_id || "").trim();
    const chatId = String(normalized?.chat_id || "").trim();
    if (!sourceMessageId || !chatId) {
      return "";
    }
    if (activeStreamCards.has(sourceMessageId)) {
      return activeStreamCards.get(sourceMessageId);
    }
    const initialText = withDeliveryNotice(await addressReply("我正在处理这条任务。"), deliveryNotice);
    const streamMessageId = await cardStreamController.create(chatId, initialText);
    if (!streamMessageId) {
      return "";
    }
    activeStreamCards.set(sourceMessageId, streamMessageId);
    await recordOutboundMessage({
      messageId: `${streamMessageId}:created`,
      text: initialText,
      chatId,
      openId: normalized.open_id,
      sourceMessageId,
      phase: "progress",
    });
    return streamMessageId;
  }

  async function handleCardActionEventImmediate(
    event,
    normalized = normalizeCardActionEvent(event),
    executionControl = null,
  ) {
    const intent = parseApprovalCardAction(normalized.callback_data);
    if (intent.kind === "none") {
      return {
        toast: {
          type: "info",
          content: "已收到",
        },
      };
    }
    const prepared = await prepareMessage(normalized);
    if (!prepared.ok) {
      return {
        toast: {
          type: "info",
          content: "已收到",
        },
      };
    }
    const approvalAction = await resolveApprovalCommand(normalized, intent);
    if (!approvalAction?.ok) {
      return {
        toast: {
          type: "failed",
          content: "授权处理失败，请稍后重试。",
        },
      };
    }
    if (approvalAction.executeApproved) {
      const currentBinding = await getPersistedBinding(getConversationKey(normalized));
      await sendReply({
        chatId: normalized.chat_id,
        openId: normalized.open_id,
        text: await buildApprovalConfirmedReply(currentBinding, {
          project_name: String(approvalAction?.approvalItem?.project_name || "").trim(),
          topic_name: String(approvalAction?.approvalItem?.metadata?.topic_name || "").trim(),
        }),
        sourceMessageId: normalized.message_id,
        phase: "approval_confirmed",
      });
      const backgroundTask = (async () => {
        try {
          const routed = await routeMessage(approvalAction.normalized, {
            executionProfile: approvedExecutionProfileForItem(approvalAction.approvalItem),
          });
          if (!routed.ok) {
            await sendReply({
              chatId: normalized.chat_id,
              openId: normalized.open_id,
              text: await addressReply(`未能执行：${routed.reason || "未知原因"}`),
              sourceMessageId: normalized.message_id,
              phase: "error",
            });
            return;
          }
          await sendReply({
            chatId: normalized.chat_id,
            openId: normalized.open_id,
            text: await buildBrokerReplyPreview(approvalAction.normalized, routed),
            sourceMessageId: normalized.message_id,
            phase: "final",
          });
        } catch (error) {
          await persistStatus({
            last_error: String(error?.message || error || "feishu_card_approval_handler_failed"),
          });
          await sendReply({
            chatId: normalized.chat_id,
            openId: normalized.open_id,
            text: await addressReply(`执行失败：${summarizeErrorText(String(error?.message || error || ""))}`),
            sourceMessageId: normalized.message_id,
            phase: "error",
          });
        }
      })();
      executionControl?.extend(backgroundTask);
      return buildApprovalCardResolutionPayload(
        await fetchApprovalTokenRecord(approvalAction.approvalToken),
        currentBinding,
        "approved",
      );
    }
    if (approvalAction.replyPreview) {
      await sendReply({
        chatId: normalized.chat_id,
        openId: normalized.open_id,
        text: approvalAction.replyPreview,
        sourceMessageId: normalized.message_id,
        phase: approvalAction.replyPhase || "reply",
      });
    }
    const currentBinding = await getPersistedBinding(getConversationKey(normalized));
    if (approvalAction.replyPhase === "approval_denied") {
      return buildApprovalCardResolutionPayload(
        await fetchApprovalTokenRecord(normalized.text.replace(/^\/deny\s+/i, "").trim()),
        currentBinding,
        "denied",
      );
    }
    if (approvalAction.replyPhase === "approval_already_recorded") {
      return buildApprovalCardResolutionPayload(
        await fetchApprovalTokenRecord(normalized.text.replace(/^\/approve\s+/i, "").trim()),
        currentBinding,
        "approved_again",
      );
    }
    if (approvalAction.replyPhase === "approval_expired") {
      return buildApprovalCardResolutionPayload(
        await fetchApprovalTokenRecord(normalized.text.replace(/^\/(?:approve|deny)\s+/i, "").trim()),
        currentBinding,
        "expired",
      );
    }
    return {
      toast: {
        type: approvalAction.replyPhase === "approval_denied" ? "info" : "success",
        content:
          approvalAction.replyPhase === "approval_denied"
            ? "已拒绝本次授权。"
            : "已收到授权结果。",
      },
    };
  }

  async function handleCardActionEvent(event) {
    const normalized = normalizeCardActionEvent(event);
    return withConversationQueue(normalized, (executionControl) =>
      handleCardActionEventImmediate(event, normalized, executionControl),
    );
  }

  function clearFollowup(messageId) {
    if (!messageId) return;
    const entry = pendingFollowups.get(messageId);
    if (entry?.ackTimer) {
      clearTimeout(entry.ackTimer);
    }
    if (entry?.progressTimer) {
      clearTimeout(entry.progressTimer);
    }
    pendingFollowups.delete(messageId);
  }

  function scheduleProgressFollowup(normalized, deliveryNotice = "", attempt = 0) {
    const messageId = String(normalized?.message_id || "").trim();
    if (!messageId) return;
    const existing = pendingFollowups.get(messageId) || {};
    if (existing.progressTimer) {
      clearTimeout(existing.progressTimer);
    }
    const progressTimer = setTimeout(async () => {
      const latest = pendingFollowups.get(messageId) || {};
      if (latest.progressTimer) {
        latest.progressTimer = null;
        pendingFollowups.set(messageId, latest);
      }
      try {
        const conversation = await getConversationSnapshot(getConversationKey(normalized));
        if (!conversation) return;
        if (!conversation.awaiting_report && !conversation.ack_pending) {
          return;
        }
        const lastRequest = String(conversation.last_user_request || "").trim();
        const currentText = String(normalized.text || "").trim();
        if (lastRequest && currentText && lastRequest !== currentText && !currentText.startsWith(lastRequest)) {
          return;
        }
        await sendReply({
          chatId: normalized.chat_id,
          openId: normalized.open_id,
          text: withDeliveryNotice(
            await addressReply(buildBackgroundFollowupText(attempt)),
            deliveryNotice,
          ),
          sourceMessageId: messageId,
          phase: "progress",
        });
        if (attempt + 1 < MAX_BACKGROUND_FOLLOWUPS) {
          scheduleProgressFollowup(normalized, "", attempt + 1);
        }
      } catch (_error) {
        // ignore followup failures
      }
    }, BACKGROUND_FOLLOWUP_REPEAT_SECONDS * 1000);
    pendingFollowups.set(messageId, {
      ...existing,
      progressTimer,
      attempt,
    });
  }

  function scheduleLongRunningAck(normalized, deliveryNotice = "") {
    const messageId = String(normalized?.message_id || "").trim();
    if (!messageId) return;
    clearFollowup(messageId);
    const ackTimer = setTimeout(async () => {
      const latest = pendingFollowups.get(messageId) || {};
      if (latest.ackTimer) {
        latest.ackTimer = null;
        pendingFollowups.set(messageId, latest);
      }
      try {
        const conversation = await getConversationSnapshot(getConversationKey(normalized));
        if (!conversation) return;
        if (!conversation.awaiting_report && !conversation.ack_pending) {
          clearFollowup(messageId);
          return;
        }
        const lastRequest = String(conversation.last_user_request || "").trim();
        const currentText = String(normalized.text || "").trim();
        if (lastRequest && currentText && lastRequest !== currentText && !currentText.startsWith(lastRequest)) {
          clearFollowup(messageId);
          return;
        }
        const streamCardId = await ensureStreamCard(normalized, deliveryNotice);
        if (!streamCardId) {
          await sendProcessingAck(normalized, deliveryNotice);
        }
        scheduleProgressFollowup(normalized, deliveryNotice, 0);
      } catch (_error) {
        // ignore ack failures
      }
    }, BACKGROUND_ACK_SECONDS * 1000);
    pendingFollowups.set(messageId, { ackTimer, progressTimer: null, attempt: -1 });
  }

  async function handleMessageEventImmediate(
    event,
    normalized = normalizeMessageEvent(event),
    _executionControl = null,
  ) {
    const prepared = await prepareMessage(normalized);
    if (!prepared.ok) {
      return prepared;
    }
    const deliveryNotice = buildDelayedReplyNotice(normalized, status);
    const approvalAction = await resolveApprovalCommand(normalized);
    if (approvalAction?.ok) {
      if (approvalAction.executeApproved) {
        const currentBinding = await getPersistedBinding(getConversationKey(normalized));
        await sendReply({
          chatId: normalized.chat_id,
          openId: normalized.open_id,
          text: withDeliveryNotice(
            await buildApprovalConfirmedReply(
              currentBinding,
              resolveMessageRouteContext(approvalAction.normalized, currentBinding),
            ),
            deliveryNotice,
          ),
          sourceMessageId: normalized.message_id,
          phase: "approval_confirmed",
        });
        await markExecutionStarted({ ackPending: false });
        const routed = await routeMessage(approvalAction.normalized, {
          executionProfile: approvedExecutionProfileForItem(approvalAction.approvalItem),
        });
        if (!routed.ok) return routed;
        return {
          ok: true,
          normalized: approvalAction.normalized,
          brokerPayload: routed.brokerPayload,
          replyPreview: withDeliveryNotice(await buildBrokerReplyPreview(approvalAction.normalized, routed), deliveryNotice),
          sessionId: routed.sessionId || "",
        };
      }
      approvalAction.replyPreview = withDeliveryNotice(approvalAction.replyPreview, deliveryNotice);
      return approvalAction;
    }
    const bindingRequirement = await shouldPromptForGroupBinding(normalized);
    if (bindingRequirement.required) {
      await persistStatus({
        last_binding_result: "prompted",
        last_binding_chat_ref: getConversationKey(normalized),
        last_binding_project: "",
        last_binding_topic: "",
      });
      return {
        ok: true,
        normalized,
        direct: true,
        replyPhase: "binding_prompt",
        replyPreview: withDeliveryNotice(await buildGroupBindingPrompt(), deliveryNotice),
      };
    }
    const bindingAttempt = await tryBindChat(normalized);
    if (bindingAttempt?.ok) {
      await persistStatus({
        last_binding_result: "bound",
        last_binding_chat_ref: getConversationKey(normalized),
        last_binding_project: bindingAttempt.binding?.project_name || "",
        last_binding_topic: bindingAttempt.binding?.topic_name || "",
      });
      const bindingConfirmation = bindingAttempt.binding;
      const scopeLabel = normalized.chat_type === "group" ? "本群" : "这个聊天";
      const messageParts = [
        `已将${scopeLabel}绑定到项目 \`${bindingConfirmation.project_name}\``,
      ];
      if (bindingConfirmation.topic_name) {
        messageParts.push(`并锁定话题 \`${bindingConfirmation.topic_name}\``);
      }
      return {
        ok: true,
        normalized,
        direct: true,
        replyPhase: "binding_bound",
        replyPreview: withDeliveryNotice(await addressReply(`${messageParts.join("，")}。`), deliveryNotice),
      };
    }
    if (bindingAttempt && bindingAttempt.ok === false) {
      await persistStatus({
        last_binding_result: "failed",
        last_binding_chat_ref: getConversationKey(normalized),
        last_binding_project: "",
        last_binding_topic: "",
        last_error: bindingAttempt.error || "binding_rejected",
      });
      return {
        ok: true,
        normalized,
        direct: true,
        replyPhase: "binding_error",
        replyPreview: withDeliveryNotice(
          await addressReply(buildBindingFailureReply(bindingAttempt.brokerPayload || { error: bindingAttempt.error })),
          deliveryNotice,
        ),
      };
    }
    const direct = await directRoute(normalized);
    if (direct.ok) {
      direct.replyPreview = withDeliveryNotice(direct.replyPreview, deliveryNotice);
      return direct;
    }
      const approvalRequirement = classifyApprovalRequirement(normalized.text);
      if (approvalRequirement.required) {
        const currentBinding = await getPersistedBinding(getConversationKey(normalized));
        const routeContext = resolveMessageRouteContext(normalized, currentBinding);
        const approval = await createPendingApproval(normalized, currentBinding, routeContext, approvalRequirement);
        return {
          ok: true,
          normalized,
          direct: true,
          replyPhase: "approval_prompt",
          replyPreview: withDeliveryNotice(await buildApprovalPromptReply(approval, currentBinding, routeContext), deliveryNotice),
          replyPayload: {
            kind: "approval_prompt",
            approval,
            binding: currentBinding,
            routeContext,
            deliveryNotice,
          },
        };
      }
    const routed = await routeMessage(normalized);
    if (!routed.ok) return routed;
    return {
      ok: true,
      normalized,
      brokerPayload: routed.brokerPayload,
      replyPhase: "reply",
      replyPreview: withDeliveryNotice(await buildBrokerReplyPreview(normalized, routed), deliveryNotice),
      sessionId: routed.sessionId || "",
    };
  }

  async function handleMessageEvent(event) {
    const normalized = normalizeMessageEvent(event);
    return withConversationQueue(normalized, (executionControl) =>
      handleMessageEventImmediate(event, normalized, executionControl),
    );
  }

  async function processMessageEventImmediate(
    event,
    normalized = normalizeMessageEvent(event),
    executionControl = null,
  ) {
    if (!shouldRunInBackground(normalized)) {
      const prepared = await prepareMessage(normalized);
      if (!prepared.ok) {
        return prepared;
      }
      const deliveryNotice = buildDelayedReplyNotice(normalized, status);
      const approvalAction = await resolveApprovalCommand(normalized);
      if (approvalAction?.ok) {
      if (approvalAction.executeApproved) {
        const currentBinding = await getPersistedBinding(getConversationKey(normalized));
        await sendReply({
          chatId: normalized.chat_id,
          openId: normalized.open_id,
          text: withDeliveryNotice(
            await buildApprovalConfirmedReply(
              currentBinding,
              resolveMessageRouteContext(approvalAction.normalized, currentBinding),
            ),
            deliveryNotice,
          ),
          sourceMessageId: normalized.message_id,
          phase: "approval_confirmed",
        });
          const routed = await routeMessage(approvalAction.normalized, {
            executionProfile: approvedExecutionProfileForItem(approvalAction.approvalItem),
          });
          if (!routed.ok) return routed;
          return {
            ok: true,
            normalized: approvalAction.normalized,
            brokerPayload: routed.brokerPayload,
            replyPreview: await buildBrokerReplyPreview(approvalAction.normalized, routed),
            sessionId: routed.sessionId || "",
          };
        }
        approvalAction.replyPreview = withDeliveryNotice(approvalAction.replyPreview, deliveryNotice);
        return approvalAction;
      }
      const bindingRequirement = await shouldPromptForGroupBinding(normalized);
      if (bindingRequirement.required) {
        await persistStatus({
          last_binding_result: "prompted",
          last_binding_chat_ref: getConversationKey(normalized),
          last_binding_project: "",
          last_binding_topic: "",
        });
        return {
          ok: true,
          normalized,
          direct: true,
          replyPhase: "binding_prompt",
          replyPreview: withDeliveryNotice(await buildGroupBindingPrompt(), deliveryNotice),
        };
      }
      const bindingAttempt = await tryBindChat(normalized);
      if (bindingAttempt?.ok) {
        await persistStatus({
          last_binding_result: "bound",
          last_binding_chat_ref: getConversationKey(normalized),
          last_binding_project: bindingAttempt.binding?.project_name || "",
          last_binding_topic: bindingAttempt.binding?.topic_name || "",
        });
        const bindingConfirmation = bindingAttempt.binding;
        const scopeLabel = normalized.chat_type === "group" ? "本群" : "这个聊天";
        const messageParts = [
          `已将${scopeLabel}绑定到项目 \`${bindingConfirmation.project_name}\``,
        ];
        if (bindingConfirmation.topic_name) {
          messageParts.push(`并锁定话题 \`${bindingConfirmation.topic_name}\``);
        }
        return {
          ok: true,
          normalized,
          direct: true,
          replyPhase: "binding_bound",
          replyPreview: withDeliveryNotice(await addressReply(`${messageParts.join("，")}。`), deliveryNotice),
        };
      }
      if (bindingAttempt && bindingAttempt.ok === false) {
        await persistStatus({
          last_binding_result: "failed",
          last_binding_chat_ref: getConversationKey(normalized),
          last_binding_project: "",
          last_binding_topic: "",
          last_error: bindingAttempt.error || "binding_rejected",
        });
        return {
          ok: true,
          normalized,
          direct: true,
          replyPhase: "binding_error",
          replyPreview: withDeliveryNotice(
            await addressReply(buildBindingFailureReply(bindingAttempt.brokerPayload || { error: bindingAttempt.error })),
            deliveryNotice,
          ),
        };
      }
      const direct = await directRoute(normalized);
      if (direct.ok) {
        direct.replyPreview = withDeliveryNotice(direct.replyPreview, deliveryNotice);
        return direct;
      }
    const approvalRequirement = classifyApprovalRequirement(normalized.text);
    if (approvalRequirement.required) {
      const currentBinding = await getPersistedBinding(getConversationKey(normalized));
      const routeContext = resolveMessageRouteContext(normalized, currentBinding);
      const approval = await createPendingApproval(normalized, currentBinding, routeContext, approvalRequirement);
      return {
        ok: true,
        normalized,
        direct: true,
        replyPhase: "approval_prompt",
        replyPreview: withDeliveryNotice(await buildApprovalPromptReply(approval, currentBinding, routeContext), deliveryNotice),
        replyPayload: {
          kind: "approval_prompt",
          approval,
          binding: currentBinding,
          routeContext,
          deliveryNotice,
        },
      };
    }
    await markExecutionStarted({ ackPending: false });
      const routed = await routeMessage(normalized);
      if (!routed.ok) return routed;
      return {
        ok: true,
        normalized,
        brokerPayload: routed.brokerPayload,
        replyPhase: "reply",
        replyPreview: await buildBrokerReplyPreview(normalized, routed),
        sessionId: routed.sessionId || "",
      };
    }
    const deliveryNotice = buildDelayedReplyNotice(normalized, status);
    const prepared = await prepareMessage(normalized);
    if (!prepared.ok) {
      return prepared;
    }
    const approvalAction = await resolveApprovalCommand(normalized);
    if (approvalAction?.ok) {
      if (approvalAction.executeApproved) {
        const currentBinding = await getPersistedBinding(getConversationKey(normalized));
        await sendReply({
          chatId: normalized.chat_id,
          openId: normalized.open_id,
          text: withDeliveryNotice(
            await buildApprovalConfirmedReply(
              currentBinding,
              resolveMessageRouteContext(approvalAction.normalized, currentBinding),
            ),
            deliveryNotice,
          ),
          sourceMessageId: normalized.message_id,
          phase: "approval_confirmed",
        });
      await markExecutionStarted({ ackPending: false });
        const backgroundTask = (async () => {
          try {
            const routed = await routeMessage(approvalAction.normalized, {
              executionProfile: approvedExecutionProfileForItem(approvalAction.approvalItem),
            });
            if (!routed.ok) {
              await sendReply({
                chatId: normalized.chat_id,
                openId: normalized.open_id,
                text: await addressReply(`未能执行：${routed.reason || "未知原因"}`),
                sourceMessageId: normalized.message_id,
                phase: "error",
              });
              return;
            }
            await sendReply({
              chatId: normalized.chat_id,
              openId: normalized.open_id,
              text: await buildBrokerReplyPreview(approvalAction.normalized, routed),
              sourceMessageId: normalized.message_id,
              phase: "final",
            });
          } catch (error) {
            await persistStatus({
              last_error: String(error?.message || error || "feishu_approval_handler_failed"),
            });
            await sendReply({
              chatId: normalized.chat_id,
              openId: normalized.open_id,
              text: await addressReply(`执行失败：${summarizeErrorText(String(error?.message || error || ""))}`),
              sourceMessageId: normalized.message_id,
              phase: "error",
            });
          }
        })();
        executionControl?.extend(backgroundTask);
        return { ok: false, reason: "background_started", normalized };
      }
      approvalAction.replyPreview = withDeliveryNotice(approvalAction.replyPreview, deliveryNotice);
      return approvalAction;
    }
    const bindingRequirement = await shouldPromptForGroupBinding(normalized);
    if (bindingRequirement.required) {
      await persistStatus({
        last_binding_result: "prompted",
        last_binding_chat_ref: getConversationKey(normalized),
        last_binding_project: "",
        last_binding_topic: "",
      });
      return {
        ok: true,
        normalized,
        direct: true,
        replyPhase: "binding_prompt",
        replyPreview: withDeliveryNotice(await buildGroupBindingPrompt(), deliveryNotice),
      };
    }
    const backgroundApprovalRequirement = classifyApprovalRequirement(normalized.text);
    if (backgroundApprovalRequirement.required) {
      const currentBinding = await getPersistedBinding(getConversationKey(normalized));
      const routeContext = resolveMessageRouteContext(normalized, currentBinding);
      const approval = await createPendingApproval(
        normalized,
        currentBinding,
        routeContext,
        backgroundApprovalRequirement,
      );
      return {
        ok: true,
        normalized,
        direct: true,
        replyPhase: "approval_prompt",
        replyPreview: withDeliveryNotice(await buildApprovalPromptReply(approval, currentBinding, routeContext), deliveryNotice),
        replyPayload: {
          kind: "approval_prompt",
          approval,
          binding: currentBinding,
          routeContext,
          deliveryNotice,
        },
      };
    }
    const backgroundTask = (async () => {
      try {
        await markExecutionStarted({ ackPending: false });
        const routed = await routeMessage(normalized);
        if (!routed.ok) {
          await sendReply({
            chatId: normalized.chat_id,
            openId: normalized.open_id,
            text: await addressReply(`未能执行：${routed.reason || "未知原因"}`),
            sourceMessageId: normalized.message_id,
            phase: "error",
          });
          return;
        }
        await sendReply({
          chatId: normalized.chat_id,
          openId: normalized.open_id,
          text: await buildBrokerReplyPreview(normalized, routed),
          sourceMessageId: normalized.message_id,
          phase: "final",
        });
      } catch (error) {
        await persistStatus({
          last_error: String(error?.message || error || "feishu_background_handler_failed"),
        });
        await sendReply({
          chatId: normalized.chat_id,
          openId: normalized.open_id,
          text: await addressReply(`执行失败：${summarizeErrorText(String(error?.message || error || ""))}`),
          sourceMessageId: normalized.message_id,
          phase: "error",
        });
      }
    })();
    executionControl?.extend(backgroundTask);
    return { ok: false, reason: "background_started", normalized };
  }

  async function processMessageEvent(event) {
    const normalized = normalizeMessageEvent(event);
    return withConversationQueue(normalized, (executionControl) =>
      processMessageEventImmediate(event, normalized, executionControl),
    );
  }

  function getStatus() {
    return {
      ...status,
      sdk_loaded: Boolean(sdk),
      dispatcher_ready: Boolean(eventDispatcher),
      settings_summary: summarizeSettings(settings),
    };
  }

  return {
    loadSettings,
    connect,
    disconnect,
    reconnect,
    sendMessage,
    handleMessageEvent,
    handleCardActionEvent,
    processMessageEvent,
    getStatus,
    normalizeMessageEvent,
    normalizeCardActionEvent,
    getBindingForConversationKey,
  };
}

module.exports = {
  DEFAULT_SETTINGS,
  classifyApprovalRequirement,
  createFeishuLongConnectionService,
  buildReplyDocTitle,
  isHighRiskRequest,
  isLocalExtensionRequest,
  normalizeMessageEvent,
  normalizeCardActionEvent,
  normalizeSdkDomain,
  resolveExecutionProfileForMessage,
  sanitizeSettings,
  shouldMirrorReplyToDoc,
  shouldUseInteractiveReply,
  summarizeSettings,
  shouldAcceptMessage,
  shouldRunInBackground,
  tryLoadFeishuSdk,
  EVENT_IDLE_AFTER_SECONDS,
  EVENT_IDLE_AFTER_SECONDS_IDLE,
  ACTIVE_EXECUTION_EVENT_IDLE_SECONDS,
  ACK_PENDING_EVENT_IDLE_GRACE_SECONDS,
};
