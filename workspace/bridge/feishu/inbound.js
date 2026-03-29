"use strict";

const TEXT_MENTION_PATTERNS = [
  /(?:^|[\s(（【\[])[@＠]_user_1(?=$|[\s,.!?:;，。！？：；)\]】）])/i,
  /(?:^|[\s(（【\[])[@＠]coco(?=$|[\s,.!?:;，。！？：；)\]】）])/i,
  /(?:^|[\s(（【\[])[@＠]可可(?=$|[\s,.!?:;，。！？：；)\]】）])/,
];

const TEXTUAL_MESSAGE_TYPES = new Set(["text", "post"]);
const TEXT_CONTAINER_KEYS = [
  "content",
  "children",
  "elements",
  "body",
  "title",
  "header",
  "paragraphs",
  "lines",
  "items",
  "zh_cn",
  "en_us",
  "ja_jp",
  "ko_kr",
];

function collectTextParts(value, parts = [], seen = new Set()) {
  if (value == null) {
    return parts;
  }
  if (typeof value === "string") {
    const text = value.trim();
    if (text) {
      parts.push(text);
    }
    return parts;
  }
  if (typeof value !== "object") {
    return parts;
  }
  if (seen.has(value)) {
    return parts;
  }
  seen.add(value);
  if (Array.isArray(value)) {
    value.forEach((item) => collectTextParts(item, parts, seen));
    return parts;
  }
  if (typeof value.text === "string") {
    const text = value.text.trim();
    if (text) {
      parts.push(text);
    }
  }
  TEXT_CONTAINER_KEYS.forEach((key) => {
    if (!(key in value)) {
      return;
    }
    collectTextParts(value[key], parts, seen);
  });
  return parts;
}

function extractMessageText(messageType, content) {
  const normalizedType = String(messageType || "text").trim().toLowerCase();
  if (!TEXTUAL_MESSAGE_TYPES.has(normalizedType)) {
    return "";
  }
  const parts = collectTextParts(content);
  if (!parts.length) return "";
  return [...new Set(parts)].join("\n").trim();
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
  return extractMessageText("post", content);
}

function normalizeMessageEvent(event) {
  const message = event?.message || {};
  const sender = event?.sender || {};
  const mentions = Array.isArray(message?.mentions) ? message.mentions : [];
  const content = safeParseContent(message?.content);
  const messageType = String(message?.message_type || "text").trim() || "text";
  const text = extractMessageText(messageType, content);
  const messageCreatedAt = normalizeEventTimestamp(message?.create_time || message?.create_at || event?.create_time);
  return {
    event_type: event?.event_type || "im.message.receive_v1",
    message_id: String(message?.message_id || "").trim(),
    message_type: messageType,
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

function parseApprovalCardAction(callbackData) {
  const text = String(callbackData || "").trim();
  const match = text.match(/^perm:(allow|deny)(?::([A-Za-z0-9_-]+))?$/i);
  if (!match) {
    return { kind: "none", token: "", mode: "" };
  }
  return {
    kind: match[1].toLowerCase() === "deny" ? "deny" : "approve",
    token: String(match[2] || "").trim(),
    mode: match[1].toLowerCase() === "deny" ? "deny" : "allow",
  };
}

function normalizeCardActionEvent(event) {
  const actionValue = event?.action?.value || event?.action?.form_value || {};
  const callbackData = String(actionValue?.callback_data || "").trim();
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

module.exports = {
  TEXT_MENTION_PATTERNS,
  normalizeEventTimestamp,
  safeParseContent,
  extractText,
  normalizeMessageEvent,
  normalizeCardActionEvent,
  parseApprovalCardAction,
};
