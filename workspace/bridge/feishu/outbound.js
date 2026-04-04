"use strict";

const { assistantName } = require("../../assistant-branding");

function trimText(value, limit = 0) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (!limit || text.length <= limit) {
    return text;
  }
  return `${text.slice(0, Math.max(0, limit - 1)).trim()}…`;
}

function stripMarkdown(text) {
  return String(text || "")
    .replace(/```[\s\S]*?```/g, " ")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, "$1")
    .replace(/^#{1,6}\s+/gm, "")
    .replace(/[*_~>-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function summarizeText(text, limit = 180) {
  const plain = stripMarkdown(text);
  if (!plain) {
    return "";
  }
  return trimText(plain, limit);
}

function phaseCardMeta(phase = "") {
  const key = String(phase || "").trim();
  const name = assistantName();
  const table = {
    ack: { title: `${name} 已收到`, template: "wathet", statusLabel: "已收到" },
    progress: { title: `${name} 正在处理`, template: "wathet", statusLabel: "处理中" },
    final: { title: `${name} 处理结果`, template: "green", statusLabel: "已完成" },
    reply: { title: `${name} 处理结果`, template: "green", statusLabel: "已完成" },
    report: { title: `${name} 汇报摘要`, template: "blue", statusLabel: "摘要" },
    status: { title: `${name} 状态摘要`, template: "blue", statusLabel: "状态" },
    thread_status: { title: `${name} 线程状态`, template: "blue", statusLabel: "状态" },
    approval_status: { title: `${name} 授权状态`, template: "orange", statusLabel: "待确认" },
    approval_confirmed: { title: `${name} 已记录授权`, template: "green", statusLabel: "已批准" },
    approval_prompt: { title: `${name} 授权请求`, template: "orange", statusLabel: "待确认" },
    binding_prompt: { title: `${name} 需要项目声明`, template: "orange", statusLabel: "待确认" },
    binding_bound: { title: `${name} 绑定成功`, template: "green", statusLabel: "已完成" },
    binding_error: { title: `${name} 绑定失败`, template: "red", statusLabel: "需处理" },
    error: { title: `${name} 执行异常`, template: "red", statusLabel: "失败" },
  };
  return table[key] || { title: `${name} 消息`, template: "wathet", statusLabel: "消息" };
}

function extractMetricPairs(text, limit = 6) {
  const pairs = [];
  for (const rawLine of String(text || "").split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line) continue;
    const match = line.match(/^(?:[-*]\s*)?([^:：]{1,40})[:：]\s*(.+)$/);
    if (!match) continue;
    pairs.push({
      label: trimText(match[1], 24),
      value: trimText(match[2], 40),
    });
    if (pairs.length >= limit) {
      break;
    }
  }
  return pairs;
}

function buildMetricColumns(metrics) {
  if (!Array.isArray(metrics) || metrics.length === 0) {
    return null;
  }
  return {
    tag: "column_set",
    flex_mode: "stretch",
    horizontal_spacing: "12px",
    columns: metrics.map((item) => ({
      tag: "column",
      width: "weighted",
      weight: 1,
      elements: [
        {
          tag: "markdown",
          content: `**${String(item.label || "").trim()}**\n${String(item.value || "").trim()}`,
          text_align: "left",
        },
      ],
    })),
  };
}

function buildReplyCardPayload({
  phase = "",
  text = "",
  docUrl = "",
  docTitle = "",
  footer = "",
  title = "",
} = {}) {
  const meta = phaseCardMeta(phase);
  const normalizedText = optimizeMarkdown(text);
  const metrics = extractMetricPairs(text, 4);
  const previewText = docUrl ? trimText(normalizedText, 900) : normalizedText;
  const elements = [
    {
      tag: "markdown",
      content: `**${meta.statusLabel}**`,
      text_align: "left",
      text_size: "normal",
    },
  ];
  const metricColumns = buildMetricColumns(metrics);
  if (metricColumns) {
    elements.push(metricColumns);
  }
  if (previewText) {
    elements.push({
      tag: "markdown",
      content: previewText,
      text_align: "left",
      text_size: "normal",
    });
  } else {
    const summary = summarizeText(normalizedText, 200) || "本次没有附带可读摘要。";
    elements.push({
      tag: "markdown",
      content: summary,
      text_align: "left",
      text_size: "normal",
    });
  }
  if (docUrl) {
    elements.push({ tag: "hr" });
    elements.push({
      tag: "markdown",
      content: `完整内容：[${trimText(docTitle || "打开 Feishu 文档", 48)}](${String(docUrl || "").trim()})`,
      text_align: "left",
      text_size: "normal",
    });
  }
  if (footer) {
    elements.push({ tag: "hr" });
    elements.push({
      tag: "markdown",
      content: trimText(footer, 200),
      text_align: "left",
      text_size: "notation",
    });
  }
  return {
    schema: "2.0",
    config: {
      wide_screen_mode: true,
    },
    header: {
      title: {
        tag: "plain_text",
        content: trimText(title || meta.title, 60) || `${assistantName()} 消息`,
      },
      template: meta.template,
    },
    body: {
      elements,
    },
  };
}

function buildMetricDigestCardPayload({
  title = `${assistantName()} 摘要`,
  summary = "",
  metrics = [],
  footer = "",
  docUrl = "",
  docTitle = "",
} = {}) {
  const elements = [];
  const digestSummary = trimText(summary, 220);
  if (digestSummary) {
    elements.push({
      tag: "markdown",
      content: digestSummary,
      text_align: "left",
      text_size: "normal",
    });
  }
  const metricColumns = buildMetricColumns(Array.isArray(metrics) ? metrics.slice(0, 4) : []);
  if (metricColumns) {
    elements.push(metricColumns);
  }
  if (docUrl) {
    elements.push({ tag: "hr" });
    elements.push({
      tag: "markdown",
      content: `查看详情：[${trimText(docTitle || "打开 Feishu 文档", 48)}](${String(docUrl || "").trim()})`,
      text_align: "left",
      text_size: "normal",
    });
  }
  if (footer) {
    elements.push({ tag: "hr" });
    elements.push({
      tag: "markdown",
      content: trimText(footer, 200),
      text_align: "left",
      text_size: "notation",
    });
  }
  return {
    schema: "2.0",
    config: {
      wide_screen_mode: true,
    },
    header: {
      title: {
        tag: "plain_text",
        content: trimText(title, 60) || `${assistantName()} 摘要`,
      },
      template: "blue",
    },
    body: {
      elements,
    },
  };
}

function optimizeMarkdown(text) {
  try {
    return _optimizeMarkdown(String(text || ""));
  } catch (_error) {
    return String(text || "");
  }
}

function _optimizeMarkdown(text) {
  const MARK = "___FEISHU_CODE_BLOCK_";
  const codeBlocks = [];
  let body = text.replace(/```[\s\S]*?```/g, (block) => `${MARK}${codeBlocks.push(block) - 1}___`);
  if (/^#{1,3}\s+/m.test(body)) {
    body = body.replace(/^#{2,6}\s+(.+)$/gm, "##### $1");
    body = body.replace(/^#\s+(.+)$/gm, "#### $1");
  }
  body = body.replace(/!\[([^\]]+)\]\((\/Users\/[^)]+)\)/g, (_full, label) => `\`${String(label || "").trim()}\``);
  codeBlocks.forEach((block, index) => {
    body = body.replace(`${MARK}${index}___`, `\n\n${block}\n\n`);
  });
  return body.replace(/[ \t]+\n/g, "\n").replace(/\n{3,}/g, "\n\n").trim();
}

function buildPostContent(text) {
  return JSON.stringify({
    zh_cn: {
      content: [[{ tag: "md", text: optimizeMarkdown(text) }]],
    },
  });
}

async function sendPostMessage(client, { chatId = "", openId = "", text = "" } = {}) {
  const receiveId = chatId || openId;
  const receiveIdType = chatId ? "chat_id" : openId ? "open_id" : "";
  if (!client || !receiveId || !receiveIdType) {
    return { ok: false, reason: "missing_reply_target" };
  }
  const response = await client.im.v1.message.create({
    params: { receive_id_type: receiveIdType },
    data: {
      receive_id: receiveId,
      msg_type: "post",
      content: buildPostContent(text),
    },
  });
  return {
    ok: true,
    messageId: String(response?.data?.message_id || response?.message_id || "").trim(),
  };
}

async function sendTextMessage(client, { chatId = "", openId = "", text = "" } = {}) {
  const receiveId = chatId || openId;
  const receiveIdType = chatId ? "chat_id" : openId ? "open_id" : "";
  if (!client || !receiveId || !receiveIdType) {
    return { ok: false, reason: "missing_reply_target" };
  }
  const response = await client.im.v1.message.create({
    params: { receive_id_type: receiveIdType },
    data: {
      receive_id: receiveId,
      msg_type: "text",
      content: JSON.stringify({ text: String(text || "") }),
    },
  });
  return {
    ok: true,
    messageId: String(response?.data?.message_id || response?.message_id || "").trim(),
  };
}

async function sendInteractiveCardMessage(client, { chatId = "", openId = "", card } = {}) {
  const receiveId = chatId || openId;
  const receiveIdType = chatId ? "chat_id" : openId ? "open_id" : "";
  if (!client || !receiveId || !receiveIdType) {
    return { ok: false, reason: "missing_reply_target" };
  }
  const response = await client.im.v1.message.create({
    params: { receive_id_type: receiveIdType },
    data: {
      receive_id: receiveId,
      msg_type: "interactive",
      content: typeof card === "string" ? card : JSON.stringify(card),
    },
  });
  return {
    ok: true,
    messageId: String(response?.data?.message_id || response?.message_id || "").trim(),
  };
}

module.exports = {
  optimizeMarkdown,
  buildPostContent,
  buildReplyCardPayload,
  buildMetricDigestCardPayload,
  phaseCardMeta,
  sendPostMessage,
  sendTextMessage,
  sendInteractiveCardMessage,
};
