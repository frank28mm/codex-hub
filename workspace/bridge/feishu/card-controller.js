"use strict";

const { buildReplyCardPayload, optimizeMarkdown } = require("./outbound");

function formatElapsed(ms) {
  if (ms < 1000) return `${ms}ms`;
  const sec = ms / 1000;
  if (sec < 60) return `${sec.toFixed(1)}s`;
  const min = Math.floor(sec / 60);
  const remSec = Math.floor(sec % 60);
  return `${min}m ${remSec}s`;
}

class FeishuCardStreamController {
  constructor(client, config = {}) {
    this.client = client;
    this.config = {
      throttleMs: Number(config.throttleMs || 200),
      footer: {
        status: config.footer?.status !== false,
        elapsed: config.footer?.elapsed !== false,
      },
    };
    this.cards = new Map();
  }

  async create(chatId, initialText = "") {
    if (!this.client?.cardkit?.v2?.card?.create) {
      return "";
    }
    const cardBody = buildReplyCardPayload({
      phase: "progress",
      text: initialText || "处理中...",
      title: "CoCo 正在处理",
    });
    cardBody.config = {
      ...(cardBody.config || {}),
      streaming_mode: true,
      summary: { content: "处理中..." },
    };
    if (Array.isArray(cardBody?.body?.elements)) {
      cardBody.body.elements = cardBody.body.elements.map((element, index) =>
        index === cardBody.body.elements.length - 1 && element?.tag === "markdown"
          ? { ...element, element_id: "streaming_content" }
          : element,
      );
    }
    const createResp = await this.client.cardkit.v2.card.create({
      data: { type: "card_json", data: JSON.stringify(cardBody) },
    });
    const cardId = createResp?.data?.card_id;
    if (!cardId) return "";
    const msgResp = await this.client.im.v1.message.create({
      params: { receive_id_type: "chat_id" },
      data: {
        receive_id: chatId,
        msg_type: "interactive",
        content: JSON.stringify({ type: "card", data: { card_id: cardId } }),
      },
    });
    const messageId = String(msgResp?.data?.message_id || msgResp?.message_id || "").trim();
    if (!messageId) return "";
    this.cards.set(messageId, {
      cardId,
      messageId,
      sequence: 0,
      lastUpdateAt: Date.now(),
      startTime: Date.now(),
      throttleTimer: null,
      pendingText: null,
    });
    return messageId;
  }

  async update(messageId, text) {
    const state = this.cards.get(messageId);
    if (!state || !this.client?.cardkit?.v2?.card?.streamContent) {
      return "fail";
    }
    state.pendingText = String(text || "");
    const elapsed = Date.now() - state.lastUpdateAt;
    if (elapsed < this.config.throttleMs) {
      if (!state.throttleTimer) {
        state.throttleTimer = setTimeout(() => {
          state.throttleTimer = null;
          void this.flushUpdate(state);
        }, this.config.throttleMs - elapsed);
      }
      return "ok";
    }
    return this.flushUpdate(state);
  }

  async flushUpdate(state) {
    if (!state.pendingText) return "ok";
    state.sequence += 1;
    const content = optimizeMarkdown(state.pendingText);
    state.pendingText = null;
    await this.client.cardkit.v2.card.streamContent({
      path: { card_id: state.cardId },
      data: { content, sequence: state.sequence },
    });
    state.lastUpdateAt = Date.now();
    return "ok";
  }

  async finalize(messageId, finalText, status = "completed", options = {}) {
    const state = this.cards.get(messageId);
    if (!state) return;
    if (state.throttleTimer) {
      clearTimeout(state.throttleTimer);
      state.throttleTimer = null;
    }
    if (!this.client?.cardkit?.v2?.card?.update) {
      this.cards.delete(messageId);
      return;
    }
    if (this.client?.cardkit?.v2?.card?.setStreamingMode) {
      state.sequence += 1;
      await this.client.cardkit.v2.card.setStreamingMode({
        path: { card_id: state.cardId },
        data: { streaming_mode: false, sequence: state.sequence },
      });
    }
    const footer = [];
    if (this.config.footer.status) {
      footer.push(
        status === "error" ? "❌ 执行失败" : status === "interrupted" ? "⚠️ 已中断" : "✅ 已完成",
      );
    }
    if (this.config.footer.elapsed) {
      footer.push(formatElapsed(Date.now() - state.startTime));
    }
    const payload = buildReplyCardPayload({
      phase: status === "error" ? "error" : "final",
      text: finalText,
      docUrl: String(options.docUrl || "").trim(),
      docTitle: String(options.docTitle || "").trim(),
      title: String(options.title || "").trim(),
      footer: footer.join(" · "),
    });
    state.sequence += 1;
    await this.client.cardkit.v2.card.update({
      path: { card_id: state.cardId },
      data: {
        type: "card_json",
        data: JSON.stringify(payload),
        sequence: state.sequence,
      },
    });
    this.cards.delete(messageId);
  }
}

function createCardStreamController(client, config) {
  return new FeishuCardStreamController(client, config);
}

module.exports = {
  createCardStreamController,
};
