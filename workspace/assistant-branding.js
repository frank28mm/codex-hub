"use strict";

const DEFAULT_ASSISTANT_NAME = "CoCo";

function assistantName() {
  return String(process.env.WORKSPACE_HUB_ASSISTANT_NAME || "").trim() || DEFAULT_ASSISTANT_NAME;
}

function assistantPrivateThreadLabel() {
  return `${assistantName()} 私聊`;
}

function assistantServiceLabel() {
  return `${assistantName()} 服务`;
}

function assistantCustomizationHint() {
  return `默认机器人昵称是 ${assistantName()}。如需自定义，可设置环境变量 WORKSPACE_HUB_ASSISTANT_NAME。`;
}

module.exports = {
  DEFAULT_ASSISTANT_NAME,
  assistantCustomizationHint,
  assistantName,
  assistantPrivateThreadLabel,
  assistantServiceLabel,
};
