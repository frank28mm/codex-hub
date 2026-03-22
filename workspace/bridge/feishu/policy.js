"use strict";

function shouldAcceptMessage(settings, normalized) {
  if (!normalized.text) {
    return { ok: false, reason: "empty_text" };
  }
  if (Array.isArray(settings.allowed_users) && settings.allowed_users.length > 0) {
    const senderRef = normalized.open_id || normalized.user_id;
    if (!settings.allowed_users.includes(senderRef)) {
      return { ok: false, reason: "sender_not_allowed" };
    }
  }
  if (
    normalized.chat_type === "group" &&
    settings.group_policy === "mentions_only" &&
    settings.require_mention
  ) {
    const mentionCount =
      Number(normalized.mentions?.length || 0) + Number(normalized.text_mentions?.length || 0);
    if (mentionCount <= 0) {
      return { ok: false, reason: "mention_required" };
    }
  }
  return { ok: true, reason: "" };
}

module.exports = {
  shouldAcceptMessage,
};
