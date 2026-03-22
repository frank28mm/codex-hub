"use strict";

const DEFAULT_SETTINGS = {
  app_id: "",
  app_secret: "",
  domain: "feishu",
  allowed_users: [],
  group_policy: "mentions_only",
  require_mention: true,
};

function sanitizeSettings(settings = {}) {
  const allowedUsers = Array.isArray(settings.allowed_users)
    ? settings.allowed_users.map((item) => String(item || "").trim()).filter(Boolean)
    : String(settings.allowed_users || "")
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean);

  return {
    app_id: String(settings.app_id || "").trim(),
    app_secret: String(settings.app_secret || "").trim(),
    domain: String(settings.domain || DEFAULT_SETTINGS.domain).trim() || DEFAULT_SETTINGS.domain,
    allowed_users: allowedUsers,
    group_policy:
      String(settings.group_policy || DEFAULT_SETTINGS.group_policy).trim() || DEFAULT_SETTINGS.group_policy,
    require_mention:
      typeof settings.require_mention === "boolean"
        ? settings.require_mention
        : String(settings.require_mention || "true").trim().toLowerCase() !== "false",
  };
}

function summarizeSettings(settings = {}) {
  return {
    has_app_id: Boolean(String(settings.app_id || "").trim()),
    has_app_secret: Boolean(String(settings.app_secret || "").trim()),
    domain: String(settings.domain || DEFAULT_SETTINGS.domain).trim() || DEFAULT_SETTINGS.domain,
    allowed_user_count: Array.isArray(settings.allowed_users) ? settings.allowed_users.length : 0,
    group_policy: String(settings.group_policy || DEFAULT_SETTINGS.group_policy).trim() || DEFAULT_SETTINGS.group_policy,
    require_mention:
      typeof settings.require_mention === "boolean"
        ? settings.require_mention
        : String(settings.require_mention || "true").trim().toLowerCase() !== "false",
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

module.exports = {
  DEFAULT_SETTINGS,
  sanitizeSettings,
  summarizeSettings,
  normalizeSdkDomain,
  tryLoadFeishuSdk,
};
