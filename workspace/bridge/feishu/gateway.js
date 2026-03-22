"use strict";

const LOG_TAG = "[feishu/gateway]";
const FALLBACK_TOAST = {
  toast: { type: "info", content: "已收到，正在处理..." },
};

class FeishuGateway {
  constructor({ sdk, settings, logger = console }) {
    this.sdk = sdk;
    this.settings = settings;
    this.logger = logger;
    this.client = null;
    this.wsClient = null;
    this.eventDispatcher = null;
    this.messageHandler = null;
    this.cardActionHandler = null;
    this.running = false;
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

  async start() {
    if (this.running) return;
    const sdk = this.sdk;
    this.client = new sdk.Client({
      appId: this.settings.app_id,
      appSecret: this.settings.app_secret,
      domain: this.settings.sdk_domain,
    });
    this.wsClient = new sdk.WSClient({
      appId: this.settings.app_id,
      appSecret: this.settings.app_secret,
      domain: this.settings.sdk_domain,
      autoReconnect: true,
      loggerLevel: sdk.LoggerLevel?.info,
    });
    this.patchWsClientForCardCallbacks();
    this.eventDispatcher = new sdk.EventDispatcher({});
    const handlers = {};
    if (this.messageHandler) {
      handlers["im.message.receive_v1"] = async (event) => {
        await this.messageHandler(event);
      };
    }
    if (this.cardActionHandler) {
      handlers["card.action.trigger"] = async (event) => this.safeCardActionHandler(event);
    }
    if (Object.keys(handlers).length > 0) {
      this.eventDispatcher = this.eventDispatcher.register(handlers);
    }
    await this.wsClient.start({ eventDispatcher: this.eventDispatcher });
    this.running = true;
    this.logger.info?.(LOG_TAG, "connected");
  }

  async stop() {
    if (this.wsClient && typeof this.wsClient.close === "function") {
      await this.wsClient.close();
    }
    this.running = false;
    this.wsClient = null;
    this.eventDispatcher = null;
    this.client = null;
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
};
