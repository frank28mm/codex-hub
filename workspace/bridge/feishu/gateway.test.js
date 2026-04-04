"use strict";

const assert = require("node:assert/strict");
const { EventEmitter } = require("node:events");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const {
  FeishuGateway,
  listChatMessagesViaLarkCli,
  normalizeCliEventEnvelope,
} = require("./gateway");

function makeSdk() {
  class FakeEventDispatcher {
    register(handlers) {
      this.handlers = handlers;
      return this;
    }
  }
  class FakeWSClient {
    constructor() {
      this.started = false;
    }
    async start() {
      this.started = true;
    }
    async close() {
      this.started = false;
    }
  }
  class FakeClient {
    constructor() {
      this.im = {
        v1: {
          message: {
            create: async () => ({ data: { message_id: "om_sdk_1" } }),
          },
        },
      };
      this.cardkit = {
        v2: {
          card: {
            create: async () => ({ data: { card_id: "card_1" } }),
            update: async () => ({}),
            streamContent: async () => ({}),
            setStreamingMode: async () => ({}),
          },
        },
      };
    }
  }
  return {
    Client: FakeClient,
    WSClient: FakeWSClient,
    EventDispatcher: FakeEventDispatcher,
    LoggerLevel: { info: "info" },
  };
}

function makeReadlineModule() {
  return {
    createInterface({ input }) {
      return {
        on(event, handler) {
          if (event === "line") {
            input.on("line", handler);
          }
        },
        close() {},
      };
    },
  };
}

async function testNormalizeCliEventEnvelope() {
  const event = normalizeCliEventEnvelope({
    header: { event_type: "im.message.receive_v1" },
    event: {
      message: { message_id: "om_1", content: "{\"text\":\"hello\"}", message_type: "text" },
      sender: { sender_id: { open_id: "ou_1" } },
    },
  });
  assert.equal(event.event_type, "im.message.receive_v1");
  assert.equal(event.message.message_id, "om_1");
  assert.equal(event.sender.sender_id.open_id, "ou_1");
}

async function testGatewayUsesCliEventTransportForMessages() {
  const previousEvent = process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT;
  const previousIm = process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT;
  process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT = "1";
  process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT = "0";
  try {
    let childRef = null;
    const spawnProcess = () => {
      const child = new EventEmitter();
      child.stdout = new EventEmitter();
      child.stderr = new EventEmitter();
      child.kill = () => {
        child.killed = true;
      };
      child.killed = false;
      childRef = child;
      return child;
    };
    const seen = [];
    const gateway = new FeishuGateway({
      sdk: makeSdk(),
      settings: { app_id: "cli_test", app_secret: "secret", sdk_domain: 0 },
      logger: { info() {}, warn() {}, error() {} },
      spawnProcess,
      readlineModule: makeReadlineModule(),
    });
    gateway.registerMessageHandler(async (event) => {
      seen.push(event);
    });

    await gateway.start();
    assert.equal(gateway.getTransport(), "lark_cli_event_plus_sdk_rest");
    childRef.stdout.emit(
      "line",
      JSON.stringify({
        header: { event_type: "im.message.receive_v1" },
        event: {
          message: {
            message_id: "om_cli_1",
            content: "{\"text\":\"hello from cli\"}",
            message_type: "text",
          },
          sender: { sender_id: { open_id: "ou_cli" } },
        },
      }),
    );
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(seen.length, 1);
    assert.equal(seen[0].event_type, "im.message.receive_v1");
    assert.equal(seen[0].message.message_id, "om_cli_1");
    await gateway.stop();
  } finally {
    if (previousEvent === undefined) {
      delete process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT;
    } else {
      process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT = previousEvent;
    }
    if (previousIm === undefined) {
      delete process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT;
    } else {
      process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT = previousIm;
    }
  }
}

async function testGatewayCliEventModeDoesNotStartSdkWsWhenCliIngressOwnsEvents() {
  const previousEvent = process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT;
  const previousIm = process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT;
  process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT = "1";
  process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT = "0";
  try {
    let lastWsClient = null;
    const sdk = makeSdk();
    const OriginalWSClient = sdk.WSClient;
    sdk.WSClient = class extends OriginalWSClient {
      constructor(...args) {
        super(...args);
        lastWsClient = this;
      }
      async start({ eventDispatcher } = {}) {
        this.eventDispatcher = eventDispatcher;
        return super.start({ eventDispatcher });
      }
    };
    const seen = [];
    const gateway = new FeishuGateway({
      sdk,
      settings: { app_id: "cli_test", app_secret: "secret", sdk_domain: 0 },
      logger: { info() {}, warn() {}, error() {} },
      spawnProcess: () => {
        const child = new EventEmitter();
        child.stdout = new EventEmitter();
        child.stderr = new EventEmitter();
        child.kill = () => {
          child.killed = true;
        };
        child.killed = false;
        return child;
      },
      readlineModule: makeReadlineModule(),
    });
    gateway.registerMessageHandler(async (event) => {
      seen.push(event);
    });
    gateway.registerCardActionHandler(async () => ({ toast: { type: "info", content: "ok" } }));

    await gateway.start();
    assert.equal(lastWsClient, null);
    assert.equal(gateway.getTransport(), "lark_cli_event_plus_sdk_rest");
    await gateway.stop();
  } finally {
    if (previousEvent === undefined) {
      delete process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT;
    } else {
      process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT = previousEvent;
    }
    if (previousIm === undefined) {
      delete process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT;
    } else {
      process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT = previousIm;
    }
  }
}

async function testGatewayCliEventModeStillDeliversMessagesFromCliWhenCardCallbacksExist() {
  const previousEvent = process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT;
  const previousIm = process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT;
  process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT = "1";
  process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT = "0";
  try {
    let childRef = null;
    let lastWsClient = null;
    const sdk = makeSdk();
    const OriginalWSClient = sdk.WSClient;
    sdk.WSClient = class extends OriginalWSClient {
      constructor(...args) {
        super(...args);
        lastWsClient = this;
      }
      async start({ eventDispatcher } = {}) {
        this.eventDispatcher = eventDispatcher;
        return super.start({ eventDispatcher });
      }
    };
    const spawnProcess = () => {
      const child = new EventEmitter();
      child.stdout = new EventEmitter();
      child.stderr = new EventEmitter();
      child.kill = () => {
        child.killed = true;
      };
      child.killed = false;
      childRef = child;
      return child;
    };
    const seen = [];
    const gateway = new FeishuGateway({
      sdk,
      settings: { app_id: "cli_test", app_secret: "secret", sdk_domain: 0 },
      logger: { info() {}, warn() {}, error() {} },
      spawnProcess,
      readlineModule: makeReadlineModule(),
    });
    gateway.registerMessageHandler(async (event) => {
      seen.push(event);
    });
    gateway.registerCardActionHandler(async () => ({ toast: { type: "info", content: "ok" } }));

    await gateway.start();
    assert.equal(lastWsClient, null);
    childRef.stdout.emit(
      "line",
      JSON.stringify({
        header: { event_type: "im.message.receive_v1" },
        event: {
          message: { message_id: "om_cli_2", content: "{\"text\":\"hello\"}", message_type: "text" },
          sender: { sender_id: { open_id: "ou_dup" } },
        },
      }),
    );
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(seen.length, 1);
    assert.equal(seen[0].message.message_id, "om_cli_2");
    await gateway.stop();
  } finally {
    if (previousEvent === undefined) {
      delete process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT;
    } else {
      process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT = previousEvent;
    }
    if (previousIm === undefined) {
      delete process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT;
    } else {
      process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT = previousIm;
    }
  }
}

async function testGatewayTerminatesCompetingCliSubscriberBeforeStarting() {
  const previousEvent = process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT;
  const previousIm = process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT;
  process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT = "1";
  process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT = "0";
  try {
    const commands = [];
    const execFileAsyncImpl = async (file, args) => {
      commands.push([file, ...args]);
      if (file === "pgrep") {
        return {
          stdout:
            "70409 /opt/lark-cli/bin/lark-cli event +subscribe --as bot --event-types im.message.receive_v1 --quiet\n",
        };
      }
      if (file === "kill") {
        return { stdout: "" };
      }
      throw new Error(`unexpected exec command: ${file}`);
    };
    let childRef = null;
    const spawnProcess = () => {
      const child = new EventEmitter();
      child.stdout = new EventEmitter();
      child.stderr = new EventEmitter();
      child.kill = () => {
        child.killed = true;
      };
      child.killed = false;
      childRef = child;
      return child;
    };
    const gateway = new FeishuGateway({
      sdk: makeSdk(),
      settings: { app_id: "cli_test", app_secret: "secret", sdk_domain: 0 },
      logger: { info() {}, warn() {}, error() {} },
      spawnProcess,
      readlineModule: makeReadlineModule(),
      execFileAsyncImpl,
      waitImpl: async () => {},
    });
    gateway.registerMessageHandler(async () => {});

    await gateway.start();
    assert.ok(childRef);
    assert.deepEqual(commands[0], [
      "pgrep",
      "-af",
      "lark-cli event \\+subscribe --as bot --event-types",
    ]);
    assert.deepEqual(commands[1], ["kill", "-TERM", "70409"]);
    await gateway.stop();
  } finally {
    if (previousEvent === undefined) {
      delete process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT;
    } else {
      process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT = previousEvent;
    }
    if (previousIm === undefined) {
      delete process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT;
    } else {
      process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT = previousIm;
    }
  }
}

async function testGatewayRemovesStaleCliSubscribeLockWithoutCompetingProcess() {
  const previousEvent = process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT;
  const previousIm = process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT;
  const previousHome = process.env.HOME;
  process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT = "1";
  process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT = "0";
  const tempHome = fs.mkdtempSync(path.join(os.tmpdir(), "codex-gateway-home-"));
  process.env.HOME = tempHome;
  try {
    const lockDir = path.join(tempHome, ".lark-cli", "locks");
    fs.mkdirSync(lockDir, { recursive: true });
    const lockPath = path.join(lockDir, "subscribe_cli_test.lock");
    fs.writeFileSync(lockPath, "", "utf8");
    const commands = [];
    const execFileAsyncImpl = async (file, args) => {
      commands.push([file, ...args]);
      if (file === "pgrep") {
        const error = new Error("not found");
        error.code = 1;
        throw error;
      }
      throw new Error(`unexpected exec command: ${file}`);
    };
    let childRef = null;
    const spawnProcess = () => {
      const child = new EventEmitter();
      child.stdout = new EventEmitter();
      child.stderr = new EventEmitter();
      child.kill = () => {
        child.killed = true;
      };
      child.killed = false;
      childRef = child;
      return child;
    };
    const gateway = new FeishuGateway({
      sdk: makeSdk(),
      settings: { app_id: "cli_test", app_secret: "secret", sdk_domain: 0 },
      logger: { info() {}, warn() {}, error() {} },
      spawnProcess,
      readlineModule: makeReadlineModule(),
      execFileAsyncImpl,
      waitImpl: async () => {},
    });
    gateway.registerMessageHandler(async () => {});

    await gateway.start();
    assert.ok(childRef);
    assert.deepEqual(commands[0], [
      "pgrep",
      "-af",
      "lark-cli event \\+subscribe --as bot --event-types",
    ]);
    assert.equal(fs.existsSync(lockPath), false);
    await gateway.stop();
  } finally {
    if (previousEvent === undefined) {
      delete process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT;
    } else {
      process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT = previousEvent;
    }
    if (previousIm === undefined) {
      delete process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT;
    } else {
      process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT = previousIm;
    }
    if (previousHome === undefined) {
      delete process.env.HOME;
    } else {
      process.env.HOME = previousHome;
    }
    fs.rmSync(tempHome, { recursive: true, force: true });
  }
}

async function testGatewayInvokesCliEventExitHandlerForUnexpectedExit() {
  const previousEvent = process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT;
  const previousIm = process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT;
  process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT = "1";
  process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT = "0";
  try {
    let childRef = null;
    const exits = [];
    const spawnProcess = () => {
      const child = new EventEmitter();
      child.stdout = new EventEmitter();
      child.stderr = new EventEmitter();
      child.kill = () => {
        child.killed = true;
      };
      child.killed = false;
      childRef = child;
      return child;
    };
    const gateway = new FeishuGateway({
      sdk: makeSdk(),
      settings: { app_id: "cli_test", app_secret: "secret", sdk_domain: 0 },
      logger: { info() {}, warn() {}, error() {} },
      spawnProcess,
      readlineModule: makeReadlineModule(),
    });
    gateway.registerMessageHandler(async () => {});
    gateway.registerCliEventExitHandler(async (payload) => {
      exits.push(payload);
    });

    await gateway.start();
    childRef.emit("exit", 1, "SIGTERM");
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.deepEqual(exits, [{ code: 1, signal: "SIGTERM" }]);
  } finally {
    if (previousEvent === undefined) {
      delete process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT;
    } else {
      process.env.WORKSPACE_HUB_FEISHU_CLI_EVENT_TRANSPORT = previousEvent;
    }
    if (previousIm === undefined) {
      delete process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT;
    } else {
      process.env.WORKSPACE_HUB_FEISHU_CLI_IM_TRANSPORT = previousIm;
    }
  }
}

async function testListChatMessagesViaLarkCliParsesMessagesArray() {
  const items = await listChatMessagesViaLarkCli({
    chatId: "oc_history",
    pageSize: 5,
    execFileAsyncImpl: async () => ({
      stdout: JSON.stringify({
        data: {
          messages: [
            {
              message_id: "om_history_1",
              content: "hello",
            },
          ],
        },
      }),
    }),
  });
  assert.equal(items.length, 1);
  assert.equal(items[0].message_id, "om_history_1");
}

async function testListChatMessagesViaLarkCliFallsBackToItemsArray() {
  const items = await listChatMessagesViaLarkCli({
    chatId: "oc_history",
    pageSize: 5,
    execFileAsyncImpl: async () => ({
      stdout: JSON.stringify({
        data: {
          items: [
            {
              message_id: "om_history_1",
              body: { content: "{\"text\":\"hello\"}" },
            },
          ],
        },
      }),
    }),
  });
  assert.equal(items.length, 1);
  assert.equal(items[0].message_id, "om_history_1");
}

async function main() {
  await testNormalizeCliEventEnvelope();
  await testGatewayUsesCliEventTransportForMessages();
  await testGatewayCliEventModeDoesNotStartSdkWsWhenCliIngressOwnsEvents();
  await testGatewayCliEventModeStillDeliversMessagesFromCliWhenCardCallbacksExist();
  await testGatewayTerminatesCompetingCliSubscriberBeforeStarting();
  await testGatewayRemovesStaleCliSubscribeLockWithoutCompetingProcess();
  await testGatewayInvokesCliEventExitHandlerForUnexpectedExit();
  await testListChatMessagesViaLarkCliParsesMessagesArray();
  await testListChatMessagesViaLarkCliFallsBackToItemsArray();
  console.log("ok");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
