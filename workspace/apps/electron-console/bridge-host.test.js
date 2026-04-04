"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const { createBridgeHost } = require("./bridge-host");

function makeTempWorkspace() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "bridge-host-test-"));
}

async function withClearedBridgeRootEnv(run) {
  const previous = process.env.WORKSPACE_HUB_FEISHU_BRIDGE_ROOT;
  delete process.env.WORKSPACE_HUB_FEISHU_BRIDGE_ROOT;
  try {
    return await run();
  } finally {
    if (previous === undefined) {
      delete process.env.WORKSPACE_HUB_FEISHU_BRIDGE_ROOT;
    } else {
      process.env.WORKSPACE_HUB_FEISHU_BRIDGE_ROOT = previous;
    }
  }
}

function writeServiceStub(root, { emitLease = false, routeCalls = [] } = {}) {
  const bridgeRoot = path.join(root, "bridge", "feishu");
  fs.mkdirSync(bridgeRoot, { recursive: true });
  fs.writeFileSync(
    path.join(bridgeRoot, "index.js"),
    `
"use strict";

const routeCalls = ${JSON.stringify(routeCalls)};

function createFeishuLongConnectionService({ brokerClient, runtimeState }) {
  return {
    async loadSettings() {},
    async connect() {
      await brokerClient.call("material-suggest", {
        project_name: "Codex Hub",
        prompt: "ping",
      });
      ${emitLease ? 'await runtimeState.saveBridgeExecutionLease({ conversation_key: "chat-lease", state: "running", session_id: "sess-lease-1" });' : ""}
      for (const routeCall of routeCalls) {
        await brokerClient.call(routeCall.command, routeCall.payload || {});
      }
      return { ok: true };
    },
    async disconnect() {
      return { ok: true };
    },
    async reconnect() {
      return { ok: true };
    },
    getStatus() {
      return { connection_status: "connected" };
    },
    async sendMessage() {
      return { ok: true };
    },
  };
}

module.exports = { createFeishuLongConnectionService };
`,
    "utf8",
  );
}

async function testBridgeHostPassesMaterialSuggestArgs() {
  await withClearedBridgeRootEnv(async () => {
    const workspaceRoot = makeTempWorkspace();
    writeServiceStub(workspaceRoot);
    const brokerCalls = [];
    const host = createBridgeHost({
      appRoot: workspaceRoot,
      workspaceRoot,
      logger: { info() {}, warn() {}, error() {} },
      async runBroker(args) {
        brokerCalls.push(args);
        const command = args[0];
        if (command === "bridge-settings") {
          return { ok: true, stdout: JSON.stringify({ settings: {} }) };
        }
        if (command === "bridge-connection") {
          return { ok: true, stdout: JSON.stringify({ ok: true }) };
        }
        if (command === "bridge-status") {
          return { ok: true, stdout: JSON.stringify({ bridge: "feishu", connection_status: "connected" }) };
        }
        if (command === "material-suggest") {
          return { ok: true, stdout: JSON.stringify({ ok: true, project_name: "Codex Hub" }) };
        }
        return { ok: true, stdout: JSON.stringify({ ok: true }) };
      },
    });

    const result = await host.connect();
    assert.equal(result.ok, true);

    const materialSuggest = brokerCalls.find((args) => args[0] === "material-suggest");
    assert.ok(materialSuggest, "expected material-suggest broker call");
    assert.deepEqual(materialSuggest.slice(0, 5), ["material-suggest", "--project-name", "Codex Hub", "--prompt", "ping"]);
  });
}

async function testBridgeHostForwardsExecutionLeaseBrokerCall() {
  await withClearedBridgeRootEnv(async () => {
    const workspaceRoot = makeTempWorkspace();
    writeServiceStub(workspaceRoot, { emitLease: true });
    const brokerCalls = [];
    const host = createBridgeHost({
      appRoot: workspaceRoot,
      workspaceRoot,
      logger: { info() {}, warn() {}, error() {} },
      async runBroker(args) {
        brokerCalls.push(args);
        const command = args[0];
        if (command === "bridge-settings") {
          return { ok: true, stdout: JSON.stringify({ settings: {} }) };
        }
        if (command === "bridge-connection") {
          return { ok: true, stdout: JSON.stringify({ ok: true }) };
        }
        if (command === "bridge-status") {
          return { ok: true, stdout: JSON.stringify({ bridge: "feishu", connection_status: "connected" }) };
        }
        return { ok: true, stdout: JSON.stringify({ ok: true }) };
      },
    });

    const result = await host.connect();
    assert.equal(result.ok, true);

    const leaseCall = brokerCalls.find((args) => args[0] === "bridge-execution-lease");
    assert.ok(leaseCall, "expected bridge-execution-lease broker call");
    assert.equal(leaseCall[leaseCall.indexOf("--conversation-key") + 1], "chat-lease");
  });
}

async function testBridgeHostForwardsApprovalTokenToApprovedCodexRoutes() {
  await withClearedBridgeRootEnv(async () => {
    const workspaceRoot = makeTempWorkspace();
    writeServiceStub(workspaceRoot, {
      routeCalls: [
        {
          command: "codex-exec",
          payload: {
            prompt: "请帮我 git push 当前分支",
            execution_profile: "feishu-approved",
            approval_token: "coco-allow-exec",
            source: "feishu",
          },
        },
        {
          command: "codex-resume",
          payload: {
            session_id: "sess-approved",
            prompt: "继续处理上一轮问题",
            execution_profile: "feishu-approved",
            approval_token: "coco-allow-resume",
            source: "feishu",
          },
        },
      ],
    });
    const brokerCalls = [];
    const host = createBridgeHost({
      appRoot: workspaceRoot,
      workspaceRoot,
      logger: { info() {}, warn() {}, error() {} },
      async runBroker(args) {
        brokerCalls.push(args);
        const command = args[0];
        if (command === "bridge-settings") {
          return { ok: true, stdout: JSON.stringify({ settings: {} }) };
        }
        if (command === "bridge-connection") {
          return { ok: true, stdout: JSON.stringify({ ok: true }) };
        }
        if (command === "bridge-status") {
          return { ok: true, stdout: JSON.stringify({ bridge: "feishu", connection_status: "connected" }) };
        }
        return { ok: true, stdout: JSON.stringify({ ok: true }) };
      },
    });

    const result = await host.connect();
    assert.equal(result.ok, true);

    const execCall = brokerCalls.find((args) => args[0] === "command-center" && args.includes("codex-exec"));
    assert.ok(execCall, "expected command-center codex-exec broker call");
    assert.ok(execCall.includes("--approval-token"));
    assert.equal(execCall[execCall.indexOf("--approval-token") + 1], "coco-allow-exec");

    const resumeCall = brokerCalls.find((args) => args[0] === "command-center" && args.includes("codex-resume"));
    assert.ok(resumeCall, "expected command-center codex-resume broker call");
    assert.ok(resumeCall.includes("--approval-token"));
    assert.equal(resumeCall[resumeCall.indexOf("--approval-token") + 1], "coco-allow-resume");
  });
}

async function testBridgeHostForwardsFeishuCallbackExecutorArgs() {
  await withClearedBridgeRootEnv(async () => {
    const workspaceRoot = makeTempWorkspace();
    writeServiceStub(workspaceRoot, {
      routeCalls: [
        {
          command: "feishu-callback-executor",
          payload: {
            action: "doc-create",
            payload_json: JSON.stringify({
              target: "oc_test",
              title: "Reply doc",
              content: "hello",
            }),
          },
        },
      ],
    });
    const brokerCalls = [];
    const host = createBridgeHost({
      appRoot: workspaceRoot,
      workspaceRoot,
      logger: { info() {}, warn() {}, error() {} },
      async runBroker(args) {
        brokerCalls.push(args);
        const command = args[0];
        if (command === "bridge-settings") {
          return { ok: true, stdout: JSON.stringify({ settings: {} }) };
        }
        if (command === "bridge-connection") {
          return { ok: true, stdout: JSON.stringify({ ok: true }) };
        }
        if (command === "bridge-status") {
          return { ok: true, stdout: JSON.stringify({ bridge: "feishu", connection_status: "connected" }) };
        }
        return { ok: true, stdout: JSON.stringify({ ok: true }) };
      },
    });

    const result = await host.connect();
    assert.equal(result.ok, true);

    const callbackCall = brokerCalls.find((args) => args[0] === "feishu-callback-executor");
    assert.ok(callbackCall, "expected feishu-callback-executor broker call");
    assert.deepEqual(callbackCall.slice(0, 5), [
      "feishu-callback-executor",
      "--action",
      "doc-create",
      "--payload-json",
      JSON.stringify({
        target: "oc_test",
        title: "Reply doc",
        content: "hello",
      }),
    ]);
  });
}

async function testBridgeHostPrefersLiveFreshnessOverBrokerSnapshotFlags() {
  await withClearedBridgeRootEnv(async () => {
    const workspaceRoot = makeTempWorkspace();
    const now = new Date().toISOString();
    const bridgeRoot = path.join(workspaceRoot, "bridge", "feishu");
    fs.mkdirSync(bridgeRoot, { recursive: true });
    fs.writeFileSync(
      path.join(bridgeRoot, "index.js"),
      `
"use strict";

function createFeishuLongConnectionService() {
  return {
    async loadSettings() {},
    async connect() { return { ok: true }; },
    async disconnect() { return { ok: true }; },
    async reconnect() { return { ok: true }; },
    getStatus() {
      return {
        connection_status: "connected",
        transport: "lark_cli_event_plus_cli_im_plus_sdk_card_callbacks",
        last_event_at: "2026-03-28T16:45:41Z",
        connected_at: ${JSON.stringify(now)},
        heartbeat_at: ${JSON.stringify(now)},
        stale_after_seconds: 90,
        event_idle_after_seconds: 1800,
      };
    },
    async sendMessage() { return { ok: true }; },
  };
}

module.exports = { createFeishuLongConnectionService };
`,
      "utf8",
    );
    const host = createBridgeHost({
      appRoot: workspaceRoot,
      workspaceRoot,
      logger: { info() {}, warn() {}, error() {} },
      async runBroker(args) {
        const command = args[0];
        if (command === "bridge-settings") {
          return { ok: true, stdout: JSON.stringify({ settings: {} }) };
        }
        if (command === "bridge-connection") {
          return { ok: true, stdout: JSON.stringify({ ok: true }) };
        }
        if (command === "bridge-status") {
          return {
            ok: true,
            stdout: JSON.stringify({
              bridge: "feishu",
              connection_status: "stale",
              transport: "lark_cli_event_plus_cli_im",
              last_event_at: "2026-03-28T16:45:41Z",
              heartbeat_at: "2026-03-29T15:42:57.534Z",
              stale: true,
              event_stalled: true,
              stale_after_seconds: 90,
              event_idle_after_seconds: 1800,
              metadata: {},
            }),
          };
        }
        return { ok: true, stdout: JSON.stringify({ ok: true }) };
      },
    });

    await host.connect();
    const current = await host.getStatus();
    assert.equal(current.ok, true);
    assert.equal(current.data.connection_status, "connected");
    assert.equal(current.data.event_stalled, false);
    assert.equal(current.data.stale, false);
    assert.equal(current.data.last_event_at, "2026-03-28T16:45:41Z");
    assert.equal(current.data.connected_at, now);
  });
}

async function testBridgeHostDoesNotTreatStatusWritesAsFreshEvents() {
  await withClearedBridgeRootEnv(async () => {
    const workspaceRoot = makeTempWorkspace();
    const fresh = new Date().toISOString();
    const staleEvent = "2026-03-28T16:45:41Z";
    const bridgeRoot = path.join(workspaceRoot, "bridge", "feishu");
    fs.mkdirSync(bridgeRoot, { recursive: true });
    fs.writeFileSync(
      path.join(bridgeRoot, "index.js"),
      `
"use strict";

function createFeishuLongConnectionService() {
  return {
    async loadSettings() {},
    async connect() { return { ok: true }; },
    async disconnect() { return { ok: true }; },
    async reconnect() { return { ok: true }; },
    getStatus() {
      return {
        connection_status: "connected",
        transport: "lark_cli_event_plus_cli_im",
        last_event_at: ${JSON.stringify(staleEvent)},
        connected_at: ${JSON.stringify(staleEvent)},
        heartbeat_at: ${JSON.stringify(fresh)},
        updated_at: ${JSON.stringify(fresh)},
        stale_after_seconds: 90,
        event_idle_after_seconds: 1800,
      };
    },
    async sendMessage() { return { ok: true }; },
  };
}

module.exports = { createFeishuLongConnectionService };
`,
      "utf8",
    );
    const host = createBridgeHost({
      appRoot: workspaceRoot,
      workspaceRoot,
      logger: { info() {}, warn() {}, error() {} },
      async runBroker(args) {
        const command = args[0];
        if (command === "bridge-settings") {
          return { ok: true, stdout: JSON.stringify({ settings: {} }) };
        }
        if (command === "bridge-connection") {
          return { ok: true, stdout: JSON.stringify({ ok: true }) };
        }
        if (command === "bridge-status") {
          return {
            ok: true,
            stdout: JSON.stringify({
              bridge: "feishu",
              connection_status: "stale",
              transport: "lark_cli_event_plus_cli_im",
              last_event_at: staleEvent,
              heartbeat_at: fresh,
              stale: true,
              event_stalled: true,
              stale_after_seconds: 90,
              event_idle_after_seconds: 1800,
              metadata: {},
            }),
          };
        }
        return { ok: true, stdout: JSON.stringify({ ok: true }) };
      },
    });

    await host.connect();
    const current = await host.getStatus();
    assert.equal(current.ok, true);
    assert.equal(current.data.connection_status, "connected");
    assert.equal(current.data.event_stalled, true);
    assert.equal(current.data.stale, true);
    assert.equal(current.data.last_event_at, staleEvent);
    assert.equal(current.data.updated_at, fresh);
  });
}

async function testBridgeHostDoesNotRegressPersistedBridgeTimestamps() {
  await withClearedBridgeRootEnv(async () => {
    const workspaceRoot = makeTempWorkspace();
    const fresh = "2026-03-29T15:46:53.508Z";
    const stale = "2026-03-28T16:02:05.709Z";
    const bridgeRoot = path.join(workspaceRoot, "bridge", "feishu");
    fs.mkdirSync(bridgeRoot, { recursive: true });
    fs.writeFileSync(
      path.join(bridgeRoot, "index.js"),
      `
"use strict";

function createFeishuLongConnectionService({ runtimeState }) {
  return {
    async loadSettings() {},
    async connect() {
      await runtimeState.saveBridgeStatus({
        connection_status: "connected",
        transport: "lark_cli_event_plus_cli_im_plus_sdk_card_callbacks",
        connected_at: ${JSON.stringify(fresh)},
        last_event_at: ${JSON.stringify(fresh)},
        heartbeat_at: ${JSON.stringify(fresh)},
        recent_message_count: 76,
        recent_reply_count: 91,
        backfill_degraded: false,
        backfill_degraded_count: 0,
        last_backfill_error: "",
        last_backfill_error_at: "",
        stale_after_seconds: 90,
        event_idle_after_seconds: 1800,
      });
      await runtimeState.saveBridgeStatus({
        connection_status: "connected",
        transport: "lark_cli_event_plus_cli_im",
        connected_at: ${JSON.stringify(stale)},
        last_event_at: ${JSON.stringify(stale)},
        heartbeat_at: "2026-03-29T15:47:27.547Z",
        recent_message_count: 75,
        recent_reply_count: 90,
        backfill_degraded: true,
        backfill_degraded_count: 2,
        last_backfill_error: "bot:permission_denied; user:missing_scope",
        last_backfill_error_at: "2026-03-29T15:47:30.000Z",
        stale_after_seconds: 90,
        event_idle_after_seconds: 1800,
      });
      return { ok: true };
    },
    async disconnect() { return { ok: true }; },
    async reconnect() { return { ok: true }; },
    getStatus() {
      return {
        connection_status: "connected",
        transport: "lark_cli_event_plus_cli_im_plus_sdk_card_callbacks",
        connected_at: ${JSON.stringify(fresh)},
        last_event_at: ${JSON.stringify(fresh)},
        heartbeat_at: "2026-03-29T15:47:27.547Z",
        recent_message_count: 76,
        recent_reply_count: 91,
        backfill_degraded: true,
        backfill_degraded_count: 2,
        last_backfill_error: "bot:permission_denied; user:missing_scope",
        last_backfill_error_at: "2026-03-29T15:47:30.000Z",
        stale_after_seconds: 90,
        event_idle_after_seconds: 1800,
      };
    },
    async sendMessage() { return { ok: true }; },
  };
}

module.exports = { createFeishuLongConnectionService };
`,
      "utf8",
    );
    const bridgeConnectionCalls = [];
    const host = createBridgeHost({
      appRoot: workspaceRoot,
      workspaceRoot,
      logger: { info() {}, warn() {}, error() {} },
      async runBroker(args) {
        const command = args[0];
        if (command === "bridge-settings") {
          return { ok: true, stdout: JSON.stringify({ settings: {} }) };
        }
        if (command === "bridge-connection") {
          const payload = JSON.parse(args[args.indexOf("--connection-json") + 1]);
          bridgeConnectionCalls.push(payload);
          return { ok: true, stdout: JSON.stringify({ ok: true }) };
        }
        if (command === "bridge-status") {
          return { ok: true, stdout: JSON.stringify({ bridge: "feishu", connection_status: "connected" }) };
        }
        return { ok: true, stdout: JSON.stringify({ ok: true }) };
      },
    });

    const result = await host.connect();
    assert.equal(result.ok, true);
    assert.ok(bridgeConnectionCalls.length >= 2, "expected multiple bridge-connection writes");
    const lastCall = bridgeConnectionCalls[bridgeConnectionCalls.length - 1];
    assert.equal(lastCall.last_event_at, fresh);
    assert.equal(lastCall.metadata.connected_at, fresh);
    assert.equal(lastCall.metadata.heartbeat_at, "2026-03-29T15:47:27.547Z");
    assert.equal(lastCall.metadata.recent_message_count, 76);
    assert.equal(lastCall.metadata.recent_reply_count, 91);
    assert.equal(lastCall.metadata.backfill_degraded, true);
    assert.equal(lastCall.metadata.backfill_degraded_count, 2);
    assert.equal(lastCall.metadata.last_backfill_error, "bot:permission_denied; user:missing_scope");
    assert.equal(lastCall.metadata.last_backfill_error_at, "2026-03-29T15:47:30.000Z");
  });
}

async function testBridgeHostSendMessageStaysOutboundOnlyWhenDisconnected() {
  await withClearedBridgeRootEnv(async () => {
    const workspaceRoot = makeTempWorkspace();
    const bridgeRoot = path.join(workspaceRoot, "bridge", "feishu");
    fs.mkdirSync(bridgeRoot, { recursive: true });
    fs.writeFileSync(
      path.join(bridgeRoot, "index.js"),
      `
"use strict";

let connectCount = 0;
let sendCount = 0;

function createFeishuLongConnectionService() {
  return {
    async loadSettings() {},
    async connect() {
      connectCount += 1;
      return { ok: true };
    },
    async disconnect() { return { ok: true }; },
    async reconnect() { return { ok: true }; },
    getStatus() {
      return { connection_status: "disconnected" };
    },
    async sendMessage(payload) {
      sendCount += 1;
      return { ok: true, delivery: payload, connectCount, sendCount };
    },
  };
}

module.exports = { createFeishuLongConnectionService };
`,
      "utf8",
    );

    const host = createBridgeHost({
      appRoot: workspaceRoot,
      workspaceRoot,
      logger: { info() {}, warn() {}, error() {} },
      async runBroker(args) {
        const command = args[0];
        if (command === "bridge-settings") {
          return { ok: true, stdout: JSON.stringify({ settings: {} }) };
        }
        if (command === "bridge-status") {
          return { ok: true, stdout: JSON.stringify({ bridge: "feishu", connection_status: "disconnected" }) };
        }
        if (command === "bridge-connection") {
          return { ok: true, stdout: JSON.stringify({ ok: true }) };
        }
        return { ok: true, stdout: JSON.stringify({ ok: true }) };
      },
    });

    const result = await host.sendMessage({
      chatRef: "oc_outbound_only",
      text: "只发一条诊断消息",
      phase: "report",
    });

    assert.equal(result.ok, true);
    assert.equal(result.result.delivery.chatId, "oc_outbound_only");
    assert.equal(result.result.connectCount, 0);
    assert.equal(result.result.sendCount, 1);
  });
}

async function main() {
  await testBridgeHostPassesMaterialSuggestArgs();
  await testBridgeHostForwardsExecutionLeaseBrokerCall();
  await testBridgeHostForwardsApprovalTokenToApprovedCodexRoutes();
  await testBridgeHostForwardsFeishuCallbackExecutorArgs();
  await testBridgeHostPrefersLiveFreshnessOverBrokerSnapshotFlags();
  await testBridgeHostDoesNotTreatStatusWritesAsFreshEvents();
  await testBridgeHostDoesNotRegressPersistedBridgeTimestamps();
  await testBridgeHostSendMessageStaysOutboundOnlyWhenDisconnected();
  console.log("ok");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});