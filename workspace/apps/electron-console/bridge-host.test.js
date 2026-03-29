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

async function main() {
  await testBridgeHostPassesMaterialSuggestArgs();
  await testBridgeHostForwardsExecutionLeaseBrokerCall();
  await testBridgeHostForwardsApprovalTokenToApprovedCodexRoutes();
  await testBridgeHostForwardsFeishuCallbackExecutorArgs();
  console.log("ok");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
