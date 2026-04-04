"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {
  captureSourceFingerprint,
  diffSourceFingerprint,
  deriveResponseDelayedState,
  mergeBridgeStatusWithThreadSummary,
  shouldAutoReconnectBridge,
  shouldDeferIdleReconnect,
  summarizeThreadRows,
} = require("./coco-bridge-service");

function testFailedAttentionThreadDoesNotCountAsResponseDelayed() {
  const summary = summarizeThreadRows([
    {
      stale_thread: false,
      needs_attention: true,
      attention_reason: "last_execution_failed",
      execution_state: "failed",
      pending_request: false,
      awaiting_report: false,
      ack_pending: false,
      thread_label: "Codex Hub",
      last_message_at: new Date(Date.now() - 10_000).toISOString(),
    },
  ]);
  assert.equal(summary.attention_threads, 1);
  assert.equal(summary.response_delayed_threads, 0);
  assert.equal(summary.last_response_delayed_thread_label, "");

  const responseState = deriveResponseDelayedState(summary);
  assert.equal(responseState.response_delayed, false);
  assert.equal(responseState.response_delayed_age_seconds, 0);
  assert.equal(responseState.response_delayed_thread_label, "");
}

function testRunningAttentionThreadCountsAsResponseDelayed() {
  const summary = summarizeThreadRows([
    {
      stale_thread: false,
      needs_attention: true,
      attention_reason: "response_delayed",
      execution_state: "running",
      pending_request: true,
      awaiting_report: false,
      ack_pending: false,
      thread_label: "增长与营销",
      last_message_at: new Date(Date.now() - 130_000).toISOString(),
    },
  ]);
  assert.equal(summary.response_delayed_threads, 1);
  assert.equal(summary.last_response_delayed_thread_label, "增长与营销");

  const responseState = deriveResponseDelayedState(summary);
  assert.equal(responseState.response_delayed, true);
  assert.equal(responseState.response_delayed_thread_label, "增长与营销");
  assert.ok(responseState.response_delayed_age_seconds >= 120);
}

function testProgressStalledThreadDoesNotCountAsResponseDelayed() {
  const summary = summarizeThreadRows([
    {
      stale_thread: false,
      needs_attention: true,
      attention_reason: "progress_stalled",
      execution_state: "running",
      pending_request: false,
      awaiting_report: true,
      ack_pending: false,
      thread_label: "CoCo 私聊",
      last_message_at: new Date(Date.now() - 200_000).toISOString(),
    },
  ]);
  assert.equal(summary.attention_threads, 1);
  assert.equal(summary.response_delayed_threads, 0);
  assert.equal(summary.last_response_delayed_thread_label, "");

  const responseState = deriveResponseDelayedState(summary);
  assert.equal(responseState.response_delayed, false);
}

function testResponseDelayedWarningDoesNotTriggerBridgeReconnect() {
  const summary = summarizeThreadRows([
    {
      stale_thread: false,
      needs_attention: true,
      attention_reason: "response_delayed",
      execution_state: "pending",
      pending_request: true,
      awaiting_report: false,
      ack_pending: false,
      thread_label: "Codex Hub",
      last_message_at: new Date(Date.now() - 130_000).toISOString(),
    },
  ]);
  const bridgeState = {
    connection_status: "connected",
    stale: false,
    event_stalled: false,
    ack_stalled: false,
    ...deriveResponseDelayedState(summary),
  };
  assert.equal(bridgeState.response_delayed, true);
  assert.equal(shouldAutoReconnectBridge(bridgeState), false);
}

function testMergedBridgeStateCarriesLiveThreadCountsForReconnectDecisions() {
  const summary = summarizeThreadRows([
    {
      stale_thread: false,
      needs_attention: true,
      attention_reason: "response_delayed",
      execution_state: "running",
      pending_request: true,
      awaiting_report: true,
      ack_pending: false,
      thread_label: "Codex Hub",
      last_message_at: new Date(Date.now() - 130_000).toISOString(),
    },
  ]);
  const bridgeState = mergeBridgeStatusWithThreadSummary(
    {
      connection_status: "connected",
      stale: true,
      event_stalled: true,
      ack_stalled: false,
    },
    summary,
  );
  assert.equal(bridgeState.running_threads, 1);
  assert.equal(bridgeState.attention_threads, 1);
  assert.equal(bridgeState.response_delayed, true);
  assert.equal(shouldAutoReconnectBridge(bridgeState), true);
}

function testIdleEventStalledTriggersReconnectEligibility() {
  const bridgeState = {
    connection_status: "connected",
    stale: false,
    event_stalled: true,
    ack_stalled: false,
    running_threads: 0,
    approval_pending_threads: 0,
    attention_threads: 0,
  };
  assert.equal(shouldAutoReconnectBridge(bridgeState), true);
}

function testIdleStaleTriggersReconnectEligibility() {
  const bridgeState = {
    connection_status: "connected",
    stale: true,
    event_stalled: false,
    ack_stalled: false,
    running_threads: 0,
    approval_pending_threads: 0,
    attention_threads: 0,
  };
  assert.equal(shouldAutoReconnectBridge(bridgeState), true);
}

function testIdleReconnectCooldownDefersFreshRetry() {
  const now = Date.now();
  const bridgeState = {
    connection_status: "connected",
    stale: true,
    event_stalled: true,
    ack_stalled: false,
    running_threads: 0,
    approval_pending_threads: 0,
    attention_threads: 0,
    event_idle_after_seconds: 1800,
  };
  const serviceState = {
    last_reconnect_attempt_at: new Date(now - 5 * 60_000).toISOString(),
  };
  assert.equal(shouldDeferIdleReconnect(bridgeState, serviceState, now), true);
}

function testIdleReconnectCooldownAllowsRetryAfterWindow() {
  const now = Date.now();
  const bridgeState = {
    connection_status: "connected",
    stale: true,
    event_stalled: true,
    ack_stalled: false,
    running_threads: 0,
    approval_pending_threads: 0,
    attention_threads: 0,
    event_idle_after_seconds: 1800,
  };
  const serviceState = {
    last_reconnect_attempt_at: new Date(now - 31 * 60_000).toISOString(),
  };
  assert.equal(shouldDeferIdleReconnect(bridgeState, serviceState, now), false);
}

function testIdleAttentionThreadDoesNotForceImmediateReconnect() {
  const now = Date.now();
  const bridgeState = {
    connection_status: "connected",
    stale: false,
    event_stalled: true,
    ack_stalled: false,
    running_threads: 0,
    approval_pending_threads: 0,
    attention_threads: 1,
    response_delayed_threads: 0,
    event_idle_after_seconds: 1800,
  };
  const serviceState = {
    last_reconnect_attempt_at: new Date(now - 5 * 60_000).toISOString(),
  };
  assert.equal(shouldDeferIdleReconnect(bridgeState, serviceState, now), true);
}

function testSourceFingerprintDetectsCodeChanges() {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "coco-bridge-service-test-"));
  const filePath = path.join(tempRoot, "watched.js");
  fs.writeFileSync(filePath, "console.log('v1');\n", "utf8");
  const before = captureSourceFingerprint([filePath]);
  fs.writeFileSync(filePath, "console.log('v2');\n", "utf8");
  const after = captureSourceFingerprint([filePath]);
  assert.deepEqual(diffSourceFingerprint(before, after), [filePath]);
}

function main() {
  testFailedAttentionThreadDoesNotCountAsResponseDelayed();
  testRunningAttentionThreadCountsAsResponseDelayed();
  testProgressStalledThreadDoesNotCountAsResponseDelayed();
  testResponseDelayedWarningDoesNotTriggerBridgeReconnect();
  testMergedBridgeStateCarriesLiveThreadCountsForReconnectDecisions();
  testIdleEventStalledTriggersReconnectEligibility();
  testIdleStaleTriggersReconnectEligibility();
  testIdleReconnectCooldownDefersFreshRetry();
  testIdleReconnectCooldownAllowsRetryAfterWindow();
  testIdleAttentionThreadDoesNotForceImmediateReconnect();
  testSourceFingerprintDetectsCodeChanges();
  console.log("ok");
}

main();
