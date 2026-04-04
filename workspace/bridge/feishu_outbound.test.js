"use strict";

const assert = require("node:assert/strict");
const {
  buildMetricDigestCardPayload,
  buildReplyCardPayload,
  optimizeMarkdown,
  phaseCardMeta,
} = require("./feishu/outbound");

function matchCount(text, pattern) {
  const matches = String(text || "").match(pattern);
  return matches ? matches.length : 0;
}

async function testBuildReplyCardPayloadIncludesDocLinkAndFooter() {
  const card = buildReplyCardPayload({
    phase: "report",
    text: "# 标题\n\n结论：升级已完成。\n\n细节说明一。\n细节说明二。",
    docUrl: "https://feishu.cn/docx/doc_123",
    docTitle: "完整报告",
    footer: "已完成 · 12s",
  });
  assert.equal(card.header.template, "blue");
  const elements = card.body.elements;
  const rendered = JSON.stringify(elements);
  assert.match(rendered, /结论：升级已完成/);
  assert.equal(matchCount(rendered, /结论：升级已完成/g), 1);
  assert.match(rendered, /完整报告/);
  assert.match(rendered, /doc_123/);
  assert.match(rendered, /已完成 · 12s/);
}

async function testBuildReplyCardPayloadAvoidsDuplicateSummaryWithoutDocLink() {
  const card = buildReplyCardPayload({
    phase: "final",
    text: "明天是 2026 年 3 月 31 日，周二。\n\n偏凉，建议带折叠伞。\n\n带一把折叠伞最稳。",
  });
  const elements = card.body.elements;
  const rendered = JSON.stringify(elements);
  assert.equal(elements[0]?.content, "**已完成**");
  assert.equal(matchCount(rendered, /偏凉，建议带折叠伞/g), 1);
}

async function testBuildMetricDigestCardPayloadIncludesMetrics() {
  const card = buildMetricDigestCardPayload({
    title: "CoCo 状态摘要",
    summary: "当前运营摘要",
    metrics: [
      { label: "总任务数", value: "45" },
      { label: "Doing", value: "7" },
      { label: "Blocked", value: "2" },
    ],
  });
  assert.equal(card.header.title.content, "CoCo 状态摘要");
  const rendered = JSON.stringify(card.body.elements);
  assert.match(rendered, /总任务数/);
  assert.match(rendered, /Doing/);
  assert.match(rendered, /Blocked/);
}

async function testOptimizeMarkdownCompactsLocalImageAndHeaders() {
  const optimized = optimizeMarkdown("# 一级标题\n\n![图](/tmp/test.png)\n\n## 二级");
  assert.match(optimized, /#### 一级标题/);
  assert.match(optimized, /!\[图\]\(\/tmp\/test\.png\)/);
  assert.match(optimized, /##### 二级/);
}

async function testPhaseCardMetaCoversCompletionAndErrors() {
  assert.deepEqual(phaseCardMeta("final"), {
    title: "CoCo 处理结果",
    template: "green",
    statusLabel: "已完成",
  });
  assert.deepEqual(phaseCardMeta("error"), {
    title: "CoCo 执行异常",
    template: "red",
    statusLabel: "失败",
  });
}

async function main() {
  await testBuildReplyCardPayloadIncludesDocLinkAndFooter();
  await testBuildReplyCardPayloadAvoidsDuplicateSummaryWithoutDocLink();
  await testBuildMetricDigestCardPayloadIncludesMetrics();
  await testOptimizeMarkdownCompactsLocalImageAndHeaders();
  await testPhaseCardMetaCoversCompletionAndErrors();
  console.log("ok");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
