#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';

const require = createRequire(import.meta.url);

async function importRuntimeCandidate(candidate) {
  if (!candidate) return null;
  try {
    if (path.isAbsolute(candidate) && fs.existsSync(candidate)) {
      return await import(pathToFileURL(candidate).href);
    }
    return await import(candidate);
  } catch (_error) {
    return null;
  }
}

async function loadOpenCLIRuntime() {
  const envCandidate = process.env.OPENCLI_RUNTIME_MODULE || '';
  const candidates = [
    envCandidate,
    '@jackwener/opencli/dist/runtime.js',
    '@jackwener/opencli/dist/runtime',
  ].filter(Boolean);
  for (const candidate of candidates) {
    const loaded = await importRuntimeCandidate(candidate);
    if (loaded?.browserSession && loaded?.getBrowserFactory) {
      return loaded;
    }
  }
  try {
    const fallbackPath = require.resolve('@jackwener/opencli/dist/runtime.js');
    const loaded = await importRuntimeCandidate(fallbackPath);
    if (loaded?.browserSession && loaded?.getBrowserFactory) {
      return loaded;
    }
  } catch (_error) {
    // ignore and fall through to the final error
  }
  fail(
    'Could not load @jackwener/opencli runtime. Install the package or set OPENCLI_RUNTIME_MODULE.',
    'missing_opencli_runtime',
  );
}

const { browserSession, getBrowserFactory } = await loadOpenCLIRuntime();

const XHS_PUBLISH_URL = 'https://creator.xiaohongshu.com/publish/publish?from=menu_left';
const XIANYU_PUBLISH_URL = 'https://www.goofish.com/publish';

function parseArgs(argv) {
  const args = { site: '', command: '', payloadJson: '{}' };
  for (let index = 0; index < argv.length; index += 1) {
    const part = argv[index];
    if (part === '--site') args.site = argv[index + 1] || '';
    if (part === '--command') args.command = argv[index + 1] || '';
    if (part === '--payload-json') args.payloadJson = argv[index + 1] || '{}';
  }
  return args;
}

function fail(message, code = 'helper_failed', details = {}) {
  const error = new Error(message);
  error.code = code;
  error.details = details;
  throw error;
}

function parsePayload(text) {
  try {
    const payload = JSON.parse(text || '{}');
    return payload && typeof payload === 'object' ? payload : {};
  } catch (error) {
    fail(`invalid payload json: ${error.message}`, 'invalid_payload');
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function readImageFile(filePath) {
  const absPath = path.resolve(filePath);
  if (!fs.existsSync(absPath)) {
    fail(`Image file not found: ${absPath}`, 'missing_image', { filePath: absPath });
  }
  const ext = path.extname(absPath).toLowerCase();
  const mimeMap = {
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.png': 'image/png',
    '.gif': 'image/gif',
    '.webp': 'image/webp',
  };
  const mimeType = mimeMap[ext];
  if (!mimeType) {
    fail(`Unsupported image format "${ext}"`, 'unsupported_image', { filePath: absPath, ext });
  }
  return {
    name: path.basename(absPath),
    mimeType,
    base64: fs.readFileSync(absPath).toString('base64'),
    absolutePath: absPath,
  };
}

async function waitFor(page, predicateJs, { timeoutMs = 20000, intervalMs = 500 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const matched = await page.evaluate(predicateJs);
    if (matched) return true;
    await sleep(intervalMs);
  }
  return false;
}

async function injectFiles(page, files, { imageOnly = true } = {}) {
  const payload = JSON.stringify(files);
  const result = await page.evaluate(`
    (async () => {
      const files = ${payload};
      const inputs = Array.from(document.querySelectorAll('input[type="file"]'));
      const pick = inputs.find((el) => {
        if (!el || el.offsetParent === null) return false;
        const accept = (el.getAttribute('accept') || '').toLowerCase();
        if (!${imageOnly}) return true;
        return accept.includes('image') || accept.includes('.png') || accept.includes('.jpg') || accept.includes('.jpeg') || accept.includes('.webp');
      }) || inputs.find((el) => el && el.offsetParent !== null) || inputs[0];
      if (!pick) return { ok: false, error: 'No file input found' };
      const dt = new DataTransfer();
      for (const item of files) {
        const binary = atob(item.base64);
        const bytes = new Uint8Array(binary.length);
        for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
        const blob = new Blob([bytes], { type: item.mimeType });
        dt.items.add(new File([blob], item.name, { type: item.mimeType }));
      }
      Object.defineProperty(pick, 'files', { value: dt.files, writable: false });
      pick.dispatchEvent(new Event('change', { bubbles: true }));
      pick.dispatchEvent(new Event('input', { bubbles: true }));
      return { ok: true, count: dt.files.length, accept: pick.getAttribute('accept') || '' };
    })()
  `);
  if (!result?.ok) fail(result?.error || 'file injection failed', 'file_injection_failed', { result });
  return result;
}

async function clickVisibleText(page, texts) {
  const payload = JSON.stringify(texts);
  const clicked = await page.evaluate(`
    (() => {
      const wanted = ${payload};
      const normalized = (value) => (value || '').replace(/\\s+/g, ' ').trim();
      const nodes = Array.from(document.querySelectorAll('button, [role="button"], .creator-tab, .title, div, span, a'));
      for (const wantedText of wanted) {
        const exact = nodes.find((el) => normalized(el.innerText || el.textContent) === wantedText && el.offsetParent !== null);
        if (exact) {
          exact.click();
          return { ok: true, text: wantedText };
        }
      }
      return { ok: false };
    })()
  `);
  if (!clicked?.ok) fail(`Could not find clickable text: ${texts.join(', ')}`, 'click_text_not_found', { texts });
  return clicked;
}

async function fillBySelectors(page, selectors, text, { fieldName } = {}) {
  const payloadSelectors = JSON.stringify(selectors);
  const payloadText = JSON.stringify(text);
  const result = await page.evaluate(`
    (() => {
      const selectors = ${payloadSelectors};
      const text = ${payloadText};
      for (const selector of selectors) {
        const candidates = Array.from(document.querySelectorAll(selector));
        for (const el of candidates) {
          if (!el || el.offsetParent === null) continue;
          el.focus();
          if (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA') {
            el.value = '';
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.value = text;
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('change', { bubbles: true }));
            return { ok: true, selector };
          }
          if ((el.getAttribute('contenteditable') || '').toLowerCase() === 'true') {
            el.textContent = text;
            el.dispatchEvent(new Event('input', { bubbles: true }));
            return { ok: true, selector };
          }
        }
      }
      return { ok: false };
    })()
  `);
  if (!result?.ok) {
    const shot = `/tmp/opencli-helper-${fieldName || 'field'}-debug.png`;
    await page.screenshot({ path: shot, fullPage: true });
    fail(`Could not find ${fieldName || 'field'} input. Debug screenshot: ${shot}`, 'field_not_found', {
      fieldName,
      selectors,
      screenshot: shot,
    });
  }
  return result;
}

async function fillLabeledInput(page, labelText, value) {
  const payloadLabel = JSON.stringify(labelText);
  const payloadValue = JSON.stringify(String(value));
  const result = await page.evaluate(`
    (() => {
      const labelText = ${payloadLabel};
      const value = ${payloadValue};
      const normalized = (input) => (input || '').replace(/\\s+/g, ' ').trim();
      const containers = Array.from(document.querySelectorAll('div, section, form, label'));
      for (const container of containers) {
        const text = normalized(container.innerText || container.textContent);
        if (!text || !text.includes(labelText)) continue;
        const input = Array.from(container.querySelectorAll('input')).find((el) => el && el.offsetParent !== null && (el.type || 'text') !== 'file');
        if (!input) continue;
        input.focus();
        input.value = '';
        input.dispatchEvent(new Event('input', { bubbles: true }));
        input.value = value;
        input.dispatchEvent(new Event('input', { bubbles: true }));
        input.dispatchEvent(new Event('change', { bubbles: true }));
        return { ok: true };
      }
      return { ok: false };
    })()
  `);
  if (!result?.ok) fail(`Could not find labeled input: ${labelText}`, 'labeled_input_not_found', { labelText });
  return result;
}

async function fillVisibleTextInputByIndex(page, index, value) {
  const payloadIndex = JSON.stringify(index);
  const payloadValue = JSON.stringify(String(value));
  const result = await page.evaluate(`
    (() => {
      const index = ${payloadIndex};
      const value = ${payloadValue};
      const inputs = Array.from(document.querySelectorAll('input'))
        .filter((el) => el && el.offsetParent !== null && (el.type || 'text') === 'text');
      const input = inputs[index];
      if (!input) return { ok: false, count: inputs.length };
      const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
      input.focus();
      if (setter) setter.call(input, '');
      else input.value = '';
      input.dispatchEvent(new Event('input', { bubbles: true }));
      if (setter) setter.call(input, value);
      else input.value = value;
      input.dispatchEvent(new Event('input', { bubbles: true }));
      input.dispatchEvent(new Event('change', { bubbles: true }));
      input.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true, key: 'Enter' }));
      input.blur();
      return { ok: true, count: inputs.length };
    })()
  `);
  if (!result?.ok) fail(`Could not fill visible text input at index ${index}`, 'text_input_not_found', { index, result });
  return result;
}

async function snapshotForm(page) {
  return page.evaluate(`
    (() => ({
      url: location.href,
      title: document.title,
      buttons: Array.from(document.querySelectorAll('button, [role="button"]')).map((el) => ({
        text: (el.innerText || el.textContent || '').trim(),
        cls: String(el.className || ''),
        visible: !!el.offsetParent,
      })).filter((row) => row.visible && row.text).slice(0, 60),
      inputs: Array.from(document.querySelectorAll('input, textarea, [contenteditable="true"]')).map((el) => ({
        tag: el.tagName,
        type: el.getAttribute('type') || '',
        placeholder: el.getAttribute('placeholder') || '',
        cls: String(el.className || ''),
        ce: el.getAttribute('contenteditable') || '',
        visible: !!el.offsetParent,
        text: (el.innerText || el.textContent || '').trim().slice(0, 80),
      })),
      fileInputs: Array.from(document.querySelectorAll('input[type="file"]')).map((el) => ({
        accept: el.getAttribute('accept') || '',
        cls: String(el.className || ''),
        visible: !!el.offsetParent,
        multiple: !!el.multiple,
      })),
    }))()
  `);
}

function resolveContent(payload, fieldName) {
  const options = payload.options || {};
  const positional = Array.isArray(payload.positional) ? payload.positional : [];
  const value = String(positional[0] || options.content || options.message || options.text || '').trim();
  if (!value) fail(`${fieldName} requires content`, 'usage');
  return value;
}

function resolveRequiredUrl(payload, fieldName) {
  const options = payload.options || {};
  const url = String(payload.url || options.url || '').trim();
  if (!url) fail(`${fieldName} requires options.url`, 'usage');
  return url;
}

async function openTarget(page, url) {
  await page.goto(url);
  await page.wait({ time: 2 });
}

async function sendInteractiveText(page, { fieldName, selectors, content, sendTexts }) {
  await fillBySelectors(page, selectors, content, { fieldName });
  await page.wait({ time: 1 });
  await clickVisibleText(page, sendTexts);
  await page.wait({ time: 3 });
  const screenshot = `/tmp/${fieldName}-after.png`;
  await page.screenshot({ path: screenshot, fullPage: true });
  const state = await page.evaluate(`
    (() => {
      const body = (document.body?.innerText || '').replace(/\\s+/g, ' ').trim();
      return {
        url: location.href,
        body,
      };
    })()
  `);
  return {
    screenshot,
    final_url: state.url,
    observed_text: String(state.body || '').slice(0, 500),
  };
}

async function publishXiaohongshu(page, payload) {
  const options = payload.options || {};
  const positional = Array.isArray(payload.positional) ? payload.positional : [];
  const title = String(options.title || '').trim();
  const content = String(positional[0] || options.content || '').trim();
  const imagesValue = options.images || '';
  const images = (Array.isArray(imagesValue) ? imagesValue : String(imagesValue).split(','))
    .map((item) => String(item).trim())
    .filter(Boolean)
    .map(readImageFile);
  if (!title) fail('xiaohongshu publish requires options.title', 'usage');
  if (!content) fail('xiaohongshu publish requires content', 'usage');
  if (!images.length) fail('xiaohongshu publish requires at least one image', 'usage');

  await page.goto(XHS_PUBLISH_URL);
  await page.wait({ time: 2 });
  await clickVisibleText(page, ['上传图文']);
  await page.wait({ time: 2 });
  const switched = await waitFor(
    page,
    `() => Array.from(document.querySelectorAll('input[type="file"]')).some((el) => {
      const accept = (el.getAttribute('accept') || '').toLowerCase();
      return el.offsetParent !== null && (accept.includes('image') || accept.includes('.png') || accept.includes('.jpg') || accept.includes('.jpeg'));
    })`,
    { timeoutMs: 15000 },
  );
  if (!switched) {
    await page.screenshot({ path: '/tmp/xhs-tab-switch-failed.png', fullPage: true });
    fail('Did not reach Xiaohongshu image publish mode', 'tab_switch_failed', { screenshot: '/tmp/xhs-tab-switch-failed.png' });
  }

  await injectFiles(page, images, { imageOnly: true });
  await page.wait({ time: 4 });
  await fillBySelectors(page, [
    'input[placeholder*="标题"]',
    'input[maxlength="20"]',
    'input[class*="title"]',
  ], title, { fieldName: 'xhs-title' });
  await fillBySelectors(page, [
    '[contenteditable="true"][placeholder*="描述"]',
    '[contenteditable="true"][placeholder*="正文"]',
    '[contenteditable="true"][placeholder*="内容"]',
    '.ql-editor[contenteditable="true"]',
    '[contenteditable="true"]',
  ], content, { fieldName: 'xhs-content' });
  await page.wait({ time: 1 });
  await clickVisibleText(page, ['发布']);
  await page.wait({ time: 5 });

  const status = await page.evaluate(`
    (() => {
      const body = (document.body?.innerText || '').replace(/\\s+/g, ' ').trim();
      return {
        url: location.href,
        body,
        success: body.includes('发布成功') || body.includes('笔记发布成功') || !location.href.includes('/publish/publish'),
      };
    })()
  `);
  const screenshot = '/tmp/xhs-publish-after.png';
  await page.screenshot({ path: screenshot, fullPage: true });
  return {
    ok: !!status.success,
    site: 'xiaohongshu',
    command: 'publish',
    result: {
      status: status.success ? 'published' : 'submitted_check_manually',
      title,
      content,
      screenshot,
      final_url: status.url,
      observed_text: String(status.body || '').slice(0, 500),
    },
  };
}

async function commentSendXiaohongshu(page, payload) {
  const url = resolveRequiredUrl(payload, 'xiaohongshu comment-send');
  const content = resolveContent(payload, 'xiaohongshu comment-send');
  await openTarget(page, url);
  const result = await sendInteractiveText(page, {
    fieldName: 'xhs-comment',
    selectors: [
      'textarea',
      '[contenteditable="true"][placeholder*="评论"]',
      '[contenteditable="true"]',
    ],
    content,
    sendTexts: ['发送', '发布'],
  });
  return {
    ok: true,
    site: 'xiaohongshu',
    command: 'comment-send',
    result: {
      status: 'sent',
      url,
      content,
      ...result,
    },
  };
}

async function dmSendXiaohongshu(page, payload) {
  const url = resolveRequiredUrl(payload, 'xiaohongshu dm-send');
  const content = resolveContent(payload, 'xiaohongshu dm-send');
  await openTarget(page, url);
  const result = await sendInteractiveText(page, {
    fieldName: 'xhs-dm',
    selectors: [
      'textarea',
      '[contenteditable="true"][placeholder*="发消息"]',
      '[contenteditable="true"][placeholder*="消息"]',
      '[contenteditable="true"]',
    ],
    content,
    sendTexts: ['发送', 'Send'],
  });
  return {
    ok: true,
    site: 'xiaohongshu',
    command: 'dm-send',
    result: {
      status: 'sent',
      url,
      content,
      ...result,
    },
  };
}

async function publishXianyu(page, payload) {
  const options = payload.options || {};
  const positional = Array.isArray(payload.positional) ? payload.positional : [];
  const content = String(positional[0] || options.content || '').trim();
  const imagesValue = options.images || '';
  const images = (Array.isArray(imagesValue) ? imagesValue : String(imagesValue).split(','))
    .map((item) => String(item).trim())
    .filter(Boolean)
    .map(readImageFile);
  const price = String(options.price || '1').trim();
  const originalPrice = String(options.original_price || options.originalPrice || '2').trim();
  if (!content) fail('xianyu publish requires content', 'usage');
  if (!images.length) fail('xianyu publish requires at least one image', 'usage');

  await page.goto(XIANYU_PUBLISH_URL);
  await page.wait({ time: 2 });
  await injectFiles(page, images, { imageOnly: true });
  await page.wait({ time: 3 });
  await fillBySelectors(page, [
    'textarea[placeholder*="描述"]',
    'textarea',
    '[contenteditable="true"]',
  ], content, { fieldName: 'xianyu-description' });
  await fillVisibleTextInputByIndex(page, 0, price);
  await fillVisibleTextInputByIndex(page, 1, originalPrice);
  await page.wait({ time: 1 });
  await clickVisibleText(page, ['发布']);
  await page.wait({ time: 5 });

  const status = await page.evaluate(`
    (() => {
      const body = (document.body?.innerText || '').replace(/\\s+/g, ' ').trim();
      return {
        url: location.href,
        body,
        success: body.includes('发布成功') || body.includes('发布中') || !location.href.includes('/publish'),
      };
    })()
  `);
  const screenshot = '/tmp/xianyu-publish-after.png';
  await page.screenshot({ path: screenshot, fullPage: true });
  return {
    ok: !!status.success,
    site: 'xianyu',
    command: 'publish',
    result: {
      status: status.success ? 'published' : 'submitted_check_manually',
      content,
      price,
      original_price: originalPrice,
      screenshot,
      final_url: status.url,
      observed_text: String(status.body || '').slice(0, 500),
    },
  };
}

async function inquiryReplyXianyu(page, payload) {
  const url = resolveRequiredUrl(payload, 'xianyu inquiry-reply');
  const content = resolveContent(payload, 'xianyu inquiry-reply');
  await openTarget(page, url);
  const result = await sendInteractiveText(page, {
    fieldName: 'xianyu-inquiry-reply',
    selectors: [
      'textarea',
      '[contenteditable="true"][placeholder*="回复"]',
      '[contenteditable="true"][placeholder*="消息"]',
      '[contenteditable="true"]',
    ],
    content,
    sendTexts: ['发送', '回复', '发送消息'],
  });
  return {
    ok: true,
    site: 'xianyu',
    command: 'inquiry-reply',
    result: {
      status: 'sent',
      url,
      content,
      ...result,
    },
  };
}

async function inspect(page, site) {
  const url = site === 'xiaohongshu' ? XHS_PUBLISH_URL : XIANYU_PUBLISH_URL;
  await page.goto(url);
  await page.wait({ time: 2 });
  if (site === 'xiaohongshu') {
    try {
      await clickVisibleText(page, ['上传图文']);
      await page.wait({ time: 2 });
    } catch {}
  }
  return {
    ok: true,
    site,
    command: 'inspect',
    result: await snapshotForm(page),
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const payload = parsePayload(args.payloadJson);
  if (!args.site || !args.command) {
    fail('site and command are required', 'usage');
  }
  const result = await browserSession(getBrowserFactory(), async (page) => {
    if (args.command === 'inspect') return inspect(page, args.site);
    if (args.site === 'xiaohongshu' && args.command === 'publish') return publishXiaohongshu(page, payload);
    if (args.site === 'xiaohongshu' && args.command === 'comment-send') return commentSendXiaohongshu(page, payload);
    if (args.site === 'xiaohongshu' && args.command === 'dm-send') return dmSendXiaohongshu(page, payload);
    if (args.site === 'xianyu' && args.command === 'publish') return publishXianyu(page, payload);
    if (args.site === 'xianyu' && args.command === 'inquiry-reply') return inquiryReplyXianyu(page, payload);
    fail(`unsupported helper operation: ${args.site}/${args.command}`, 'unsupported_command', { site: args.site, command: args.command });
  });
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

main().catch((error) => {
  process.stdout.write(`${JSON.stringify({
    ok: false,
    error: error.message,
    error_code: error.code || 'helper_failed',
    details: error.details || {},
  }, null, 2)}\n`);
  process.exit(1);
});
