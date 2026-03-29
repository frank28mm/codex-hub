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

async function loadDaemonClient() {
  const envCandidate = process.env.OPENCLI_DAEMON_CLIENT_MODULE || '';
  const candidates = [
    envCandidate,
    '@jackwener/opencli/dist/browser/daemon-client.js',
    '@jackwener/opencli/dist/browser/daemon-client',
  ].filter(Boolean);
  for (const candidate of candidates) {
    const loaded = await importRuntimeCandidate(candidate);
    if (loaded?.sendCommand) {
      return loaded.sendCommand;
    }
  }
  try {
    const fallbackPath = require.resolve('@jackwener/opencli/dist/browser/daemon-client.js');
    const loaded = await importRuntimeCandidate(fallbackPath);
    if (loaded?.sendCommand) {
      return loaded.sendCommand;
    }
  } catch (_error) {
    // ignore and fall through to final error
  }
  fail(
    'Could not load @jackwener/opencli daemon client. Install the package or set OPENCLI_DAEMON_CLIENT_MODULE.',
    'missing_opencli_daemon_client',
  );
}

const sendCommand = await loadDaemonClient();

const XHS_HOME_URL = 'https://www.xiaohongshu.com';
const XHS_CREATOR_HOME_URL = 'https://creator.xiaohongshu.com/new/home';

const XHS_CREATOR_COMMANDS = new Set([
  'creator-profile',
  'creator-notes',
  'creator-notes-summary',
  'creator-note-detail',
  'creator-stats',
  'publish',
]);

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

function parsePayload(text) {
  try {
    const payload = JSON.parse(text || '{}');
    return payload && typeof payload === 'object' ? payload : {};
  } catch (error) {
    const err = new Error(`invalid payload json: ${error.message}`);
    err.code = 'invalid_payload';
    throw err;
  }
}

function fail(message, code = 'warmup_failed', details = {}) {
  const error = new Error(message);
  error.code = code;
  error.details = details;
  throw error;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function resolveWarmupTarget(site, command, payload) {
  if (site !== 'xiaohongshu') {
    return null;
  }
  const normalizedCommand = String(command || '').trim().toLowerCase();
  const options = payload.options && typeof payload.options === 'object' ? payload.options : {};
  if (XHS_CREATOR_COMMANDS.has(normalizedCommand)) {
    return {
      workspace: 'site:xiaohongshu',
      initialUrl: XHS_HOME_URL,
      finalUrl: XHS_CREATOR_HOME_URL,
      verifyCreator: true,
      verifyEndpoint: '/api/galaxy/creator/home/personal_info',
    };
  }
  if (normalizedCommand === 'comment-send' || normalizedCommand === 'dm-send') {
    return {
      workspace: 'site:xiaohongshu',
      initialUrl: XHS_HOME_URL,
      finalUrl: String(payload.url || options.url || '').trim() || XHS_HOME_URL,
      verifyCreator: false,
      verifyEndpoint: '',
    };
  }
  return null;
}

const SAFE_STATE_JS = `(() => {
  const pick = (fn) => {
    try {
      return fn();
    } catch (error) {
      return 'ERR:' + (error?.message || String(error));
    }
  };
  return {
    href: pick(() => location.href),
    title: pick(() => document.title),
    cookie: pick(() => document.cookie.slice(0, 200)),
    bodyText: pick(() => (document.body?.innerText || '').slice(0, 400)),
  };
})()`;

async function verifyCreatorSession(workspace, tabId, endpoint) {
  const verification = await sendCommand('exec', {
    workspace,
    tabId,
    code: `(async () => {
      const resp = await fetch(${JSON.stringify(endpoint)}, { credentials: 'include' }).catch((error) => ({ __err: String(error) }));
      if (resp && resp.__err) {
        return { fetchError: resp.__err, state: ${SAFE_STATE_JS} };
      }
      const text = await resp.text();
      return {
        status: resp.status,
        ok: resp.ok,
        responseText: text.slice(0, 1000),
        state: ${SAFE_STATE_JS},
      };
    })()`,
  });
  if (!verification || verification.status !== 200 || verification.ok !== true) {
    fail(
      'creator session warmup could not verify authenticated creator access',
      'creator_session_unavailable',
      { verification },
    );
  }
  return verification;
}

async function runWarmup(site, command, payload) {
  const target = resolveWarmupTarget(site, command, payload);
  if (!target) {
    return {
      ok: true,
      site,
      command,
      warmed: false,
      skipped: true,
      reason: 'no_warmup_required',
    };
  }
  const attempts = [];
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      await sendCommand('close-window', { workspace: target.workspace }).catch(() => {});
      if (attempt > 1) {
        await sleep(400);
      }
      const created = await sendCommand('tabs', {
        workspace: target.workspace,
        op: 'new',
        url: target.initialUrl,
      });
      const tabId = created?.tabId;
      if (!tabId) {
        fail('warmup could not allocate a xiaohongshu automation tab', 'tab_create_failed', { created, attempt });
      }
      await sendCommand('navigate', { workspace: target.workspace, tabId, url: target.initialUrl });
      if (target.finalUrl && target.finalUrl !== target.initialUrl) {
        await sendCommand('navigate', { workspace: target.workspace, tabId, url: target.finalUrl });
      }
      const state = await sendCommand('exec', { workspace: target.workspace, tabId, code: SAFE_STATE_JS });
      const verification = target.verifyCreator
        ? await verifyCreatorSession(target.workspace, tabId, target.verifyEndpoint)
        : null;
      return {
        ok: true,
        site,
        command,
        warmed: true,
        skipped: false,
        workspace: target.workspace,
        attempt,
        tab_id: tabId,
        initial_url: target.initialUrl,
        final_url: target.finalUrl,
        state,
        verification,
      };
    } catch (error) {
      attempts.push({
        attempt,
        message: error?.message || String(error),
        code: error?.code || 'warmup_failed',
      });
      await sendCommand('close-window', { workspace: target.workspace }).catch(() => {});
    }
  }
  fail('session warmup exhausted retries', 'warmup_retry_exhausted', { attempts, target });
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.site || !args.command) {
    fail('site and command are required', 'usage');
  }
  const payload = parsePayload(args.payloadJson);
  const result = await runWarmup(String(args.site || '').trim().toLowerCase(), args.command, payload);
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

main().catch((error) => {
  process.stdout.write(`${JSON.stringify({
    ok: false,
    error: error.message,
    error_code: error.code || 'warmup_failed',
    details: error.details || {},
  }, null, 2)}\n`);
  process.exit(1);
});
