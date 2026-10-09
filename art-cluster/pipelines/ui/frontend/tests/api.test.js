import assert from 'node:assert/strict';
import { setImmediate } from 'node:timers/promises';
import test from 'node:test';
import { AuthenticationError, createApi } from '../src/api.js';

function json(body, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });
}

function setup(handler) {
  const calls = [];
  const redirects = [];
  const stored = new Map();
  const browser = {
    location: {
      pathname: '/', search: '?view=history', hash: '#/run/tenant/run-1?uid=abc&tab=logs',
      assign: (url) => redirects.push(url),
    },
    sessionStorage: {
      getItem: (key) => stored.get(key),
      setItem: (key, value) => stored.set(key, value),
      removeItem: (key) => stored.delete(key),
    },
  };
  const fetch = async (path, options) => {
    calls.push({ path, options });
    return handler(path, options);
  };
  return { api: createApi({ fetch, browser }), browser, fetch, calls, redirects, stored };
}

test('session validation bypasses caching and clears the recovery marker', async () => {
  const state = setup(() => json({ user: 'alice', csrfToken: 'csrf' }));
  state.stored.set('art-pipelines-reauth-pending', '1');
  assert.deepEqual(await state.api.request('/api/session'), { user: 'alice', csrfToken: 'csrf' });
  assert.equal(state.calls[0].options.cache, 'no-store');
  assert.equal(state.calls[0].options.credentials, 'same-origin');
  assert.equal(state.stored.size, 0);
});

test('an expired session starts login and preserves the complete return path', async () => {
  const state = setup(() => json({ detail: 'Unauthorized' }, 401));
  await assert.rejects(state.api.request('/api/pipelines'), AuthenticationError);
  assert.deepEqual(state.calls.map((call) => call.path), ['/api/pipelines', '/api/session']);
  assert.equal(state.redirects.length, 1);
  const url = new URL(state.redirects[0], 'https://ui.example');
  assert.equal(url.pathname, '/oauth/start');
  assert.equal(url.searchParams.get('rd'), '/?view=history#/run/tenant/run-1?uid=abc&tab=logs');
  assert.equal(state.stored.get('art-pipelines-reauth-pending'), '1');
});

test('concurrent failures share one session check and redirect', async () => {
  let release;
  const ready = new Promise((resolve) => { release = resolve; });
  const state = setup(async (path) => {
    if (path === '/api/session') await ready;
    return json({ detail: 'Unauthorized' }, 401);
  });
  const requests = Promise.allSettled([
    state.api.request('/api/pipelines'), state.api.request('/api/runs'), state.api.checkSession(),
  ]);
  await setImmediate();
  assert.equal(state.calls.filter((call) => call.path === '/api/session').length, 1);
  release();
  const results = await requests;
  assert.ok(results.every((result) => result.status === 'rejected' && result.reason instanceof AuthenticationError));
  assert.equal(state.redirects.length, 1);
});

test('a service 401 with a valid cluster session remains an error', async () => {
  const state = setup((path) => path === '/api/session'
    ? json({ user: 'alice', csrfToken: 'csrf' }) : json({ detail: 'Results rejected the request' }, 401));
  await assert.rejects(state.api.request('/api/runs'), { message: 'Results rejected the request' });
  assert.equal(state.redirects.length, 0);
});

test('a proxy HTML sign-in response starts login', async () => {
  const state = setup(() => new Response('<html>Sign in</html>', {
    status: 403, headers: { 'content-type': 'text/html; charset=utf-8' },
  }));
  await assert.rejects(state.api.request('/api/pipelines'), AuthenticationError);
  assert.equal(state.calls.length, 1);
  assert.equal(state.redirects.length, 1);
});

test('JSON permission errors do not start login or check the session', async () => {
  const state = setup(() => json({ detail: 'Forbidden' }, 403));
  await assert.rejects(state.api.request('/api/pipelines'), { message: 'Forbidden' });
  assert.equal(state.calls.length, 1);
  assert.equal(state.redirects.length, 0);
});

test('service outages and failed session checks do not start login', async () => {
  for (const status of [502, 503]) {
    const state = setup(() => json({ detail: 'Unavailable' }, status));
    await assert.rejects(state.api.checkSession(), { message: 'Unavailable' });
    assert.equal(state.redirects.length, 0);
  }
  const state = setup((path) => path === '/api/session'
    ? json({ detail: 'Unavailable' }, 502) : json({ detail: 'Unauthorized' }, 401));
  await assert.rejects(state.api.request('/api/runs'), { message: 'Unauthorized' });
  assert.equal(state.redirects.length, 0);
});

test('network errors and unexpected HTML do not start login', async () => {
  const offline = setup(() => { throw new TypeError('Failed to fetch'); });
  await assert.rejects(offline.api.request('/api/runs'), { message: 'Failed to fetch' });
  assert.equal(offline.redirects.length, 0);
  const html = setup(() => new Response('<html>Error</html>', { headers: { 'content-type': 'text/html' } }));
  await assert.rejects(html.api.request('/api/runs'), { message: 'Unexpected response from the server' });
  assert.equal(html.redirects.length, 0);
});

test('failed recovery pauses requests until explicit sign-in', async () => {
  const state = setup(() => json({ detail: 'Unauthorized' }, 401));
  await assert.rejects(state.api.checkSession(), AuthenticationError);
  const returned = createApi({ fetch: state.fetch, browser: state.browser });
  const notifications = [];
  returned.subscribeAuthentication((error) => notifications.push(error));
  await assert.rejects(returned.checkSession(), /Sign-in did not restore your session/);
  assert.equal(state.redirects.length, 1);
  const calls = state.calls.length;
  await assert.rejects(returned.request('/api/runs'), AuthenticationError);
  await assert.rejects(returned.checkSession(), AuthenticationError);
  assert.equal(state.calls.length, calls);
  assert.match(notifications.at(-1).message, /Sign-in did not restore/);
  returned.signIn();
  assert.equal(state.redirects.length, 2);
});

test('a successful fresh session permits another recovery later', async () => {
  const state = setup(() => json({ detail: 'Unauthorized' }, 401));
  await assert.rejects(state.api.checkSession(), AuthenticationError);
  let valid = true;
  const returned = createApi({ browser: state.browser, fetch: async () => valid
    ? json({ user: 'alice', csrfToken: 'csrf' }) : json({ detail: 'Unauthorized' }, 401) });
  await returned.checkSession();
  valid = false;
  await assert.rejects(returned.checkSession(), AuthenticationError);
  assert.equal(state.redirects.length, 2);
});

test('an in-flight successful session check cannot erase a new recovery marker', async () => {
  let release;
  const ready = new Promise((resolve) => { release = resolve; });
  const state = setup(async (path) => {
    if (path === '/api/session') { await ready; return json({ user: 'alice', csrfToken: 'csrf' }); }
    return new Response('<html>Sign in</html>', { status: 403, headers: { 'content-type': 'text/html' } });
  });
  const session = state.api.checkSession();
  await assert.rejects(state.api.request('/api/runs'), AuthenticationError);
  release();
  await session;
  assert.equal(state.stored.get('art-pipelines-reauth-pending'), '1');
});

test('abandoned requests do not initiate recovery', async () => {
  const controller = new AbortController();
  const state = setup(() => { controller.abort(); return json({ detail: 'Unauthorized' }, 401); });
  await assert.rejects(state.api.request('/api/runs', { signal: controller.signal }), { message: 'Unauthorized' });
  assert.equal(state.calls.length, 1);
  assert.equal(state.redirects.length, 0);
});

test('Start/Rebuild submissions are never replayed during recovery', async () => {
  const state = setup(() => json({ detail: 'Unauthorized' }, 401));
  const options = { method: 'POST', body: JSON.stringify({ pipeline: 'test' }) };
  await assert.rejects(state.api.request('/api/runs', options), AuthenticationError);
  assert.equal(state.calls.filter((call) => call.path === '/api/runs').length, 1);
  assert.equal(state.calls[0].options.body, options.body);
  assert.equal(state.calls[0].options.method, 'POST');
});

test('unavailable browser storage does not prevent sign-in', async () => {
  const state = setup(() => json({ detail: 'Unauthorized' }, 401));
  Object.defineProperty(state.browser, 'sessionStorage', { get: () => { throw new Error('Storage disabled'); } });
  await assert.rejects(state.api.checkSession(), AuthenticationError);
  assert.equal(state.redirects.length, 1);
});
