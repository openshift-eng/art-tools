const recoveryKey = 'art-pipelines-reauth-pending';

export class AuthenticationError extends Error {}

export function createApi({ fetch = globalThis.fetch, browser = window } = {}) {
  let sessionCheck = null;
  let authenticationError = null;
  const listeners = new Set();

  function pendingRecovery() {
    try { return browser.sessionStorage.getItem(recoveryKey) === '1'; } catch { return false; }
  }

  function markRecovery(pending) {
    try {
      if (pending) browser.sessionStorage.setItem(recoveryKey, '1');
      else browser.sessionStorage.removeItem(recoveryKey);
    } catch { /* Browser storage may be unavailable. */ }
  }

  function pause(message) {
    authenticationError = new AuthenticationError(message);
    listeners.forEach((listener) => listener(authenticationError));
  }

  function signIn() {
    markRecovery(true);
    pause('Session expired. Signing in…');
    const { pathname, search, hash } = browser.location;
    browser.location.assign(`/oauth/start?rd=${encodeURIComponent(pathname + search + hash)}`);
  }

  function recover() {
    if (!authenticationError) {
      if (pendingRecovery()) pause('Sign-in did not restore your session. Sign in again to retry.');
      else signIn();
    }
    throw authenticationError;
  }

  async function fetchJson(path, options = {}) {
    const response = await fetch(path, { credentials: 'same-origin', ...options });
    const contentType = response.headers.get('content-type') || '';
    const body = await response.json().catch(() => ({}));
    if (!response.ok) {
      const error = new Error(body.detail || `Request failed (${response.status})`);
      error.status = response.status;
      error.proxyLogin = response.status === 403 && contentType.includes('text/html');
      throw error;
    }
    if (!contentType.includes('application/json')) throw new Error('Unexpected response from the server');
    return body;
  }

  function checkSession() {
    if (authenticationError) return Promise.reject(authenticationError);
    if (!sessionCheck) {
      sessionCheck = fetchJson('/api/session', { cache: 'no-store' })
        .then((identity) => {
          if (!authenticationError) markRecovery(false);
          return identity;
        })
        .catch((error) => {
          if (error.status === 401 || error.proxyLogin) recover();
          throw error;
        })
        .finally(() => { sessionCheck = null; });
    }
    return sessionCheck;
  }

  async function request(path, options = {}) {
    if (authenticationError) throw authenticationError;
    if (path === '/api/session') return checkSession();
    try {
      return await fetchJson(path, options);
    } catch (error) {
      if (options.signal?.aborted) throw error;
      if (error.proxyLogin) recover();
      if (error.status === 401) {
        try { await checkSession(); } catch (sessionError) {
          if (sessionError instanceof AuthenticationError) throw sessionError;
        }
      }
      throw error;
    }
  }

  function subscribeAuthentication(listener) {
    listeners.add(listener);
    listener(authenticationError);
    return () => listeners.delete(listener);
  }

  return { request, checkSession, signIn, subscribeAuthentication };
}
