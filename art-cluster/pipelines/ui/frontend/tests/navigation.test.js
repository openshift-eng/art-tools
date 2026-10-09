import assert from 'node:assert/strict';
import test from 'node:test';
import { isPlainClick, navigateRow, parseHash, pipelineTabs, runTabs, viewHref } from '../src/navigation.js';

test('list routes preserve the tenant and accept existing URLs', () => {
  for (const kind of ['pipelines', 'runs', 'health']) {
    for (const filterNamespace of ['', 'art-acm-tenant']) {
      assert.deepEqual(parseHash(viewHref({ kind, filterNamespace })), { kind, filterNamespace });
    }
  }
  assert.deepEqual(parseHash(''), { kind: 'pipelines', filterNamespace: '' });
});

test('detail links preserve tabs, resource namespace, filter namespace, encoding, and run UID', () => {
  for (const kind of ['pipeline', 'run']) {
    for (const tab of kind === 'pipeline' ? pipelineTabs : runTabs) {
      const view = { kind, namespace: 'resource-tenant', name: 'name /?#', filterNamespace: 'filter-tenant', tab,
        ...(kind === 'run' ? { uid: 'uid /?&' } : {}) };
      assert.deepEqual(parseHash(viewHref(view)), view);
    }
  }
});

test('missing and invalid tabs default to Details without losing archived identity', () => {
  for (const kind of ['pipeline', 'run']) {
    for (const query of ['', '?tab=invalid', '?tab=logs&uid=archived']) {
      const view = parseHash(`#/${kind}/tenant/name${query}`);
      assert.equal(view.tab, kind === 'run' && query.includes('tab=logs') ? 'logs' : 'details');
      if (kind === 'run') assert.equal(view.uid, query.includes('uid=archived') ? 'archived' : null);
    }
  }
});

function setupEvent(overrides = {}, nested = false) {
  const opened = [];
  const browser = { location: { hash: '#/pipelines' }, open: (...args) => opened.push(args) };
  const event = {
    type: 'click', button: 0, defaultPrevented: false, target: { closest: () => nested ? {} : null },
    preventDefault() { this.defaultPrevented = true; }, ...overrides,
  };
  return { browser, event, opened };
}

test('ordinary row clicks and Enter navigate in the same tab', () => {
  for (const overrides of [{}, { type: 'keydown', key: 'Enter', button: undefined }]) {
    const { browser, event, opened } = setupEvent(overrides);
    navigateRow(event, '#/runs', browser);
    assert.equal(browser.location.hash, '#/runs');
    assert.equal(event.defaultPrevented, true);
    assert.deepEqual(opened, []);
  }
});

test('modified row clicks and middle-click open exactly once and preserve the current page', () => {
  const href = '#/run/tenant/name?uid=archived&tab=logs&namespace=tenant';
  for (const overrides of [{ ctrlKey: true }, { metaKey: true }, { shiftKey: true }, { type: 'auxclick', button: 1 }]) {
    const { browser, event, opened } = setupEvent(overrides);
    navigateRow(event, href, browser);
    assert.equal(browser.location.hash, '#/pipelines');
    assert.equal(event.defaultPrevented, true);
    assert.deepEqual(opened, [[href, '_blank', 'noopener,noreferrer']]);
  }
});

test('nested links and controls retain native click, middle-click, and keyboard behavior', () => {
  for (const overrides of [{}, { ctrlKey: true }, { metaKey: true }, { type: 'auxclick', button: 1 }, { type: 'keydown', key: 'Enter' }]) {
    const { browser, event, opened } = setupEvent(overrides, true);
    navigateRow(event, '#/runs', browser);
    assert.equal(browser.location.hash, '#/pipelines');
    assert.equal(event.defaultPrevented, false);
    assert.deepEqual(opened, []);
  }
});

test('right-click, Alt-click, unrelated keys, and prevented events do not navigate rows', () => {
  for (const overrides of [{ button: 2 }, { altKey: true }, { type: 'keydown', key: 'Tab' }, { defaultPrevented: true }]) {
    const { browser, event, opened } = setupEvent(overrides);
    navigateRow(event, '#/runs', browser);
    assert.equal(browser.location.hash, '#/pipelines');
    assert.deepEqual(opened, []);
  }
});

test('tab handlers intercept only ordinary primary clicks', () => {
  assert.equal(isPlainClick(setupEvent().event), true);
  for (const overrides of [{ ctrlKey: true }, { metaKey: true }, { shiftKey: true }, { altKey: true }, { button: 1 }, { button: 2 }, { defaultPrevented: true }]) {
    assert.equal(isPlainClick(setupEvent(overrides).event), false);
  }
});
