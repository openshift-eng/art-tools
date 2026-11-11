import assert from 'node:assert/strict';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { build } from 'vite';
import { releaseStages } from '../src/releaseStatusUtils.js';

const loaded = (async () => {
  const bundle = await build({
    configFile: false, logLevel: 'silent', ssr: { noExternal: ['react', '@patternfly/react-core'] },
    plugins: [{
      name: 'release-workspace-render-test', enforce: 'pre',
      resolveId(id) {
        if (id === 'react') return { id: import.meta.resolve('react'), external: true };
        if (id === '@patternfly/react-core') return '\0patternfly-release-stub';
      },
      load(id) {
        if (id === '\0patternfly-release-stub') return `
          import React from 'react';
          export const Label = ({children}) => React.createElement('span', null, children);
          export const Button = ({children, isDisabled, icon, variant, ...props}) => React.createElement('button', {...props, disabled: isDisabled}, icon, children);
          export const Alert = () => null;
          export const Spinner = () => null;
        `;
      },
    }],
    build: { ssr: new URL('../src/ReleaseStatus.jsx', import.meta.url).pathname, write: false },
  });
  const code = bundle.output.find((item) => item.type === 'chunk').code;
  return import(`data:text/javascript;base64,${Buffer.from(code).toString('base64')}`);
})();

test('the connected rail exposes seven observed stages and keeps unknown evidence pending', async () => {
  const { ObservedStageRail } = await loaded;
  const stages = releaseStages({ preparation: { status: 'Unknown' }, mr: { state: 'merged' } }, null);
  const html = renderToStaticMarkup(createElement(ObservedStageRail, { stages, onSelect() {} }));
  assert.equal((html.match(/<button /g) || []).length, 7);
  assert.match(html, /release-stage pending/);
  assert.match(html, /Prepare release<\/strong><small>Unknown/);
  assert.match(html, /Merge<\/strong><small>Complete/);
});

test('the sidebar distinguishes same-version MRs and marks the selected record', async () => {
  const { ReleaseSelector } = await loaded;
  const items = [42, 43].map((iid) => ({ id: `mr:${iid}`, group: 'acm-5.0', assembly: '5.0.0', mr: { iid, state: 'merged' }, preparation: { status: 'Succeeded', attempts: [] } }));
  const html = renderToStaticMarkup(createElement(ReleaseSelector, { items, selected: 'mr:43', search: '', status: 'all', days: 30, onSelect() {}, onSearch() {}, onStatus() {}, onDays() {} }));
  assert.match(html, /aria-label="Release selector"/);
  assert.match(html, /Shipment !42/);
  assert.match(html, /Shipment !43/);
  assert.equal((html.match(/aria-pressed="true"/g) || []).length, 1);
  assert.doesNotMatch(html, /<table/);
});

test('previous preparation failures stay nested beneath the latest same-version shipment', async () => {
  const { ReleaseSelector } = await loaded;
  const failed = { id: 'run:failed', group: 'acm-5.0', assembly: '5.0.0', updated: '2026-11-04T12:00:00Z', preparation: { status: 'Failed', attempts: [{ name: 'failed-run' }] } };
  const merged = { ...failed, id: 'mr:1142', updated: '2026-11-06T12:00:00Z', mr: { iid: 1142, state: 'merged' }, preparation: { status: 'Succeeded', attempts: [] } };
  const html = renderToStaticMarkup(createElement(ReleaseSelector, { items: [failed, merged], selected: merged.id, search: '', status: 'all', days: 7 }));
  assert.ok(html.indexOf('Shipment !1142') < html.indexOf('Previous attempts'));
  assert.ok(html.indexOf('Previous attempts') < html.indexOf('failed-run'));
  assert.match(html, /<details class="live-selector-history"><summary>Previous attempts/);
  assert.match(html, /Preparation failed/);
  assert.match(html, /MR not linked/);
  const expanded = renderToStaticMarkup(createElement(ReleaseSelector, { items: [failed, merged], selected: failed.id, search: '', status: 'all', days: 7 }));
  assert.match(expanded, /<details class="live-selector-history" open=""/);
});

test('the live page defaults to seven days plus open MRs', async () => {
  const { default: ReleaseStatus } = await loaded;
  const html = renderToStaticMarkup(createElement(ReleaseStatus, { namespace: 'art-acm-tenant', request() {} }));
  assert.match(html, /<option value="7" selected="">Last 7 days \+ open MRs/);
  assert.doesNotMatch(html, /Open orchestration demo/);
  assert.doesNotMatch(html, /Loading current GitLab evidence/);
});

test('refresh stays busy while selected GitLab details are pending after the list finishes', async () => {
  const { ReleaseRefreshControl } = await loaded;
  const html = renderToStaticMarkup(createElement(ReleaseRefreshControl, { detailLoading: true, updated: true, hasData: true }));
  assert.match(html, /aria-busy="true"/);
  assert.match(html, /disabled=""/);
  assert.match(html, /Refreshing GitLab/);
  assert.match(html, /Showing last observed data/);
  assert.doesNotMatch(html, />Updated</);
});

test('a partial source failure enables retry and cannot report a successful refresh', async () => {
  const { ReleaseRefreshControl } = await loaded;
  const html = renderToStaticMarkup(createElement(ReleaseRefreshControl, { incomplete: true, updated: true, hasData: true }));
  assert.match(html, /Refresh incomplete · Retry/);
  assert.match(html, /Some sources could not be refreshed/);
  assert.doesNotMatch(html, /disabled=""/);
  assert.doesNotMatch(html, />Updated</);
});

test('the same control shows list loading and completed refresh without extra buttons', async () => {
  const { ReleaseRefreshControl } = await loaded;
  const busy = renderToStaticMarkup(createElement(ReleaseRefreshControl, { loading: true }));
  assert.match(busy, /Refreshing releases/);
  assert.match(busy, /Loading release data/);
  const done = renderToStaticMarkup(createElement(ReleaseRefreshControl, { updated: true, observedAt: '2026-11-10T20:00:00Z' }));
  assert.equal((done.match(/<button /g) || []).length, 1);
  assert.match(done, />Updated</);
  assert.match(done, /Last observed/);
  assert.doesNotMatch(done, /disabled=""/);
});
