import assert from 'node:assert/strict';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { build } from 'vite';

test('global incident banner filters inactive and opted-out incidents and cycles serially', async () => {
  const bundle = await build({
    configFile: false,
    logLevel: 'silent',
    ssr: { noExternal: ['react', '@patternfly/react-core'] },
    plugins: [{
      name: 'incident-banner-render-test',
      enforce: 'pre',
      resolveId(id) {
        if (id === 'react') return { id: import.meta.resolve('react'), external: true };
        if (id === '@patternfly/react-core') return '\0patternfly-stub';
      },
      load(id) {
        if (id === '\0patternfly-stub') return `
          import React from 'react';
          export const Button = ({ children, ...props }) => React.createElement('button', props, children);
        `;
      },
    }],
    build: { ssr: new URL('../src/IncidentBanner.jsx', import.meta.url).pathname, write: false },
  });
  const code = bundle.output.find((output) => output.type === 'chunk').code;
  const { default: IncidentBanner, activeBannerIncidents, nextBannerIndex, startBannerCycle, INCIDENT_BANNER_CYCLE_MS } = await import(`data:text/javascript;base64,${Buffer.from(code).toString('base64')}`);
  const incidents = [
    { id: 'one', message: 'Kerberos scans may fail.', status: 'investigating', updated: '2026-10-14T08:00:00Z', severity: 'warning', links: [{ label: 'Ticket', url: 'https://issues.example/1' }] },
    { id: 'off', message: 'Do not announce this.', status: 'investigating', showOnBanner: false },
    { id: 'closed', message: 'Already resolved.', status: 'resolved' },
    { id: 'deleted', message: 'Removed.', status: 'deleted' },
    { id: 'two', message: 'Another service issue.', status: 'monitoring', showOnBanner: true, updated: '2026-10-14T09:00:00Z', severity: 'danger', links: [{ label: 'Ticket', url: 'https://issues.example/2' }] },
  ];
  const candidates = activeBannerIncidents(incidents);
  assert.deepEqual(candidates.map(({ id }) => id), ['two', 'one']);
  assert.equal(INCIDENT_BANNER_CYCLE_MS, 8000);
  assert.equal(nextBannerIndex(1, 2), 0);
  let intervalMs = 0;
  let clearedTimer = null;
  const cleanup = startBannerCycle(() => {}, {
    setInterval(_callback, duration) { intervalMs = duration; return 42; },
    clearInterval(timer) { clearedTimer = timer; },
  });
  assert.equal(intervalMs, 8000);
  cleanup();
  assert.equal(clearedTimer, 42);

  const markup = renderToStaticMarkup(createElement(IncidentBanner, { incidents }));
  assert.match(markup, /class="incident-announcement severity-danger"/);
  assert.match(markup, /Another service issue\./);
  assert.match(markup, /Ticket/);
  assert.match(markup, /Previous incident/);
  assert.match(markup, /Next incident/);
  assert.match(markup, /1 of 2/);
  assert.doesNotMatch(markup, /Do not announce|Already resolved|Removed\./);

  const single = renderToStaticMarkup(createElement(IncidentBanner, { incidents: [incidents[0]] }));
  assert.doesNotMatch(single, /Previous incident|Next incident|1 of 1/);
  assert.equal(activeBannerIncidents([]).length, 0);
});
