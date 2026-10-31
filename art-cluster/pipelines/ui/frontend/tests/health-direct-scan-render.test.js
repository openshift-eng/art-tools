import assert from 'node:assert/strict';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { build } from 'vite';

test('health shows direct scan branches and retains failed FBC attempts during recovery', async () => {
  const bundle = await build({
    configFile: false,
    logLevel: 'silent',
    ssr: { noExternal: ['react', '@patternfly/react-core'] },
    plugins: [{
      name: 'health-render-test',
      enforce: 'pre',
      resolveId(id) {
        if (id === 'react') return { id: import.meta.resolve('react'), external: true };
        if (id === '@patternfly/react-core') return '\0patternfly-stub';
      },
      load(id) {
        if (id === '\0patternfly-stub') return `
          import React from 'react';
          export const Alert = ({ title }) => React.createElement('section', null, title);
          export const Button = ({ children }) => React.createElement('button', null, children);
          export const Label = ({ children }) => React.createElement('span', null, children);
          export const Spinner = () => null;
          export const Title = ({ children }) => React.createElement('h1', null, children);
        `;
      },
    }],
    build: { ssr: new URL('../src/PipelineHealth.jsx', import.meta.url).pathname, write: false },
  });
  const code = bundle.output.find((output) => output.type === 'chunk').code;
  const { ChainDetails } = await import(`data:text/javascript;base64,${Buffer.from(code).toString('base64')}`);
  const run = (name, pipeline, values = {}) => ({
    name, pipeline, uid: `uid-${name}`, namespace: 'tenant', status: 'Succeeded',
    created: '2026-10-29T23:01:29Z', ...values,
  });
  const root = run('scan', 'layered-products-scan');
  const directBundle = run('direct-bundle', 'olm-bundle-konflux', { parent: 'scan', status: 'Failed' });
  const originalFbc = run('failed-fbc', 'build-fbc', { parent: 'scan', status: 'Failed', target: '4.20' });
  const retryFbc = run('retry-fbc', 'build-fbc', {
    parent: 'scan', status: 'Running', target: '4.20', rebuiltFrom: originalFbc.uid,
  });
  const chain = {
    root, recovered: true, issues: [], failures: [directBundle],
    attempts: [root, directBundle, originalFbc, retryFbc],
    stages: [
      { pipeline: root.pipeline, runs: [root] },
      { pipeline: 'build-layered-products', runs: [] },
      { pipeline: directBundle.pipeline, runs: [directBundle] },
      { pipeline: retryFbc.pipeline, runs: [retryFbc] },
    ],
  };
  const html = renderToStaticMarkup(createElement(ChainDetails, { chain }));
  assert.match(html, /direct-bundle\?uid=uid-direct-bundle/);
  assert.equal((html.match(/Triggered directly by scan/g) || []).length, 2);
  assert.match(html, /OCP 4.20 · Retry/);
  assert.match(html, /Not triggered by this scan/);
  assert.doesNotMatch(html, /Blocked by upstream failure/);
  assert.match(html, /Recovery history · 4 scan, image, bundle, and FBC attempts/);
  assert.match(html, /failed-fbc\?uid=uid-failed-fbc/);
  assert.match(html, /retry-fbc\?uid=uid-retry-fbc/);
  assert.match(html, /Running/);
  assert.match(html, /Failed/);

  root.triggered = [directBundle.name, retryFbc.name];
  for (const parent of [undefined, null, 'another-parent']) {
    directBundle.parent = parent;
    retryFbc.parent = parent;
    const annotatedHtml = renderToStaticMarkup(createElement(ChainDetails, { chain }));
    assert.equal((annotatedHtml.match(/Triggered directly by scan/g) || []).length, 2);
  }

  root.triggered = ['unrelated-run'];
  const unrelatedHtml = renderToStaticMarkup(createElement(ChainDetails, { chain }));
  assert.doesNotMatch(unrelatedHtml, /Triggered directly by scan/);
});
