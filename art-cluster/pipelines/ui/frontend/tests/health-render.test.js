import assert from 'node:assert/strict';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { build } from 'vite';

test('LP rebase warnings render orange and link to the affected image run', async () => {
  const entry = new URL('../src/PipelineHealth.jsx', import.meta.url).pathname;
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
          export const Label = ({ children, color }) => React.createElement('span', { 'data-color': color }, children);
          export const Alert = () => null;
          export const Button = () => null;
          export const Spinner = () => null;
          export const Title = () => null;
        `;
      },
      transform(code, id) {
        if (id === entry) return code + '\nexport { HealthStatus, ChainRail, Attention };';
      },
    }],
    build: { ssr: entry, write: false },
  });
  const code = bundle.output.find((output) => output.type === 'chunk').code;
  const { HealthStatus, ChainRail, Attention } = await import(`data:text/javascript;base64,${Buffer.from(code).toString('base64')}`);
  const run = {
    namespace: 'art-acm-tenant', name: 'images-partial', uid: 'images-uid',
    status: 'Warning', message: 'Images excluded after rebase failure: must-gather',
  };
  const chain = {
    status: 'Warning', active: [], failures: [],
    stages: ['layered-products-scan', 'build-layered-products', 'olm-bundle-konflux', 'build-fbc']
      .map((pipeline) => ({ pipeline, runs: pipeline === 'build-layered-products' ? [run] : [] })),
  };
  const status = renderToStaticMarkup(createElement(HealthStatus, { status: 'Warning' }));
  assert.match(status, /data-color="orange"/);
  assert.match(status, /Warning/);
  const rail = renderToStaticMarkup(createElement(ChainRail, { chain }));
  assert.match(rail, /health-rail-node health-warning/);
  assert.doesNotMatch(rail, /health-succeeded/);
  const attention = renderToStaticMarkup(createElement(Attention, { item: { currentChain: chain, health: 'Warning' } }));
  assert.match(attention, /Images excluded after rebase failure: must-gather/);
  assert.match(attention, /#\/run\/art-acm-tenant\/images-partial\?uid=images-uid/);
  assert.doesNotMatch(attention, /Inspect incomplete chain/);
});
