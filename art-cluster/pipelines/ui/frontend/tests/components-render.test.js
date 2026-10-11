import assert from 'node:assert/strict';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { build } from 'vite';

test('component table renders zero attempts explicitly and omits missing attempts', async () => {
  const bundle = await build({
    configFile: false,
    logLevel: 'silent',
    ssr: { noExternal: ['react', '@patternfly/react-core'] },
    plugins: [{
      name: 'component-render-test',
      enforce: 'pre',
      resolveId(id) {
        if (id === 'react') return { id: import.meta.resolve('react'), external: true };
        if (id === '@patternfly/react-core') return '\0alert-stub';
      },
      load(id) {
        // The table has no reporting warning; exclude the Alert's browser-only CSS.
        if (id === '\0alert-stub') return 'export const Alert = () => null;';
      },
    }],
    build: { ssr: new URL('../src/Components.jsx', import.meta.url).pathname, write: false },
  });
  const code = bundle.output.find((output) => output.type === 'chunk').code;
  const { default: Components } = await import(`data:text/javascript;base64,${Buffer.from(code).toString('base64')}`);
  const Status = ({ value }) => createElement('span', null, value);
  const render = (attempt) => renderToStaticMarkup(createElement(Components, {
    Status,
    run: {
      componentReporting: { available: true },
      components: [{
        id: 'component', name: 'operator', displayName: 'operator', type: 'image', status: 'Succeeded',
        pipelineRuns: [{ name: 'build', namespace: 'tenant', attempt, status: 'Succeeded' }],
      }],
    },
  }));
  assert.match(render(0), /<small>Attempt 0<\/small>/);
  assert.match(render(1), /<small>Attempt 1<\/small>/);
  assert.doesNotMatch(render(null), /Attempt /);
  assert.doesNotMatch(render(undefined), /Attempt /);
  assert.match(render(1), /<option value="Has failed attempts">Has failed attempts<\/option>/);
});
