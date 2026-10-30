import assert from 'node:assert/strict';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { build } from 'vite';

async function loadComponents() {
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
  return (await import(`data:text/javascript;base64,${Buffer.from(code).toString('base64')}`)).default;
}

const loaded = loadComponents();

test('component table renders zero attempts explicitly and omits missing attempts', async () => {
  const Components = await loaded;
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

test('conditional check columns show blocking failures, optional checks, and disabled rows', async () => {
  const Components = await loaded;
  const checks = [
    { kind: 'conforma', name: 'Conforma', blocking: true, status: 'Failed', reason: 'Policy rejected', url: 'https://konflux.example/ec' },
    { kind: 'custom', name: 'qe', blocking: false, status: 'Skipped', reason: 'Earlier check failed' },
  ];
  const component = {
    id: 'operator', name: 'operator', type: 'image', status: 'Succeeded', resultStatus: 'Failed', checksRecorded: true,
    checks, pipelineRuns: [{ name: 'build', status: 'Succeeded', checks, attempt: 1 }],
  };
  const Status = ({ value }) => createElement('span', null, value);
  const render = (components) => renderToStaticMarkup(createElement(Components, {
    Status, run: { componentReporting: { available: true }, components },
  }));
  const html = render([component, { ...component, id: 'disabled', name: 'disabled', checks: [], pipelineRuns: [] }]);
  assert.match(html, /<th>Conforma<\/th><th>Custom ITS<\/th>/);
  assert.match(html, /<td><span>Failed<\/span><\/td><td><span>Succeeded<\/span><\/td>/);
  assert.match(html, /href="https:\/\/konflux.example\/ec" target="_blank" rel="noopener noreferrer"/);
  assert.match(html, /Policy rejected/);
  assert.match(html, /Optional/);
  assert.match(html, /Not run/);
  assert.match(html, /Not enabled/);
  assert.match(html, /<details><summary>Checks<\/summary>/);
  const disabled = render([{ ...component, checks: [] }]);
  assert.doesNotMatch(disabled, /<th>Conforma<\/th>|<th>Custom ITS<\/th>/);
  const legacy = render([{ ...component, checks: [], checksRecorded: false }]);
  assert.match(legacy, /Check results were not recorded/);
  assert.doesNotMatch(legacy, /<th>Conforma<\/th>|<th>Custom ITS<\/th>/);
});
