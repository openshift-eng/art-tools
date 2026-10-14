import assert from 'node:assert/strict';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { build } from 'vite';

test('incident page shows timestamps, references, and controls to admins', async () => {
  const bundle = await build({
    configFile: false,
    logLevel: 'silent',
    ssr: { noExternal: ['react', '@patternfly/react-core'] },
    plugins: [{
      name: 'incident-render-test',
      enforce: 'pre',
      resolveId(id) {
        if (id === 'react') return { id: import.meta.resolve('react'), external: true };
        if (id === '@patternfly/react-core') return '\0patternfly-stub';
      },
      load(id) {
        if (id === '\0patternfly-stub') return `
          import React from 'react';
          export const Alert = ({ title, children, className }) => React.createElement('section', { className }, React.createElement('h2', null, title), children);
          export const Button = ({ children, ...props }) => React.createElement('button', props, children);
          export const Label = ({ children }) => React.createElement('span', null, children);
          export const Title = ({ children }) => React.createElement('h1', null, children);
        `;
      },
    }],
    build: { ssr: new URL('../src/Incidents.jsx', import.meta.url).pathname, write: false },
  });
  const code = bundle.output.find((output) => output.type === 'chunk').code;
  const { default: Incidents } = await import(`data:text/javascript;base64,${Buffer.from(code).toString('base64')}`);
  const incident = {
    id: 'kerberos-1', title: 'Kerberos issue', message: 'Scans may fail', severity: 'warning',
    status: 'investigating', started: '2026-10-14T08:00:00Z', updated: '2026-10-14T09:00:00Z',
    description: 'Token renewal is intermittent.',
    links: [{ label: 'Slack thread', url: 'https://slack.example/thread/1' }],
  };
  const render = (canManage) => renderToStaticMarkup(createElement(Incidents, {
    incidents: [incident], canManage, csrfToken: 'csrf', request: async () => ({}), onChange() {},
  }));

  const adminPage = render(true);
  assert.match(adminPage, /Create incident/);
  assert.match(adminPage, /Close incident/);
  assert.match(adminPage, /Edit/);
  assert.match(adminPage, /dateTime="2026-10-14T08:00:00Z"/);
  assert.match(adminPage, /dateTime="2026-10-14T09:00:00Z"/);
  assert.match(adminPage, /Slack thread/);
  assert.match(adminPage, /rel="noopener noreferrer"/);

  const readerPage = render(false);
  assert.doesNotMatch(readerPage, /Create incident|Close incident/);
  assert.match(readerPage, /Kerberos issue/);
});

test('resolved incidents display their close time without active controls', async () => {
  const bundle = await build({
    configFile: false,
    logLevel: 'silent',
    ssr: { noExternal: ['react', '@patternfly/react-core'] },
    plugins: [{
      name: 'incident-render-test',
      enforce: 'pre',
      resolveId(id) {
        if (id === 'react') return { id: import.meta.resolve('react'), external: true };
        if (id === '@patternfly/react-core') return '\0patternfly-stub';
      },
      load(id) {
        if (id === '\0patternfly-stub') return `
          import React from 'react';
          export const Alert = ({ title, children, className }) => React.createElement('section', { className }, React.createElement('h2', null, title), children);
          export const Button = ({ children, ...props }) => React.createElement('button', props, children);
          export const Label = ({ children }) => React.createElement('span', null, children);
          export const Title = ({ children }) => React.createElement('h1', null, children);
        `;
      },
    }],
    build: { ssr: new URL('../src/Incidents.jsx', import.meta.url).pathname, write: false },
  });
  const code = bundle.output.find((output) => output.type === 'chunk').code;
  const { default: Incidents } = await import(`data:text/javascript;base64,${Buffer.from(code).toString('base64')}`);
  const markup = renderToStaticMarkup(createElement(Incidents, {
    canManage: true,
    incidents: [{
      id: 'resolved-1', title: 'Resolved issue', message: 'Scans recovered', severity: 'info',
      status: 'resolved', started: '2026-10-14T08:00:00Z', updated: '2026-10-14T10:00:00Z',
      resolvedAt: '2026-10-14T10:00:00Z', links: [],
    }],
  }));

  assert.match(markup, /Closed <time dateTime="2026-10-14T10:00:00Z">/);
  assert.doesNotMatch(markup, /Close incident|>Edit</);
  assert.match(markup, /Resolved issue/);
});
