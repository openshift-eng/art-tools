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
          export const Button = ({ children, isDisabled, variant, ...props }) => React.createElement('button', { ...props, disabled: isDisabled }, children);
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
    showOnBanner: true,
    history: [
      { timestamp: '2026-10-14T08:00:00Z', note: 'Incident created.' },
      { timestamp: '2026-10-14T09:00:00Z', note: 'Kerberos renewal is still intermittent.' },
    ],
    description: 'Token renewal is intermittent.',
    links: [{ label: 'Slack thread', url: 'https://slack.example/thread/1' }],
  };
  const render = (canManage) => renderToStaticMarkup(createElement(Incidents, {
    incidents: [incident], canManage, csrfToken: 'csrf', request: async () => ({}), onChange() {},
  }));

  const adminPage = render(true);
  assert.match(adminPage, /Create incident/);
  assert.match(adminPage, /Close incident/);
  assert.match(adminPage, /Delete incident/);
  assert.match(adminPage, /Edit/);
  assert.match(adminPage, /dateTime="2026-10-14T08:00:00Z"/);
  assert.match(adminPage, /dateTime="2026-10-14T09:00:00Z"/);
  assert.match(adminPage, /Slack thread/);
  assert.match(adminPage, /rel="noopener noreferrer"/);
  assert.match(adminPage, /<details class="incident-history">/);
  assert.doesNotMatch(adminPage, /<details class="incident-history" open/);
  assert.match(adminPage, /Incident history/);
  assert.match(adminPage, /2 updates/);
  assert.match(adminPage, /Kerberos renewal is still intermittent/);

  const readerPage = render(false);
  assert.doesNotMatch(readerPage, /Create incident|Close incident|Delete incident/);
  assert.match(readerPage, /Kerberos issue/);
});

test('incident editor exposes banner visibility and edit notes', async () => {
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
          export const Button = ({ children, isDisabled, variant, ...props }) => React.createElement('button', { ...props, disabled: isDisabled }, children);
          export const Label = ({ children }) => React.createElement('span', null, children);
          export const Title = ({ children }) => React.createElement('h1', null, children);
        `;
      },
    }],
    build: { ssr: new URL('../src/Incidents.jsx', import.meta.url).pathname, write: false },
  });
  const code = bundle.output.find((output) => output.type === 'chunk').code;
  const { IncidentEditor, buildIncidentPayload } = await import(`data:text/javascript;base64,${Buffer.from(code).toString('base64')}`);
  const empty = { title: '', message: '', severity: 'warning', status: 'investigating', description: '', links: [], showOnBanner: true, updateNote: '' };
  const createMarkup = renderToStaticMarkup(createElement(IncidentEditor, {
    incident: null, csrfToken: 'csrf', request: async () => ({}), onChange() {}, onClose() {},
  }));
  assert.match(createMarkup, /Show in banner/);
  assert.match(createMarkup, /type="checkbox" checked/);
  assert.doesNotMatch(createMarkup, /Short update note/);

  const editMarkup = renderToStaticMarkup(createElement(IncidentEditor, {
    incident: { id: 'i-1', ...empty, showOnBanner: false },
    csrfToken: 'csrf', request: async () => ({}), onChange() {}, onClose() {},
  }));
  assert.match(editMarkup, /Short update note/);
  assert.match(editMarkup, /required/);
  assert.doesNotMatch(editMarkup, /type="checkbox" checked/);
  assert.deepEqual(buildIncidentPayload({ ...empty, updateNote: '  status update  ' }, true), {
    ...empty, updateNote: 'status update',
  });
  assert.deepEqual(buildIncidentPayload(empty, false), {
    title: '', message: '', severity: 'warning', status: 'investigating', description: '', links: [], showOnBanner: true,
  });
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
          export const Button = ({ children, isDisabled, variant, ...props }) => React.createElement('button', { ...props, disabled: isDisabled }, children);
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
    }, {
      id: 'deleted-1', title: 'Removed from active incidents', message: 'Kept for history', severity: 'warning',
      status: 'deleted', started: '2026-10-14T08:00:00Z', updated: '2026-10-14T11:00:00Z',
      deletedAt: '2026-10-14T11:00:00Z', links: [],
    }],
  }));

  assert.match(markup, /Closed <time dateTime="2026-10-14T10:00:00Z">/);
  assert.doesNotMatch(markup, /Close incident|>Edit</);
  assert.match(markup, /Resolved issue/);
  assert.match(markup, /Deleted from active incidents/);
  assert.match(markup, /Deleted <time dateTime="2026-10-14T11:00:00Z">/);
  assert.match(markup, /Removed from active incidents/);
});
