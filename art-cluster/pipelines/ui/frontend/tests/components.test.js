import assert from 'node:assert/strict';
import test from 'node:test';
import { componentCounts, filterComponents } from '../src/componentResults.js';
import { parseHash, viewHref } from '../src/navigation.js';

const components = [
  { name: 'first', displayName: 'first-bundle', status: 'Succeeded',
    pipelineRuns: [{ status: 'Failed' }, { status: 'Succeeded' }] },
  { name: 'second', status: 'Succeeded' },
  { name: 'third', status: 'Failed' },
];

test('counts successful and failed component builds', () => {
  assert.deepEqual(componentCounts(components), { Succeeded: 2, Failed: 1 });
});

test('filters component names by their build result', () => {
  assert.deepEqual(filterComponents(components, '', 'Failed').map((c) => c.name), ['third']);
  assert.deepEqual(filterComponents(components, '', 'Succeeded').map((c) => c.name), ['first', 'second']);
});

test('search matches component and display names case insensitively', () => {
  assert.equal(filterComponents(components, ' FIRST-BUNDLE ', '')[0].name, 'first');
  assert.equal(filterComponents(components, 'third', 'Failed')[0].name, 'third');
  assert.equal(filterComponents(components, 'first', 'Failed').length, 0);
});

test('failed-attempt filter includes recovered retries and final failures', () => {
  assert.deepEqual(filterComponents(components, '', 'Has failed attempts').map((c) => c.name), ['first', 'third']);
  assert.deepEqual(filterComponents(components, 'BUNDLE', 'Has failed attempts').map((c) => c.name), ['first']);
  assert.equal(filterComponents(components, 'second', 'Has failed attempts').length, 0);
  assert.deepEqual(componentCounts(components), { Succeeded: 2, Failed: 1 });
});

test('component tab deep links preserve archived UID and tenant', () => {
  const view = { kind: 'run', namespace: 'tenant', name: 'run', uid: 'archived-uid',
    tab: 'components', filterNamespace: 'tenant' };
  assert.deepEqual(parseHash(viewHref(view)), view);
});
