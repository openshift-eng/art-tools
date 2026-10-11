import assert from 'node:assert/strict';
import test from 'node:test';
import { componentCounts, filterComponents, primaryResources } from '../src/componentResults.js';
import { parseHash, viewHref } from '../src/navigation.js';

const components = [
  { name: 'first', displayName: 'first-bundle', status: 'Succeeded', hasFailures: false, nvr: 'first-1-2' },
  { name: 'second', status: 'Succeeded', hasFailures: true, operatorNvr: 'operator-2-1' },
  { name: 'third', status: 'Failed', hasFailures: true },
];

test('component counts use required overall outcomes', () => {
  assert.deepEqual(componentCounts(components), { Succeeded: 2, Failed: 1 });
});

test('failure filter includes optional failures and recovered attempts', () => {
  assert.deepEqual(filterComponents(components, '', true).map((c) => c.name), ['second', 'third']);
});

test('search matches display names and NVRs case insensitively', () => {
  assert.equal(filterComponents(components, ' FIRST-BUNDLE ', false)[0].name, 'first');
  assert.equal(filterComponents(components, 'operator-2', true)[0].name, 'second');
  assert.equal(filterComponents(components, 'first-1', true).length, 0);
});

test('main links select latest resource of each role while expanded data keeps attempts', () => {
  const resources = [{ role: 'build', name: 'first', url: 'https://example/first' },
    { role: 'build', name: 'retry', url: 'https://example/retry' },
    { role: 'EC', name: 'ec', url: 'https://example/ec' }, { role: 'release', name: 'missing', url: null }];
  assert.deepEqual(primaryResources(resources).map((r) => r.name), ['retry', 'ec']);
  assert.equal(resources.length, 4);
});

test('component tab deep links preserve archived UID and tenant', () => {
  const view = { kind: 'run', namespace: 'tenant', name: 'run', uid: 'archived-uid',
    tab: 'components', filterNamespace: 'tenant' };
  assert.deepEqual(parseHash(viewHref(view)), view);
});
