import assert from 'node:assert/strict';
import test from 'node:test';
import { filterReleases, groupReleaseItems, releaseAttention, releaseColor, releaseStages } from '../src/releaseStatusUtils.js';
import { parseHash, viewHref } from '../src/navigation.js';

const release = { group: 'acm-5.0', assembly: '5.0.0', preparation: { status: 'Succeeded' },
  mr: { iid: 1142, state: 'opened', title: 'Shipment for rhacm2 5.0.0' }, ci: { current: true, status: 'Succeeded' } };

test('an old successful pipeline cannot look current or ready to merge', () => {
  assert.equal(releaseAttention({ ...release, ci: { current: false, status: 'Succeeded' } }), 'Current revision unverified');
  assert.equal(releaseColor('Historical'), 'orange');
});

test('MR state, preparation failures, and manual gates remain distinct', () => {
  assert.equal(releaseAttention({ ...release, mr: { ...release.mr, state: 'merged' } }), 'Merged');
  assert.equal(releaseAttention({ ...release, mr: { ...release.mr, state: 'closed' } }), 'Closed');
  assert.equal(releaseAttention({ ...release, mr: null, preparation: { status: 'Failed' } }), 'Preparation failed');
  assert.equal(releaseAttention({ ...release, ci: { current: true, status: 'Manual action' } }), 'Manual action required');
  assert.equal(releaseAttention({ ...release, dryRun: true }), 'Dry run');
  assert.equal(releaseAttention({ ...release, testMode: true }), 'Test / debug');
});

test('live filters find MR numbers and include unlinked preparations in active work', () => {
  const orphan = { ...release, mr: null, ci: null, preparation: { status: 'Failed' } };
  assert.deepEqual(filterReleases([release, orphan], '1142', 'all'), [release]);
  assert.deepEqual(filterReleases([orphan], 'ACM', 'active'), [orphan]);
  assert.deepEqual(filterReleases([orphan], '', 'attention'), [orphan]);
  assert.deepEqual(filterReleases([orphan], '', 'merged'), []);
});

test('the status dashboard has a tenant-aware route', () => {
  for (const kind of ['release-status']) {
    const view = { kind, filterNamespace: 'art-acm-tenant' };
    assert.deepEqual(parseHash(viewHref(view)), view);
  }
});

test('a merged shipment does not turn missing preparation or pipeline evidence green', () => {
  const merged = { ...release, preparation: { status: 'Unknown' }, mr: { ...release.mr, state: 'merged', sha: 'source' } };
  const stages = releaseStages(merged, null);
  assert.equal(stages[0].status, 'Unknown');
  assert.equal(stages[0].state, 'pending');
  assert.equal(stages[2].status, 'Unknown');
  assert.equal(stages[6].state, 'complete');
  assert.equal(stages.some((stage) => stage.state === 'current'), false);
});

test('observed approval and release stages select the current step without inventing success', () => {
  const item = { ...release, mr: { ...release.mr, sha: 'source' } };
  const detail = { mr: { sha: 'source' }, approvals: { status: 'Waiting' }, stages: [
    { name: 'Validation', status: 'Succeeded' }, { name: 'Stage release', status: 'Succeeded' }, { name: 'Production', status: 'Manual action' },
  ] };
  const stages = releaseStages(item, detail);
  assert.equal(stages[4].state, 'current');
  assert.equal(stages[4].section, 'Approvals');
  assert.equal(stages[3].state, 'complete');
  assert.equal(releaseStages(item, detail, true)[3].status, 'Unknown');
  assert.equal(releaseStages(item, { ...detail, mr: { sha: 'old' } })[3].status, 'Unknown');
});

test('pending validation stays pending and does not skip ahead to approvals', () => {
  const item = { ...release, mr: { ...release.mr, sha: 'source' } };
  const detail = { mr: { sha: 'source' }, approvals: { status: 'Waiting' }, stages: [
    { name: 'Validation', status: 'Pending' }, { name: 'Stage release', status: 'Pending' }, { name: 'Production', status: 'Manual action' },
  ] };
  const stages = releaseStages(item, detail);
  assert.equal(stages[2].status, 'Pending');
  assert.equal(stages[2].state, 'current');
  assert.equal(stages[4].state, 'pending');
  assert.equal(stages.some((stage) => stage.status === 'Running'), false);
  assert.equal(releaseColor('Pending'), 'grey');
});

test('versions group newest to oldest without changing independent attempt outcomes', () => {
  const failed = { ...release, id: 'failed', mr: null, preparation: { status: 'Failed' }, updated: '2026-11-04T12:00:00Z' };
  const merged = { ...release, id: 'merged', mr: { ...release.mr, state: 'merged' }, updated: '2026-11-06T12:00:00Z' };
  const other = { ...release, id: 'other', group: 'mce-5.0', updated: '2026-11-07T12:00:00Z' };
  const closed = { ...release, id: 'closed', mr: { ...release.mr, state: 'closed' }, updated: '2026-11-05T12:00:00Z' };
  const items = [failed, merged, other, closed];
  const groups = groupReleaseItems(items);
  assert.equal(groups.length, 2);
  assert.deepEqual(groups[0].items, [other]);
  assert.deepEqual(groups[1].items, [merged, closed, failed]);
  assert.equal(releaseAttention(groups[1].items[0]), 'Merged');
  assert.equal(releaseAttention(groups[1].items[2]), 'Preparation failed');
  assert.deepEqual(items, [failed, merged, other, closed]);
});

test('different tenants or assemblies do not share a version history', () => {
  const items = [release, { ...release, namespace: 'another-tenant' }, { ...release, assembly: '5.0.1' }];
  assert.equal(groupReleaseItems(items).length, 3);
});
