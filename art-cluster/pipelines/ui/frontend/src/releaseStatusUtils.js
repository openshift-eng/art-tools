export function releaseAttention(release) {
  if (release.dryRun) return 'Dry run';
  if (release.testMode) return 'Test / debug';
  if (!release.mr) return ['Failed', 'Cancelled'].includes(release.preparation.status) ? 'Preparation failed' : release.preparation.status === 'Running' ? 'Preparing' : 'MR not linked';
  if (release.mr.state === 'merged') return 'Merged';
  if (release.mr.state === 'closed') return 'Closed';
  if (release.mr.draft) return 'Draft';
  if (!release.ci) return 'No pipeline observed';
  if (!release.ci.current) return 'Current revision unverified';
  if (release.ci.status === 'Failed') return 'Pipeline failed';
  if (release.ci.status === 'Manual action') return 'Manual action required';
  if (release.ci.status === 'Succeeded') return 'Awaiting merge';
  return release.ci.status;
}

export function releaseColor(status) {
  if (['Succeeded', 'Merged'].includes(status)) return 'green';
  if (['Failed', 'Pipeline failed', 'Preparation failed'].includes(status)) return 'red';
  if (['Running', 'Preparing'].includes(status)) return 'blue';
  if (['Waiting', 'Manual action', 'Manual action required', 'Current revision unverified', 'Historical', 'Awaiting merge'].includes(status)) return 'orange';
  return 'grey';
}

export function filterReleases(items, search, status) {
  const query = search.toLowerCase();
  return items.filter((release) => `${release.group} ${release.assembly} ${release.mr?.iid || ''} ${release.mr?.title || ''}`.toLowerCase().includes(query)
    && (status === 'all' || (status === 'active' ? !['merged', 'closed'].includes(release.mr?.state)
      : status === 'attention' ? !['Merged', 'Closed', 'Running', 'Preparing'].includes(releaseAttention(release)) : release.mr?.state === status)));
}

export function releaseStages(release, detail, unavailable = false) {
  const verified = Boolean(detail && release.mr && detail.mr.sha === release.mr.sha && !unavailable);
  const observed = (name) => verified ? detail.stages.find((stage) => stage.name === name)?.status || 'Unknown' : 'Unknown';
  const values = [
    ['Prepare release', release.dryRun ? 'Dry run' : release.preparation.status, 'Overview'],
    ['Shipment MR', release.mr ? release.mr.draft ? 'Draft' : release.mr.state === 'closed' ? 'Closed' : 'Created' : 'Not linked', 'Shipments'],
    ['Validate', observed('Validation'), 'Validations'],
    ['Stage release', observed('Stage release'), 'Activity'],
    ['Approvals', verified ? detail.approvals.status : 'Unknown', 'Approvals'],
    ['Production', observed('Production'), 'Activity'],
    ['Merge', release.mr?.state === 'merged' ? 'Merged' : release.mr?.state === 'closed' ? 'Closed' : 'Not merged', 'Overview'],
  ];
  let current = values.findIndex(([, status]) => status === 'Running');
  if (current < 0) current = values.findIndex(([, status]) => ['Waiting', 'Manual action', 'Draft'].includes(status));
  if (['merged', 'closed'].includes(release.mr?.state)) current = -1;
  return values.map(([name, status, section], index) => ({
    name, status, section,
    state: ['Succeeded', 'Created', 'Merged'].includes(status) ? 'complete'
      : ['Failed', 'Cancelled'].includes(status) ? 'failed' : index === current ? 'current' : 'pending',
  }));
}

export function groupReleaseItems(items) {
  const versions = new Map();
  const newestFirst = [...items].sort((a, b) => (b.updated || '').localeCompare(a.updated || ''));
  for (const item of newestFirst) {
    const id = JSON.stringify([item.namespace, item.group, item.assembly]);
    if (!versions.has(id)) versions.set(id, { id, name: `${item.group} / ${item.assembly || 'Unknown assembly'}`, items: [] });
    versions.get(id).items.push(item);
  }
  return [...versions.values()];
}
