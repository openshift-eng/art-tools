export const componentResult = (component) => component.resultStatus || component.status;

export function checkColumns(components) {
  return [{ kind: 'conforma', label: 'Conforma' }, { kind: 'custom', label: 'Custom ITS' }]
    .filter(({ kind }) => components.some((component) => (component.checks || []).some((check) => check.kind === kind)));
}

const failedCheck = (checks) => (checks || []).some((check) => ['Failed', 'Cancelled'].includes(check.status));

/** Filter component outcomes or any failed build/check attempt, alongside names. */
export function filterComponents(components, search, result) {
  const query = search.trim().toLowerCase();
  return components.filter((component) =>
    (!result || (result === 'Has failed attempts'
      ? componentResult(component) === 'Failed' || failedCheck(component.checks)
        || (component.pipelineRuns || []).some((build) => build.status === 'Failed' || failedCheck(build.checks))
      : componentResult(component) === result)) &&
    `${component.name} ${component.displayName || ''}`.toLowerCase().includes(query)
  );
}

/** Count components by their build and blocking-check outcome. */
export function componentCounts(components) {
  return components.reduce((counts, component) => {
    const result = componentResult(component);
    counts[result] = (counts[result] || 0) + 1;
    return counts;
  }, {});
}
