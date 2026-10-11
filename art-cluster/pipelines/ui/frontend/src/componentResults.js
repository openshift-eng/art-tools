/** Filter final build results or any failed attempt, alongside component names. */
export function filterComponents(components, search, result) {
  const query = search.trim().toLowerCase();
  return components.filter((component) =>
    (!result || (result === 'Has failed attempts'
      ? component.status === 'Failed' || (component.pipelineRuns || []).some((build) => build.status === 'Failed')
      : component.status === result)) &&
    `${component.name} ${component.displayName || ''}`.toLowerCase().includes(query)
  );
}

/** Count components by their final build result, independently of retry history. */
export function componentCounts(components) {
  return components.reduce((counts, component) => {
    counts[component.status] = (counts[component.status] || 0) + 1;
    return counts;
  }, {});
}
