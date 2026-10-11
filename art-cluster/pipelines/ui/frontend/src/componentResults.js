export function filterComponents(components, search, failuresOnly) {
  const query = search.trim().toLowerCase();
  return components.filter((component) =>
    (!failuresOnly || component.hasFailures) &&
    `${component.name} ${component.displayName || ''} ${component.nvr || ''} ${component.operatorNvr || ''}`.toLowerCase().includes(query)
  );
}

export function componentCounts(components) {
  return components.reduce((counts, component) => {
    counts[component.status] = (counts[component.status] || 0) + 1;
    return counts;
  }, {});
}

export function primaryResources(resources) {
  const latest = new Map();
  for (const resource of resources) {
    if (!resource.url) continue;
    const previous = latest.get(resource.role);
    if (previous?.attempt && resource.attempt && previous.attempt > resource.attempt) continue;
    latest.set(resource.role, resource);
  }
  return [...latest.values()];
}
