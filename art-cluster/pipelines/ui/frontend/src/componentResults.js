export function filterComponents(components, search, result) {
  const query = search.trim().toLowerCase();
  return components.filter((component) =>
    (!result || component.status === result) &&
    `${component.name} ${component.displayName || ''}`.toLowerCase().includes(query)
  );
}

export function componentCounts(components) {
  return components.reduce((counts, component) => {
    counts[component.status] = (counts[component.status] || 0) + 1;
    return counts;
  }, {});
}
