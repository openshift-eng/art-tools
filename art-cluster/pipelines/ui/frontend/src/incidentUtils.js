export function sortIncidentsNewestFirst(incidents = []) {
  return [...incidents].sort((left, right) => {
    const timestamp = (incident) => {
      const updated = Date.parse(incident.updated || '');
      if (!Number.isNaN(updated)) return updated;
      const started = Date.parse(incident.started || '');
      return Number.isNaN(started) ? 0 : started;
    };
    return timestamp(right) - timestamp(left);
  });
}

export function linkifyText(value) {
  const text = String(value || '');
  const urlPattern = /https?:\/\/[^\s<>"']+/g;
  const nodes = [];
  let cursor = 0;

  for (const match of text.matchAll(urlPattern)) {
    const rawUrl = match[0];
    const url = rawUrl.replace(/[.,!?;:)\]}]+$/, '');
    if (!url) continue;
    nodes.push(text.slice(cursor, match.index));
    nodes.push({ url, suffix: rawUrl.slice(url.length) });
    cursor = match.index + rawUrl.length;
  }
  nodes.push(text.slice(cursor));
  return nodes;
}
