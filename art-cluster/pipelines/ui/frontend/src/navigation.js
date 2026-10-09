export const pipelineTabs = ['details', 'runs'];
export const runTabs = ['details', 'logs', 'events', 'parameters', 'tasks'];

export function parseHash(hash = window.location.hash) {
  const [path, search] = hash.replace(/^#\/?/, '').split('?');
  const [kind, namespace, name] = path.split('/');
  const params = new URLSearchParams(search);
  const filterNamespace = params.get('namespace') || '';
  if ((kind === 'pipeline' || kind === 'run') && namespace && name) {
    const tabs = kind === 'pipeline' ? pipelineTabs : runTabs;
    return {
      kind, namespace: decodeURIComponent(namespace), name: decodeURIComponent(name), filterNamespace,
      ...(kind === 'run' ? { uid: params.get('uid') } : {}),
      tab: tabs.includes(params.get('tab')) ? params.get('tab') : 'details',
    };
  }
  return { kind: ['runs', 'health'].includes(kind) ? kind : 'pipelines', filterNamespace };
}

export function viewHref(view) {
  const params = new URLSearchParams();
  let path = ['runs', 'health'].includes(view.kind) ? view.kind : 'pipelines';
  if (view.kind === 'pipeline' || view.kind === 'run') {
    path = `${view.kind}/${encodeURIComponent(view.namespace)}/${encodeURIComponent(view.name)}`;
    if (view.kind === 'run' && view.uid) params.set('uid', view.uid);
    if (view.tab) params.set('tab', view.tab);
  }
  if (view.filterNamespace) params.set('namespace', view.filterNamespace);
  return `#/${path}${params.size ? `?${params}` : ''}`;
}

export function isPlainClick(event) {
  return event.button === 0 && !event.ctrlKey && !event.metaKey && !event.shiftKey && !event.altKey && !event.defaultPrevented;
}

export function navigateRow(event, href, browser = window) {
  if (event.defaultPrevented || event.target.closest('a, button, input, select, textarea, summary, [role="button"]')) return;
  if (event.type === 'keydown' ? event.key !== 'Enter' : ![0, 1].includes(event.button)) return;
  if (event.altKey) return;
  event.preventDefault();
  if (event.ctrlKey || event.metaKey || event.shiftKey || event.button === 1) browser.open(href, '_blank', 'noopener,noreferrer');
  else browser.location.hash = href;
}
