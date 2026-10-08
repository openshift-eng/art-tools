import React, { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { Alert, Button, Label, Spinner, Title } from '@patternfly/react-core';

async function request(path, options = {}) {
  const response = await fetch(path, { credentials: 'same-origin', ...options });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.detail || `Request failed (${response.status})`);
  return body;
}

const runTabs = ['details', 'logs', 'events', 'parameters', 'tasks'];

function parseHash() {
  const parts = window.location.hash.slice(2).split('/');
  if (parts[0] === 'pipeline' && parts[1] && parts[2]) {
    return { kind: 'pipeline', namespace: parts[1], name: decodeURIComponent(parts[2]) };
  }
  if (parts[0] === 'run' && parts[1] && parts[2]) {
    const [name, search] = parts[2].split('?');
    const params = new URLSearchParams(search);
    const tab = params.get('tab');
    return {
      kind: 'run',
      namespace: parts[1],
      name: decodeURIComponent(name),
      uid: params.get('uid'),
      tab: runTabs.includes(tab) ? tab : 'details',
    };
  }
  return { kind: parts[0] === 'runs' ? 'runs' : 'pipelines' };
}

function navigate(view) {
  if (view.kind === 'pipeline') window.location.hash = `#/pipeline/${view.namespace}/${encodeURIComponent(view.name)}`;
  else if (view.kind === 'run') window.location.hash = `#/run/${view.namespace}/${encodeURIComponent(view.name)}?uid=${encodeURIComponent(view.uid || '')}`;
  else window.location.hash = view.kind === 'runs' ? '#/runs' : '#/pipelines';
}

function formatDate(value) {
  return value ? new Date(value).toLocaleString() : '—';
}

function formatDuration(started, completed, now) {
  if (!started) return '—';
  const seconds = Math.floor((new Date(completed).getTime() || now) / 1000 - new Date(started).getTime() / 1000);
  if (!Number.isFinite(seconds)) return '—';
  const elapsed = Math.max(0, seconds);
  const hours = Math.floor(elapsed / 3600);
  const minutes = Math.floor((elapsed % 3600) / 60);
  const remainder = elapsed % 60;
  return [hours && `${hours}h`, (hours || minutes) && `${minutes}m`, `${remainder}s`].filter(Boolean).join(' ');
}

function statusColor(status) {
  if (status === 'Succeeded') return 'green';
  if (status === 'Running') return 'blue';
  if (status === 'Failed' || status?.includes('Timeout')) return 'red';
  if (status === 'Cancelled') return 'orange';
  return 'grey';
}

function Status({ value }) {
  return <Label color={statusColor(value)}>{value || 'Unknown'}</Label>;
}

function RunDetailField({ label, children }) {
  return <div className="run-detail-field"><strong>{label}</strong><div>{children ?? '—'}</div></div>;
}

function MetadataValues({ values, chips = false }) {
  const entries = Object.entries(values || {}).sort(([left], [right]) => left.localeCompare(right));
  if (!entries.length) return <span className="muted">None</span>;
  return <div className={chips ? 'metadata-chips' : 'metadata-rows'}>{entries.map(([key, value]) => chips
    ? <span className="metadata-chip" key={key}><code>{key}</code><span>=</span><code>{String(value)}</code></span>
    : <div className="metadata-row" key={key}><code>{key}</code><span>{String(value)}</span></div>)}</div>;
}

function RunDetails({ run }) {
  const pipelineHref = run.pipeline
    ? `#/pipeline/${encodeURIComponent(run.namespace)}/${encodeURIComponent(run.pipeline)}`
    : null;
  const parentHref = run.parentPipelineRun
    ? `#/run/${encodeURIComponent(run.namespace)}/${encodeURIComponent(run.parentPipelineRun)}`
    : null;
  const consoleHref = `https://console-openshift-console.apps.artc2023.pc3z.p1.openshiftapps.com/k8s/ns/${encodeURIComponent(run.namespace)}/tekton.dev~v1~PipelineRun/${encodeURIComponent(run.name)}/logs`;
  return <section className="panel run-details-panel" role="tabpanel" aria-label="PipelineRun details">
    <h2>PipelineRun details</h2>
    <div className="run-details-grid">
      <div className="run-detail-column">
        <RunDetailField label="Name">{run.name}</RunDetailField>
        <RunDetailField label="Namespace">{run.namespace}</RunDetailField>
        <RunDetailField label="Labels"><MetadataValues values={run.labels} chips /></RunDetailField>
        <RunDetailField label="Annotations"><MetadataValues values={run.annotations} /></RunDetailField>
        <RunDetailField label="Parent PipelineRun">{parentHref ? <a href={parentHref}>{run.parentPipelineRun}</a> : null}</RunDetailField>
        <RunDetailField label="OpenShift Console"><a href={consoleHref} target="_blank" rel="noopener noreferrer">View in OpenShift Console</a></RunDetailField>
      </div>
      <div className="run-detail-column">
        <RunDetailField label="Status"><Status value={run.status} />{run.message && <small className="run-detail-message">{run.message}</small>}</RunDetailField>
        <RunDetailField label="Pipeline">{pipelineHref ? <a href={pipelineHref}>{run.pipeline}</a> : null}</RunDetailField>
        <RunDetailField label="Created">{formatDate(run.created)}</RunDetailField>
        <RunDetailField label="Start time">{formatDate(run.started)}</RunDetailField>
        <RunDetailField label="Completion time">{formatDate(run.completed)}</RunDetailField>
        <RunDetailField label="Duration">{formatDuration(run.started, run.completed, Date.now())}</RunDetailField>
        <RunDetailField label="UID">{run.uid}</RunDetailField>
        <RunDetailField label="Source">{run.source === 'archive' ? 'Tekton Results' : 'Live cluster'}</RunDetailField>
      </div>
    </div>
  </section>;
}

function Field({ label, children, hint }) {
  return <label className="field"><span className="field-label">{label}</span>{children}{hint && <span className="field-hint">{hint}</span>}</label>;
}

function LogText({ text, streaming }) {
  const logRef = useRef(null);
  const followTail = useRef(true);
  useLayoutEffect(() => {
    if (streaming && followTail.current && logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [text, streaming]);
  const onScroll = () => {
    const element = logRef.current;
    if (element) followTail.current = element.scrollHeight - element.scrollTop - element.clientHeight < 48;
  };
  const content = useMemo(() => {
    const parts = [];
    const urlPattern = /https?:\/\/[^\s<>"'`]+/g;
    let cursor = 0;
    for (const match of text.matchAll(urlPattern)) {
      const url = match[0].replace(/[.,;:!?)}\]]+$/, '');
      parts.push(text.slice(cursor, match.index));
      parts.push(<a key={match.index} href={url} target="_blank" rel="noopener noreferrer">{url}</a>);
      cursor = match.index + url.length;
    }
    parts.push(text.slice(cursor));
    return parts;
  }, [text]);
  return <pre ref={logRef} onScroll={onScroll}>{content}</pre>;
}

function ThemeIcon({ theme }) {
  return <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    {theme === 'dark' ? <><circle cx="12" cy="12" r="4" /><path d="M12 2v2m0 16v2M4.93 4.93l1.42 1.42m11.3 11.3 1.42 1.42M2 12h2m16 0h2M4.93 19.07l1.42-1.42m11.3-11.3 1.42-1.42" /></> : <path d="M20.985 12.486A9 9 0 0 1 11.514 3.015 9 9 0 1 0 20.985 12.486Z" />}
  </svg>;
}

function App() {
  const [view, setView] = useState(parseHash);
  const [namespaces, setNamespaces] = useState([]);
  const [namespace, setNamespace] = useState('');
  const [session, setSession] = useState(null);
  const [error, setError] = useState('');
  const [theme, setTheme] = useState(() => document.documentElement.dataset.theme || 'light');

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    document.documentElement.classList.toggle('pf-v6-theme-dark', theme === 'dark');
    try { window.localStorage.setItem('art-pipelines-theme', theme); } catch { /* Browser storage may be unavailable. */ }
  }, [theme]);

  useEffect(() => {
    const onHashChange = () => setView(parseHash());
    window.addEventListener('hashchange', onHashChange);
    Promise.all([request('/api/session'), request('/api/namespaces')])
      .then(([identity, result]) => { setSession(identity); setNamespaces(result.namespaces); })
      .catch((cause) => setError(cause.message));
    return () => window.removeEventListener('hashchange', onHashChange);
  }, []);

  return <div className="app-shell">
    <header className="topbar">
      <div className="brand" onClick={() => navigate({ kind: 'pipelines' })} role="button" tabIndex={0} onKeyDown={(event) => event.key === 'Enter' && navigate({ kind: 'pipelines' })}>
        <span className="brand-mark">ART</span><span>Pipelines</span>
      </div>
      <nav className="topnav" aria-label="Workspace navigation">
        <button className={view.kind === 'pipelines' || view.kind === 'pipeline' ? 'nav active' : 'nav'} onClick={() => navigate({ kind: 'pipelines' })}>Pipelines</button>
        <button className={view.kind === 'runs' || view.kind === 'run' ? 'nav active' : 'nav'} onClick={() => navigate({ kind: 'runs' })}>PipelineRuns</button>
      </nav>
      <div className="topbar-right">
        <select className="namespace-select" value={namespace} onChange={(event) => setNamespace(event.target.value)} aria-label="Namespace">
          <option value="">All accessible tenants</option>
          {namespaces.map((item) => <option key={item} value={item}>{item}</option>)}
        </select>
        <span className="cluster-name">artc2023</span>
        <button className="theme-toggle" type="button" aria-label={theme === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'} onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}><ThemeIcon theme={theme} /></button>
        <span className="user-name">{session?.user || 'OpenShift user'}</span>
      </div>
    </header>
    <main className="content">
      {error && <Alert variant="danger" title={error} className="notice" />}
      {view.kind === 'pipelines' && <Pipelines namespace={namespace} />}
      {view.kind === 'runs' && <Runs namespace={namespace} />}
      {view.kind === 'pipeline' && <PipelineDetail view={view} csrfToken={session?.csrfToken} />}
      {view.kind === 'run' && <RunDetail view={view} csrfToken={session?.csrfToken} />}
    </main>
  </div>;
}

function Pipelines({ namespace }) {
  const [items, setItems] = useState([]);
  const [latestRuns, setLatestRuns] = useState({});
  const [latestLoading, setLatestLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [refreshKey, setRefreshKey] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [latestError, setLatestError] = useState('');
  useEffect(() => {
    let active = true;
    setLoading(true);
    setError('');
    request(`/api/pipelines${namespace ? `?namespace=${encodeURIComponent(namespace)}` : ''}`)
      .then((data) => { if (active) setItems(data.items); })
      .catch((cause) => { if (active) setError(cause.message); })
      .finally(() => { if (active) setLoading(false); });
    return () => { active = false; };
  }, [namespace, refreshKey]);
  useEffect(() => {
    let active = true;
    setLatestRuns({});
    setLatestLoading(true);
    setLatestError('');
    request(`/api/pipelines/latest-runs${namespace ? `?namespace=${encodeURIComponent(namespace)}` : ''}`)
      .then((data) => {
        if (!active) return;
        setLatestRuns(Object.fromEntries(data.items.map((item) => [`${item.namespace}/${item.pipeline}`, item])));
        if (data.errors?.length) setLatestError(`Some latest runs could not be loaded (${data.errors.map((item) => item.namespace).join(', ')})`);
      })
      .catch((cause) => { if (active) setLatestError(cause.message); })
      .finally(() => { if (active) setLatestLoading(false); });
    return () => { active = false; };
  }, [namespace, refreshKey]);
  const filtered = items.filter((item) => item.name.toLowerCase().includes(search.toLowerCase()));
  return <>
    <div className="page-heading"><div><div className="eyebrow">PIPELINE CATALOG</div><Title headingLevel="h1" size="2xl">Pipelines</Title><p>Start a pipeline with its current parameters.</p></div><span className="count">{filtered.length} pipelines</span></div>
    <div className="toolbar"><input className="search-input" placeholder="Search pipeline names" value={search} onChange={(event) => setSearch(event.target.value)} aria-label="Search pipelines" /><Button variant="secondary" onClick={() => setRefreshKey((value) => value + 1)}>Refresh</Button></div>
    {error && <Alert variant="danger" title={error} className="notice" />}
    {latestError && <Alert variant="warning" title={latestError} className="notice" />}
    {loading ? <div className="loading"><Spinner size="lg" /></div> : filtered.length === 0 ? <div className="empty">No pipelines match this search.</div> :
      <div className="table-wrap"><table className="data-table"><thead><tr><th>Pipeline</th><th>Namespace</th><th>Last run</th><th>Last run status</th><th>Last run time</th></tr></thead><tbody>
        {filtered.map((item) => {
          const latest = latestRuns[`${item.namespace}/${item.name}`];
          return <tr key={`${item.namespace}/${item.name}`} onClick={() => navigate({ kind: 'pipeline', ...item })} tabIndex={0} onKeyDown={(event) => event.key === 'Enter' && navigate({ kind: 'pipeline', ...item })}>
            <td className="primary-cell">{item.name}</td><td>{item.namespace}</td>
            <td>{latest ? <a href={`#/run/${latest.namespace}/${encodeURIComponent(latest.name)}?uid=${encodeURIComponent(latest.uid || '')}`} onClick={(event) => event.stopPropagation()} onKeyDown={(event) => event.stopPropagation()}>{latest.name}</a> : latestLoading ? 'Loading…' : '—'}</td>
            <td>{latest ? <Status value={latest.status} /> : '—'}</td><td>{latest ? formatDate(latest.created) : '—'}</td>
          </tr>;
        })}
      </tbody></table></div>}
  </>;
}

function Runs({ namespace, pipeline = '', embedded = false }) {
  const [items, setItems] = useState([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [search, setSearch] = useState('');
  const [status, setStatus] = useState('');
  const [since, setSince] = useState('');
  const [until, setUntil] = useState('');
  const [submitted, setSubmitted] = useState({ search: '', status: '', since: '', until: '' });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [warnings, setWarnings] = useState([]);
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const interval = window.setInterval(() => setNow(Date.now()), 30000);
    return () => window.clearInterval(interval);
  }, []);
  useEffect(() => {
    const params = new URLSearchParams({ page: String(page), page_size: '50' });
    if (namespace) params.set('namespace', namespace);
    if (pipeline) params.set('pipeline', pipeline);
    if (submitted.search) params.set('q', submitted.search);
    if (submitted.status) params.set('status', submitted.status);
    if (submitted.since) params.set('since', new Date(`${submitted.since}T00:00:00`).toISOString());
    if (submitted.until) params.set('until', new Date(`${submitted.until}T23:59:59`).toISOString());
    setLoading(true);
    request(`/api/runs?${params}`)
      .then((data) => { setItems(data.items); setTotal(data.total); setWarnings(data.errors || []); setError(''); })
      .catch((cause) => setError(cause.message)).finally(() => setLoading(false));
  }, [namespace, pipeline, page, submitted]);
  const searchRuns = (event) => {
    event.preventDefault(); setPage(1); setSubmitted({ search, status, since, until });
  };
  return <>
    {embedded ? <div className="embedded-runs-heading"><h2>PipelineRuns</h2><span className="count">{total} runs</span></div> : <div className="page-heading"><div><div className="eyebrow">EXECUTION HISTORY</div><Title headingLevel="h1" size="2xl">PipelineRuns</Title><p>Find live and archived runs, inspect logs, or rebuild with new values.</p></div><span className="count">{total} runs</span></div>}
    <form className="toolbar run-filters" onSubmit={searchRuns}>
      <input className="search-input" placeholder={pipeline ? 'Search run names' : 'Pipeline or run name'} value={search} onChange={(event) => setSearch(event.target.value)} aria-label="Search runs" />
      <select value={status} onChange={(event) => setStatus(event.target.value)} aria-label="Status"><option value="">All statuses</option>{['Succeeded', 'Failed', 'Running', 'Cancelled', 'PipelineRunTimeout'].map((item) => <option key={item}>{item}</option>)}</select>
      <input type="date" value={since} onChange={(event) => setSince(event.target.value)} aria-label="From date" />
      <input type="date" value={until} onChange={(event) => setUntil(event.target.value)} aria-label="To date" />
      <Button type="submit" variant="primary">Search</Button>
    </form>
    {error && <Alert variant="danger" title={error} className="notice" />}
    {warnings.length > 0 && <Alert variant="warning" title={`Some history could not be loaded (${warnings.map((item) => item.namespace).join(', ')})`} className="notice" />}
    {loading ? <div className="loading"><Spinner size="lg" /></div> : items.length === 0 ? <div className="empty">No runs match these filters.</div> : <div className="table-wrap"><table className="data-table"><thead><tr><th>PipelineRun</th><th>Pipeline</th><th>Namespace</th><th>Status</th><th>Run state</th><th>Started</th><th>Duration</th><th>Source</th></tr></thead><tbody>
      {items.map((item) => <tr key={`${item.namespace}/${item.uid}`} onClick={() => navigate({ kind: 'run', ...item })} tabIndex={0} onKeyDown={(event) => event.key === 'Enter' && navigate({ kind: 'run', ...item })}>
        <td className="primary-cell">{item.name}</td><td>{item.pipeline || '—'}</td><td>{item.namespace}</td><td><Status value={item.status} /></td><td><span className={`run-state-bar ${statusColor(item.status)}`} role="img" aria-label={`${item.status} run state`} /></td><td>{formatDate(item.started || item.created)}</td><td>{formatDuration(item.started, item.completed, now)}</td><td><span className={item.source === 'archive' ? 'source archived' : 'source'}>{item.source === 'archive' ? 'Results' : 'Live'}</span></td>
      </tr>)}</tbody></table></div>}
    <div className="pagination"><span>{total === 0 ? '0' : (page - 1) * 50 + 1}–{Math.min(page * 50, total)} of {total}</span><Button variant="secondary" isDisabled={page <= 1 || loading} onClick={() => setPage(page - 1)}>Previous</Button><Button variant="secondary" isDisabled={page * 50 >= total || loading} onClick={() => setPage(page + 1)}>Next</Button></div>
  </>;
}

function PipelineDetail({ view, csrfToken }) {
  const [pipeline, setPipeline] = useState(null);
  const [activeTab, setActiveTab] = useState('details');
  const [form, setForm] = useState(false);
  const [error, setError] = useState('');
  useEffect(() => {
    setActiveTab('details');
    request(`/api/pipelines/${view.namespace}/${view.name}`).then(setPipeline).catch((cause) => setError(cause.message));
  }, [view.namespace, view.name]);
  return <>
    <Button variant="link" className="back" onClick={() => navigate({ kind: 'pipelines' })}>← Pipelines</Button>
    {error && <Alert variant="danger" title={error} className="notice" />}
    {!pipeline ? <div className="loading"><Spinner size="lg" /></div> : <>
      <div className="page-heading detail-heading"><div><div className="eyebrow">{pipeline.namespace}</div><Title headingLevel="h1" size="2xl">{pipeline.name}</Title><p>{pipeline.description || 'Tekton Pipeline'}</p></div><Button variant="primary" onClick={() => setForm(true)}>Start pipeline</Button></div>
      <div className="detail-tabs" role="tablist" aria-label="Pipeline views">
        <button type="button" role="tab" aria-selected={activeTab === 'details'} className={activeTab === 'details' ? 'active' : ''} onClick={() => setActiveTab('details')}>Details</button>
        <button type="button" role="tab" aria-selected={activeTab === 'runs'} className={activeTab === 'runs' ? 'active' : ''} onClick={() => setActiveTab('runs')}>PipelineRuns</button>
      </div>
      {activeTab === 'details' ? <section className="panel"><h2>Parameters <span className="muted">{pipeline.parameters.length}</span></h2><div className="definition-list">{pipeline.parameters.map((param) => <div key={param.name}><strong>{param.name}</strong><span>{param.description || 'No description'}</span><small>{param.type || 'string'} · {Object.hasOwn(param, 'default') ? 'Has default' : 'Required'}</small></div>)}</div></section> : <Runs namespace={view.namespace} pipeline={view.name} embedded />}
      {form && <RunForm namespace={view.namespace} pipeline={view.name} csrfToken={csrfToken} onClose={() => setForm(false)} />}
    </>}
  </>;
}

function RunDetail({ view, csrfToken }) {
  const [run, setRun] = useState(null);
  const [children, setChildren] = useState(null);
  const [childrenError, setChildrenError] = useState('');
  const [logs, setLogs] = useState(null);
  const [logsError, setLogsError] = useState('');
  const [streamKey, setStreamKey] = useState(0);
  const [events, setEvents] = useState(null);
  const [eventsError, setEventsError] = useState('');
  const [activeTab, setActiveTab] = useState(() => runTabs.includes(view.tab) ? view.tab : 'details');
  const [form, setForm] = useState(false);
  const [error, setError] = useState('');
  const runKey = `${view.namespace}/${view.name}/${view.uid || ''}`;
  const currentRun = useRef(null);
  const pending = useRef({});
  const url = `/api/runs/${view.namespace}/${view.name}?uid=${encodeURIComponent(view.uid || '')}`;
  const childrenUrl = `/api/runs/${view.namespace}/${view.name}/children`;
  const logsUrl = `/api/runs/${view.namespace}/${view.name}/logs?uid=${encodeURIComponent(view.uid || '')}`;
  const streamUrl = `/api/runs/${view.namespace}/${view.name}/logs/stream?uid=${encodeURIComponent(view.uid || '')}`;
  const eventsUrl = `/api/runs/${view.namespace}/${view.name}/events?uid=${encodeURIComponent(view.uid || '')}`;
  const selectedRun = run?.namespace === view.namespace && run?.name === view.name && (!view.uid || run.uid === view.uid);
  const liveRun = selectedRun && run.source === 'live' && !run.completed;
  useLayoutEffect(() => {
    currentRun.current = runKey;
    return () => {
      currentRun.current = null;
      Object.values(pending.current).forEach((controller) => controller.abort());
      pending.current = {};
    };
  }, [runKey]);
  const load = useCallback((kind, path, onSuccess, onFailure) => {
    pending.current[kind]?.abort();
    const controller = new AbortController();
    pending.current[kind] = controller;
    request(path, { signal: controller.signal })
      .then((data) => {
        if (currentRun.current === runKey && !controller.signal.aborted) onSuccess(data);
      })
      .catch((cause) => {
        if (currentRun.current === runKey && !controller.signal.aborted) onFailure(cause);
      });
  }, [runKey]);
  const refresh = useCallback(() => {
    setError('');
    load('run', url, setRun, (cause) => setError(cause.message));
  }, [load, url]);
  const refreshChildren = useCallback(() => {
    setChildrenError('');
    load('children', childrenUrl, setChildren, (cause) => setChildrenError(cause.message));
  }, [load, childrenUrl]);
  const refreshEvents = useCallback(() => {
    setEventsError('');
    load('events', eventsUrl, setEvents, (cause) => setEventsError(cause.message));
  }, [load, eventsUrl]);
  useEffect(() => { setActiveTab(runTabs.includes(view.tab) ? view.tab : 'details'); setRun(null); setChildren(null); setChildrenError(''); setLogs(null); setLogsError(''); setEvents(null); setEventsError(''); setForm(false); refresh(); refreshChildren(); }, [refresh, refreshChildren, view.tab]);
  useEffect(() => {
    if (!selectedRun || liveRun) return undefined;
    setLogsError('');
    load('logs', logsUrl, setLogs, (cause) => setLogsError(cause.message));
    return undefined;
  }, [selectedRun, liveRun, logsUrl, load]);
  useEffect(() => {
    if (!liveRun) return undefined;
    let sections = new Map();
    let truncated = false;
    let flushTimer = null;
    const source = new EventSource(streamUrl);
    const update = () => {
      if (flushTimer) return;
      flushTimer = window.setTimeout(() => {
        flushTimer = null;
        setLogs({ source: 'live', text: [...sections.values()].map((section) => `${section.title}\n${section.text}`).join('\n\n'), truncated });
      }, 100);
    };
    source.addEventListener('reset', () => { window.clearTimeout(flushTimer); flushTimer = null; sections = new Map(); truncated = false; setLogsError(''); setLogs({ source: 'live', text: '', truncated: false }); });
    source.addEventListener('chunk', (event) => {
      const { key, title, text } = JSON.parse(event.data);
      const section = sections.get(key) || { title, text: '' };
      sections.set(key, { title, text: section.text + text });
      update();
    });
    source.addEventListener('truncated', () => { truncated = true; update(); });
    source.addEventListener('failure', (event) => setLogsError(JSON.parse(event.data).message));
    source.addEventListener('done', () => {
      source.close();
      window.clearTimeout(flushTimer);
      load('logs', logsUrl, (data) => { setLogs(data); setLogsError(''); }, (cause) => setLogsError(cause.message));
    });
    source.onerror = () => setLogsError('Log stream interrupted. Reconnecting…');
    return () => { source.close(); window.clearTimeout(flushTimer); };
  }, [liveRun, streamKey, streamUrl, logsUrl, load]);
  useEffect(() => { if (activeTab === 'events') refreshEvents(); }, [activeTab, refreshEvents]);
  useEffect(() => {
    if (!liveRun) return undefined;
    const interval = window.setInterval(() => { refresh(); if (activeTab === 'events') refreshEvents(); }, 10000);
    return () => window.clearInterval(interval);
  }, [liveRun, activeTab, refresh, refreshEvents]);
  useEffect(() => {
    if (!liveRun) return undefined;
    const interval = window.setInterval(refreshChildren, 30000);
    return () => window.clearInterval(interval);
  }, [liveRun, refreshChildren]);
  const refreshVisible = () => { refresh(); refreshChildren(); if (liveRun) setStreamKey((value) => value + 1); else load('logs', logsUrl, setLogs, (cause) => setLogsError(cause.message)); if (activeTab === 'events') refreshEvents(); };
  const selectTab = (tab) => {
    setActiveTab(tab);
    const params = new URLSearchParams();
    if (view.uid) params.set('uid', view.uid);
    params.set('tab', tab);
    window.history.replaceState(null, '', `#/run/${encodeURIComponent(view.namespace)}/${encodeURIComponent(view.name)}?${params}`);
  };
  return <>
    <Button variant="link" className="back" onClick={() => navigate({ kind: 'runs' })}>← PipelineRuns</Button>
    {error && <Alert variant="danger" title={error} className="notice" />}
    {!run ? <div className="loading"><Spinner size="lg" /></div> : <>
      <div className="page-heading detail-heading"><div><div className="eyebrow">{run.namespace} / {run.pipeline || 'PipelineRun'}</div><Title headingLevel="h1" size="2xl">{run.name}</Title><div className="detail-meta"><Status value={run.status} /><span>Created {formatDate(run.created)}</span><span>{run.source === 'archive' ? 'Tekton Results' : 'Live cluster'}</span>{run.parentPipelineRun && <span>Parent PipelineRun: <a href={`#/run/${encodeURIComponent(run.namespace)}/${encodeURIComponent(run.parentPipelineRun)}`}>{run.parentPipelineRun}</a></span>}</div>{children?.items.length > 0 && <div className="related-runs"><strong>Triggered PipelineRuns</strong><div>{children.items.map((child) => <a key={child.uid} href={`#/run/${encodeURIComponent(run.namespace)}/${encodeURIComponent(child.name)}?uid=${encodeURIComponent(child.uid)}`}>{child.name}</a>)}</div></div>}</div><div className="detail-actions"><Button variant="secondary" onClick={refreshVisible}>Refresh</Button>{run.pipeline && <Button variant="primary" onClick={() => setForm(true)}>Rebuild with parameters</Button>}</div></div>
      {childrenError && <Alert variant="warning" title={`Child PipelineRuns could not be loaded: ${childrenError}`} className="notice" />}
      {children?.errors?.length > 0 && <Alert variant="warning" title={`Some child PipelineRuns could not be loaded (${children.errors.map((item) => item.source).join(', ')})`} className="notice" />}
      <div className="detail-tabs" role="tablist" aria-label="PipelineRun views">
        <button type="button" role="tab" aria-selected={activeTab === 'details'} className={activeTab === 'details' ? 'active' : ''} onClick={() => selectTab('details')}>Details</button>
        <button type="button" role="tab" aria-selected={activeTab === 'logs'} className={activeTab === 'logs' ? 'active' : ''} onClick={() => selectTab('logs')}>Logs</button>
        <button type="button" role="tab" aria-selected={activeTab === 'events'} className={activeTab === 'events' ? 'active' : ''} onClick={() => selectTab('events')}>Events</button>
        <button type="button" role="tab" aria-selected={activeTab === 'parameters'} className={activeTab === 'parameters' ? 'active' : ''} onClick={() => selectTab('parameters')}>Parameters ({run.parameters.length})</button>
        <button type="button" role="tab" aria-selected={activeTab === 'tasks'} className={activeTab === 'tasks' ? 'active' : ''} onClick={() => selectTab('tasks')}>Task runs ({run.tasks.length})</button>
      </div>
      {activeTab === 'details' && <RunDetails run={run} />}
      {activeTab === 'logs' && <section className="panel logs-panel" role="tabpanel"><div className="section-heading"><h2>Logs</h2><span className="source">{logs?.source === 'archive' ? 'Tekton Results' : 'Cluster pods'}</span></div>{logsError && <Alert variant="warning" title={logsError} className="notice" />}{logs?.truncated && <Alert variant="warning" title="Log display is limited to the first 8 MB per step" className="notice" />}<LogText text={logs?.text || 'Loading logs…'} streaming={liveRun} /></section>}
      {activeTab === 'events' && <section className="panel events-panel" role="tabpanel"><div className="section-heading"><h2>Events</h2><span className="source">Cluster events</span></div><p className="muted">Updates every 10 seconds while the run is active.</p>{eventsError && <Alert variant="danger" title={eventsError} className="notice" />}{!events && !eventsError ? <div className="loading"><Spinner size="lg" /></div> : events?.items.length ? <div className="table-wrap"><table className="data-table events-table"><thead><tr><th>Last seen</th><th>Type</th><th>Resource</th><th>Reason</th><th>Message</th><th>Count</th></tr></thead><tbody>{events.items.map((event) => <tr key={event.uid}><td>{formatDate(event.lastSeen)}</td><td><Label color={event.type === 'Warning' ? 'red' : 'grey'}>{event.type}</Label></td><td><strong>{event.kind}</strong><small>{event.object}</small></td><td>{event.reason || '—'}</td><td className="event-message">{event.message || '—'}</td><td>{event.count}</td></tr>)}</tbody></table></div> : !eventsError && <p className="muted">{run.source === 'archive' ? 'No cluster events remain for this archived run.' : 'No cluster events have been recorded for this run yet.'}</p>}</section>}
      {activeTab === 'parameters' && <section className="panel" role="tabpanel"><h2>Parameters <span className="muted">{run.parameters.length}</span></h2>{run.parameters.length ? <div className="value-list">{run.parameters.map((param) => <div key={param.name}><span>{param.name}</span><code>{typeof param.value === 'string' ? param.value : JSON.stringify(param.value)}</code></div>)}</div> : <p className="muted">This run did not specify parameters.</p>}</section>}
      {activeTab === 'tasks' && <section className="panel" role="tabpanel">{run.message && <div className="run-message">{run.message}</div>}<h2>Task runs</h2>{run.tasks.length ? <div className="task-list">{run.tasks.map((task) => <div key={task.name}><strong>{task.pipelineTaskName || task.name}</strong><small>{task.name}</small></div>)}</div> : <p className="muted">No task runs recorded.</p>}</section>}
      {form && <RunForm namespace={run.namespace} pipeline={run.pipeline} sourceRun={{ name: run.name, uid: run.uid }} csrfToken={csrfToken} onClose={() => setForm(false)} />}
    </>}
  </>;
}

function RunForm({ namespace, pipeline, sourceRun, csrfToken, onClose }) {
  const [form, setForm] = useState(null);
  const [values, setValues] = useState({});
  const [workspaces, setWorkspaces] = useState('[]');
  const [error, setError] = useState('');
  const [saving, setSaving] = useState(false);
  const params = new URLSearchParams();
  if (sourceRun) { params.set('source_run', sourceRun.name); params.set('source_uid', sourceRun.uid); }
  useEffect(() => {
    request(`/api/pipelines/${namespace}/${pipeline}/form?${params}`)
      .then((data) => {
        setForm(data);
        setValues(Object.fromEntries(data.parameters.map((item) => {
          const value = item.enum?.length && !item.enum.includes(item.value) ? '' : item.value ?? '';
          return [item.name, item.type === 'string' ? value : JSON.stringify(item.value ?? (item.type === 'array' ? [] : {}), null, 2)];
        })));
        setWorkspaces(JSON.stringify(data.workspaces, null, 2));
      }).catch((cause) => setError(cause.message));
  }, [namespace, pipeline, sourceRun?.uid]);
  const submit = async (event) => {
    event.preventDefault(); setError(''); setSaving(true);
    try {
      const typedValues = {};
      for (const item of form.parameters) typedValues[item.name] = item.type === 'string' ? values[item.name] : JSON.parse(values[item.name]);
      const bindings = JSON.parse(workspaces);
      if (!Array.isArray(bindings)) throw new Error('Workspace bindings must be a JSON array.');
      const created = await request('/api/runs', {
        method: 'POST', headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrfToken },
        body: JSON.stringify({ namespace, pipeline, resourceVersion: form.resourceVersion, values: typedValues, workspaces: bindings, sourceRun: sourceRun || null }),
      });
      onClose(); navigate({ kind: 'run', ...created });
    } catch (cause) { setError(cause.message); }
    finally { setSaving(false); }
  };
  return <div className="modal-backdrop" role="presentation" onMouseDown={(event) => event.target === event.currentTarget && onClose()}><div className="modal" role="dialog" aria-modal="true" aria-label={sourceRun ? 'Rebuild PipelineRun' : 'Start PipelineRun'}>
    <div className="modal-header"><div><div className="eyebrow">{namespace} / {pipeline}</div><h2>{sourceRun ? 'Rebuild with parameters' : 'Start pipeline'}</h2><p>{sourceRun ? `Values from ${sourceRun.name} are prefilled. Current pipeline defaults fill any missing values.` : 'Review parameters before creating a new PipelineRun.'}</p></div><button className="close" type="button" onClick={onClose} aria-label="Close">×</button></div>
    {!form ? <div className="loading"><Spinner size="lg" /></div> : <form onSubmit={submit}>
      <div className="modal-body">
        {form.removedParameters.length > 0 && <Alert variant="warning" title={`These old parameters are no longer in the pipeline and will be omitted: ${form.removedParameters.join(', ')}`} className="notice" />}
        {form.parameters.map((item) => <Field key={item.name} label={<>{item.name} <span className="origin">{item.source === 'run' ? 'Previous run' : item.source === 'default' ? 'Pipeline default' : 'Required'}</span></>} hint={item.description || (item.type !== 'string' ? `Enter a JSON ${item.type}` : '')}>
          {item.enum?.length ? <select value={values[item.name] ?? ''} onChange={(event) => setValues({ ...values, [item.name]: event.target.value })} required={!item.enum.includes('')}>{!item.enum.includes('') && <option value="" disabled>Select a value</option>}{item.enum.map((option) => <option key={option} value={option}>{option}</option>)}</select> :
            <textarea rows={item.type === 'string' ? 2 : 5} value={values[item.name] ?? ''} onChange={(event) => setValues({ ...values, [item.name]: event.target.value })} required={item.source === 'required'} />}
        </Field>)}
        {form.workspaceDefinitions.length > 0 && <Field label="Workspace bindings" hint="JSON array of Tekton workspace bindings; required workspace names must be present."><textarea rows={7} value={workspaces} onChange={(event) => setWorkspaces(event.target.value)} /></Field>}
        {error && <Alert variant="danger" title={error} className="notice" />}
      </div>
      <div className="modal-footer"><Button variant="secondary" onClick={onClose}>Cancel</Button><Button type="submit" variant="primary" isDisabled={saving || !csrfToken}>{saving ? 'Starting…' : 'Start PipelineRun'}</Button></div>
    </form>}
  </div></div>;
}

export default App;
