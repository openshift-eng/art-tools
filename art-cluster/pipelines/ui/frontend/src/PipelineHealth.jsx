import React, { useEffect, useState } from 'react';
import { Alert, Button, Label, Spinner, Title } from '@patternfly/react-core';
import { viewHref } from './navigation';

const stageNames = ['Scan', 'Images', 'Bundle', 'FBC'];
const colors = { Succeeded: 'green', Failed: 'red', Running: 'blue', Cancelled: 'orange' };
const symbols = { Succeeded: '✓', Failed: '✕', Running: '◷', Cancelled: '⊘' };
const FilterNamespace = React.createContext('');
const groupKey = (item) => `${item.namespace}/${item.group}/${item.assembly}`;
const date = (value) => value ? new Date(value).toLocaleString() : '—';

function HealthStatus({ status, children }) {
  return <Label color={colors[status] || 'grey'}><span className="health-symbol" aria-hidden="true">{symbols[status] || '—'}</span>{children || status}</Label>;
}

function RunLink({ run, children, className = '' }) {
  const filterNamespace = React.useContext(FilterNamespace);
  return <a className={className} href={viewHref({ kind: 'run', ...run, filterNamespace })}>{children || run.name}</a>;
}

function stageStatus(stage) {
  if (!stage.runs.length) return 'Not started';
  for (const status of ['Failed', 'Cancelled', 'Running', 'Unknown']) {
    if (stage.runs.some((run) => run.status === status)) return status;
  }
  return 'Succeeded';
}

function ChainRail({ chain, onExpand }) {
  if (!chain) return <span className="health-subline">No downstream build chain observed</span>;
  return <div className="health-rail">{chain.stages.map((stage, index) => {
    const status = stageStatus(stage);
    const passed = stage.runs.filter((run) => run.status === 'Succeeded').length;
    const text = <><span className="health-symbol" aria-hidden="true">{symbols[status] || '—'}</span>{stageNames[index]}{index === 3 && stage.runs.length > 0 && ` ${passed}/${stage.runs.length}`}</>;
    const className = `health-rail-node health-${status.toLowerCase().replaceAll(' ', '-')}`;
    return <React.Fragment key={stage.pipeline}>
      {index > 0 && <span className="health-arrow" aria-hidden="true">→</span>}
      {stage.runs.length === 1 ? <RunLink run={stage.runs[0]} className={className}>{text}</RunLink> : stage.runs.length > 1 ? <button type="button" className={className} onClick={onExpand} aria-label={`Show ${stageNames[index]} runs`}>{text}</button> : <span className={className}>{text}</span>}
    </React.Fragment>;
  })}</div>;
}

function ChainDetails({ chain }) {
  return <div className="health-chain-detail">
    <div className="health-chain-meta"><span>Scan-rooted chain · {date(chain.root.started || chain.root.created)}{chain.recovered && ' · Linked retries'}</span><RunLink run={chain.root}>Open root scan</RunLink></div>
    {chain.issues.length > 0 && <Alert variant="warning" title={chain.issues.join('. ')} isInline className="notice" />}
    <div className="health-step-grid">{chain.stages.map((stage, index) => <section className="health-step" key={stage.pipeline} aria-label={stageNames[index]}>
      <div className="health-step-number">STAGE {index + 1}</div><h3>{stageNames[index]}</h3>
      {stage.runs.length === 0 ? <span className="health-subline">{chain.failures.length ? 'Blocked by upstream failure' : 'Not started'}</span> : index === 3 ? <div className="health-targets">{stage.runs.map((run) => <div className="health-target" key={run.uid}><RunLink run={run}>OCP {run.target || 'default'}</RunLink><HealthStatus status={run.status} /></div>)}</div> : stage.runs.map((run) => <div className="health-step-run" key={run.uid}><HealthStatus status={run.status} /><RunLink run={run} /><span className="health-subline">{date(run.started || run.created)} · {run.source === 'archive' ? 'Results' : 'Live'}{run.rebuiltFrom && ' · Retry'}</span></div>)}
    </section>)}</div>
    {chain.recovered && <details className="health-history"><summary>Recovery history · {chain.attempts.filter((run) => run.pipeline !== 'build-fbc').length} scan, image, and bundle attempts</summary><div>{chain.attempts.filter((run) => run.pipeline !== 'build-fbc').map((run) => <div className="health-attempt" key={run.uid}><HealthStatus status={run.status} /><RunLink run={run} /><span>{run.rebuiltFrom ? 'Retry' : 'Original attempt'} · {date(run.created)}</span></div>)}</div></details>}
  </div>;
}

function Attention({ item, onExpand }) {
  const chain = item.currentChain;
  const failures = chain?.failures.length ? chain.failures : ['Failed', 'Cancelled'].includes(item.health) ? item.lastCompletedChain?.failures || [] : [];
  if (failures.length) return <div className="health-attention">{failures.map((run) => <RunLink key={run.uid} run={run} className="health-failure-link">{!chain?.failures.length && 'Last failure: '}{run.pipeline}{run.target && ` · OCP ${run.target}`} {run.status.toLowerCase()}</RunLink>)}{chain?.active.length > 0 && <RunLink run={chain.active[0]}>View current running stage</RunLink>}</div>;
  if (chain?.active.length) return <><RunLink run={chain.active[0]}>{chain.recovered ? 'View recovering chain' : 'View running stage'}</RunLink><span className="health-subline">{chain.active.length} active {chain.active.length === 1 ? 'run' : 'runs'}</span></>;
  if (chain?.status === 'Succeeded') return <RunLink run={chain.stages[3].runs[0]}>Open successful FBC</RunLink>;
  if (chain) return <button className="health-text-button" type="button" onClick={onExpand}>Inspect incomplete chain</button>;
  return <RunLink run={item.latestScan}>Open latest scan</RunLink>;
}

export default function PipelineHealth({ namespace, request }) {
  const [items, setItems] = useState([]);
  const [warnings, setWarnings] = useState([]);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [updated, setUpdated] = useState(null);
  const [search, setSearch] = useState('');
  const [expanded, setExpanded] = useState(new Set());
  const [refreshKey, setRefreshKey] = useState(0);
  useEffect(() => { setExpanded(new Set()); setItems([]); setUpdated(null); }, [namespace]);
  useEffect(() => {
    const controller = new AbortController();
    let active = true;
    let timer;
    setLoading(true);
    const load = async () => {
      let delay = 30000;
      try {
        const data = await request(`/api/pipeline-health${namespace ? `?namespace=${encodeURIComponent(namespace)}` : ''}`, { signal: controller.signal });
        if (!active) return;
        setItems(data.items); setWarnings(data.errors || []); setError(''); setUpdated(Date.now());
        if (data.items.some((item) => item.active)) delay = 10000;
      } catch (cause) {
        if (active && !controller.signal.aborted) setError(cause.message);
      } finally {
        if (active) {
          setLoading(false);
          timer = window.setTimeout(() => document.hidden ? schedule() : load(), delay);
        }
      }
    };
    const schedule = () => { timer = window.setTimeout(() => document.hidden ? schedule() : load(), 30000); };
    load();
    return () => { active = false; controller.abort(); window.clearTimeout(timer); };
  }, [namespace, request, refreshKey]);
  const filtered = items.filter((item) => `${item.group} ${item.assembly} ${item.namespace}`.toLowerCase().includes(search.toLowerCase()));
  const toggle = (key) => setExpanded((previous) => { const next = new Set(previous); next.has(key) ? next.delete(key) : next.add(key); return next; });
  const expand = (key) => setExpanded((previous) => new Set([...previous, key]));
  return <FilterNamespace.Provider value={namespace}>
    <div className="page-heading"><div><div className="eyebrow">END-TO-END BUILD STATUS</div><Title headingLevel="h1" size="2xl">Pipeline health <span className="health-beta">Beta</span></Title><p>From source scan to every FBC target. See what passed and where work stopped.</p></div><span className="count">{updated ? `Updated ${new Date(updated).toLocaleTimeString()}` : 'Loading health…'}</span></div>
    <div className="toolbar"><input className="search-input" placeholder="Find a group…" aria-label="Search group health" value={search} onChange={(event) => setSearch(event.target.value)} /><div className="health-counts"><span>{items.length} groups</span><span>{items.filter((item) => item.health === 'Failed').length} failed</span><span>{items.filter((item) => item.currentChain?.active.length).length} running</span><span>{items.filter((item) => item.health === 'Succeeded').length} E2E green</span></div><Button variant="secondary" isDisabled={loading} onClick={() => setRefreshKey((value) => value + 1)}>Refresh</Button></div>
    {error && <Alert variant="danger" title={error} className="notice" />}
    {warnings.length > 0 && <Alert variant="warning" title={`Some history could not be loaded (${[...new Set(warnings.map((item) => item.namespace))].join(', ')}). Health may be incomplete.`} className="notice" />}
    {loading && !items.length ? <div className="loading"><Spinner size="lg" /></div> : filtered.length === 0 ? <div className="empty">No scan-rooted build groups match this view.</div> : <div className="table-wrap"><table className="data-table health-table"><thead><tr><th>Group</th><th>Last completed E2E</th><th>Latest build chain</th><th>Attention</th></tr></thead><tbody>{filtered.map((item) => {
      const key = groupKey(item);
      const detailId = `health-${encodeURIComponent(key)}`;
      const chain = item.currentChain;
      return <React.Fragment key={key}><tr><td><button className="health-group-toggle" type="button" aria-expanded={expanded.has(key)} aria-controls={detailId} onClick={() => toggle(key)}><span className={expanded.has(key) ? 'health-chevron expanded' : 'health-chevron'} aria-hidden="true">▾</span>{item.group}</button><span className="health-subline">{item.assembly}{!namespace && ` · ${item.namespace}`}</span></td>
        <td><HealthStatus status={item.health}>{item.health === 'Succeeded' ? 'E2E green' : item.health === 'Unknown' ? 'No confirmed E2E' : `E2E ${item.health.toLowerCase()}`}</HealthStatus><span className="health-subline">{item.incompleteHistory ? 'History unavailable in part' : item.lastCompletedChain ? date(item.lastCompletedChain.completed || item.lastCompletedChain.root.created) : 'No completed build chain'}</span>{item.lastSuccessfulChain && item.health !== 'Succeeded' && <span className="health-subline">Previous green: {date(item.lastSuccessfulChain.completed)}</span>}</td>
        <td><ChainRail chain={chain} onExpand={() => expand(key)} /><span className="health-subline">Latest scan {date(item.latestScan.created)} · {item.latestScan.status}{chain?.recovered && ' · Linked retries'}</span></td><td><Attention item={item} onExpand={() => expand(key)} /></td></tr>
        <tr className="health-detail-row" id={detailId} hidden={!expanded.has(key)}><td colSpan="4">{chain ? <ChainDetails chain={chain} /> : <div className="health-scan-only">Latest scan: {item.latestScan.status}. No linked downstream builds observed. <RunLink run={item.latestScan}>Open scan</RunLink></div>}{item.history.filter((entry) => entry.root.uid !== chain?.root.uid).length > 0 && <details className="health-history"><summary>Earlier build chains</summary>{item.history.filter((entry) => entry.root.uid !== chain?.root.uid).map((entry) => <div key={entry.root.uid} className="health-earlier-chain"><HealthStatus status={entry.status} /><ChainDetails chain={entry} /></div>)}</details>}</td></tr>
      </React.Fragment>;
    })}</tbody></table></div>}
    <p className="health-definition">Green requires a successful scan, image build, bundle build, and every triggered FBC target. Current activity appears separately from the last completed result. Scans without builds do not replace E2E health. Expand a group to see target runs, retries, and earlier chains.</p>
  </FilterNamespace.Provider>;
}
