import React, { useEffect, useRef, useState } from 'react';
import { Alert, Button, Label, Spinner } from '@patternfly/react-core';
import { viewHref } from './navigation';
import { filterReleases, groupReleaseItems, releaseAttention, releaseColor, releaseStages } from './releaseStatusUtils';
import './releaseWorkspace.css';
import './releaseStatus.css';

const formatDate = (value) => value ? new Date(value).toLocaleString() : 'Unknown';
const workspaceTabs = ['Overview', 'Shipments', 'Validations', 'Approvals', 'Activity'];

export function ReleaseBadge({ status }) {
  return <Label color={releaseColor(status)}>{status || 'Unknown'}</Label>;
}

function Preparation({ release, namespace }) {
  return <section className="live-release-section"><h3>Preparation · release-from-fbc</h3><p>{release.preparation.message}</p>
    {release.preparation.attempts.length > 0 ? <div className="table-wrap"><table className="data-table live-release-jobs"><thead><tr><th>Attempt</th><th>Status</th><th>Started</th><th>Source</th></tr></thead><tbody>{release.preparation.attempts.map((run) => <tr key={run.uid}><td><a href={viewHref({ kind: 'run', namespace: run.namespace, name: run.name, uid: run.uid, filterNamespace: namespace })}>{run.name}</a>{run.dryRun && <small>Dry run</small>}{run.testMode && <small>Test / debug</small>}</td><td><ReleaseBadge status={run.status} /></td><td>{formatDate(run.started || run.created)}</td><td>{run.source === 'archive' ? 'Tekton Results' : 'Live cluster'}</td></tr>)}</tbody></table></div> : <p className="release-inline-note">Preparation status is unknown. An MR alone does not prove that release-from-fbc succeeded.</p>}
  </section>;
}

function ShipmentFiles({ files }) {
  return <section className="live-release-section"><h3>Shipment files & results</h3><div className="table-wrap"><table className="data-table release-files live-release-jobs"><thead><tr><th>Shipment</th><th>Stage evidence</th><th>Production evidence</th></tr></thead><tbody>{files.map((file) => <tr key={file.path}><td><strong>{file.application} · {file.kind}</strong><code>{file.path}</code></td>{['stage', 'prod'].map((env) => {
    const evidence = file.environments[env];
    return <td key={env}><span>{evidence.releasePlan || 'Plan not recorded'}</span>{evidence.advisoryUrl && <a href={evidence.advisoryUrl} target="_blank" rel="noopener noreferrer">Advisory ↗</a>}{evidence.pipelineUrl && <a href={evidence.pipelineUrl} target="_blank" rel="noopener noreferrer">Recorded result ↗</a>}{!evidence.advisoryUrl && !evidence.pipelineUrl && <small>No result recorded</small>}</td>;
  })}</tr>)}</tbody></table></div></section>;
}

function ApprovalGroups({ approvals }) {
  return <section className="live-release-section"><div className="release-section-title"><h3>GitLab approval rules</h3><ReleaseBadge status={approvals.status} /></div>
    {approvals.rules.length > 0 ? <div className="live-approval-grid">{approvals.rules.map((rule, index) => <article key={`${rule.name}-${index}`}><div><strong>{rule.name}</strong><ReleaseBadge status={!rule.required ? 'Not required' : rule.approved ? 'Succeeded' : 'Waiting'} /></div><p>{rule.required} required · {rule.approvedBy.length} recorded</p><small>{rule.approvedBy.length ? rule.approvedBy.join(', ') : 'No recorded reviewers'}</small></article>)}</div> : <p>{approvals.status === 'Unknown' ? 'Approval rules could not be loaded.' : 'No approval groups are required by GitLab.'}</p>}
  </section>;
}

function ObservedJobs({ detail, validations = false }) {
  const [environment, setEnvironment] = useState('all');
  const jobs = detail.jobs.filter((job) => validations
    ? /validate|check-approvals|verify-docs|generate-config/.test(job.name)
    : environment === 'all' || (job.environment || 'parent') === environment);
  return <section className="live-release-section"><div className="release-section-title"><h3>{validations ? 'Validation evidence' : 'Pipeline jobs & attempts'}</h3>{!validations && <select aria-label="Job environment" value={environment} onChange={(event) => setEnvironment(event.target.value)}><option value="all">All environments</option><option value="parent">MR pipeline</option><option value="stage">Stage</option><option value="prod">Production</option></select>}</div><p>{validations ? 'Observed validation and approval-check jobs.' : 'Jobs from the latest MR pipeline and its stage/production children. Earlier job attempts remain visible.'} {detail.current ? 'Input revision verified.' : 'Current input revision is not verified.'}</p>
    {jobs.length > 0 ? <div className="table-wrap"><table className="data-table live-release-jobs"><thead><tr><th>Job</th><th>Environment</th><th>Status</th><th>Started</th></tr></thead><tbody>{jobs.map((job) => <tr key={job.id}><td>{job.url ? <a href={job.url} target="_blank" rel="noopener noreferrer">{job.name} ↗</a> : job.name}<small>Job #{job.id}{job.allowFailure ? ' · Optional' : ''}</small></td><td>{job.environment || 'MR pipeline'}</td><td><ReleaseBadge status={job.status} /></td><td>{formatDate(job.started)}</td></tr>)}</tbody></table></div> : <p>No job evidence available.</p>}
    {!validations && <details className="live-pipeline-history"><summary>MR pipeline history · {detail.pipelines.length} attempts</summary><div>{detail.pipelines.map((pipeline) => <p key={pipeline.id}><a href={pipeline.url} target="_blank" rel="noopener noreferrer">Pipeline #{pipeline.id} ↗</a><ReleaseBadge status={pipeline.status} /><code>{pipeline.sha?.slice(0, 12)}</code><span>{formatDate(pipeline.created)}</span></p>)}</div></details>}
  </section>;
}

export function ObservedStageRail({ stages, onSelect }) {
  return <ol className="release-rail live-observed-rail" aria-label="Observed release stages">{stages.map((stage, index) => <li key={stage.name} className={`release-stage ${stage.state}`} aria-current={stage.state === 'current' ? 'step' : undefined}>
    <button type="button" onClick={() => onSelect(stage.section)}>
      <span className="release-stage-dot" aria-hidden="true">{stage.state === 'complete' ? '✓' : stage.state === 'failed' ? '!' : index + 1}</span>
      <strong>{stage.name}</strong><small>{stage.state === 'complete' ? 'Complete' : stage.status}</small>
    </button>
  </li>)}</ol>;
}

function ReleaseOverview({ release, stages, namespace }) {
  const descriptions = {
    Validate: 'Shipment schema, paths, and release-plan checks.',
    'Stage release': 'Image and catalog release jobs, with results recorded on the MR.',
    Approvals: 'GitLab approval rules for this shipment revision.',
    Production: 'Production release jobs and per-shipment result writeback.',
    Merge: 'Observed shipment MR state in GitLab.',
  };
  return <>
    <details className="release-inputs"><summary>Release inputs · {release.group} / {release.assembly}</summary><dl className="release-review">
      <div><dt>Tenant</dt><dd>{release.namespace}</dd></div><div><dt>Assembly</dt><dd>{release.assembly || 'Not recorded'}</dd></div>
      <div><dt>Shipment files</dt><dd>{release.shipments.length || 'Not linked'}</dd></div><div><dt>Input revision</dt><dd><code>{release.mr?.sha || 'No linked shipment MR'}</code></dd></div>
    </dl></details>
    <div className="release-section-title"><h3>Release progress</h3><span>Observed stages</span></div>
    <div className="release-plan">{stages.filter((stage) => descriptions[stage.name]).map((stage) => <div key={stage.name}>
      <span className={`release-plan-mark ${stage.state === 'complete' ? 'done' : ''}`} aria-hidden="true">{stage.state === 'complete' ? '✓' : stage.state === 'failed' ? '!' : '·'}</span>
      <div><strong>{stage.name}</strong><p>{descriptions[stage.name]}</p></div><ReleaseBadge status={stage.status} />
    </div>)}</div>
    <details className="live-preparation-history" open={!release.mr}><summary>Preparation attempts · {release.preparation.attempts.length}</summary><Preparation release={release} namespace={namespace} /></details>
  </>;
}

function ReleaseSidebar({ release, evidence, stages, loading, onSelect }) {
  const outstanding = evidence?.approvals.rules.filter((rule) => rule.required > 0 && !rule.approved) || [];
  const failed = stages.find((stage) => stage.state === 'failed');
  const current = stages.find((stage) => stage.state === 'current');
  let title = releaseAttention(release);
  let description = release.mr ? 'Inspect the current shipment MR and its recorded evidence.' : release.preparation.message;
  let section = 'Activity';
  if (release.mr?.state === 'merged') {
    title = 'Shipment merged'; description = 'The shipment MR has been merged. Missing evidence remains unknown.';
  } else if (release.mr?.state === 'closed') {
    title = 'Shipment closed'; description = 'This MR is closed. Its preparation and release history remain available.';
  } else if (failed) {
    title = `${failed.name} failed`; description = 'Inspect the failed attempt and its logs in the source system.';
  } else if (outstanding.length > 0) {
    title = 'Awaiting approvals'; description = `${outstanding.length} approval ${outstanding.length === 1 ? 'group is' : 'groups are'} still required for this revision.`; section = 'Approvals';
  } else if (current) {
    title = current.status === 'Running' ? `${current.name} running` : current.status;
    description = `The release is currently waiting at ${current.name.toLowerCase()}.`; section = current.section;
  } else if (loading && !evidence && release.mr) {
    title = 'Loading release evidence'; description = 'Checking the latest pipeline and approval state.';
  }
  return <aside className="release-sidebar live-release-sidebar"><div className="release-gate"><span className="release-kicker">CURRENT STATUS</span><h3>{title}</h3><p>{description}</p><Button variant="primary" onClick={() => onSelect(section)}>{section === 'Approvals' ? 'Review approvals' : 'View activity'}</Button><small>View only · release actions stay in GitLab.</small></div>
    <div className="release-sidebar-section"><h4>Release record</h4><dl>
      <div><dt>Source of truth</dt><dd>{release.mr ? <a href={release.mr.url} target="_blank" rel="noopener noreferrer">Shipment !{release.mr.iid} ↗</a> : 'No linked shipment MR'}</dd></div>
      <div><dt>Repository</dt><dd>ocp-shipment-data</dd></div><div><dt>Execution</dt><dd>artc2023 + GitLab CI</dd></div>
      <div><dt>Input revision</dt><dd><code>{release.mr?.sha?.slice(0, 12) || 'Not recorded'}</code></dd></div>
      <div><dt>Preparation</dt><dd>release-from-fbc</dd></div>{release.ci && <div><dt>Latest pipeline</dt><dd><a href={release.ci.url} target="_blank" rel="noopener noreferrer">#{release.ci.id} ↗</a><small className="live-sidebar-note">{release.ci.current ? release.ci.status : 'Historical / unverified'}</small></dd></div>}
    </dl></div>
  </aside>;
}

function ReleaseDetails({ release, detail, error, loading, namespace }) {
  const [tab, setTab] = useState('Overview');
  const evidence = detail && detail.mr.sha === release.mr?.sha ? detail : null;
  const stages = releaseStages(release, evidence, Boolean(error));
  const requiredRules = evidence?.approvals.rules.filter((rule) => rule.required > 0) || [];
  const files = (evidence?.shipments || release.shipments).filter((file) => file.group === release.group && file.assembly === release.assembly);
  return <section className="release-detail live-release-detail" aria-label={`Release ${release.group} ${release.assembly}`}>
    <div className="release-detail-header"><div><div className="release-kicker">RELEASE WORKSPACE / {release.mr ? `SHIPMENT !${release.mr.iid}` : 'PREPARATION'}</div><h2>{release.group} <span>/</span> {release.assembly}</h2><p>{release.namespace} · {release.mr ? <a href={release.mr.url} target="_blank" rel="noopener noreferrer">Shipment !{release.mr.iid} ↗</a> : 'Preparation without a linked MR'}{release.mr && <> · <code>{release.mr.sha?.slice(0, 12)}</code></>}</p></div><ReleaseBadge status={releaseAttention(release)} /></div>
    <ObservedStageRail stages={stages} onSelect={setTab} />
    <div className="release-detail-layout"><div className="release-main">
      <div className="release-tabs" role="group" aria-label="Live release sections">{workspaceTabs.map((name) => <button key={name} type="button" className={tab === name ? 'active' : ''} aria-pressed={tab === name} onClick={() => setTab(name)}>{name}{name === 'Approvals' && <span>{evidence ? `${requiredRules.filter((rule) => rule.approved).length}/${requiredRules.length}` : '—'}</span>}</button>)}</div>
      <div className="release-tab-content live-workspace-content">
        {error && <Alert variant="warning" isInline title={`Release details could not be refreshed: ${error}`} className="notice" />}
        {detail && !evidence && <Alert variant="warning" isInline title="The MR revision changed. Refresh the release list to load its current evidence." className="notice" />}
        {evidence?.issues.map((message) => <Alert key={message} variant="warning" isInline title={message} className="notice" />)}
        {tab === 'Overview' && <ReleaseOverview release={release} stages={stages} namespace={namespace} />}
        {tab === 'Shipments' && (files.length ? <ShipmentFiles files={files} /> : <p className="release-copy">No shipment files are linked to this preparation attempt.</p>)}
        {tab === 'Validations' && (evidence ? <ObservedJobs key="validation" detail={evidence} validations /> : <p className="release-copy">Validation evidence is not available yet.</p>)}
        {tab === 'Approvals' && (evidence ? <ApprovalGroups approvals={evidence.approvals} /> : <p className="release-copy">Approval evidence is not available yet.</p>)}
        {tab === 'Activity' && <><Preparation release={release} namespace={namespace} />{evidence && <ObservedJobs key="activity" detail={evidence} />}</>}
      </div>
    </div><ReleaseSidebar release={release} evidence={error ? null : evidence} stages={stages} loading={loading} onSelect={setTab} /></div>
  </section>;
}

export function ReleaseSelector({ items, selected, onSelect, search, onSearch, status, onStatus, days, onDays, loading }) {
  const recordsRef = useRef(null);
  useEffect(() => {
    const container = recordsRef.current;
    const choice = container?.querySelector('[aria-pressed="true"]');
    if (!choice) return;
    const viewport = container.getBoundingClientRect();
    const row = choice.getBoundingClientRect();
    if (row.top < viewport.top) container.scrollTop += row.top - viewport.top;
    else if (row.bottom > viewport.bottom) container.scrollTop += row.bottom - viewport.bottom;
  }, [selected]);
  return <aside className="live-release-selector" aria-label="Release selector">
    <div className="live-selector-heading"><strong>Releases</strong><span>{items.length} records</span></div>
    <div className="live-selector-controls">
      <input aria-label="Search live releases" placeholder="Find a release…" value={search} onChange={(event) => onSearch(event.target.value)} />
      <select aria-label="Live release status" value={status} onChange={(event) => onStatus(event.target.value)}><option value="all">All statuses</option><option value="active">Active / unlinked</option><option value="attention">Needs attention</option><option value="merged">Merged</option><option value="closed">Closed</option></select>
      <select aria-label="Release history window" value={days} onChange={(event) => onDays(Number(event.target.value))}><option value="7">Last 7 days + open MRs</option><option value="30">Last 30 days + open MRs</option><option value="90">Last 90 days + open MRs</option></select>
    </div>
    <div className="live-selector-records" ref={recordsRef}>{loading && !items.length ? <p className="live-selector-empty">Loading release records…</p> : items.length ? groupReleaseItems(items).map((group) => {
      const rows = group.items.map((item, index) => {
        const attention = releaseAttention(item);
        return <button key={item.id} type="button" className="live-release-choice" aria-pressed={selected === item.id} onClick={() => onSelect(item.id)}>
          <strong>{item.mr ? `Shipment !${item.mr.iid}` : 'Preparation attempt'}</strong>
          <small>{index === 0 ? 'Latest record' : 'Previous record'}{!item.mr && ` · ${item.preparation.attempts[0]?.name || 'Unknown run'}`}</small>
          <span className={`live-choice-status ${releaseColor(attention)}`}><i aria-hidden="true" />{attention}</span>
          {!item.mr && <small>MR not linked</small>}
          <time>{formatDate(item.updated)}</time>
        </button>;
      });
      return <section key={group.id} className="live-selector-version"><h2 className="live-selector-group">{group.name}</h2>{rows[0]}{rows.length > 1 && <details className="live-selector-history" open={Boolean(search) || status !== 'all' || group.items.slice(1).some((item) => item.id === selected)}><summary>Previous attempts<span>{rows.length - 1}</span></summary>{rows.slice(1)}</details>}</section>;
    }) : <p className="live-selector-empty">No matching releases.</p>}</div>
    <div className="live-selector-footer">Newest first within each version. Each record keeps its own status.</div>
  </aside>;
}

export function ReleaseRefreshControl({ loading, detailLoading, incomplete, updated, observedAt, hasData, onRefresh }) {
  const busy = loading || detailLoading;
  const label = loading ? 'Refreshing releases…' : detailLoading ? 'Refreshing GitLab…' : incomplete ? 'Refresh incomplete · Retry' : updated ? 'Updated' : 'Refresh';
  const note = busy ? hasData ? 'Showing last observed data' : 'Loading release data' : incomplete ? 'Some sources could not be refreshed' : observedAt ? `Last observed ${formatDate(observedAt)}` : 'No source snapshot';
  return <div className="live-refresh-control">
    <Button variant="secondary" className={`live-release-refresh${!busy && updated && !incomplete ? ' complete' : ''}${!busy && incomplete ? ' incomplete' : ''}`} isDisabled={busy} aria-busy={Boolean(busy)} onClick={onRefresh} icon={busy ? <Spinner size="sm" aria-hidden="true" /> : <span aria-hidden="true">{updated && !incomplete ? '✓' : '↻'}</span>}>{label}</Button>
    <small role="status">{note}</small>
  </div>;
}

function ReleaseStatusView({ namespace, request }) {
  const [data, setData] = useState(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [days, setDays] = useState(7);
  const [refresh, setRefresh] = useState(0);
  const [search, setSearch] = useState('');
  const [status, setStatus] = useState('all');
  const [selected, setSelected] = useState(null);
  const [detailState, setDetailState] = useState(null);
  const [updated, setUpdated] = useState(false);
  useEffect(() => {
    let active = true;
    let timer;
    let pending = false;
    const controller = new AbortController();
    const load = async () => {
      if (!active || pending || document.hidden) return;
      pending = true; setLoading(true);
      try {
        const result = await request(`/api/releases?days=${days}${namespace ? `&namespace=${encodeURIComponent(namespace)}` : ''}`, { signal: controller.signal });
        if (active) { setData(result); setError(''); }
      } catch (cause) { if (active) setError(cause.message); }
      finally {
        pending = false;
        if (active) { setLoading(false); timer = window.setTimeout(load, 30000); }
      }
    };
    const visible = () => { if (!document.hidden) { window.clearTimeout(timer); load(); } };
    load(); document.addEventListener('visibilitychange', visible);
    return () => { active = false; controller.abort(); window.clearTimeout(timer); document.removeEventListener('visibilitychange', visible); };
  }, [namespace, days, refresh, request]);
  const items = data?.items || [];
  const visible = filterReleases(items, search, status);
  const latestRecords = groupReleaseItems(items).map((group) => group.items[0]);
  const release = visible.find((item) => item.id === selected) || groupReleaseItems(visible)[0]?.items[0];
  const detailKey = release?.mr ? JSON.stringify([release.id, release.mr.sha, data.observedAt]) : null;
  useEffect(() => {
    if (!detailKey) return undefined;
    let active = true;
    const controller = new AbortController();
    setDetailState((previous) => ({ key: detailKey, releaseId: release.id, detail: previous?.releaseId === release.id ? previous.detail : null, error: '', loading: true }));
    request(`/api/releases/mrs/${release.mr.iid}?sha=${encodeURIComponent(release.mr.sha)}&namespace=${encodeURIComponent(release.namespace)}`, { signal: controller.signal })
      .then((detail) => { if (active) setDetailState((previous) => ({ ...previous, detail, loading: false })); })
      .catch((cause) => { if (active) setDetailState((previous) => ({ ...previous, error: cause.message, loading: false })); });
    return () => { active = false; controller.abort(); };
  }, [detailKey, request]);
  const detail = detailState?.releaseId === release?.id ? detailState?.detail : null;
  const detailError = detailState?.key === detailKey ? detailState?.error || '' : '';
  const detailLoading = Boolean(detailKey && (detailState?.key !== detailKey || detailState.loading));
  const currentDetail = detail && detail.mr.sha === release?.mr?.sha ? detail : null;
  const incomplete = Boolean(error || data?.errors.length || detailError || currentDetail?.issues.length);
  const busy = loading || detailLoading;
  useEffect(() => {
    if (!data || busy || incomplete) { setUpdated(false); return undefined; }
    setUpdated(true);
    const timer = window.setTimeout(() => setUpdated(false), 2500);
    return () => window.clearTimeout(timer);
  }, [busy, incomplete, data?.observedAt, detailKey]);
  return <div className="releases-page live-releases-page">
    <div className="page-heading"><div><div className="eyebrow">RELEASE OBSERVABILITY</div><h1>ReleaseStatus <span className="beta-badge">Read only</span></h1><p>Follow release preparation and the shipment MR through existing GitLab CI.</p></div><div className="release-heading-actions"><ReleaseRefreshControl loading={loading} detailLoading={detailLoading} incomplete={incomplete} updated={updated} observedAt={data?.observedAt} hasData={Boolean(data)} onRefresh={() => { setLoading(true); setRefresh((value) => value + 1); }} /></div></div>
    <div className="live-feed-summary"><span>{latestRecords.length} versions · {items.length} records</span><span>{latestRecords.filter((item) => releaseAttention(item).includes('failed')).length} failed latest records</span></div>
    {error && <Alert variant="danger" isInline title={`${error}${data ? ' — showing the previous snapshot.' : ''}`} className="notice" />}
    {data?.errors.map((message) => <Alert key={message} variant="warning" isInline title={message} className="notice" />)}
    <div className="live-release-layout">
      <ReleaseSelector items={visible} selected={release?.id} onSelect={setSelected} search={search} onSearch={setSearch} status={status} onStatus={setStatus} days={days} onDays={(value) => { setLoading(true); setDays(value); setData(null); setSelected(null); }} loading={loading} />
      <div className="live-release-workspace">{release ? <ReleaseDetails key={release.id} release={release} detail={detail} error={detailError} loading={detailLoading} namespace={namespace} /> : <div className="empty">{data && !data.supportedNamespaces.length ? 'No release-from-fbc pipeline is available in the selected tenants.' : !data && loading ? 'Select a release to inspect its progress.' : error ? 'Release information could not be loaded.' : 'No releases match the selected period and filters.'}</div>}</div>
    </div>
    <p className="release-footnote">Refreshes every 30 seconds while visible. GitLab responses may be cached for up to 20 seconds. Missing preparation links and unavailable evidence remain unknown; a merged MR is shown as merged, without inferring that every release step succeeded.</p>
  </div>;
}

export default function ReleaseStatus(props) {
  return <ReleaseStatusView key={props.namespace || 'all'} {...props} />;
}
