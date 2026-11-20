import React, { useEffect, useState } from 'react';
import { Alert, Button, Label, Spinner, Title } from '@patternfly/react-core';
import { navigateRow, openshiftResourceHref, viewHref } from './navigation';

function useCronJobData(path, request) {
  const [result, setResult] = useState(null);
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    request(path, { signal: controller.signal })
      .then((data) => {
        if (!controller.signal.aborted) setResult({ path, revision, data });
      })
      .catch((error) => {
        if (!controller.signal.aborted) setResult({ path, revision, error: error.message });
      });
    return () => controller.abort();
  }, [path, request, revision]);
  const current = result?.path === path && result.revision === revision ? result : null;
  return { data: current?.data, error: current?.error, loading: !current, refresh: () => setRevision((value) => value + 1) };
}

function CronJobState({ suspended }) {
  return <Label color={suspended ? 'grey' : 'green'}>{suspended ? 'Suspended' : 'Enabled'}</Label>;
}

export function CronJobs({ namespace, request, formatDate }) {
  const [search, setSearch] = useState('');
  const path = `/api/cronjobs${namespace ? `?namespace=${encodeURIComponent(namespace)}` : ''}`;
  const { data, error, loading, refresh } = useCronJobData(path, request);
  const items = (data?.items || []).filter((item) => item.name.toLowerCase().includes(search.trim().toLowerCase()));
  return <>
    <div className="page-heading">
      <Title headingLevel="h1" size="2xl">CronJobs</Title>
      {data && <span className="count">{items.length} CronJobs</span>}
    </div>
    <div className="toolbar">
      <input
        className="search-input" placeholder="Search CronJob names" aria-label="Search CronJobs"
        value={search} onChange={(event) => setSearch(event.target.value)}
      />
      <Button variant="secondary" onClick={refresh}>Refresh</Button>
    </div>
    {error && <Alert variant="danger" title={error} className="notice" />}
    {loading && <div className="loading"><Spinner size="lg" aria-label="Loading CronJobs" /></div>}
    {data && (items.length ? (
      <div className="table-wrap">
        <table className="data-table">
          <thead><tr><th>CronJob</th><th>Namespace</th><th>Schedule</th><th>Time zone</th><th>State</th><th>Last scheduled</th><th>Active Jobs</th></tr></thead>
          <tbody>{items.map((item) => {
            const href = viewHref({ kind: 'cronjob', namespace: item.namespace, name: item.name, filterNamespace: namespace });
            const onNavigate = (event) => navigateRow(event, href);
            return <tr key={`${item.namespace}/${item.name}`} onClick={onNavigate} onAuxClick={onNavigate} onKeyDown={onNavigate} tabIndex={0}>
              <td className="primary-cell"><a href={href}>{item.name}</a></td>
              <td>{item.namespace}</td>
              <td><code>{item.schedule}</code></td>
              <td>{item.timeZone || 'Controller default'}</td>
              <td><CronJobState suspended={item.suspended} /></td>
              <td>{formatDate(item.lastScheduled)}</td>
              <td>{item.activeJobs}</td>
            </tr>;
          })}</tbody>
        </table>
      </div>
    ) : <div className="empty">{search.trim() ? 'No CronJobs match this search.' : 'No visible CronJobs.'}</div>)}
  </>;
}

export function CronJobDetail({ view, request, formatDate, Status }) {
  const path = `/api/cronjobs/${encodeURIComponent(view.namespace)}/${encodeURIComponent(view.name)}`;
  const { data: cronjob, error, loading, refresh } = useCronJobData(path, request);
  return <>
    <Button component="a" variant="link" className="back" href={viewHref({ kind: 'cronjobs', filterNamespace: view.filterNamespace })}>← CronJobs</Button>
    {error && <Alert variant="danger" title={error} className="notice" />}
    {loading && <div className="loading"><Spinner size="lg" aria-label="Loading CronJob" /></div>}
    {cronjob && <>
      <div className="page-heading detail-heading">
        <div><div className="eyebrow">{cronjob.namespace}</div><Title headingLevel="h1" size="2xl">{cronjob.name}</Title></div>
        <div className="detail-actions">
          <Button variant="secondary" onClick={refresh}>Refresh</Button>
          <Button component="a" variant="secondary" href={openshiftResourceHref(cronjob.namespace, 'batch~v1~CronJob', cronjob.name)}>View in OpenShift</Button>
        </div>
      </div>
      <section className="panel">
        <h2>Schedule</h2>
        <div className="definition-list">
          <div><strong>Cron expression</strong><code>{cronjob.schedule}</code></div>
          <div><strong>Time zone</strong><span>{cronjob.timeZone || 'Controller default'}</span></div>
          <div><strong>State</strong><CronJobState suspended={cronjob.suspended} /></div>
          <div><strong>Active Jobs</strong><span>{cronjob.activeJobs}</span></div>
          <div><strong>Last scheduled</strong><span>{formatDate(cronjob.lastScheduled)}</span></div>
          <div><strong>Last successful</strong><span>{formatDate(cronjob.lastSuccessful)}</span></div>
        </div>
      </section>
      <section className="panel">
        <h2>Recent Jobs</h2>
        {cronjob.jobs === null ? <div className="empty">Job history is unavailable with your namespace permissions.</div> :
          cronjob.jobs.length ? (
            <div className="table-wrap">
              <table className="data-table">
                <thead><tr><th>Job</th><th>Status</th><th>Created</th><th>Started</th><th>Completed</th></tr></thead>
                <tbody>{cronjob.jobs.map((run) => (
                  <tr key={run.name}>
                    <td className="primary-cell"><a href={openshiftResourceHref(cronjob.namespace, 'batch~v1~Job', run.name)}>{run.name}</a></td>
                    <td><Status value={run.status} /></td>
                    <td>{formatDate(run.created)}</td>
                    <td>{formatDate(run.started)}</td>
                    <td>{formatDate(run.completed)}</td>
                  </tr>
                ))}</tbody>
              </table>
            </div>
          ) : <div className="empty">No jobs recorded for this CronJob.</div>}
      </section>
    </>}
  </>;
}
