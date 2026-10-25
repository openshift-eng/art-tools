import React, { useState } from 'react';
import { Alert } from '@patternfly/react-core';
import { checkColumns, componentCounts, componentResult, filterComponents } from './componentResults';

function CheckResults({ checks, Status }) {
  return <ul className="component-build-links">{checks.map((check) => <li key={`${check.kind}/${check.name}`}>
    {check.url ? <a href={check.url} target="_blank" rel="noopener noreferrer">{check.name}</a> : <span>{check.name}</span>}
    {' '}<Status value={check.status === 'Skipped' ? 'Not run' : check.status} />
    {check.kind === 'custom' && <small>{check.blocking ? 'Blocking' : 'Optional'}</small>}
    {check.reason && <small className="component-reason">{check.reason}</small>}
  </li>)}</ul>;
}

export default function Components({ run, Status }) {
  const [search, setSearch] = useState('');
  const [result, setResult] = useState('');
  const components = run.components || [];
  const reporting = run.componentReporting || {};
  const counts = componentCounts(components);
  const visible = filterComponents(components, search, result);
  const columns = checkColumns(components);
  return <section className="panel" role="tabpanel" aria-label="Components">
    <h2>Component results <span className="muted">{components.length}</span></h2>
    {reporting.partial && <Alert variant="warning" isInline title="Some build or check results were not recorded">{reporting.reason || 'See the available PipelineRuns for details.'}</Alert>}
    {!reporting.available ? <p className="muted">Component build results were not recorded for this run.</p> : !components.length ?
      <p className="muted">{reporting.reason || 'Waiting for component selection.'}</p> : <>
        {components.some((component) => !component.checksRecorded) && <p className="muted">Check results were not recorded for older component reports.</p>}
        <div className="component-counts">{Object.entries(counts).map(([status, count]) => <span key={status}><Status value={status} /> {count}</span>)}</div>
        <div className="toolbar"><input aria-label="Search components" placeholder="Search component names" value={search} onChange={(event) => setSearch(event.target.value)} />
          <select aria-label="Filter component results" value={result} onChange={(event) => setResult(event.target.value)}>
            <option value="">All component results</option>
            <option value="Has failed attempts">Has failed attempts</option>
            {Object.keys(counts).map((status) => <option key={status} value={status}>{status}</option>)}
          </select>
        </div>
        <div className="table-wrap"><table className="data-table components-table"><thead><tr><th>Component</th><th>Result</th><th>Build result</th>{columns.map(({ kind, label }) => <th key={kind}>{label}</th>)}<th>Konflux build PipelineRuns</th></tr></thead><tbody>
          {visible.map((component) => <tr key={component.id}>
            <td><strong>{component.displayName || component.name}</strong><small>{component.type}{component.displayName !== component.name && ` · ${component.name}`}</small></td>
            <td><Status value={componentResult(component)} /></td>
            <td><Status value={component.status} />{component.reason && component.status !== 'Succeeded' && <small className="component-reason">{component.reason}</small>}</td>
            {columns.map(({ kind }) => {
              const checks = (component.checks || []).filter((check) => check.kind === kind);
              return <td key={kind}>{checks.length ? <CheckResults checks={checks} Status={Status} />
                : <span className="muted">{component.checksRecorded ? 'Not enabled' : 'Not recorded'}</span>}</td>;
            })}
            <td>{component.pipelineRuns.length ? <ul className="component-build-links">{component.pipelineRuns.map((build) => <li key={`${build.namespace}/${build.name}`}>
              {build.url ? <a href={build.url} target="_blank" rel="noopener noreferrer">{build.name}</a> : <span>{build.name}</span>}
              {build.attempt != null && <small>Attempt {build.attempt}</small>} <Status value={build.status} />
              {build.checks?.length > 0 && <details><summary>Checks</summary><CheckResults checks={build.checks} Status={Status} /></details>}
            </li>)}</ul> : <span className="muted">No build PipelineRun triggered</span>}</td>
          </tr>)}
        </tbody></table></div>{!visible.length && <p className="muted">No components match these filters.</p>}
      </>}
  </section>;
}
