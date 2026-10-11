import React, { useState } from 'react';
import { Alert } from '@patternfly/react-core';
import { componentCounts, filterComponents } from './componentResults';

export default function Components({ run, Status }) {
  const [search, setSearch] = useState('');
  const [result, setResult] = useState('');
  const components = run.components || [];
  const reporting = run.componentReporting || {};
  const counts = componentCounts(components);
  const visible = filterComponents(components, search, result);
  return <section className="panel" role="tabpanel" aria-label="Components">
    <h2>Component builds <span className="muted">{components.length}</span></h2>
    {reporting.partial && <Alert variant="warning" isInline title="Some build results were not recorded">{reporting.reason || 'See the available build PipelineRuns for details.'}</Alert>}
    {!reporting.available ? <p className="muted">Component build results were not recorded for this run.</p> : !components.length ?
      <p className="muted">{reporting.reason || 'Waiting for component selection.'}</p> : <>
        <div className="component-counts">{Object.entries(counts).map(([status, count]) => <span key={status}><Status value={status} /> {count}</span>)}</div>
        <div className="toolbar"><input aria-label="Search components" placeholder="Search component names" value={search} onChange={(event) => setSearch(event.target.value)} />
          <select aria-label="Filter build results" value={result} onChange={(event) => setResult(event.target.value)}>
            <option value="">All build results</option>
            <option value="Has failed attempts">Has failed attempts</option>
            {Object.keys(counts).map((status) => <option key={status} value={status}>{status}</option>)}
          </select>
        </div>
        <div className="table-wrap"><table className="data-table components-table"><thead><tr><th>Component</th><th>Build result</th><th>Konflux build PipelineRuns</th></tr></thead><tbody>
          {visible.map((component) => <tr key={component.id}>
            <td><strong>{component.displayName || component.name}</strong><small>{component.type}{component.displayName !== component.name && ` · ${component.name}`}</small></td>
            <td><Status value={component.status} />{component.reason && component.status !== 'Succeeded' && <small className="component-reason">{component.reason}</small>}</td>
            <td>{component.pipelineRuns.length ? <ul className="component-build-links">{component.pipelineRuns.map((build) => <li key={`${build.namespace}/${build.name}`}>
              {build.url ? <a href={build.url} target="_blank" rel="noopener noreferrer">{build.name}</a> : <span>{build.name}</span>}
              {build.attempt != null && <small>Attempt {build.attempt}</small>} <Status value={build.status} />
            </li>)}</ul> : <span className="muted">No build PipelineRun triggered</span>}</td>
          </tr>)}
        </tbody></table></div>{!visible.length && <p className="muted">No components match these filters.</p>}
      </>}
  </section>;
}
