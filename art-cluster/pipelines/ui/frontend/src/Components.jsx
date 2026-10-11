import React, { useState } from 'react';
import { Alert } from '@patternfly/react-core';
import { componentCounts, filterComponents, primaryResources } from './componentResults';

function ResourceLink({ resource }) {
  return resource.url ? <a href={resource.url} target="_blank" rel="noopener noreferrer">{resource.name}</a> : <span>{resource.name}</span>;
}

export default function Components({ run, Status }) {
  const [search, setSearch] = useState('');
  const [failuresOnly, setFailuresOnly] = useState(false);
  const [expanded, setExpanded] = useState(new Set());
  const components = run.components || [];
  const reporting = run.componentReporting || {};
  const visible = filterComponents(components, search, failuresOnly);
  function toggle(id) {
    setExpanded((previous) => {
      const next = new Set(previous);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  }
  return <section className="panel" role="tabpanel" aria-label="Components">
    <h2>Components <span className="muted">{components.length}</span></h2>
    {reporting.partial && <Alert variant="warning" isInline title="Component reporting is incomplete">{reporting.reason || 'Some component updates were not recorded. Unknown stages cannot establish a successful result.'}</Alert>}
    {!reporting.available ? <p className="muted">Component details were not recorded for this run.</p> : !components.length ?
      <p className="muted">{reporting.reason || 'Waiting for component selection.'}</p> : <>
        <div className="component-counts">{Object.entries(componentCounts(components)).map(([status, count]) => <span key={status}><Status value={status} /> {count}</span>)}</div>
        <div className="toolbar"><input aria-label="Search components" placeholder="Search components or NVRs" value={search} onChange={(event) => setSearch(event.target.value)} />
          <label><input type="checkbox" checked={failuresOnly} onChange={(event) => setFailuresOnly(event.target.checked)} /> Has failures</label>
          <span className="muted">Includes optional checks, releases, and earlier attempts.</span>
        </div>
        <div className="table-wrap"><table className="data-table components-table"><thead><tr><th>Component</th><th>Overall</th><th>Build</th><th>Checks</th><th>Release</th><th>Links</th></tr></thead><tbody>
          {visible.map((component) => <React.Fragment key={component.id}>
            <tr><td><button className="component-toggle" onClick={() => toggle(component.id)} aria-expanded={expanded.has(component.id)} aria-controls={`component-${component.id}`}>
              {expanded.has(component.id) ? '−' : '+'} {component.displayName || component.name}</button><small>{component.type} · {component.name}</small></td>
              <td><Status value={component.status} /></td><td><Status value={component.summaries.build} /></td><td><Status value={component.summaries.check} /></td><td><Status value={component.summaries.release} /></td>
              <td><div className="component-links">{primaryResources(component.resources).map((resource) => <a key={`${resource.kind}/${resource.namespace}/${resource.name}`} href={resource.url} target="_blank" rel="noopener noreferrer">{resource.role}</a>)}</div></td></tr>
            {expanded.has(component.id) && <tr className="component-expanded"><td colSpan={6} id={`component-${component.id}`}>
              {component.nvr && <p><strong>NVR:</strong> {component.nvr}</p>}{component.operatorNvr && <p><strong>Operator NVR:</strong> {component.operatorNvr}</p>}
              {component.fbcExcluded && <Alert variant="warning" isInline title="Excluded from downstream FBC builds because stage release failed" />}
              <h3>Stages</h3><div className="component-stages">{component.stages.map((stage) => <div key={stage.name}><strong>{stage.name}</strong> <Status value={stage.status} /> <small>{stage.required ? 'Required' : 'Optional'}</small>{stage.reason && <p>{stage.reason}</p>}</div>)}</div>
              {!!component.resources.length && <><h3>Konflux resources and attempts</h3><ul>{component.resources.map((resource) => <li key={`${resource.kind}/${resource.namespace}/${resource.name}`}><strong>{resource.role}</strong>{resource.attempt && ` · attempt ${resource.attempt}`} · <ResourceLink resource={resource} /> · <Status value={resource.status} /><small>{resource.kind} · {resource.namespace}</small>{resource.reason && <p>{resource.reason}</p>}</li>)}</ul></>}
              {!!component.members?.length && <><h3>Snapshot members</h3><p className="muted">The release outcome applies to this snapshot as a whole.</p><ul>{component.members.map((member) => <li key={member.name}><strong>{member.name}</strong><code>{member.containerImage}</code></li>)}</ul></>}
            </td></tr>}
          </React.Fragment>)}
        </tbody></table></div>{!visible.length && <p className="muted">No components match these filters.</p>}
      </>}
  </section>;
}
