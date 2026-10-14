import React from 'react';
import { Alert, Label, Title } from '@patternfly/react-core';

const severityVariant = { warning: 'warning', danger: 'danger', info: 'info' };
const statusColors = { investigating: 'red', identified: 'orange', monitoring: 'blue', resolved: 'green' };

function formatDate(value) {
  return value ? new Date(value).toLocaleString() : '—';
}

function isHttpUrl(url) {
  return typeof url === 'string' && (url.startsWith('http://') || url.startsWith('https://'));
}

export default function Incidents({ incidents }) {
  const active = incidents.filter((item) => item.status !== 'resolved').sort((a, b) => (b.started || '').localeCompare(a.started || ''));
  const resolved = incidents.filter((item) => item.status === 'resolved').sort((a, b) => (b.started || '').localeCompare(a.started || ''));
  const sorted = [...active, ...resolved];
  return <>
    <div className="page-heading"><div><div className="eyebrow">SERVICE STATUS</div><Title headingLevel="h1" size="2xl">Incidents</Title><p>Current and past incidents affecting ART Pipelines.</p></div><span className="count">{active.length} active</span></div>
    {sorted.length === 0 ? <div className="empty">No incidents to display.</div> : <div className="incidents-list">{sorted.map((incident) =>
      <Alert key={incident.id} variant={severityVariant[incident.severity] || 'info'} title={incident.title} className="incident-card" isInline>
        <div className="incident-meta">
          <Label color={statusColors[incident.status] || 'grey'}>{incident.status}</Label>
          <span>Started {formatDate(incident.started)}</span>
          <span>Updated {formatDate(incident.updated)}</span>
        </div>
        {incident.description && <p className="incident-description">{incident.description}</p>}
        {incident.links?.length > 0 && <div className="incident-links">{incident.links.filter((link) => isHttpUrl(link.url)).map((link, index) =>
          <a key={index} href={link.url} target="_blank" rel="noopener noreferrer">{link.label || link.url}</a>
        )}</div>}
      </Alert>
    )}</div>}
  </>;
}
