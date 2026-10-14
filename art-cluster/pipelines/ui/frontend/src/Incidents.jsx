import React, { useState } from 'react';
import { Alert, Button, Label, Title } from '@patternfly/react-core';

const statusLabels = {
  investigating: 'Investigating',
  identified: 'Identified',
  monitoring: 'Monitoring',
  resolved: 'Resolved',
};

const severityColors = { danger: 'red', warning: 'orange', info: 'blue' };
const emptyForm = { title: '', message: '', severity: 'warning', status: 'investigating', description: '', links: [] };

function formatDate(value) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString(undefined, {
    year: 'numeric', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', timeZoneName: 'short',
  });
}

function IncidentCard({ incident, canManage, onEdit, onClose, closing }) {
  const started = formatDate(incident.started);
  const updated = formatDate(incident.updated);
  const resolved = formatDate(incident.resolvedAt);
  const active = incident.status !== 'resolved';
  return <Alert variant={incident.severity} title={incident.title} isInline className="incident-item">
    <div className="incident-meta">
      <Label color={severityColors[incident.severity] || 'grey'}>{statusLabels[incident.status] || incident.status}</Label>
      {started && <span>Started <time dateTime={incident.started}>{started}</time></span>}
      {updated && <span>Last updated <time dateTime={incident.updated}>{updated}</time></span>}
      {resolved && <span>Closed <time dateTime={incident.resolvedAt}>{resolved}</time></span>}
    </div>
    <p className="incident-message">{incident.message}</p>
    {incident.description && <p>{incident.description}</p>}
    {incident.links?.length > 0 && <div className="incident-links">{incident.links.map((link) =>
      <a key={`${link.label}-${link.url}`} href={link.url} target="_blank" rel="noopener noreferrer">{link.label}</a>
    )}</div>}
    {canManage && active && <div className="incident-actions">
      <Button variant="secondary" onClick={() => onEdit(incident)}>Edit</Button>
      <Button variant="secondary" isDisabled={closing} onClick={() => onClose(incident)}>{closing ? 'Closing…' : 'Close incident'}</Button>
    </div>}
  </Alert>;
}

function IncidentEditor({ incident, csrfToken, request, onChange, onClose }) {
  const [form, setForm] = useState(() => incident ? {
    title: incident.title,
    message: incident.message,
    severity: incident.severity,
    status: incident.status === 'resolved' ? 'investigating' : incident.status,
    description: incident.description || '',
    links: incident.links || [],
  } : { ...emptyForm, links: [] });
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  const update = (key, value) => setForm((current) => ({ ...current, [key]: value }));
  const updateLink = (index, key, value) => setForm((current) => ({
    ...current,
    links: current.links.map((link, linkIndex) => linkIndex === index ? { ...link, [key]: value } : link),
  }));

  const submit = async (event) => {
    event.preventDefault();
    setError('');
    const links = form.links.map((link) => ({ label: link.label.trim(), url: link.url.trim() })).filter((link) => link.label || link.url);
    if (links.some((link) => !link.label || !link.url)) {
      setError('Add both a label and URL for each incident link.');
      return;
    }
    setSaving(true);
    try {
      const url = incident ? `/api/incidents/${encodeURIComponent(incident.id)}` : '/api/incidents';
      const result = await request(url, {
        method: incident ? 'PUT' : 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrfToken },
        body: JSON.stringify({ ...form, links }),
      });
      onChange(result);
      onClose();
    } catch (cause) {
      setError(cause.message);
    } finally {
      setSaving(false);
    }
  };

  return <div className="modal-backdrop" role="presentation" onMouseDown={(event) => event.target === event.currentTarget && onClose()}>
    <div className="modal" role="dialog" aria-modal="true" aria-label={incident ? 'Edit incident' : 'Create incident'}>
      <div className="modal-header">
        <div><div className="eyebrow">SERVICE STATUS</div><h2>{incident ? 'Update incident' : 'Report an incident'}</h2><p>Changes appear in the banner and incident list.</p></div>
        <button className="close" type="button" onClick={onClose} aria-label="Close">×</button>
      </div>
      <form onSubmit={submit}>
        <div className="modal-body">
          <label className="field"><span className="field-label">Title</span><input maxLength={160} required value={form.title} onChange={(event) => update('title', event.target.value)} /></label>
          <label className="field"><span className="field-label">Banner message</span><input maxLength={240} required value={form.message} onChange={(event) => update('message', event.target.value)} /></label>
          <div className="incident-form-row">
            <label className="field"><span className="field-label">Severity</span><select value={form.severity} onChange={(event) => update('severity', event.target.value)}><option value="info">Information</option><option value="warning">Warning</option><option value="danger">Critical</option></select></label>
            <label className="field"><span className="field-label">Status</span><select value={form.status} onChange={(event) => update('status', event.target.value)}><option value="investigating">Investigating</option><option value="identified">Identified</option><option value="monitoring">Monitoring</option></select></label>
          </div>
          <label className="field"><span className="field-label">Details</span><textarea rows={4} maxLength={4000} value={form.description} onChange={(event) => update('description', event.target.value)} /></label>
          <div className="field">
            <span className="field-label">Slack or ticket links</span>
            {form.links.map((link, index) => <div className="incident-link-form" key={index}>
              <input aria-label={`Link ${index + 1} label`} placeholder="Slack thread or ticket" maxLength={80} value={link.label} onChange={(event) => updateLink(index, 'label', event.target.value)} />
              <input aria-label={`Link ${index + 1} URL`} type="url" placeholder="https://…" value={link.url} onChange={(event) => updateLink(index, 'url', event.target.value)} />
              <Button type="button" variant="link" onClick={() => update('links', form.links.filter((_item, linkIndex) => linkIndex !== index))}>Remove</Button>
            </div>)}
            {form.links.length < 10 && <Button type="button" variant="link" onClick={() => update('links', [...form.links, { label: '', url: '' }])}>Add a link</Button>}
          </div>
          {error && <Alert variant="danger" title={error} className="notice" />}
        </div>
        <div className="modal-footer"><Button type="button" variant="secondary" onClick={onClose}>Cancel</Button><Button type="submit" variant="primary" isDisabled={saving || !csrfToken}>{saving ? 'Saving…' : incident ? 'Save changes' : 'Create incident'}</Button></div>
      </form>
    </div>
  </div>;
}

export default function Incidents({ incidents = [], canManage = false, csrfToken, error, onChange, request }) {
  const active = incidents.filter((incident) => incident.status !== 'resolved');
  const resolved = incidents.filter((incident) => incident.status === 'resolved');
  const [editor, setEditor] = useState(null);
  const [closingId, setClosingId] = useState('');
  const [actionError, setActionError] = useState('');

  const closeIncident = async (incident) => {
    setActionError('');
    setClosingId(incident.id);
    try {
      const result = await request(`/api/incidents/${encodeURIComponent(incident.id)}/close`, {
        method: 'POST', headers: { 'X-CSRF-Token': csrfToken },
      });
      onChange(result);
    } catch (cause) {
      setActionError(cause.message);
    } finally {
      setClosingId('');
    }
  };

  return <>
    <div className="page-heading">
      <div><div className="eyebrow">SERVICE STATUS</div><Title headingLevel="h1" size="2xl">Incidents</Title><p>Current service issues and their latest updates.</p></div>
      <div className="incident-heading-actions"><span className="count">{active.length} active</span>{canManage && <Button variant="primary" onClick={() => setEditor({ ...emptyForm, links: [] })}>Create incident</Button>}</div>
    </div>
    {(error || actionError) && <Alert variant="danger" title={error ? 'Incident information could not be loaded' : 'Incident update failed'} className="notice">{error || actionError}</Alert>}
    {!error && active.length === 0 && <section className="panel incident-empty"><h2>No active incidents</h2><p>There are no reported issues right now.</p></section>}
    {active.map((incident) => <IncidentCard key={incident.id} incident={incident} canManage={canManage} onEdit={setEditor} onClose={closeIncident} closing={closingId === incident.id} />)}
    {resolved.length > 0 && <section className="resolved-incidents">
      <h2>Resolved</h2>
      {resolved.map((incident) => <IncidentCard key={incident.id} incident={incident} />)}
    </section>}
    {editor && <IncidentEditor incident={editor.id ? editor : null} csrfToken={csrfToken} request={request} onChange={onChange} onClose={() => setEditor(null)} />}
  </>;
}
