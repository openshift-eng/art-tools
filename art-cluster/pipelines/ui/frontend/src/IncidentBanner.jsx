import React, { useEffect, useState } from 'react';

export const INCIDENT_BANNER_CYCLE_MS = 8000;

export function activeBannerIncidents(incidents = []) {
  return incidents.filter((incident) =>
    !['resolved', 'deleted'].includes(incident.status) && incident.showOnBanner !== false
  );
}

export function nextBannerIndex(index, count) {
  return count > 0 ? (index + 1) % count : 0;
}

export function startBannerCycle(advance, scheduler = window) {
  const timer = scheduler.setInterval(advance, INCIDENT_BANNER_CYCLE_MS);
  return () => scheduler.clearInterval(timer);
}

function previousBannerIndex(index, count) {
  return count > 0 ? (index - 1 + count) % count : 0;
}

export default function IncidentBanner({ incidents = [], error = '' }) {
  const candidates = activeBannerIncidents(incidents);
  const [selectedIndex, setSelectedIndex] = useState(0);
  const currentIndex = candidates.length ? selectedIndex % candidates.length : 0;
  const incident = candidates[currentIndex];
  const candidateKey = candidates.map((item) => item.id).join('|');

  useEffect(() => {
    setSelectedIndex(0);
  }, [candidateKey]);

  useEffect(() => {
    if (candidates.length < 2) return undefined;
    return startBannerCycle(() => {
      setSelectedIndex((index) => nextBannerIndex(index, candidates.length));
    });
  }, [candidateKey, candidates.length]);

  if (!incident) {
    if (!error) return null;
    return <div className="incident-announcement incident-announcement-unavailable" role="status">
      <span className="incident-announcement-icon" aria-hidden="true">ⓘ</span>
      <span>Incident information is temporarily unavailable.</span>
    </div>;
  }

  return <div className="incident-announcement" role="region" aria-label="Active incident announcements">
    <span className="incident-announcement-icon" aria-hidden="true">ⓘ</span>
    <span className="incident-announcement-message" aria-live="polite">{incident.message}</span>
    {incident.links?.map((link) => <a key={`${incident.id}-${link.label}-${link.url}`} href={link.url} target="_blank" rel="noopener noreferrer">{link.label}</a>)}
    {candidates.length > 1 && <div className="incident-announcement-controls">
      <button type="button" aria-label="Previous incident" title="Previous incident" onClick={() => setSelectedIndex((index) => previousBannerIndex(index, candidates.length))}>‹</button>
      <span aria-live="polite">{currentIndex + 1} of {candidates.length}</span>
      <button type="button" aria-label="Next incident" title="Next incident" onClick={() => setSelectedIndex((index) => nextBannerIndex(index, candidates.length))}>›</button>
    </div>}
  </div>;
}
