"""Collect CVE mentions from exact Brew RPM changelogs and Errata advisories."""

import re

from errata_tool import Erratum
from errata_tool.build import Build

from elliottlib.rhcos_rpm_evidence.models import RpmAdvisoryEvidence, RpmBuildEvidence
from elliottlib.runtime import Runtime

_CVE_RE = re.compile(r"CVE-\d{4}-\d+", re.IGNORECASE)


class KojiErrataRpmBuildEvidenceSource:
    """
    Load changelog and advisory CVE references for exact RPM builds.
    """

    def __init__(self, runtime: Runtime) -> None:
        """
        Store the Elliott runtime used to create a cached Brew client.
        """
        self._runtime = runtime

    def collect(self, nvrs: set[str]) -> tuple[RpmBuildEvidence, ...]:
        """
        Collect build evidence independently so one failed lookup does not hide others.
        """
        try:
            koji = self._runtime.build_retrying_koji_client(caching=True)
            koji_warning = None
        except Exception:
            koji = None
            koji_warning = "BREW_CHANGELOG_CLIENT_UNAVAILABLE: Unable to initialize Brew changelog client"

        evidence = []
        for nvr in sorted(nvrs):
            warnings = []
            changelog_cves: set[str] = set()
            changelog_complete = False
            if koji is None:
                warnings.append(koji_warning or "BREW_CHANGELOG_CLIENT_UNAVAILABLE: Brew changelog client unavailable")
            else:
                try:
                    entries = koji.getChangelogEntries(nvr, strict=True)
                    if entries is None:
                        raise ValueError("No changelog returned")
                    for entry in entries:
                        if isinstance(entry, dict):
                            text = entry.get("text") or entry.get("changelog") or ""
                        else:
                            text = str(entry)
                        changelog_cves.update(cve.upper() for cve in _CVE_RE.findall(text))
                    changelog_complete = True
                except Exception:
                    warnings.append(f"BREW_CHANGELOG_LOOKUP_FAILED: Changelog lookup failed for build {nvr}")

            advisories: list[RpmAdvisoryEvidence] = []
            advisory_lookup_complete = False
            try:
                for advisory_id in Build(nvr).all_errata_ids or []:
                    advisory = Erratum(errata_id=advisory_id)
                    advisory_cves = tuple(sorted({cve.upper() for cve in _CVE_RE.findall(advisory.cve_names or "")}))
                    advisories.append(
                        RpmAdvisoryEvidence(
                            advisory_id=str(advisory_id),
                            url=str(advisory.url()),
                            state=str(advisory.errata_state or "unknown"),
                            cves=advisory_cves,
                        )
                    )
                advisory_lookup_complete = True
            except Exception:
                warnings.append(f"ERRATA_ADVISORY_LOOKUP_FAILED: Advisory lookup failed for build {nvr}")

            evidence.append(
                RpmBuildEvidence(
                    nvr=nvr,
                    changelog_cves=tuple(sorted(changelog_cves)),
                    changelog_complete=changelog_complete,
                    advisories=tuple(advisories),
                    advisory_lookup_complete=advisory_lookup_complete,
                    warnings=tuple(warnings),
                )
            )
        return tuple(evidence)
