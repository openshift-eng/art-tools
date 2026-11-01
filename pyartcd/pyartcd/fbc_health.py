"""Bounded FBC outcome accounting shared by CLI and direct catalog producers."""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, unquote

from artcommonlib import redis

from pyartcd.record import parse_record_log

RETENTION_SECONDS = 30 * 24 * 60 * 60
OUTCOMES = {'succeeded', 'failed', 'reused', 'skipped'}


def prepare_evidence(path: Path, logger: logging.Logger) -> bool:
    """Prevent stale outcomes from establishing recovery without failing the build."""
    try:
        path.unlink(missing_ok=True)
        return True
    except OSError:
        logger.warning('Unable to clear previous FBC tracking evidence')
        return False


def counter_prefix(group: str, variant: str, assembly: str, target: str) -> str:
    """Identify a recoverable scope independently of versions or selected artifacts."""
    return 'count:fbc-failure:konflux:' + ':'.join(
        quote(str(value), safe='') for value in (group, variant, assembly, target)
    )


@dataclass
class FbcEvidence:
    """Terminal outcomes and coverage evidence from this invocation only."""

    outcomes: dict[str, dict] = field(default_factory=dict)
    complete: bool = False
    unavailable: bool = False
    target: str | None = None
    ambiguous: set[str] = field(default_factory=set)
    conflicting_targets: set[str] = field(default_factory=set)


def read_evidence(path: Path) -> FbcEvidence:
    """Preserve valid terminal records even when other records are malformed."""
    evidence = FbcEvidence()
    seen = set()
    targets = {}
    try:
        with path.open() as stream:
            records = parse_record_log(stream)
        for entry in records.get('fbc_outcome', []):
            name, outcome = entry.get('name'), entry.get('outcome')
            if name:
                if name in seen:
                    evidence.ambiguous.add(name)
                    evidence.unavailable = True
                seen.add(name)
                if entry.get('target'):
                    if name in targets and targets[name] != entry['target']:
                        evidence.conflicting_targets.add(name)
                    targets[name] = entry['target']
            if not name or outcome not in OUTCOMES or (outcome in {'reused', 'skipped'} and not entry.get('reason')):
                evidence.unavailable = True
                if name:
                    evidence.ambiguous.add(name)
                continue
            if name in evidence.outcomes:
                evidence.unavailable = True
                if outcome == 'failed':
                    evidence.outcomes[name] = entry
                continue
            evidence.outcomes[name] = entry
        markers = records.get('fbc_invocation', [])
        evidence.complete = (
            len(markers) == 1
            and markers[0].get('status') == 'complete'
            and int(markers[0]['operators']) == len(evidence.outcomes)
        )
        if evidence.complete:
            evidence.target = markers[0].get('target')
    except (OSError, ValueError, KeyError, TypeError):
        evidence.unavailable = True
    evidence.unavailable |= not evidence.complete
    return evidence


@redis.handle_connection
async def _store_state(conn, key: str, metadata: dict, increment: bool) -> None:
    """Commit count, metadata and inactivity expiry as one Redis transaction."""
    async with conn.pipeline(transaction=True) as transaction:
        if increment:
            transaction.hincrby(key, 'failure_count', 1)
        transaction.hset(key, mapping=metadata)
        transaction.expire(key, RETENTION_SECONDS)
        await transaction.execute()


async def update_counters(
    prefix: str,
    version: str,
    artifacts: str,
    evidence: FbcEvidence,
    failed: bool,
    dry_run: bool,
    logger: logging.Logger,
) -> None:
    """Track failures best-effort without altering the producer's original result."""
    if dry_run:
        return
    metadata = {
        'build_version': version,
        'operator_nvrs': artifacts,
        'last_failure_at': datetime.now(timezone.utc).isoformat(),
    }
    operations = []
    attributable = False
    operator_failures = False
    for name, entry in evidence.outcomes.items():
        outcome = entry['outcome']
        if outcome not in {'succeeded', 'failed'}:
            continue
        if name in evidence.conflicting_targets or (outcome == 'succeeded' and name in evidence.ambiguous):
            continue
        if outcome == 'succeeded' and not entry.get('target'):
            evidence.unavailable = True
            continue
        attributable = True
        operator_failures |= outcome == 'failed'
        artifact_metadata = {key: entry[key] for key in ('operator_nvr', 'bundle_nvr', 'fbc_nvr') if entry.get(key)}
        operations.append(
            (f'{prefix}:operator:{quote(name, safe="")}', outcome == 'failed', 'operator', artifact_metadata)
        )
    invocation = f'{prefix}:invocation:latest'
    if failed and not operator_failures:
        operations.append((invocation, True, 'invocation', {}))
    elif evidence.complete and not evidence.unavailable and attributable:
        operations.append((invocation, False, 'invocation', {}))
    operations.append((f'{prefix}:tracking:latest', evidence.unavailable, 'tracking', {}))
    for key, store, kind, artifact_metadata in operations:
        try:
            if store:
                await _store_state(key, {**metadata, **artifact_metadata, 'kind': kind}, increment=kind != 'tracking')
            elif kind != 'tracking' or evidence.complete:
                await redis.delete_key(key)
        except Exception:
            logger.warning('Unable to update FBC tracking state')


async def account_run(
    path: Path,
    group: str,
    variant: str,
    assembly: str,
    target: str,
    version: str,
    artifacts: str,
    failed: bool,
    dry_run: bool,
    logger: logging.Logger,
    fresh: bool = True,
    resolve_unknown_target: bool = False,
) -> None:
    """Read fresh producer evidence and account once at its owning entrypoint."""
    if dry_run:
        return
    try:
        evidence = read_evidence(path) if fresh else FbcEvidence(unavailable=True)
        if target == 'default':
            targets = {entry['target'] for entry in evidence.outcomes.values() if entry.get('target')}
            target = evidence.target or (next(iter(targets)) if len(targets) == 1 else 'unknown')
            resolve_unknown_target = target != 'unknown'
        for name, entry in list(evidence.outcomes.items()):
            if entry['outcome'] in {'succeeded', 'failed'} and entry.get('target') not in {None, target}:
                del evidence.outcomes[name]
                evidence.unavailable = True
        await update_counters(
            counter_prefix(group, variant, assembly, target),
            version,
            artifacts,
            evidence,
            failed,
            False,
            logger,
        )
        unresolved_scopes = set()
        if variant != 'unknown':
            unresolved_scopes.add(counter_prefix(group, 'unknown', assembly, target))
        if resolve_unknown_target and target != 'unknown':
            unresolved_scopes.add(counter_prefix(group, variant, assembly, 'unknown'))
            unresolved_scopes.add(counter_prefix(group, 'unknown', assembly, 'unknown'))
        for unresolved in unresolved_scopes:
            for kind in ('invocation', 'tracking'):
                await redis.delete_key(f'{unresolved}:{kind}:latest')
    except Exception:
        logger.warning('Unable to read FBC tracking evidence')


@dataclass
class FbcHealthReport:
    """General report data deliberately excludes diagnostic URLs and artifacts."""

    failures: dict[str, dict] = field(default_factory=dict)
    error: bool = False

    def summary(self) -> str:
        parts = []
        count = sum(entry['kind'] != 'tracking' for entry in self.failures.values())
        if count:
            parts.append(f'{count} tracked FBC failure(s)')
        if self.error or any(entry['kind'] == 'tracking' for entry in self.failures.values()):
            parts.append('FBC tracking unavailable')
        return '; '.join(parts)

    def details(self) -> str:
        """Render counts and scope, never raw execution URL metadata."""
        sections = []
        if self.error:
            sections.append(':warning: FBC tracking unavailable; collection incomplete, recovery cannot be determined.')
        for entry in self.failures.values():
            scope = f"variant `{entry['variant']}`, assembly `{entry['assembly']}`, OCP target `{entry['target']}`"
            if entry['kind'] == 'tracking':
                sections.append(f'- FBC outcome evidence unavailable ({scope})')
            else:
                entity = f"operator `{entry['entity']}`" if entry['kind'] == 'operator' else 'unattributed invocation'
                sections.append(
                    f"- {entity} ({scope}, last build version `{entry['version']}`): "
                    f"Failed {entry['failure_count']} time(s)"
                )
        return '\n'.join(sections)


async def collect_fbc_health(group: str, logger: logging.Logger, variant: str | None = None) -> FbcHealthReport:
    """Read current tracked evidence without refreshing retention or reading URLs."""
    report = FbcHealthReport()
    pattern = f'count:fbc-failure:konflux:{quote(group, safe="")}:*'
    try:
        keys = sorted(await redis.get_keys(pattern))
    except Exception:
        logger.warning('Unable to collect FBC tracking state')
        report.error = True
        return report
    for key in keys:
        try:
            parts = key.split(':')
            if len(parts) != 9 or parts[7] not in {'operator', 'invocation', 'tracking'}:
                raise ValueError('Malformed FBC state key')
            values = dict(zip(('variant', 'assembly', 'target', 'kind', 'entity'), map(unquote, parts[4:])))
            if variant is not None and values['variant'] != variant:
                continue
            count, version, timestamp = await redis.call(
                'hmget', key, 'failure_count', 'build_version', 'last_failure_at'
            )
            if count is None and version is None and timestamp is None:
                continue
            if not timestamp or version is None:
                raise ValueError('Incomplete FBC state metadata')
            values.update(failure_count=int(count or 0), version=version or '')
            if values['kind'] != 'tracking' and values['failure_count'] < 1:
                raise ValueError('Invalid FBC count')
            report.failures[key] = values
        except Exception:
            logger.warning('Unable to collect an FBC tracking entry')
            report.error = True
    return report
