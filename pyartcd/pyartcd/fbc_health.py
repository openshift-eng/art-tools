import hashlib
import json
import logging
from dataclasses import dataclass, field
from urllib.parse import quote, unquote

from artcommonlib import redis

from pyartcd import util


def counter_prefix(group: str, assembly: str, version: str, target: str) -> str:
    return 'count:fbc-failure:konflux:' + ':'.join(
        quote(str(value), safe='') for value in (group, assembly, version, target)
    )


def invocation_scope(**selection) -> str:
    normalized = dict(selection)
    for name in ('only', 'exclude', 'operator_nvrs'):
        normalized[name] = sorted({part.strip() for part in (selection.get(name) or '').split(',') if part.strip()})
    normalized['skip_tasks'] = sorted(set(selection.get('skip_tasks') or ()))
    return hashlib.sha256(json.dumps(normalized, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


async def update_counters(
    prefix: str,
    scope: str,
    records: dict[str, list[dict]] | None,
    failed: bool,
    dry_run: bool,
    logger: logging.Logger,
    url: str | None = None,
) -> None:
    if dry_run:
        return
    records = records or {}
    successes = {entry['name'] for entry in records.get('build_success', [])}
    failures = {entry['name'] for entry in records.get('build_failure', [])}
    failures.update(entry['name'] for entry in records.get('fbc_failure', []))
    failures.update(entry['name'].removesuffix('-fbc') for entry in records.get('rebase_failure', []))
    successes -= failures
    operations = [(f'{prefix}:operator:{quote(name, safe="")}', False) for name in sorted(successes)]
    operations.extend((f'{prefix}:operator:{quote(name, safe="")}', True) for name in sorted(failures))
    fallback = f'{prefix}:invocation:{scope}'
    if not failures and (failed or not records.get('skipped')):
        operations.append((fallback, failed or not successes))
    for branch, increment in operations:
        try:
            if increment:
                await util.increment_fail_counter(branch, jenkins_url=url)
            else:
                await util.reset_fail_counter(branch)
        except Exception:
            logger.exception('Failed to update FBC failure counter %s', branch)


@dataclass
class FbcHealthReport:
    failures: dict[str, dict] = field(default_factory=dict)
    error: bool = False

    def summary(self) -> str:
        if self.error:
            return 'FBC health unavailable (Redis read failed)'
        return f'{len(self.failures)} FBC failure(s)' if self.failures else ''

    def details(self) -> str:
        sections = []
        if self.error:
            sections.append(':warning: FBC health unavailable (Redis read failed); recovery cannot be determined.')
        if self.failures:
            sections.append(f'*FBC Failures ({len(self.failures)}):*')
        for failure in self.failures.values():
            identity = (
                f"assembly `{failure['assembly']}`, build version `{failure['version']}`, "
                f"OCP target `{failure['target']}`"
            )
            entity = (
                f"operator `{failure['entity']}`"
                if failure['kind'] == 'operator'
                else f"invocation `{failure['entity']}`"
            )
            line = f"- {entity} ({identity}): Failed {failure['failure_count']} time(s)"
            if failure.get('url'):
                line += f" (<{failure['url']}|Last failure>)"
            sections.append(line)
        return '\n'.join(sections)


async def collect_fbc_health(group: str, logger: logging.Logger) -> FbcHealthReport:
    report = FbcHealthReport()
    pattern = f'count:fbc-failure:konflux:{quote(group, safe="")}:*:failure'
    try:
        for key in sorted(await redis.get_keys(pattern)):
            parts = key.split(':')
            if len(parts) != 10 or parts[7] not in ('operator', 'invocation'):
                raise ValueError('Malformed FBC failure key')
            count = await redis.get_value(key)
            if count is None:
                continue
            count = int(count)
            if count < 1:
                continue
            url = await redis.get_value(f'{key.removesuffix(":failure")}:jenkins_url')
            failure = dict(zip(('assembly', 'version', 'target', 'kind', 'entity'), map(unquote, parts[4:9])))
            failure.update(failure_count=count, url=url)
            report.failures[key] = failure
    except Exception:
        logger.exception('Failed to collect FBC health for %s', group)
        report.error = True
    return report
