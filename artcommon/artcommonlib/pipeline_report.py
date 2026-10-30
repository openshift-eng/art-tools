"""Best-effort component reporting on the ART PipelineRun that invoked a tool.

Konflux clients use their own credentials. Reporting deliberately uses ``oc``'s
ART cluster credentials instead, and never changes build decisions.
"""

import asyncio
import copy
import hashlib
import json
import logging
import os
import time
import weakref
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from functools import partial, wraps

LOGGER = logging.getLogger(__name__)
COMPONENT_PREFIX = 'art.openshift.io/component-build-'
REPORT_ANNOTATION = 'art.openshift.io/component-report'
REPORT_BUDGET = 192 * 1024
ANNOTATION_LIMIT = 256 * 1024 - 1024
_reporters = weakref.WeakKeyDictionary()


def _now():
    return datetime.now(timezone.utc).isoformat()


def component_annotation(artifact_type: str, name: str) -> str:
    """Return the stable annotation key for one component and artifact type."""
    identity = f'{artifact_type}:{name}'.encode()
    return COMPONENT_PREFIX + hashlib.sha256(identity).hexdigest()[:20]


def resource_dict(resource):
    return resource.to_dict() if hasattr(resource, 'to_dict') else resource


class PipelineReporter:
    """Publish component results without changing the invoking tool's outcome."""

    def __init__(self, namespace: str = '', name: str = '', uid: str = ''):
        self.namespace = namespace
        self.name = name
        self.uid = uid
        self.enabled = bool(namespace and name)
        self._loaded = False
        self._annotations = {}
        self._components = {}
        self._lock = asyncio.Lock()
        self._partial = False
        self._retry_after = 0

    async def _command(self, args):
        """Run an ART cluster command with a bounded lifetime."""
        process = await asyncio.create_subprocess_exec(
            'oc', *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=6)
        except BaseException:
            if process.returncode is None:
                process.kill()
            await process.wait()
            raise
        if process.returncode:
            raise RuntimeError(stderr.decode(errors='replace').strip()[:500])
        return stdout.decode()

    async def _load(self):
        """Load existing reports and verify the parent PipelineRun identity."""
        if self._loaded:
            return
        run = json.loads(await self._command(['get', 'pipelinerun', self.name, '-n', self.namespace, '-o', 'json']))
        metadata = run['metadata']
        if self.uid and self.uid != metadata['uid']:
            raise RuntimeError('ART PipelineRun UID changed')
        self.uid = metadata['uid']
        self._annotations = metadata.get('annotations') or {}
        for key, value in self._annotations.items():
            if key.startswith(COMPONENT_PREFIX):
                try:
                    component = json.loads(value)
                    if (
                        component.get('version') == 1
                        and isinstance(component.get('status'), str)
                        and isinstance(component.get('pipelineRuns'), list)
                    ):
                        self._components[key] = component
                    else:
                        self._partial = True
                except (ValueError, AttributeError):
                    self._partial = True
        try:
            self._partial |= bool(json.loads(self._annotations.get(REPORT_ANNOTATION, '{}')).get('partial'))
        except (ValueError, AttributeError):
            self._partial = True
        self._loaded = True

    async def _patch(self, annotations):
        """Merge only the supplied annotations into the verified parent run."""
        patch = {'metadata': {'uid': self.uid, 'annotations': annotations}}
        await self._command(
            [
                'patch',
                'pipelinerun',
                self.name,
                '-n',
                self.namespace,
                '--type=merge',
                '-p',
                json.dumps(patch, separators=(',', ':')),
            ]
        )
        self._annotations.update(annotations)

    async def note(self, reason: str, *, state="pending"):
        """Publish selection progress while retaining any reporting limitation."""
        if not self.enabled:
            return
        async with self._lock:
            try:
                await self._load()
                marker = json.loads(self._annotations.get(REPORT_ANNOTATION, '{}'))
                if not isinstance(marker, dict):
                    marker = {}
                await self._patch(
                    {
                        REPORT_ANNOTATION: json.dumps(
                            {
                                **marker,
                                'version': 1,
                                'partial': self._partial,
                                'reason': marker.get('reason')
                                if self._partial and marker.get('reason')
                                else reason[:500],
                                'state': state,
                            }
                        )
                    }
                )
            except Exception as exc:
                self._failed(exc)

    def _failed(self, error):
        """Mark reporting partial and log only a safe failure category."""
        if not self._partial:
            category = (
                'timeout'
                if isinstance(error, TimeoutError)
                else 'command'
                if isinstance(error, RuntimeError)
                else 'reporting'
            )
            LOGGER.warning('Component reporting is incomplete (%s)', category)
        self._partial = True
        self._retry_after = time.monotonic() + 30

    def component(self, artifact_type: str, name: str, *, display_name: str = '', dry_run: bool = False):
        """Create a reporting handle for a component operation."""
        return ComponentReport(self, artifact_type, name, display_name, dry_run)

    async def update(self, component, mutate):
        """Serialize a component mutation and publish it within annotation limits."""
        if not self.enabled or time.monotonic() < self._retry_after:
            return
        async with self._lock:
            if time.monotonic() < self._retry_after:
                return
            try:
                await self._load()
                key = component_annotation(component.artifact_type, component.name)
                entry = self._components.setdefault(
                    key,
                    {
                        'version': 1,
                        'type': component.artifact_type,
                        'name': component.name,
                        'displayName': component.display_name or component.name,
                        'status': 'Pending',
                        'reason': '',
                        'pipelineRuns': [],
                    },
                )
                previous = copy.deepcopy(entry)
                if component.display_name:
                    entry['displayName'] = component.display_name
                mutate(entry)
                published = json.loads(self._annotations.get(key, 'null'))
                marker = json.loads(self._annotations.get(REPORT_ANNOTATION, '{}'))
                if not isinstance(marker, dict):
                    marker = {}
                if previous == entry and entry == published and bool(marker.get('partial')) == self._partial:
                    return
                entry['updatedAt'] = _now()
                encoded = json.dumps(entry, separators=(',', ':'), ensure_ascii=False)
                marker.update(version=1, partial=self._partial)
                encoded_marker = json.dumps(marker)
                annotations = {**self._annotations, key: encoded, REPORT_ANNOTATION: encoded_marker}

                def size(values):
                    return sum(len(k.encode()) + len(v.encode()) for k, v in values.items())

                reporting = {k: v for k, v in annotations.items() if k.startswith(COMPONENT_PREFIX)}
                if size(reporting) > REPORT_BUDGET or size(annotations) > ANNOTATION_LIMIT:
                    self._failed('Annotation capacity exceeded; published links have been retained')
                    await self._patch(
                        {
                            REPORT_ANNOTATION: json.dumps(
                                {
                                    **marker,
                                    'version': 1,
                                    'partial': True,
                                    'reason': 'Component reporting exceeded annotation capacity',
                                }
                            )
                        }
                    )
                    return
                await self._patch({key: encoded, REPORT_ANNOTATION: encoded_marker})
            except Exception as exc:
                self._failed(exc)


def best_effort(method):
    """Keep reporting errors from escaping into build execution."""

    @wraps(method)
    async def report(self, *args, **kwargs):
        try:
            return await method(self, *args, **kwargs)
        except Exception as exc:
            self.reporter._failed(exc)

    return report


class ComponentReport:
    """Track one operation while retaining earlier native build attempts."""

    def __init__(self, reporter, artifact_type, name, display_name='', dry_run=False):
        self.reporter = reporter
        self.artifact_type = artifact_type
        self.name = name
        self.display_name = display_name
        self.dry_run = dry_run
        self._previous_runs = None
        self._check_definitions = []
        self._current_run = None

    @best_effort
    async def configure_checks(self, checks):
        """Declare enabled checks before building; keep results with their build attempt."""
        self._check_definitions = [
            {**check, 'status': 'DryRun' if self.dry_run else 'Pending', 'reason': '', 'url': ''} for check in checks
        ]
        await self.reporter.update(self, lambda entry: entry.update(checks=copy.deepcopy(self._check_definitions)))

    def check_callback(self, kind):
        """Bind updates to this attempt, including while a later retry is running."""
        return partial(self._update_check, self._current_run, kind)

    @best_effort
    async def _update_check(self, identity, kind, name, status, *, url='', reason=''):
        if self.dry_run:
            return

        def mutate(entry):
            run = next((run for run in entry['pipelineRuns'] if (run['namespace'], run['name']) == identity), None)
            if run is None:
                raise ValueError('Check result has no recorded build attempt')
            check = next(check for check in run['checks'] if check['kind'] == kind and check['name'] == name)
            check.update(status=status, url=str(url or ''), reason=str(reason or '')[:500])
            if run is max(entry['pipelineRuns'], key=lambda run: run['attempt']):
                entry['checks'] = copy.deepcopy(run['checks'])

        await self.reporter.update(self, mutate)

    @best_effort
    async def finish_checks(self):
        """Mark checks that could not start after an earlier failure as not run."""

        def mutate(entry):
            for check in entry.get('checks', []):
                if check['status'] == 'Pending':
                    check.update(status='Skipped', reason='Not run because an earlier step failed')
            if self._current_run:
                for run in entry['pipelineRuns']:
                    if (run['namespace'], run['name']) == self._current_run:
                        for check in run.get('checks', []):
                            if check['status'] == 'Pending':
                                check.update(status='Skipped', reason='Not run because an earlier step failed')

        await self.reporter.update(self, mutate)

    async def set_status(self, status, *, reason=''):
        """Set an operation result, capturing earlier runs when starting work."""

        def mutate(entry):
            if status == 'Pending':
                self._previous_runs = {(run['namespace'], run['name']) for run in entry['pipelineRuns']}
            entry['status'] = 'DryRun' if self.dry_run else status
            entry['reason'] = str(reason)[:500]

        await self.reporter.update(self, mutate)

    async def failed(self, reason):
        """Record preparation errors without overriding a native build result."""

        def mutate(entry):
            runs = [
                run
                for run in entry['pipelineRuns']
                if self._previous_runs is not None and (run['namespace'], run['name']) not in self._previous_runs
            ]
            latest = max(runs, key=lambda run: run['attempt']) if runs else None
            if latest and latest['status'] in ('Succeeded', 'Failed', 'Cancelled'):
                return
            entry['status'] = 'DryRun' if self.dry_run else 'Unknown' if runs else 'Failed'
            entry['reason'] = str(reason)[:500]

        await self.reporter.update(self, mutate)

    @asynccontextmanager
    async def preparing(self):
        """Report preparation errors and propagate the original exception."""
        await self.set_status('Pending')
        try:
            yield self
        except Exception as exc:
            await self.failed(str(exc))
            raise

    @best_effort
    async def observe(self, resource, client, *, attempt):
        """Record a native build with attempt numbers increasing across invocations."""
        if not self.reporter.enabled or self.dry_run:
            return
        data = resource_dict(resource)
        metadata = data.get('metadata') or {}
        if not metadata.get('name'):
            return
        self._current_run = (metadata.get('namespace', ''), metadata['name'])
        status = data.get('status') or {}
        condition = next((c for c in status.get('conditions', []) if c.get('type') == 'Succeeded'), {})
        item = {
            'name': metadata['name'],
            'namespace': metadata.get('namespace', ''),
            'url': resource_url(client, data),
            'status': pipeline_status(data),
            'attempt': attempt,
            'reason': str(condition.get('message') or condition.get('reason') or '')[:500],
        }

        def mutate(entry):
            runs = entry['pipelineRuns']
            identity = (item['namespace'], item['name'])
            if self._previous_runs is None:
                self._previous_runs = {(run['namespace'], run['name']) for run in runs} - {identity}
            for existing in runs:
                if (existing['namespace'], existing['name']) == identity:
                    if existing['status'] in ('Succeeded', 'Failed', 'Cancelled') and item['status'] == 'Running':
                        return
                    existing.update({**item, 'attempt': existing['attempt']})
                    break
            else:
                item['attempt'] = max(attempt, max((run['attempt'] for run in runs), default=0) + 1)
                item['checks'] = copy.deepcopy(self._check_definitions)
                runs.append(item)
            if item['status'] in ('Failed', 'Cancelled'):
                for run in runs:
                    if (run['namespace'], run['name']) == identity:
                        for check in run.get('checks', []):
                            if check['status'] == 'Pending':
                                check.update(status='Skipped', reason='Not run because the build failed')
            latest = max(runs, key=lambda run: run['attempt'])
            entry['status'] = latest['status']
            entry['reason'] = latest['reason']
            if 'checks' in latest:
                entry['checks'] = copy.deepcopy(latest['checks'])

        await self.reporter.update(self, mutate)


def resource_url(client, data):
    """Return a build link without an unavailable application segment."""
    url = client.resource_url(data)
    if '/applications/unknown-application/' in url:
        return url.replace('/applications/unknown-application', '')
    return url


def pipeline_status(resource):
    """Translate the native Tekton condition into a build result."""
    data = resource_dict(resource)
    condition = next((c for c in (data.get('status') or {}).get('conditions', []) if c.get('type') == 'Succeeded'), {})
    status = {'True': 'Succeeded', 'False': 'Failed', 'Unknown': 'Running'}.get(condition.get('status'), 'Running')
    if status == 'Failed' and 'cancel' in condition.get('reason', '').lower():
        status = 'Cancelled'
    return status


def get_reporter():
    """Share one reporter across parallel builders within an invocation."""
    namespace = os.getenv('TASKRUN_NAMESPACE', '')
    name = os.getenv('TEKTON_PIPELINERUN_NAME', '')
    if not (os.getenv('TASKRUN_NAME') and namespace and name):
        return PipelineReporter()
    loop = asyncio.get_running_loop()
    context = (namespace, name)
    reporters = _reporters.setdefault(loop, {})
    return reporters.setdefault(context, PipelineReporter(namespace, name, os.getenv('TEKTON_PIPELINERUN_UID', '')))
