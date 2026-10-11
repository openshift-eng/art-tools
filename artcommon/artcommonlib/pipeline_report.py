"""Best-effort component reporting on the ART PipelineRun that invoked a tool.

Konflux clients use their own credentials. Reporting deliberately uses ``oc``'s
ART cluster credentials instead, and never changes build or release decisions.
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
from functools import wraps
from urllib.parse import quote

LOGGER = logging.getLogger(__name__)
COMPONENT_PREFIX = 'art.openshift.io/component-build-'
REPORT_ANNOTATION = 'art.openshift.io/component-report'
REPORT_BUDGET = 192 * 1024
ANNOTATION_LIMIT = 256 * 1024 - 1024
_reporters = weakref.WeakKeyDictionary()


def _now():
    return datetime.now(timezone.utc).isoformat()


def component_annotation(artifact_type: str, name: str) -> str:
    identity = f'{artifact_type}:{name}'.encode()
    return COMPONENT_PREFIX + hashlib.sha256(identity).hexdigest()[:20]


def resource_dict(resource):
    return resource.to_dict() if hasattr(resource, 'to_dict') else resource


class PipelineReporter:
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
                        and isinstance(component.get('stages'), dict)
                        and isinstance(component.get('resources'), list)
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
        if not self.enabled:
            return
        async with self._lock:
            try:
                await self._load()
                await self._patch(
                    {
                        REPORT_ANNOTATION: json.dumps(
                            {
                                'version': 1,
                                'partial': self._partial,
                                'reason': reason[:500],
                                'state': state,
                            }
                        )
                    }
                )
            except Exception as exc:
                self._failed(exc)

    async def stage_all(self, artifact_type, key, status, *, operator_nvrs=None, **kwargs):
        if not self.enabled:
            return
        try:
            async with self._lock:
                self._loaded = False
                await self._load()
                components = list(self._components.values())
            for entry in components:
                if entry['type'] == artifact_type and (
                    operator_nvrs is None or entry.get('operatorNvr') in operator_nvrs
                ):
                    fields = dict(kwargs)
                    previous = entry.get('stages', {}).get(key, {})
                    if status == 'Failed' and previous.get('reason'):
                        fields['reason'] = previous['reason']
                    await self.component(artifact_type, entry['name']).stage(key, status, **fields)
        except Exception as exc:
            self._failed(exc)

    def _failed(self, error):
        if not self._partial:
            LOGGER.warning('Component reporting is incomplete for %s/%s: %s', self.namespace, self.name, error)
        self._partial = True
        self._retry_after = time.monotonic() + 30

    def component(self, artifact_type: str, name: str, *, display_name: str = '', dry_run: bool = False):
        return ComponentReport(self, artifact_type, name, display_name, dry_run)

    async def update(self, component, mutate):
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
                        'stages': {},
                        'resources': [],
                    },
                )
                previous = copy.deepcopy(entry)
                if component.display_name:
                    entry['displayName'] = component.display_name
                if component.dry_run:
                    entry['dryRun'] = True
                mutate(entry)
                published = json.loads(self._annotations.get(key, 'null'))
                marker = json.loads(self._annotations.get(REPORT_ANNOTATION, '{}'))
                if not isinstance(marker, dict):
                    marker = {}
                if previous == entry and entry == published and bool(marker.get('partial')) == self._partial:
                    return
                entry['updatedAt'] = _now()
                encoded = json.dumps(entry, separators=(',', ':'), ensure_ascii=False)
                marker = json.dumps({'version': 1, 'partial': self._partial})
                annotations = {**self._annotations, key: encoded, REPORT_ANNOTATION: marker}

                def size(values):
                    return sum(len(k.encode()) + len(v.encode()) for k, v in values.items())

                reporting = {k: v for k, v in annotations.items() if k.startswith(COMPONENT_PREFIX)}
                if size(reporting) > REPORT_BUDGET or size(annotations) > ANNOTATION_LIMIT:
                    self._failed('Annotation capacity exceeded; published links have been retained')
                    await self._patch(
                        {
                            REPORT_ANNOTATION: json.dumps(
                                {
                                    'version': 1,
                                    'partial': True,
                                    'reason': 'Component reporting exceeded annotation capacity',
                                }
                            )
                        }
                    )
                    return
                await self._patch({key: encoded, REPORT_ANNOTATION: marker})
            except Exception as exc:
                self._failed(exc)


def best_effort(method):
    @wraps(method)
    async def report(self, *args, **kwargs):
        try:
            return await method(self, *args, **kwargs)
        except Exception as exc:
            self.reporter._failed(exc)

    return report


class ComponentReport:
    def __init__(self, reporter, artifact_type, name, display_name='', dry_run=False):
        self.reporter = reporter
        self.artifact_type = artifact_type
        self.name = name
        self.display_name = display_name
        self.dry_run = dry_run

    @asynccontextmanager
    async def phase(self, key, *, category='preparation', required=True, **fields):
        await self.stage(key, 'Running', category=category, required=required, **fields)
        try:
            yield self
        except Exception as exc:
            await self.stage(key, 'Failed', category=category, required=required, reason=str(exc), **fields)
            raise
        else:
            await self.stage(key, 'Succeeded', category=category, required=required, **fields)

    async def observe_release(self, resource, client, *, role='stage-release', required=True):
        if not self.reporter.enabled:
            return
        data = resource_dict(resource)
        if data.get('kind') == 'Snapshot':
            await self.resource(data, resource_url(client, data), role='release-snapshot')
        else:
            await self.release(data, client, role=role, required=required)

    async def stage(self, key, status, *, category='build', required=True, reason='', **fields):
        def mutate(entry):
            entry.update({k: v for k, v in fields.items() if v is not None})
            stage = {
                'name': key,
                'category': category,
                'required': required,
                'status': 'DryRun' if self.dry_run else status,
                'reason': str(reason)[:500],
                'updatedAt': _now(),
            }
            previous = entry['stages'].get(key, {})
            if previous.get('status') == stage['status'] and not reason:
                stage['reason'] = previous.get('reason', '')
            if any(previous.get(k) != v for k, v in stage.items() if k != 'updatedAt'):
                entry['stages'][key] = stage

        await self.reporter.update(self, mutate)

    @best_effort
    async def resource(
        self, resource, url, *, role, status='Unknown', attempt=None, reason='', started=None, completed=None
    ):
        if not self.reporter.enabled or self.dry_run or not resource:
            return
        data = resource_dict(resource)
        metadata = data.get('metadata') or {}
        if not metadata.get('name'):
            return
        item = {
            'kind': data.get('kind', 'PipelineRun'),
            'role': role,
            'name': metadata['name'],
            'namespace': metadata.get('namespace', ''),
            'uid': metadata.get('uid'),
            'url': url,
            'status': status,
            'attempt': attempt,
            'reason': str(reason)[:500] if reason else None,
            'started': started,
            'completed': completed,
        }

        def mutate(entry):
            identity = (item['kind'], item['namespace'], item['name'])
            for existing in entry['resources']:
                if (existing['kind'], existing['namespace'], existing['name']) == identity:
                    changes = {k: v for k, v in item.items() if v is not None}
                    if existing.get('status') in ('Succeeded', 'Failed', 'Cancelled') and status in (
                        'Running',
                        'Unknown',
                    ):
                        changes.pop('status', None)
                    existing.update(changes)
                    break
            else:
                entry['resources'].append(item)

        await self.reporter.update(self, mutate)

    def observer(self, client, role, *, attempt=None, category=None, required=True):
        async def observe(resource):
            if not self.reporter.enabled:
                return
            try:
                data = resource_dict(resource)
                status = data.get('status') or {}
                condition = next((c for c in status.get('conditions', []) if c.get('type') == 'Succeeded'), {})
                reason = condition.get('message') or condition.get('reason', '')
                await self.resource(
                    data,
                    resource_url(client, data),
                    role=role,
                    status=pipeline_status(data),
                    attempt=attempt,
                    reason=reason,
                    started=status.get('startTime'),
                    completed=status.get('completionTime'),
                )
                if category:
                    await self.stage(role, pipeline_status(data), category=category, required=required, reason=reason)
            except Exception as exc:
                self.reporter._failed(exc)

        return observe

    @best_effort
    async def release(self, resource, client, *, role='stage-release', required=True):
        if not self.reporter.enabled:
            return
        data = resource_dict(resource)
        status = data.get('status') or {}
        released = next((c for c in status.get('conditions', []) if c.get('type') == 'Released'), {})
        outcome = (
            'Succeeded'
            if released.get('status') == 'True'
            else (
                'Failed'
                if released.get('reason') not in (None, 'Unknown', 'Progressing', 'NotFound', 'Not Found')
                else 'Running'
            )
        )
        await self.resource(
            data, resource_url(client, data), role=role, status=outcome, reason=released.get("message", "")
        )
        await self.stage(role, outcome, category='release', required=required, reason=released.get('message', ''))
        processing = [
            ('managed-release', {**p, '_attempt': index + 1})
            for index, p in enumerate(status.get('managedPipelineAttempts', []))
        ]
        processing.extend(
            (label, status.get(field) or {})
            for label, field in (
                ('managed-release', 'managedProcessing'),
                ('tenant-release', 'tenantProcessing'),
                ('final-release', 'finalProcessing'),
            )
        )
        collectors = status.get('collectorsProcessing') or {}
        processing.extend(
            (label, collectors.get(field) or {})
            for label, field in (
                ('managed-collector', 'managedCollectorsProcessing'),
                ('tenant-collector', 'tenantCollectorsProcessing'),
            )
        )
        app = (data.get('metadata', {}).get('labels') or {}).get('appstudio.openshift.io/application')
        for label, info in processing:
            namespace, sep, name = (info.get('pipelineRun') or '').partition('/')
            if not sep or not namespace or not name:
                continue
            # Release PipelineRuns can be in a managed namespace. Use the same
            # application and releaseName context as the Konflux release view.
            url_data = {
                'kind': 'PipelineRun',
                'metadata': {
                    'namespace': namespace,
                    'name': name,
                    'labels': {'appstudio.openshift.io/application': app} if app else {},
                },
            }
            url = resource_url(client, url_data) + '?releaseName=' + quote(data['metadata']['name'], safe='')
            observed_status = info.get('status')
            if observed_status not in ('Succeeded', 'Failed', 'Running', 'Pending'):
                observed_status = 'Unknown' if info.get('completionTime') else 'Running'
            await self.resource(
                url_data,
                url,
                role=label,
                status=observed_status,
                attempt=info.get('_attempt'),
                reason=info.get("failureReason", ""),
                started=info.get("startTime"),
                completed=info.get("completionTime"),
            )

    @best_effort
    async def integration_tests(self, snapshot, client, blocking):
        if not self.reporter.enabled:
            return
        data = resource_dict(snapshot)
        await self.resource(data, resource_url(client, data), role='integration-snapshot')
        annotation = (data.get('metadata', {}).get('annotations') or {}).get('test.appstudio.openshift.io/status', '[]')
        try:
            entries = json.loads(annotation)
        except (ValueError, TypeError):
            return
        for entry in entries:
            name = entry.get('scenario')
            if not name:
                continue
            native = entry.get('status')
            status = (
                'Succeeded'
                if native in ('TestPassed', 'TestWarning')
                else 'Running'
                if native in (None, 'Pending', 'InProgress', 'BuildPLRInProgress')
                else 'Failed'
            )
            await self.stage(
                'ITS: ' + name, status, category='check', required=name in blocking, reason=entry.get('details', '')
            )
            run = entry.get('testPipelineRunName')
            if run:
                run_data = {
                    'kind': 'PipelineRun',
                    'metadata': {
                        'name': run,
                        'namespace': data['metadata'].get('namespace', ''),
                        'labels': data['metadata'].get('labels', {}),
                    },
                }
                await self.resource(run_data, resource_url(client, run_data), role='ITS: ' + name, status=status)


def resource_url(client, data):
    url = client.resource_url(data)
    if '/applications/unknown-application/' in url:
        return url.replace('/applications/unknown-application', '')
    return url


def pipeline_status(resource):
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
