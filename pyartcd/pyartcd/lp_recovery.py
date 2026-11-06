"""Preserve ART build-chain relationships when an LP scan recovers downstream jobs."""

import base64
import json
import logging
import os
import re
import ssl
import subprocess
from datetime import datetime, timezone
from functools import cached_property
from urllib.parse import quote, urlparse

import httpx

from pyartcd import tekton

LOGGER = logging.getLogger(__name__)
PREFIX = 'art.openshift.io/'


def parameters(run: dict) -> dict:
    return {item['name']: item.get('value') for item in run.get('spec', {}).get('params', [])}


def pipeline_name(run: dict) -> str:
    return (run.get('spec', {}).get('pipelineRef') or {}).get('name') or (
        run.get('metadata', {}).get('labels') or {}
    ).get('tekton.dev/pipeline', '')


def is_active(run: dict) -> bool:
    condition = next(
        (item for item in run.get('status', {}).get('conditions', []) if item.get('type') == 'Succeeded'), {}
    )
    return condition.get('status') not in ('True', 'False')


def operator_nvrs(run: dict) -> set[str]:
    return set(filter(None, re.split(r'[\s,]+', parameters(run).get('operator-nvrs') or '')))


class PipelineRunLookup:
    """Read live and archived runs using the scan's existing cluster credentials."""

    def __init__(self):
        self.namespace = tekton._get_namespace()
        self._runs = {}
        self._children = {}

    def _oc(self, *args):
        result = subprocess.run(['oc', *args], check=True, capture_output=True, text=True, timeout=30)
        return result.stdout.strip()

    @cached_property
    def results_client(self):
        ca_file = os.environ.get('RESULTS_CA_FILE')
        if ca_file:
            context = ssl.create_default_context(cafile=ca_file)
        else:
            ca = json.loads(
                self._oc('get', 'configmap', 'openshift-service-ca.crt', '-n', self.namespace, '-o', 'json')
            )
            context = ssl.create_default_context(cadata=ca['data']['service-ca.crt'])
        token = self._oc('whoami', '--show-token')
        return httpx.Client(
            base_url=os.environ.get(
                'RESULTS_API_URL', 'https://tekton-results-api-service.openshift-pipelines.svc:8080'
            ),
            headers={'Authorization': f'Bearer {token}'},
            verify=context,
            timeout=30,
        )

    def close(self):
        if 'results_client' in self.__dict__:
            self.results_client.close()

    def _archived(self, query):
        params = {'filter': f'data_type == PIPELINE_RUN && {query}', 'page_size': 1000}
        runs = []
        while True:
            response = self.results_client.get(
                f'/apis/results.tekton.dev/v1alpha2/parents/{quote(self.namespace)}/results/-/records', params=params
            )
            response.raise_for_status()
            page = response.json()
            for record in page.get('records', []):
                runs.append(json.loads(base64.b64decode(record['data']['value'])))
            token = page.get('next_page_token') or page.get('nextPageToken')
            if not token:
                return runs
            params['page_token'] = token

    def from_url(self, url, expected_pipeline, group, assembly, built_at=None):
        if not isinstance(url, str):
            return None
        parsed = urlparse(url)
        match = re.fullmatch(r'/k8s/ns/([a-z0-9-]+)/tekton.dev~v1~PipelineRun/([a-z0-9.-]+)', parsed.path)
        if (
            parsed.scheme != 'https'
            or parsed.netloc != urlparse(tekton._get_console_url()).netloc
            or not match
            or match[1] != self.namespace
        ):
            return None
        name = match[2]
        if name not in self._runs:
            raw = self._oc('get', 'pipelinerun', name, '-n', self.namespace, '-o', 'json', '--ignore-not-found')
            matches = [json.loads(raw)] if raw else self._archived(f"data.metadata.name == '{name}'")
            self._runs[name] = matches[0] if len(matches) == 1 else None
        run = self._runs[name]
        if run:
            if isinstance(built_at, datetime):
                created = datetime.fromisoformat(run['metadata']['creationTimestamp'].replace('Z', '+00:00'))
                if created > built_at.replace(tzinfo=built_at.tzinfo or timezone.utc):
                    LOGGER.warning('PipelineRun name %s was reused after the recorded build', name)
                    return None
            values = parameters(run)
            if (
                pipeline_name(run) == expected_pipeline
                and run['metadata']['namespace'] == self.namespace
                and values.get('group') == group
                and values.get('assembly', 'stream') == assembly
                and values.get('dry-run', 'false') == 'false'
            ):
                return run
        return None

    def children(self, parent):
        name = parent['metadata']['name']
        if name not in self._children:
            selector = f'{PREFIX}parent-pipelinerun={name}'
            live = json.loads(self._oc('get', 'pipelineruns', '-n', self.namespace, '-l', selector, '-o', 'json'))
            try:
                archived = self._archived(f"data.metadata.labels['{PREFIX}parent-pipelinerun'] == '{name}'")
            except Exception:
                LOGGER.warning(
                    'Archived recovery attempts for %s are unavailable; using live runs', name, exc_info=True
                )
                archived = []
            by_uid = {run['metadata']['uid']: run for run in archived + live['items']}
            # A name reused after pruning cannot safely establish a relationship to an older parent.
            self._children[name] = [
                run
                for run in by_uid.values()
                if run['metadata']['namespace'] == self.namespace
                and (run['metadata'].get('labels') or {}).get(f'{PREFIX}parent-pipelinerun') == name
                and run['metadata'].get('creationTimestamp', '') >= parent['metadata'].get('creationTimestamp', '')
            ]
        return self._children[name]


class RecoveryPlanner:
    def __init__(self, group, assembly):
        self.group = group
        self.assembly = assembly
        self.lookup = PipelineRunLookup()

    def close(self):
        self.lookup.close()

    def plan(self, pipeline, params, parents, rebuilding_images=()):
        """Return distinct parent/retry batches; unresolved runs remain visibly unlinked.

        ``parents`` maps requested operator NVRs to the operator (bundle recovery)
        or successful bundle (FBC recovery) build records that supplied their inputs.
        """
        batches = {}
        active_workloads = {}
        expected_parent = 'build-layered-products' if pipeline == 'olm-bundle-konflux' else 'olm-bundle-konflux'
        for nvr, record in parents.items():
            parent = source = None
            try:
                parent = self.lookup.from_url(
                    getattr(record, 'art_job_url', ''),
                    expected_parent,
                    self.group,
                    self.assembly,
                    getattr(record, 'start_time', None),
                )
                if parent:
                    if is_active(parent):
                        LOGGER.info('Deferring recovery of %s while its parent is active', nvr)
                        continue
                    candidates = []
                    for child in self.lookup.children(parent):
                        values = parameters(child)
                        if (
                            pipeline_name(child) == pipeline
                            and values.get('group') == self.group
                            and values.get('assembly', 'stream') == self.assembly
                            and values.get('dry-run', 'false') == 'false'
                            and values.get('ocp-target-version', '') == params.get('ocp-target-version', '')
                        ):
                            if is_active(child):
                                active_workloads.setdefault(parent['metadata']['uid'], set()).update(
                                    operator_nvrs(child)
                                )
                            if nvr in operator_nvrs(child):
                                candidates.append(child)
                    if candidates:
                        source = max(
                            candidates,
                            key=lambda run: (run['metadata'].get('creationTimestamp', ''), run['metadata']['uid']),
                        )
                        if is_active(source):
                            LOGGER.info('Recovery already active for %s: %s', nvr, source['metadata']['name'])
                            continue
            except Exception:
                LOGGER.warning('Could not resolve recovery lineage for %s', nvr, exc_info=True)
                source = None

            if not parent:
                LOGGER.warning('Recovery of %s has no verified %s parent', nvr, expected_parent)
            key = (parent['metadata']['uid'] if parent else None, source['metadata']['uid'] if source else None)
            batch = batches.setdefault(key, {'nvrs': set(), 'parent_run': parent, 'rebuilt_from': source})
            batch['nvrs'].add(nvr)
            if source:
                # Health replaces a whole ART attempt. Replay its complete workload,
                # even when only one operator in that attempt still needs recovery.
                batch['nvrs'].update(operator_nvrs(source))

        plans = []
        for batch in batches.values():
            parent = batch['parent_run']
            if parent and batch['nvrs'] & active_workloads.get(parent['metadata']['uid'], set()):
                LOGGER.info('Deferring recovery batch overlapping an active downstream run')
                continue
            if any(nvr.rsplit('-', 2)[0] in rebuilding_images for nvr in batch['nvrs']):
                LOGGER.info('Deferring recovery batch containing an operator already being rebuilt')
                continue
            plans.append(
                {
                    'pipeline_name': pipeline,
                    'params': {**params, 'operator-nvrs': ','.join(sorted(batch.pop('nvrs')))},
                    **batch,
                    'use_current_parent': False,
                }
            )
        return plans
