"""Plan metadata-only repairs of unresolved LP recovery attempts.

Sources must have completed before the recorded recovery scan, and their full
operator workload must be covered. Partial batches and ambiguous names are never
claimed as recovered. Callers must persist and review the plan before applying it.
"""

import json
from collections import defaultdict

from pyartcd.lp_recovery import PREFIX, is_active, operator_nvrs, parameters, pipeline_name


def plan_lineage_repairs(runs: list[dict]) -> tuple[list[dict], list[dict]]:
    by_name = defaultdict(list)
    for run in runs:
        by_name[(run['metadata']['namespace'], run['metadata']['name'])].append(run)

    def identity(run):
        values = parameters(run)
        return run['metadata']['namespace'], values.get('group'), values.get('assembly', 'stream')

    def created(run):
        return run['metadata'].get('creationTimestamp', '')

    def real(run):
        return parameters(run).get('dry-run', 'false') == 'false'

    def parent(run):
        name = run['metadata'].get('labels', {}).get(f'{PREFIX}parent-pipelinerun')
        matches = by_name[(run['metadata']['namespace'], name)]
        if len(matches) != 1:
            return None
        candidate = matches[0]
        expected = 'build-layered-products' if pipeline_name(run) == 'olm-bundle-konflux' else 'olm-bundle-konflux'
        if (
            pipeline_name(candidate) in (expected, 'layered-products-scan')
            and identity(candidate) == identity(run)
            and real(candidate)
        ):
            if created(candidate) and created(candidate) <= created(run):
                return candidate
        return None

    plans, unresolved = [], []
    for retry in sorted(runs, key=created):
        metadata = retry['metadata']
        annotations = metadata.get('annotations', {})
        if annotations.get(f'{PREFIX}recovery-lineage') != 'unresolved':
            continue
        reason = None
        scan_matches = by_name[(metadata['namespace'], annotations.get(f'{PREFIX}recovery-scan'))]
        scan = scan_matches[0] if len(scan_matches) == 1 else None
        workload = operator_nvrs(retry)
        values = parameters(retry)
        if annotations.get(f'{PREFIX}rebuilt-from') or annotations.get(f'{PREFIX}rebuilt-from-uids'):
            reason = 'Existing retry metadata requires separate review'
        elif metadata.get('labels', {}).get(f'{PREFIX}parent-pipelinerun'):
            reason = 'Existing parent metadata requires separate review'
        elif pipeline_name(retry) not in ('olm-bundle-konflux', 'build-fbc') or not workload or not real(retry):
            reason = 'Not an eligible bundle/FBC workload'
        elif not scan or pipeline_name(scan) != 'layered-products-scan' or identity(scan) != identity(retry):
            reason = 'Recovery scan is missing or ambiguous'
        elif not real(scan) or not created(scan) or created(scan) > created(retry):
            reason = 'Recovery scan does not precede the real recovery attempt'

        sources = []
        ambiguous = False
        if reason is None:
            candidates = []
            for source in runs:
                if identity(source) != identity(retry) or pipeline_name(source) != pipeline_name(retry):
                    continue
                if not real(source) or is_active(source) or not parent(source):
                    continue
                if parameters(source).get('ocp-target-version', '') != values.get('ocp-target-version', ''):
                    continue
                completed = source.get('status', {}).get('completionTime', '')
                if (
                    not created(source)
                    or not completed
                    or completed > created(scan)
                    or created(source) >= created(scan)
                ):
                    continue
                inputs = operator_nvrs(source)
                if inputs and inputs <= workload:
                    candidates.append(source)
            selected = {}
            for nvr in sorted(workload):
                matches = [source for source in candidates if nvr in operator_nvrs(source)]
                if not matches:
                    continue
                latest_time = max(map(created, matches))
                latest = [source for source in matches if created(source) == latest_time]
                if len(latest) != 1:
                    ambiguous = True
                    break
                selected[latest[0]['metadata']['uid']] = latest[0]
            sources = sorted(selected.values(), key=lambda run: (created(run), run['metadata']['uid']))
            if ambiguous or not sources:
                reason = 'No unambiguous, fully covered source attempt with a verified parent'

        if reason:
            unresolved.append({'namespace': metadata['namespace'], 'name': metadata['name'], 'reason': reason})
            continue

        primary = sources[-1]['metadata']
        changes = {
            f'{PREFIX}rebuilt-from': primary['uid'],
            f'{PREFIX}rebuilt-from-name': primary['name'],
            f'{PREFIX}recovery-lineage': 'resolved',
        }
        if len(sources) > 1:
            changes[f'{PREFIX}rebuilt-from-uids'] = json.dumps([source['metadata']['uid'] for source in sources])
        # A batched historical recovery can span several parents. Retry edges
        # express that relationship without inventing one parent for the batch.
        covered = set().union(*(operator_nvrs(source) for source in sources))
        plans.append(
            {
                'namespace': metadata['namespace'],
                'name': metadata['name'],
                'uid': metadata['uid'],
                'annotations': changes,
                'sources': [source['metadata']['name'] for source in sources],
                'covered_nvrs': sorted(covered),
                'unmatched_nvrs': sorted(workload - covered),
            }
        )
    return plans, unresolved
