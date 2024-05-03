import click
import sys
import traceback

from artcommonlib import exectools
from artcommonlib.model import Missing
from doozerlib import Runtime
from doozerlib.cli import cli, pass_runtime
from typing import Optional, Tuple
from doozerlib.olm.bundle import OLMBundle


@cli.command('olm-bundle:list-olm-operators', short_help='List all images that are OLM operators')
@pass_runtime
def list_olm_operators(runtime: Runtime):
    """
    Example:
    $ doozer --group openshift-4.5 olm-bundle:list-olm-operators
    """
    runtime.initialize(clone_distgits=False)

    for image in runtime.image_metas():
        if image.enabled and image.config['update-csv'] is not Missing:
            print(image.get_component_name())


@cli.command("olm-bundle:print", short_help="Print data for each operator",
             context_settings=dict(
                 ignore_unknown_options=True,  # permit patterns starting with dash; allows printing yaml lists
             ))
@click.option('--skip-missing', default=False, is_flag=True,
              help='If no build has been performed, skip printing pattern that would require it')
@click.argument("pattern", default="{component}", nargs=1)
@pass_runtime
def olm_bundles_print(runtime: Runtime, skip_missing, pattern: Optional[str]):
    """
    Prints data from each distgit. The pattern specified should be a string
    with replacement fields:

    \b
    {distgit_key} - The distgit key of the operator
    {component} - The component identified in the Dockerfile
    {nvr} - NVR of latest operator build
    {bundle_component} - The operator's bundle component name
    {paired_bundle_nvr} - NVR of bundle associated with the latest operator NVR (may not exist)
    {paired_bundle_pullspec} - The pullspec for the bundle associated with the latest operator NVR (may not exist)
    {bundle_nvr} - NVR of latest operator bundle build
    {bundle_pullspec} - The pullspec for the latest build
    {lf} - Line feed

    If pattern contains no braces, it will be wrapped with them automatically. For example:
    "component" will be treated as "{component}"
    """

    runtime.initialize(clone_distgits=False)

    # If user omitted braces, add them.
    if "{" not in pattern:
        pattern = "{%s}" % pattern.strip()

    for image in runtime.ordered_image_metas():
        if not image.enabled or image.config['update-csv'] is Missing:
            continue
        olm_bundle = OLMBundle(runtime, dry_run=False)

        s = pattern
        s = s.replace("{lf}", "\n")
        s = s.replace("{distgit_key}", image.distgit_key)
        s = s.replace("{component}", image.get_component_name())

        build_info = image.get_latest_build(default=None)
        if build_info is None:
            if "{" in s:
                if skip_missing:
                    runtime.logger.warning(
                        f'No build has been performed for {image.distgit_key} by {s} requires it; skipping')
                    continue
                else:
                    raise IOError(
                        f"Fields remaining in pattern, but no build was found for {image.distgit_key} with which to populate those fields: {s}")
        else:
            nvr = build_info['nvr']
            s = s.replace('{nvr}', nvr)
            olm_bundle.get_operator_buildinfo(nvr=nvr)  # Populate the object for the operator we are interested in.
            s = s.replace('{bundle_component}', olm_bundle.bundle_brew_component)
            bundle_image_name = olm_bundle.bundle_image_name

            if '{paired_' in s:
                # Paired bundle values must correspond exactly to the latest operator NVR.
                paired_bundle_nvr = olm_bundle.find_bundle_for(nvr)
                if not paired_bundle_nvr:
                    paired_bundle_nvr = 'None'  # Doesn't exist
                    paired_pullspec = 'None'
                else:
                    paired_version_release = paired_bundle_nvr[len(olm_bundle.bundle_brew_component) + 1:]
                    paired_pullspec = runtime.resolve_brew_image_url(f'{bundle_image_name}:{paired_version_release}')
                s = s.replace('{paired_bundle_nvr}', paired_bundle_nvr)
                s = s.replace('{paired_bundle_pullspec}', paired_pullspec)

            if '{bundle_' in s:
                # Unpaired is just whatever bundle build was most recent
                build = olm_bundle.get_latest_bundle_build()
                if not build:
                    bundle_nvr = 'None'
                    bundle_pullspec = 'None'
                else:
                    bundle_nvr = build['nvr']
                    version_release = bundle_nvr[len(olm_bundle.bundle_brew_component) + 1:]
                    # Build pullspec like: registry-proxy.engineering.redhat.com/rh-osbs/openshift-ose-clusterresourceoverride-operator-bundle:v4.7.0.202012082225.p0-1
                    bundle_pullspec = runtime.resolve_brew_image_url(f'{bundle_image_name}:{version_release}')

                s = s.replace('{bundle_nvr}', bundle_nvr)
                s = s.replace('{bundle_pullspec}', bundle_pullspec)

        if "{" in s:
            raise IOError("Unrecognized fields remaining in pattern: %s" % s)

        click.echo(s)


@cli.command('olm-bundle:rebase', short_help='Update bundle distgit repo with manifests from given operators')
@click.argument('operator_nvrs', nargs=-1, required=False)
@click.option('--dry-run', default=False, is_flag=True, help='Do not push to distgit.')
@pass_runtime
def rebase_olm_bundle(runtime: Runtime, operator_nvrs: Tuple[str, ...], dry_run: bool):
    """
    Examples:
    $ doozer --group openshift-4.9 --assembly art3171 olm-bundle:rebase
    $ doozer --group openshift-4.9 --assembly art3171 -i cluster-nfd-operator,ptp-operator olm-bundle:rebase
    $ doozer --group openshift-4.2 olm-bundle:rebase \
        sriov-network-operator-container-v4.2.30-202004200449 \
        elasticsearch-operator-container-v4.2.30-202004240858 \
        cluster-logging-operator-container-v4.2.30-202004240858
    """
    runtime.initialize(clone_distgits=False)
    if not operator_nvrs:
        # If this verb is run without operator NVRs, query Brew for all operator builds selected by the assembly
        operator_metas = [meta for meta in runtime.ordered_image_metas() if
                          meta.enabled and meta.config['update-csv'] is not Missing]
        results = exectools.parallel_exec(lambda meta, _: meta.get_latest_build(), operator_metas)
        operator_builds = results.get()
    else:
        operator_builds = list(operator_nvrs)
    exectools.parallel_exec(lambda operator, _: OLMBundle(runtime, dry_run).rebase(operator), operator_builds).get()


@cli.command('olm-bundle:build', short_help='Build bundle containers of given operators')
@click.argument('operator_names', nargs=-1, required=False)
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but print what would be done.')
@pass_runtime
def build_olm_bundle(runtime: Runtime, operator_names: Tuple[str, ...], dry_run: bool):
    """
    Example:
    $ doozer --group openshift-4.9 --assembly art3171 olm-bundle:build
    $ doozer --group openshift-4.9 --assembly art3171 -i cluster-nfd-operator,ptp-operator olm-bundle:build
    $ doozer --group openshift-4.2 olm-bundle:build \
        sriov-network-operator \
        elasticsearch-operator \
        cluster-logging-operator
    """
    runtime.initialize(clone_distgits=False)
    if not operator_names:
        operator_names = [meta.name for meta in runtime.ordered_image_metas() if
                          meta.enabled and meta.config['update-csv'] is not Missing]

    def _build_bundle(operator):
        record = {
            'status': -1,
            "task_id": "",
            "task_url": "",
            "operator_distgit": operator,
            "bundle_nvr": "",
            "message": "Unknown failure",
        }
        try:
            olm_bundle = OLMBundle(runtime, dry_run)
            runtime.logger.info("%s - Building bundle distgit repo", operator)
            task_id, task_url, bundle_nvr = olm_bundle.build(operator)
            record['status'] = 0
            record['message'] = 'Success'
            record['task_id'] = task_id
            record['task_url'] = task_url
            record['bundle_nvr'] = bundle_nvr
        except Exception as err:
            traceback.print_exc()
            runtime.logger.error('Error during build for: {}'.format(operator))
            record['message'] = str(err)
        finally:
            runtime.add_record("build_olm_bundle", **record)
            return record

    results = exectools.parallel_exec(lambda operator, _: _build_bundle(operator), operator_names).get()

    for record in results:
        if record['status'] == 0:
            runtime.logger.info('Successfully built %s', record['bundle_nvr'])
            click.echo(record['bundle_nvr'])
        else:
            runtime.logger.error('Error building bundle for %s: %s', record['operator_nvr'], record['message'])

    rc = 0 if all(map(lambda i: i['status'] == 0, results)) else 1

    if rc:
        runtime.logger.error('One or more bundles failed')

    sys.exit(rc)


@cli.command('olm-bundle:rebase-and-build', short_help='Shortcut for olm-bundle:rebase and olm-bundle:build')
@click.argument('operator_nvrs', nargs=-1, required=False)
@click.option("-f", "--force", required=False, is_flag=True,
              help="Perform a build even if previous bundles for given NVRs already exist")
@click.option('--dry-run', default=False, is_flag=True,
              help='Do not push to distgit or build anything, but print what would be done.')
@pass_runtime
def rebase_and_build_olm_bundle(runtime: Runtime, operator_nvrs: Tuple[str, ...], force: bool, dry_run: bool):
    """Rebase and build operator bundles.

    Run this command with operator NVRs to build bundles for the given operator builds.
    Run this command without operator NVRs to build bundles for operator NVRs selected by the runtime assembly.

    If `--force` option is not specified and there's already a bundle build for given operator builds, this command will do nothing but just print the most recent bundle NVR of that operator.

    Examples:
    $ doozer --group openshift-4.9 --assembly art3171 olm-bundle:rebase-and-build
    $ doozer --group openshift-4.9 --assembly art3171 -i cluster-nfd-operator,ptp-operator olm-bundle:rebase-and-build
    $ doozer --group openshift-4.2 olm-bundle:rebase-and-build \
        sriov-network-operator-container-v4.2.30-202004200449 \
        elasticsearch-operator-container-v4.2.30-202004240858 \
        cluster-logging-operator-container-v4.2.30-202004240858
    """

    runtime.initialize(config_only=True)
    clone_distgits = bool(runtime.group_config.canonical_builders_from_upstream)
    runtime.initialize(clone_distgits=clone_distgits)

    if not operator_nvrs:
        # If this verb is run without operator NVRs, query Brew for all operator builds
        operator_metas = [meta for meta in runtime.ordered_image_metas() if
                          meta.enabled and meta.config['update-csv'] is not Missing]
        results = exectools.parallel_exec(lambda meta, _: meta.get_latest_build(), operator_metas)
        operator_builds = results.get()
    else:
        operator_builds = list(operator_nvrs)

    def rebase_and_build(olm_bundle: OLMBundle):
        record = {
            'status': -1,
            "task_id": "",
            "task_url": "",
            "operator_nvr": "",
            "bundle_nvr": "",
            "message": "Unknown failure",
        }
        operator_nvr = olm_bundle.operator_nvr
        try:
            record['operator_nvr'] = operator_nvr
            if not force:
                runtime.logger.info("%s - Finding most recent bundle build", operator_nvr)
                bundle_nvr = olm_bundle.find_bundle_image()
                if bundle_nvr:
                    runtime.logger.info("%s - Found bundle build %s", operator_nvr, bundle_nvr)
                    record['status'] = 0
                    record['message'] = 'Already built'
                    record['bundle_nvr'] = bundle_nvr
                    return record
                runtime.logger.info("%s - No bundle build found", operator_nvr)
            runtime.logger.info("%s - Rebasing bundle distgit repo", operator_nvr)
            olm_bundle.rebase()
            runtime.logger.info("%s - Building bundle distgit repo", operator_nvr)
            task_id, task_url, bundle_nvr = olm_bundle.build()
            record['status'] = 0
            record['message'] = 'Success'
            record['task_id'] = task_id
            record['task_url'] = task_url
            record['bundle_nvr'] = bundle_nvr
        except Exception as err:
            traceback.print_exc()
            runtime.logger.error('Error during rebase or build for: {}'.format(operator_nvr))
            record['message'] = str(err)
        finally:
            runtime.add_record("build_olm_bundle", **record)
            return record

    olm_bundles = [OLMBundle(runtime, op, dry_run=dry_run) for op in operator_builds]
    get_branches_results = exectools.parallel_exec(lambda bundle, _: bundle.does_bundle_branch_exist(),
                                                   olm_bundles).get()
    if not all([result[0] for result in get_branches_results]):
        runtime.logger.error('One or more bundle branches do not exist: '
                             f'{[result[1] for result in get_branches_results if not result[0]]}. Please create them first.')
        sys.exit(1)

    results = exectools.parallel_exec(lambda bundle, _: rebase_and_build(bundle), olm_bundles).get()

    for record in results:
        if record['status'] == 0:
            runtime.logger.info('Successfully built %s', record['bundle_nvr'])
            click.echo(record['bundle_nvr'])
        else:
            runtime.logger.error('Error building bundle for %s: %s', record['operator_nvr'], record['message'])

    rc = 0 if all(map(lambda i: i['status'] == 0, results)) else 1

    if rc:
        runtime.logger.error('One or more bundles failed')

    sys.exit(rc)
