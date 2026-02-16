import asyncio
import concurrent.futures
import datetime
import json
import os
import re
from collections import deque
from itertools import chain
from multiprocessing import cpu_count
from multiprocessing.dummy import Pool as ThreadPool
from sys import getsizeof, stderr
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, cast

import click
import yaml
from artcommonlib import exectools
from artcommonlib.build_visibility import get_build_system
from artcommonlib.constants import GOLANG_BUILDER_IMAGE_NAME, GOLANG_RPM_PACKAGE_NAME
from artcommonlib.format_util import green_prefix, green_print, red_prefix
from artcommonlib.konflux.konflux_build_record import Engine, KonfluxBuildRecord
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.logutil import get_logger
from artcommonlib.util import extract_related_images_from_fbc
from errata_tool import Erratum

from elliottlib import brew, constants
from elliottlib.exceptions import BrewBuildException

# -----------------------------------------------------------------------------
# Constants and defaults
# -----------------------------------------------------------------------------
default_release_date = datetime.datetime(1970, 1, 1, 0, 0)
now = datetime.datetime.now()
YMD = '%Y-%b-%d'
LOGGER = get_logger(__name__)
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)


def exit_unauthenticated():
    """Standard response when an API call returns 'unauthenticated' (401)"""
    red_prefix("Error Unauthenticated: ")
    click.echo("401 - user is not authenticated, are you sure you have a kerberos ticket?")
    exit(1)


def exit_unauthorized():
    """Standard response when an API call returns 'unauthorized' (403)"""
    red_prefix("Error Unauthorized: ")
    click.echo("403 - user is authenticated, but unauthorized to perform this action")
    exit(1)


def ensure_erratatool_auth():
    """Test (cheaply) that we at least have authentication to erratatool"""
    Erratum(errata_id=1)


def validate_release_date(ctx, param, value):
    """Ensures dates are provided in the correct format"""
    if value is None:
        return None
    try:
        release_date = datetime.datetime.strptime(value, YMD)
        if release_date == default_release_date:
            # Default date, nothing special to note
            pass
        else:
            # User provided date passed validation, they deserve a
            # hearty thumbs-up!
            green_prefix("User provided release date: ")
            click.echo("{} - Validated".format(release_date.strftime(YMD)))
        return value
    except ValueError:
        raise click.BadParameter('Release date (--date) must be in YYYY-Mon-DD format')


def validate_email_address(ctx, param, value):
    """Ensure that email addresses provided are valid email strings"""
    # Really just check to match /^[^@]+@[^@]+\.[^@]+$/
    email_re = re.compile(r'^[^@ ]+@[^@ ]+\.[^@ ]+$')
    if not email_re.match(value):
        raise click.BadParameter("Invalid email address for {}: {}".format(param, value))

    return value


def pbar_header(msg_prefix='', msg='', seq=[], char='*', file=None):
    """Generate a progress bar header for a given iterable or
    sequence. The given sequence must have a countable length. A bar of
    `char` characters is printed between square brackets.

        :param string msg_prefix: Header text to print in heavy green text
        :param string msg: Header text to print in the default char face
        :param sequence seq: A sequence (iterable) to size the progress
        bar against
        :param str char: The character to use when drawing the progress
        :param file: the file to print the progress. None means stdout.
        bar

    For example:

        pbar_header("Foo: ", "bar", seq=[None, None, None], char='-')

    would produce:

        Foo: bar
        [---]

    where 'Foo: ' is printed using green_prefix() and 'bar' is in the
    default console fg color and weight.

    TODO: This would make a nice context wrapper.

    """
    green_prefix(msg_prefix, file=file)
    click.echo(msg, file=file)
    click.echo("[" + (char * len(seq)) + "]", file=file)


def progress_func(func, char='*', file=None):
    """Use to wrap functions called in parallel. Prints a character for
    each function call.

        :param lambda-function func: A 'lambda wrapped' function to call
        after printing a progress character
        :param str char: The character (or multi-char string, if you
        really wanted to) to print before calling `func`
        :param file: the file to print the progress. None means stdout.

        Usage examples:
          * See find-builds command
    """
    click.secho(char, fg='green', nl=False, file=file)
    return func()


def parallel_results_with_progress(inputs, func, file=None):
    """Run a function against a list of inputs with a progress bar

    :param sequence inputs : A sequence of items to iterate over in parallel
    :param lambda-function func: A lambda function to call with one arg to process

    Usage examples:
      * See find-builds command

        candidate_build_infos = parallel_results_with_progress(
            candidate_builds,
            lambda build: build.get_latest_build_info()
        )

    Example output:
    [****************]

    """
    click.secho('[', nl=False, file=file)
    pool = ThreadPool(cpu_count())
    results = pool.map(lambda it: progress_func(lambda: func(it), file=file), inputs)

    # Wait for results
    pool.close()
    pool.join()
    click.echo(']', file=file)

    return results


def get_release_version(pv):
    """known formats of product_version:
    - OSE-4.1-RHEL-8
    - RHEL-7-OSE-4.1
    - ...-FOR-POWER-LE and similar suffixes we probably no longer need
    - OSE-IRONIC-4.11-RHEL-8

    this will break and need fixing if we introduce more.
    """
    return re.search(r'OSE-(IRONIC-)?(\d+\.\d+)', pv).groups()[1]


def convert_remote_git_to_https(source):
    """
    Accepts a source git URL in ssh or https format and return it in a normalized
    https format:
        - https protocol
        - no trailing /
    :param source: Git remote
    :return: Normalized https git URL
    """
    url = re.sub(
        pattern=r'[^@]+@([^:/]+)[:/]([^\.]+)',
        repl='https://\\1/\\2',
        string=source.strip(),
    )
    return re.sub(string=url, pattern=r'\.git$', repl='').rstrip('/')


def minor_version_tuple(bz_target):
    """
    Turns '4.5' or '4.5.z' or '4.5.0' into numeric (4, 5)
    Assume the target version begins with numbers 'x.y' - explode otherwise

    :param bz_target: Target version string like "4.5.0"
    :return: A tuple like (4, 5)
    """
    if bz_target == '---':
        return 0, 0

    match = re.match(r'^(\d+).(\d+)(.0|.z)?$', bz_target)
    return int(match.groups()[0]), int(match.groups()[1])


def get_component_by_delivery_repo(runtime, delivery_repo_name: str) -> Optional[str]:
    """Get the component name from the delivery repo name
    For example, "openshift4/ose-sriov-network-device-plugin-rhel9" -> "sriov-network-device-plugin-container"
    """
    if not runtime.image_metas():
        raise ValueError("No image metas found. Forgot to initialize runtime with mode='images'?")

    # strip off the -rhel{digit} suffix
    def _strip(name: str) -> str:
        return re.sub(r"-rhel\d+$", "", name)

    for image in runtime.image_metas():
        if _strip(delivery_repo_name) in [_strip(r) for r in image.config.delivery.delivery_repo_names]:
            return image.get_component_name()
    return None


def get_golang_version_from_build_log(log):
    # TODO add a test for this
    # Based on below greps:
    # $ grep -m1 -o -E '(go-toolset-1[^ ]*|golang-(bin-|))[0-9]+.[0-9]+.[0-9]+[^ ]*' ./3.11/*.log | sed 's/:.*\([0-9]\+\.[0-9]\+\.[0-9]\+.*\)/: \1/'
    # $ grep -m1 -o -E '(go-toolset-1[^ ]*|golang.*module[^ ]*).*[0-9]+.[0-9]+.[0-9]+[^ ]*' ./4.5/*.log | sed 's/\:.*\([^a-z][0-9]\+\.[0-9]\+\.[0-9]\+[^ ]*\)/:\ \1/'
    m = re.search(r'(go-toolset-1\S+-golang\S+|golang-bin).*[0-9]+\.[0-9]+\.[0-9]+[^\s]*', log)
    s = m.group(0).split()

    # if we get a result like:
    #   "golang-bin               x86_64  1.14.12-1.module+el8.3.0+8784+380394dc"
    if len(s) > 1:
        go_version = s[-1]
    else:
        # if we get a result like (more common for RHEL7 build logs):
        #   "go-toolset-1.14-golang-1.14.9-2.el7.x86_64"
        go_version = s[0]
        # extract version and release
        m = re.search(r'[0-9a-zA-z\.]+-[0-9a-zA-z\.]+$', s[0])
        s = m.group(0).split()
        if len(s) == 1:
            go_version = s[0]

    return go_version


def isolate_el_version_in_brew_tag(tag: str) -> Optional[int]:
    """
    Given a brew tag (target) name, determines whether is contains
    a RHEL version. If it does, it returns the version value.
    If it is not found, None is returned.
    """
    el_version_match = re.search(r"rhel-(\d+)", tag)
    return int(el_version_match[1]) if el_version_match else None


def split_nvr_epoch(nvre):
    """Split nvre to N-V-R and E.

    @param nvre: E:N-V-R or N-V-R:E string
    @type nvre: str
    @return: (N-V-R, E)
    @rtype: (str, str)
    """

    if ":" in nvre:
        if nvre.count(":") != 1:
            raise ValueError("Invalid NVRE: %s" % nvre)

        nvr, epoch = nvre.rsplit(":", 1)
        if "-" in epoch:
            if "-" not in nvr:
                # switch nvr with epoch
                nvr, epoch = epoch, nvr
            else:
                # it's probably N-E:V-R format, handle it after the split
                nvr, epoch = nvre, ""
    else:
        nvr, epoch = nvre, ""

    return (nvr, epoch)


def parse_nvr(nvre):
    """Split N-V-R into a dictionary.

    @param nvre: N-V-R:E, E:N-V-R or N-E:V-R string
    @type nvre: str
    @return: {name, version, release, epoch}
    @rtype: dict
    """

    if "/" in nvre:
        nvre = nvre.split("/")[-1]

    nvr, epoch = split_nvr_epoch(nvre)

    nvr_parts = nvr.rsplit("-", 2)
    if len(nvr_parts) != 3:
        raise ValueError("Invalid NVR: %s" % nvr)

    # parse E:V
    if epoch == "" and ":" in nvr_parts[1]:
        epoch, nvr_parts[1] = nvr_parts[1].split(":", 1)

    # check if epoch is empty or numeric
    if epoch != "":
        try:
            int(epoch)
        except ValueError:
            raise ValueError("Invalid epoch '%s' in '%s'" % (epoch, nvr))

    result = dict(zip(["name", "version", "release"], nvr_parts))
    result["epoch"] = epoch
    return result


def to_nvre(build_record: Dict):
    """
    From a build record object (such as an entry returned by listTagged),
    returns the full nvre in the form n-v-r:E.
    """
    nvr = build_record['nvr']
    if 'epoch' in build_record and build_record["epoch"] and build_record["epoch"] != 'None':
        return f'{nvr}:{build_record["epoch"]}'
    return nvr


def strip_epoch(nvr: str):
    """
    If an NVR string is N-V-R:E, returns only the NVR portion. Otherwise
    returns NVR exactly as-is.
    """
    return nvr.split(':')[0]


# https://code.activestate.com/recipes/577504/
def total_size(o, handlers={}, verbose=False):
    """Returns the approximate memory footprint an object and all of its contents.

    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, deque, dict, set and frozenset.
    To search other containers, add handlers to iterate over their contents:

        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}

    """

    def dict_handler(d):
        return chain.from_iterable(d.items())

    all_handlers = {
        tuple: iter,
        list: iter,
        deque: iter,
        dict: dict_handler,
        set: iter,
        frozenset: iter,
    }
    all_handlers.update(handlers)  # user handlers take precedence
    seen = set()  # track which object id's have already been seen
    default_size = getsizeof(0)  # estimate sizeof object without __sizeof__

    def sizeof(o):
        if id(o) in seen:  # do not double count the same object
            return 0
        seen.add(id(o))
        s = getsizeof(o, default_size)

        if verbose:
            print(s, type(o), repr(o), file=stderr)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)


def isolate_timestamp_in_release(release: str) -> Optional[str]:
    """
    Given a release field, determines whether is contains
    a timestamp. If it does, it returns the timestamp.
    If it is not found, None is returned.
    """
    match = re.search(r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})", release)  # yyyyMMddHHmm
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        hour = int(match.group(4))
        minute = int(match.group(5))
        if year >= 2000 and month >= 1 and month <= 12 and day >= 1 and day <= 31 and hour <= 23 and minute <= 59:
            return match.group(0)
    return None


def get_golang_container_nvrs(nvrs: List[Tuple[str, str, str]], logger) -> Dict[str, Dict[str, str]]:
    """
    :param nvrs: a list of tuples containing (name, version, release) in order
    :param logger: logger

    :return: a dict mapping go version string to a list of nvrs built from that go version
    """
    # quickly determine build system from given nvrs using build_visibility suffix
    build_system = None
    golang_builder_nvrs = []
    for nvr in nvrs:
        # release is something like 202508201021.p2.gb7cfbf8.assembly.stream.el8
        # we just want the p2 part
        try:
            nvr_build_system = get_build_system(nvr[2].split('.')[1])
        except ValueError:
            # golang builder NVRs are a special case
            # They historically did not have build visibility suffix
            # so for backward compatibility we will try fetching nvrs from both brew and konflux
            if GOLANG_BUILDER_IMAGE_NAME not in nvr[0]:
                raise
            logger.debug(f'Could not determine build system from release {nvr[2]} for {nvr}')
            golang_builder_nvrs.append(nvr)
            continue

        if build_system is not None:
            assert build_system == nvr_build_system, (
                f'Build system mismatch for {nvr}: {build_system} != {nvr_build_system}'
            )
        else:
            build_system = nvr_build_system

    if build_system is None and golang_builder_nvrs:
        try:
            golang_map = get_golang_container_nvrs_konflux(golang_builder_nvrs, logger)
        except Exception as e:
            logger.info(f'Failed to find golang builder nvrs in ART build DB: {e}')
            golang_map = get_golang_container_nvrs_brew(golang_builder_nvrs, logger)
        return golang_map

    if build_system == 'brew':
        return get_golang_container_nvrs_brew(nvrs, logger)
    elif build_system == 'konflux':
        return get_golang_container_nvrs_konflux(nvrs, logger)


def get_golang_container_nvrs_brew(nvrs: List[Tuple[str, str, str]], logger) -> Dict[str, Dict[str, str]]:
    """
    :param nvrs: a list of tuples containing (name, version, release) in order
    :param logger: logger

    :return: a dict mapping go version string to a list of nvrs built from that go version
    """
    all_build_objs = brew.get_build_objects(['{}-{}-{}'.format(*n) for n in nvrs])
    go_nvr_map = {}
    for build, nvr_param in zip(all_build_objs, nvrs):
        if not build:
            raise ValueError(f'Brew build object not found for {"-".join(nvr_param)}.')
        go_version = None
        try:
            nvr = (build['name'], build['version'], build['release'])
        except TypeError:
            logger.error(f'Error parsing {build}')
            raise
        name = nvr[0]
        if name == 'openshift-golang-builder-container' or 'go-toolset' in name:
            go_version = golang_builder_version(nvr, logger)
            if not go_version:
                raise ValueError(f'Cannot find go version for {name}')
            if go_version not in go_nvr_map:
                go_nvr_map[go_version] = set()
            go_nvr_map[go_version].add(nvr)
            continue

        try:
            parents = build['extra']['image']['parent_image_builds']
        except KeyError:
            logger.debug(f'Could not find parent build image for {nvr}')
            continue

        for p, pinfo in parents.items():
            # registry-proxy.engineering.redhat.com/rh-osbs/openshift-golang-builder:v1.20.10-202310161945.el9.gbbb66ea
            if 'openshift-golang-builder' in p or 'go-toolset' in p:
                go_version = pinfo.get('nvr')
                break

        if not go_version:
            logger.debug(f'Could not find parent Go builder image for {nvr}')
            continue

        if go_version not in go_nvr_map:
            go_nvr_map[go_version] = set()
        go_nvr_map[go_version].add(nvr)
    return go_nvr_map


def get_golang_container_nvrs_konflux(nvrs: List[Tuple[str, str, str]], logger) -> dict[str, set[tuple[str, str, str]]]:
    """
    :param nvrs: a list of tuples containing (name, version, release) in order
    :param logger: logger

    :return: a dict mapping go version string to a set of nvrs built from that go version
    """
    konflux_db = KonfluxDb()
    konflux_db.bind(KonfluxBuildRecord)

    logger.info(f'Getting build records for {len(nvrs)} nvrs from KonfluxDB')

    # Need installed_packages column for golang builder image detection
    all_build_objs = _executor.submit(
        lambda: asyncio.run(konflux_db.get_build_records_by_nvrs(['{}-{}-{}'.format(*n) for n in nvrs]))
    ).result()

    return get_golang_container_nvrs_for_konflux_record(cast(list[KonfluxBuildRecord], all_build_objs), logger)


def get_golang_container_nvrs_for_konflux_record(
    build_objs: Iterable[KonfluxBuildRecord], logger
) -> dict[str, set[tuple[str, str, str]]]:
    """
    :param build_objs: a list of konflux build records
    :param logger: logger

    :return: a dict mapping go version string to a set of nvrs built from that go version
    """
    go_nvr_map: dict[str, set[tuple[str, str, str]]] = {}
    for build in build_objs:
        nvr_dict = parse_nvr(build.nvr)
        nvr = (nvr_dict['name'], nvr_dict['version'], nvr_dict['release'])
        go_version = None
        # The golang-builder container needs special handling,
        # because we need to look at included golang rpms instead of parent images.
        # Unlike `get_golang_container_nvrs_brew`, we don't accept 'go-toolset*' containers
        # because they don't exist in KonfluxDB.
        if build.name == GOLANG_BUILDER_IMAGE_NAME:
            go_package = next(
                (
                    pkg_nvr
                    for pkg in build.installed_packages
                    if (pkg_nvr := parse_nvr(pkg))['name'] == GOLANG_RPM_PACKAGE_NAME
                ),
                None,
            )
            if not go_package:
                raise ValueError(f'Cannot find go version for {build.nvr}')
            go_version = f"{go_package['version']}-{go_package['release']}"
            if go_version not in go_nvr_map:
                go_nvr_map[go_version] = set()
            go_nvr_map[go_version].add(nvr)
            continue

        parents = build.parent_images
        for p in parents:
            # parent_images contains NVRs, or pullspecs for external images where NVR couldn't be determined.
            # NVRs never contain '/', so we can distinguish them from pullspecs.
            if '/' not in p:
                # This is an NVR - look for golang builder (e.g. openshift-golang-builder-container-v1.24.4-...)
                if p.startswith(f'{constants.GOLANG_BUILDER_CVE_COMPONENT}-'):
                    go_version = p
                    break
            else:
                # This is a pullspec (NVR extraction failed) - parse it to construct NVR
                spec = p.split('/')[-1]
                if spec.startswith('openshift-golang-builder:'):
                    go_version = spec.replace(':', '-container-')
                    break
                elif spec.startswith('art-images:golang-builder-'):
                    go_version = spec.replace(
                        'art-images:golang-builder-', f'{constants.GOLANG_BUILDER_CVE_COMPONENT}-'
                    )
                    break

        if not go_version:
            logger.debug(f'Could not find parent Go builder image for {nvr}')
            continue

        if go_version not in go_nvr_map:
            go_nvr_map[go_version] = set()
        go_nvr_map[go_version].add(nvr)
    logger.info(f'Found {len(go_nvr_map)} golang builder nvrs: {sorted(go_nvr_map.keys())}')
    return go_nvr_map


def golang_builder_version(nvr, logger):
    go_version = None
    try:
        build_log = brew.get_nvr_arch_log(*nvr)
    except BrewBuildException:
        logger.debug(f'Could not brew log for {nvr}')
    else:
        try:
            go_version = get_golang_version_from_build_log(build_log)
        except AttributeError:
            logger.debug(f'Could not find Go version in build log for {nvr}')
    return go_version


def get_golang_rpm_nvrs(nvrs, logger):
    go_nvr_map = {}
    for nvr in nvrs:
        go_version = None
        # what we build in brew as openshift
        # is called openshift-hyperkube in rhcos
        if nvr[0] == 'openshift-hyperkube':
            n = 'openshift'
            nvr = (n, nvr[1], nvr[2])

        try:
            root_log = brew.get_nvr_root_log(*nvr)
        except BrewBuildException:
            logger.debug(f'Could not find brew log for {nvr}')
        else:
            try:
                go_version = get_golang_version_from_build_log(root_log)
            except AttributeError:
                logger.debug(f'Could not find go version in root log for {nvr}')

        if not go_version:
            continue

        if go_version not in go_nvr_map:
            go_nvr_map[go_version] = set()
        go_nvr_map[go_version].add(nvr)
    return go_nvr_map


def pretty_print_nvrs_go(go_nvr_map, report=False):
    for go_version in sorted(go_nvr_map.keys()):
        nvrs = go_nvr_map[go_version]
        if report:
            green_print(f'Builds built with {go_version}: {len(nvrs)}')
        else:
            green_print(f'* Following nvrs ({len(nvrs)}) are built with {go_version}:')
            for nvr in sorted(nvrs):
                pretty_nvr = '-'.join(nvr)
                print(pretty_nvr)


def pretty_print_nvrs_go_json(go_nvr_map, report=False):
    out = []
    for go_nvr in go_nvr_map.keys():
        if report:
            out.append({"builder_nvr": go_nvr, "building_count": len(go_nvr_map[go_nvr])})
        else:
            nvrs = ['-'.join(n) for n in sorted(go_nvr_map[go_nvr])]
            out.append({"builder_nvr": go_nvr, "building_nvrs": nvrs})
    # sort
    if report:
        out = sorted(out, key=lambda x: x['building_count'], reverse=True)
    else:
        out = sorted(out, key=lambda x: len(x['building_nvrs']), reverse=True)
    print(json.dumps(out, indent=4))


def chunk(a_sequence: Sequence[Any], chunk_size: int) -> List[Any]:
    for i in range(0, len(a_sequence), chunk_size):
        yield a_sequence[i : i + chunk_size]


def all_same(items: Iterable[Any]):
    """Determine if all items are the same"""
    it = iter(items)
    first = next(it, None)
    return all(x == first for x in it)


async def get_nvrs_from_release(pullspec_or_imagestream, rhcos_images, logger=None):
    """
    Get all payload NVRs from a release pullspec or imagestream.
    """

    def log(msg):
        if logger:
            logger.info(msg)

    # a pullspec looks like this: quay.io/openshift-release-dev/ocp-release:4.17.14-x86_64
    # an imagestream looks like this: 4.17-art-assembly-4.17.14
    if '@' in pullspec_or_imagestream or ':' in pullspec_or_imagestream:
        is_pullspec = True
    else:
        is_pullspec = False

    all_payload_nvrs = {}
    log("Fetching release info...")
    if is_pullspec:
        rc, stdout, stderr = await exectools.cmd_gather_async(
            f'oc adm release info -o json -n ocp {pullspec_or_imagestream}'
        )
        tags = json.loads(stdout)['references']['spec']['tags']
    else:  # it is an imagestream
        # get image_stream and name_space out of pullspec_or_imagestream
        image_stream = pullspec_or_imagestream.split("/")[-1]  # Get the part after /
        name_space = pullspec_or_imagestream.split("/")[0]  # Get the part before /

        rc, stdout, stderr = await exectools.cmd_gather_async(f'oc -o json -n {name_space} get is/{image_stream}')
        tags = json.loads(stdout)['spec']['tags']

    log("Looping over payload images...")
    log(f"{len(tags)} images to check")
    cmds = [['oc', 'image', 'info', '-o', 'json', tag['from']['name']] for tag in tags]

    log("Querying image infos...")
    cmd_results = await asyncio.gather(*[exectools.cmd_gather_async(cmd) for cmd in cmds])

    for image, cmd, cmd_result in zip(tags, cmds, cmd_results):
        image_name = image['name']
        rc, stdout, stderr = cmd_result
        if rc != 0:
            # Probably no point in continuing.. can't contact brew?
            msg = f"Unable to run oc image info: cmd={cmd!r}, out={stdout}  ; err={stderr}"
            raise RuntimeError(msg)

        image_info = json.loads(stdout)
        labels = image_info['config']['config']['Labels']

        # RHCOS images are not built in brew, so skip them
        if image_name in rhcos_images:
            log(f"Skipping rhcos image {image_name}")
            continue

        if not labels or any(i not in labels for i in ['version', 'release', 'com.redhat.component']):
            msg = f"For image {image_name} expected labels don't exist"
            raise ValueError(msg)
        component = labels['com.redhat.component']
        v = labels['version']
        r = labels['release']
        all_payload_nvrs[component] = (v, r)
    return all_payload_nvrs


def get_common_advisory_template(runtime):
    out = runtime.get_file_from_branch(branch="main", filename="config/advisory_templates.yml")
    return yaml.safe_load(out)


def get_advisory_boilerplate(runtime, et_data, art_advisory_key, errata_type):
    # rhsa/rhba keys are in lower case: https://github.com/openshift-eng/ocp-build-data/blob/main/config/advisory_templates.yml#L3
    # Also if advisory type is RHEA, use the RHBA advisory template
    errata_type = errata_type.lower()
    errata_type = "rhba" if errata_type == "rhea" else errata_type
    et_data["errata_type"] = errata_type

    # Group level overrides common config present in openshift-eng/ocp-build-data main branch
    # Try to get the group level boilerplate first
    boilerplate = et_data.get("boilerplates", {})
    if not boilerplate:
        # If group level is missing, use common one in openshift#main branch
        common_advisory_template = get_common_advisory_template(runtime)
        boilerplate = common_advisory_template.get("boilerplates", {})

    if not boilerplate:
        raise ValueError("`boilerplates` is required in erratatool.yml")
    if art_advisory_key not in boilerplate:
        if art_advisory_key == "rhcos" and "image" in boilerplate:
            # For backwards compatibility with older versions of erratatool.yml, i.e. rhcos key does not exist
            art_advisory_key = "image"
        else:
            raise ValueError(f"Boilerplate {art_advisory_key} not found in erratatool.yml")

    # Get the boilerplate for a type of errata and advisory type
    try:
        advisory_boilerplate = boilerplate[art_advisory_key][errata_type]
    except KeyError:
        # For backwards compatibility with older versions of erratatool.yml, i.e. rhsa, rhba keys does not exist
        advisory_boilerplate = boilerplate[art_advisory_key]

    return advisory_boilerplate


async def extract_nvrs_from_fbc(fbc_pullspec: str, product: str) -> list[str]:
    """
    Extract NVRs from FBC image using ORAS workflow.
    Reuses the logic from elliott snapshot CLI.
    """
    logger = get_logger(__name__)
    logger.info(f"Extracting NVRs from FBC image: {fbc_pullspec}")

    related_images = await extract_related_images_from_fbc(fbc_pullspec, product)

    # Transform image URLs and extract NVRs in parallel
    logger.info(f"Extracting NVRs from {len(related_images)} related images in parallel...")

    async def extract_nvr_from_image(image_url: str) -> tuple[str, str, str]:
        """Extract NVR from a single image. Returns (image_url, nvr_or_empty, error_msg_or_empty)"""
        try:
            logger.debug(f"Running oc image info for: {image_url}")
            oc_cmd = ['oc', 'image', 'info', image_url, '--filter-by-os', 'amd64', '-o', 'json']

            # Add registry config for authentication if available
            konflux_art_images_auth_file = os.getenv("KONFLUX_ART_IMAGES_AUTH_FILE")
            if konflux_art_images_auth_file:
                oc_cmd.extend(['--registry-config', konflux_art_images_auth_file])

            _, image_info_output, _ = await exectools.cmd_gather_async(oc_cmd)
            image_info = json.loads(image_info_output)

            # Extract labels
            labels = image_info.get('config', {}).get('config', {}).get('Labels', {})
            component = labels.get('com.redhat.component')
            version = labels.get('version')
            release = labels.get('release')

            logger.debug(
                f"Image labels for {image_url} - component: {component}, version: {version}, release: {release}"
            )

            if component and version and release:
                nvr = f"{component}-{version}-{release}"
                logger.debug(f"✓ Extracted NVR from {image_url}: {nvr}")
                return (image_url, nvr, "")
            else:
                missing_labels = []
                if not component:
                    missing_labels.append('com.redhat.component')
                if not version:
                    missing_labels.append('version')
                if not release:
                    missing_labels.append('release')
                error_msg = f"Missing required labels: {', '.join(missing_labels)}"
                logger.debug(f"✗ {error_msg} for {image_url}")
                return (image_url, "", error_msg)

        except Exception as e:
            error_msg = f"Failed to get image info: {e}"
            logger.debug(f"✗ {error_msg} for {image_url}")
            return (image_url, "", error_msg)

    # Run all oc image info commands in parallel
    tasks = [extract_nvr_from_image(image_url) for image_url in related_images]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results
    nvrs = []
    failed_images = []

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            failed_images.append(f"{related_images[i]} (exception: {result})")
            continue

        image_url, nvr, error_msg = result
        if nvr:
            nvrs.append(nvr)
            logger.info(f"✓ Extracted NVR: {nvr}")
        else:
            failed_images.append(f"{image_url} ({error_msg})")
            logger.warning(f"✗ Failed to extract NVR from {image_url}: {error_msg}")

    logger.info(f"NVR extraction completed: {len(nvrs)} successful, {len(failed_images)} failed")
    if failed_images:
        logger.warning(f"Failed images: {failed_images}")

    if not nvrs:
        logger.error("No NVRs could be extracted from any images")
        logger.error("This could be due to:")
        logger.error("1. Images not being accessible (authentication required)")
        logger.error("2. Incorrect image URL transformation")
        logger.error("3. Images missing required labels")
        logger.error("4. Network connectivity issues")
        raise RuntimeError(f"Failed to extract NVRs from all {len(related_images)} images. Check logs for details.")

    logger.info(f"Extracted {len(nvrs)} NVRs from FBC image")
    return nvrs
