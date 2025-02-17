import asyncio
import json
import datetime
import re
import click
from collections import deque
from itertools import chain
from multiprocessing import cpu_count
from multiprocessing.dummy import Pool as ThreadPool
from sys import getsizeof, stderr
from typing import Dict, Iterable, List, Optional, Tuple, Sequence, Any

from artcommonlib import exectools
from artcommonlib.format_util import red_prefix, green_prefix, green_print
from elliottlib import brew
from elliottlib.exceptions import BrewBuildException

from errata_tool import Erratum

# -----------------------------------------------------------------------------
# Constants and defaults
# -----------------------------------------------------------------------------
default_release_date = datetime.datetime(1970, 1, 1, 0, 0)
now = datetime.datetime.now()
YMD = '%Y-%b-%d'


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
        raise click.BadParameter(
            "Invalid email address for {}: {}".format(param, value))

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
    results = pool.map(
        lambda it: progress_func(lambda: func(it), file=file),
        inputs)

    # Wait for results
    pool.close()
    pool.join()
    click.echo(']', file=file)

    return results


def get_release_version(pv):
    """ known formats of product_version:
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
    """ Returns the approximate memory footprint an object and all of its contents.

    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, deque, dict, set and frozenset.
    To search other containers, add handlers to iterate over their contents:

        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}

    """
    dict_handler = lambda d: chain.from_iterable(d.items())
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
    all_build_objs = brew.get_build_objects([
        '{}-{}-{}'.format(*n) for n in nvrs
    ])
    go_nvr_map = {}
    for build in all_build_objs:
        go_version = None
        nvr = (build['name'], build['version'], build['release'])
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
        yield a_sequence[i:i + chunk_size]


def all_same(items: Iterable[Any]):
    """ Determine if all items are the same """
    it = iter(items)
    first = next(it, None)
    return all(x == first for x in it)


async def get_nvrs_from_release(pullspec_or_imagestream, rhcos_images, logger=None):
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
            f'oc adm release info -o json -n ocp {pullspec_or_imagestream}')
        tags = json.loads(stdout)['references']['spec']['tags']
    else:  # it is an imagestream
        # get image_stream and name_space out of pullspec_or_imagestream
        image_stream = pullspec_or_imagestream.split("/")[-1]  # Get the part after /
        name_space = pullspec_or_imagestream.split("/")[0]  # Get the part before /

        rc, stdout, stderr = await exectools.cmd_gather_async(
            f'oc -o json -n {name_space} get is/{image_stream}')
        tags = json.loads(stdout)['spec']['tags']

    log("Looping over payload images...")
    log(f"{len(tags)} images to check")
    cmds = [['oc', 'image', 'info', '-o', 'json', tag['from']['name']] for tag in
            tags]

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
