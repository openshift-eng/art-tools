import copy
import typing
from datetime import datetime, timezone
from enum import Enum

from artcommonlib.model import ListModel, Missing, Model


class AssemblyTypes(Enum):
    STREAM = "stream"  # Default assembly type - indicates continuous build and no basis event
    STANDARD = "standard"  # All constraints / checks enforced (e.g. consistent RPMs / siblings)
    CANDIDATE = "candidate"  # Indicates release candidate or feature candidate
    CUSTOM = "custom"  # No constraints enforced
    PREVIEW = "preview"  # Indcates a .next (internal name) or preview release


class AssemblyIssueCode(Enum):
    # A class of issue so severe that we should not allow permits around it.
    IMPERMISSIBLE = 0

    # Expected member images to contain an assembly-specified override
    # dependency at a specified NVR, but it was found installed at a
    # different version, indicating that image needs a rebuild or a
    # reconfiguration.
    CONFLICTING_INHERITED_DEPENDENCY = 1

    # Different members of the assembly installed different versions of the
    # same group RPM build, so may omit bug fixes or introduce subtle bugs.
    CONFLICTING_GROUP_RPM_INSTALLED = 2

    # Two containers built from the same source are not built from the same
    # commit. Devs warn this can introduce subtle compatibility bugs.
    MISMATCHED_SIBLINGS = 3

    # The container includes a different version of an RPM than the assembly
    # so there is a risk of not officially shipping the RPM we are using, or
    # of misreporting bug fixes. We may either rebuild the container or change
    # the assembly to resolve.
    OUTDATED_RPMS_IN_STREAM_BUILD = 4

    # The arch-specific builds of RHCOS did not all install the same RPM
    # versions (if they installed at all).
    INCONSISTENT_RHCOS_RPMS = 5

    # A dependency was defined explicitly for an assembly member, but it is not
    # installed, i.e. the ARTists expected something to be installed, but it
    # wasn't found in the final image.
    # In rare circumstances the specified RPM may be used by a build stage of
    # the Dockerfile and not installed in the final stage, in which case, the
    # assembly definition should be altered to permit it.
    MISSING_INHERITED_DEPENDENCY = 6

    # group config specifies an RHCOS container expected in the RHCOS build
    # metadata, but it is not present for the current build. We experienced
    # this when RHCOS was introducing a new container, but it was not yet
    # configured in the RHCOS pipeline for all arches.
    MISSING_RHCOS_CONTAINER = 7

    # A consistency requirement for packages between members (currently only
    # specifiable in group.yml rhcos config) failed -- the members did not have
    # the same version installed.
    FAILED_CONSISTENCY_REQUIREMENT = 8

    # A consistency requirement for different packages failed
    # Currently this failure only occurs if kernel and kernel-rt rpms
    # in x86_64 RHCOS have different versions
    FAILED_CROSS_RPM_VERSIONS_REQUIREMENT = 9

    # If build sync for named assemblies contain embargoed builds, this code will be reported. If payload is being
    # promoted after embargoed lift, then permit can be added
    EMBARGOED_CONTENT = 10

    # If a kernel included in rhcos or images has the early-kernel-stop-ship tag,
    # this code will be emitted:
    UNSHIPPABLE_KERNEL = 11


class AssemblyIssue:
    """
    An encapsulation of an issue with an assembly. Some issues are critical for any
    assembly and some are on relevant for assemblies we intend for GA.
    """

    def __init__(self, msg: str, component: str, code: AssemblyIssueCode = AssemblyIssueCode.IMPERMISSIBLE):
        """
        :param msg: A human readable message describing the issue.
        :param component: The name of the entity associated with the issue, if any.
        :param code: A code that that classifies the issue. Assembly definitions can optionally permit some of these codes.
        """
        self.msg: str = msg
        self.component: str = component or "general"
        self.code = code

    def __str__(self):
        return self.msg

    def __repr__(self):
        return self.msg

    def to_dict(self):
        return {
            "code": self.code.value,
            "component": self.component,
            "msg": self.msg,
        }


def assembly_type(releases_config: Model, assembly: typing.Optional[str]) -> AssemblyTypes:
    # If the assembly is not defined in releases.yml, it defaults to stream
    if not assembly or not isinstance(releases_config, Model):
        return AssemblyTypes.STREAM
    target_assembly = releases_config.releases[assembly].assembly
    if not target_assembly:
        return AssemblyTypes.STREAM

    str_type = assembly_config_struct(releases_config, assembly, "type", "standard")
    for assem_type in AssemblyTypes:
        if str_type == assem_type.value:
            return assem_type

    raise ValueError(f"Unknown assembly type: {str_type}")


def assembly_config_struct(releases_config: Model, assembly: typing.Optional[str], key: str, default):
    """
    If a key is directly under the 'assembly' (e.g. rhcos), then this method will
    recurse the inheritance tree to build you a final version of that key's value.
    The key may refer to a list or dict (set default value appropriately).
    """
    if not assembly or not isinstance(releases_config, Model):
        key_struct = default
    else:
        key_struct = assembly_field(releases_config.primitive(), assembly, key, default)
    if isinstance(default, dict):
        return Model(dict_to_model=key_struct)
    elif isinstance(default, list):
        return ListModel(list_to_model=key_struct)
    elif isinstance(default, (bool, int, float, str, bytes, type(None))):
        return key_struct
    else:
        raise ValueError(f"Unknown how to derive for default type: {type(default)}")


def _check_recursion(releases_config: dict, assembly: str):
    if not isinstance(releases_config, dict):
        raise TypeError("releases_config must be a dict")
    if not isinstance(assembly, str):
        raise TypeError("assembly must be a str")
    found = set()
    releases = releases_config.get("releases", {})
    next_assembly = assembly
    while next_assembly and next_assembly in releases:
        if next_assembly in found:
            raise ValueError(f"Infinite recursion in {assembly} detected; {next_assembly} detected twice in chain")
        found.add(next_assembly)
        target_assembly = releases.get(next_assembly, {}).get("assembly")
        next_assembly = target_assembly.get("basis", {}).get("assembly")


def _merger(a, b):
    """
    Merges two, potentially deep, objects into a new one and returns the result.
    Conceptually, 'a' is layered over 'b' and is dominant when
    necessary. The output is 'c'.
    1. if 'a' specifies a primitive value, regardless of depth, 'c' will contain that value.
    2. if a key in 'a' specifies a list and 'b' has the same key/list, a's list will be appended to b's for c's list.
       Duplicates entries will be removed and primitive (str, int, ...) lists will be returned in sorted order.
    3. if a key ending with '!' in 'a' specifies a value, c's key (sans !) will be to set that value exactly.
    4. if a key ending with a '?' in 'a' specifies a value, c's key (sans ?)
       will be set to that value IF 'c' does not contain the key.
    4. if a key ending with a '-' in 'a' specifies a value, c's will not be populated
       with the key (sans -) regardless of 'a' or  'b's key value.
    """
    if type(a) in [bool, int, float, str, bytes, type(None)]:
        return a

    c = copy.deepcopy(b)

    if isinstance(a, Model):
        a = a.primitive()

    if isinstance(a, list):
        if not isinstance(c, list):
            return a
        for entry in a:
            if entry not in c:  # do not include duplicates
                c.append(entry)

        if c and type(c[0]) in [str, int, float]:
            return sorted(c)
        return c

    if isinstance(a, dict):
        if not isinstance(c, dict):
            return a
        for k, v in a.items():
            if k.endswith("!"):  # full dominant key
                k = k[:-1]
                c[k] = v
            elif k.endswith("?"):  # default value key
                k = k[:-1]
                if k not in c:
                    c[k] = v
            elif k.endswith("-"):  # remove key entirely
                k = k[:-1]
                c.pop(k, None)
            else:
                if k in c:
                    c[k] = _merger(a[k], c[k])
                else:
                    c[k] = a[k]
        return c

    raise TypeError(f"Unexpected value type: {type(a)}: {a}")


def assembly_permits(releases_config: Model, group_config: Model, assembly: typing.Optional[str]) -> ListModel:
    """
    :param releases_config: The content of releases.yml in Model form.
    :param group_config: The content of group.yml in Model form.
    :param assembly: The name of the assembly to assess
    Returns computed permits config model for a given assembly. If no
    permits are defined ListModel([]) is returned.
    """

    phase = group_config.software_lifecycle.phase
    if phase is not Missing and phase == "pre-release":
        defined_permits = assembly_config_struct(releases_config, assembly, "prerelease_permits", [])
        if not defined_permits:
            defined_permits = assembly_config_struct(releases_config, assembly, "permits", [])
    else:
        defined_permits = assembly_config_struct(releases_config, assembly, "permits", [])

    for permit in defined_permits:
        if permit.code == AssemblyIssueCode.IMPERMISSIBLE.name:
            raise ValueError(f"IMPERMISSIBLE cannot be permitted in any assembly (assembly: {assembly})")
        if permit.code not in ["*", *[aic.name for aic in AssemblyIssueCode]]:
            raise ValueError(f"Undefined permit code in assembly {assembly}: {permit.code}")
    return defined_permits


def assembly_rhcos_config(releases_config: Model, assembly: str) -> Model:
    """
    :param releases_config: The content of releases.yml in Model form.
    :param assembly: The name of the assembly to assess
    Returns the computed rhcos config model for a given assembly.
    """
    return assembly_config_struct(releases_config, assembly, "rhcos", {})


def assembly_field(releases_config: dict, assembly: typing.Optional[str], key: str, default: typing.Any) -> typing.Any:
    """If a key is directly under the 'assembly' (e.g. rhcos), then this method will
    recurse the inheritance tree to build you a final version of that key's value.

    This function differs from assembly_config_struct in that it returns the raw value rather than a merged model.

    if default is a non-empty dict or list, it will be merged with the found value.

    If the field is not found in the assembly, default will be returned.
    If any of releases_config and assembly are None, default will be returned.

    :param releases_config: The content of releases.yml in Model form.
    :param assembly: The name of the assembly to assess
    :param key: the field name
    :param default: the default value to return if the field is not found
    :return: the computed field for a given assembly.
    """
    if assembly is None or releases_config is None:
        return default
    if not isinstance(assembly, str):
        raise TypeError("assembly must be a string")
    if not isinstance(releases_config, dict):
        raise TypeError("releases_config must be a dict or None")
    _check_recursion(releases_config, assembly)
    target_assembly = releases_config.get("releases", {}).get(assembly, {}).get("assembly", {})
    if basis_assembly := target_assembly.get("basis", {}).get("assembly"):
        # Recursive apply ancestor assemblies
        parent_val = assembly_field(releases_config, basis_assembly, key, default)
        val = _merger(target_assembly[key], parent_val) if key in target_assembly else parent_val
    else:
        val = _merger(target_assembly[key], default) if key in target_assembly else default
    return val


def assembly_basis_event(
    releases_config: Model, assembly: str, strict: bool = False, build_system: str = "brew"
) -> typing.Optional[typing.Union[int, datetime]]:
    """
    :param releases_config: The content of releases.yml in Model form.
    :param assembly: The name of the assembly to assess
    :param strict: Raise exception if assembly not found
    Returns the basis event for a given assembly. If the assembly has no basis event,
    None is returned.
    """
    if not assembly or not isinstance(releases_config, Model):
        if strict:
            raise ValueError("given assembly not found in releases config")
        return None

    _check_recursion(releases_config, assembly)
    target_assembly = releases_config.releases[assembly].assembly

    if not target_assembly.basis.brew_event and not target_assembly.basis.time:
        if target_assembly.basis.assembly:
            return assembly_basis_event(
                releases_config,
                target_assembly.basis.assembly,
                strict=strict,
            )
        if strict:
            raise ValueError(f"Assembly {assembly} has no basis.brew_event or basis.time or basis.assembly defined")
        return None

    def _basis_brew_event() -> int:
        return int(target_assembly.basis.brew_event)

    def _basis_time() -> datetime:
        time_str = target_assembly.basis.time
        if not isinstance(time_str, str):
            raise ValueError(f"Invalid time format for assembly {assembly}: {time_str}")
        # Handle 'Z' suffix for UTC which is not supported in Python 3.10's fromisoformat
        if time_str.endswith("Z"):
            time_str = time_str[:-1] + "+00:00"
        dt = datetime.fromisoformat(time_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    # IMPORTANT: this logic is a bit special. We want to support these command invocations:
    # - (old default) --build-system=brew and basis.brew_event defined
    # - (new interop) --build-system=brew and basis.time defined. In this case, brew event is automatically calculated from the time
    # - (new default) --build-system=konflux and basis.time defined
    # * do not support --build-system=konflux and no basis.time defined
    if build_system == "brew":
        if target_assembly.basis.brew_event:
            return _basis_brew_event()
        elif target_assembly.basis.time:
            return _basis_time()
    elif build_system == "konflux":
        if not target_assembly.basis.time:
            raise ValueError(f"Assembly {assembly} has no basis.time defined")
        return _basis_time()
    else:
        raise ValueError(f"Unsupported build system: {build_system}. Supported systems are 'brew' and 'konflux'.")


def assembly_group_config(releases_config: Model, assembly: typing.Optional[str], group_config: Model) -> Model:
    """
    Returns a group config based on the assembly information
    and the input group config.
    :param releases_config: A Model for releases.yaml.
    :param assembly: The name of the assembly
    :param group_config: The group config to merge into a new group config (original Model will not be altered)
    """
    raw_group_config = group_config.primitive() if hasattr(group_config, "primitive") else group_config
    return assembly_config_struct(releases_config, assembly, "group", raw_group_config)


def assembly_basis(releases_config: Model, assembly: typing.Optional[str]) -> Model:
    """
    :param releases_config: The content of releases.yml in Model form.
    :param assembly: The name of the assembly to assess
    Returns the basis dict for a given assembly. If the assembly has no basis,
    Model({}) is returned.
    """
    return assembly_config_struct(releases_config, assembly, "basis", {})


def assembly_issues_config(releases_config: Model, assembly: str) -> Model:
    """
    :param releases_config: The content of releases.yml in Model form.
    :param assembly: The name of the assembly to assess
    Returns the a computed issues config model for a given assembly.
    """
    return assembly_config_struct(releases_config, assembly, "issues", {})


def assembly_streams_config(releases_config: Model, assembly: typing.Optional[str], streams_config: Model) -> Model:
    """
    Returns a streams config based on the assembly information
    and the input group config.
    :param releases_config: A Model for releases.yaml.
    :param assembly: The name of the assembly
    :param streams_config: The streams config to merge into a new streams config (original Model will not be altered)
    """
    val = assembly_config_struct(releases_config, assembly, "streams", streams_config.primitive())
    return Model(dict_to_model=val)


def assembly_metadata_config(
    releases_config: Model, assembly: typing.Optional[str], meta_type: str, distgit_key: str, meta_config: Model
) -> Model:
    """
    Returns a group member's metadata configuration based on the assembly information
    and the initial file-based config.
    :param releases_config: A Model for releases.yaml.
    :param assembly: The name of the assembly
    :param meta_type: 'rpm' or 'image'
    :param distgit_key: The member's distgit_key
    :param meta_config: The meta's config object
    :return: Returns a computed config for the metadata (e.g. value for meta.config).
    """
    if not assembly or not isinstance(releases_config, Model):
        return meta_config

    _check_recursion(releases_config, assembly)
    target_assembly = releases_config.releases[assembly].assembly

    if target_assembly.basis.assembly:  # Does this assembly inherit from another?
        # Recursive apply ancestor assemblies
        meta_config = assembly_metadata_config(
            releases_config, target_assembly.basis.assembly, meta_type, distgit_key, meta_config
        )

    config_dict = meta_config.primitive()

    component_list = target_assembly.members[f"{meta_type}s"]
    for component_entry in component_list:
        if (
            component_entry.distgit_key == "*"
            or component_entry.distgit_key == distgit_key
            and component_entry.metadata
        ):
            config_dict = _merger(component_entry.metadata.primitive(), config_dict)

    return Model(dict_to_model=config_dict)
