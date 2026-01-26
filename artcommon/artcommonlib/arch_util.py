BREW_ARCHES = ["x86_64", "s390x", "ppc64le", "aarch64", "multi"]
BREW_ARCH_SUFFIXES = ["", "-s390x", "-ppc64le", "-aarch64", "-multi"]
GO_ARCHES = ["amd64", "s390x", "ppc64le", "arm64", "multi"]
GO_ARCH_SUFFIXES = ["", "-s390x", "-ppc64le", "-arm64", "-multi"]

RHCOS_BREW_COMPONENTS = {f"rhcos-{arch}" for arch in BREW_ARCHES} - {"rhcos-multi"}


def go_arch_for_brew_arch(brew_arch: str) -> str:
    if brew_arch in GO_ARCHES:
        return brew_arch  # allow to already be a go arch, just keep same
    if brew_arch in BREW_ARCHES:
        return GO_ARCHES[BREW_ARCHES.index(brew_arch)]
    raise Exception(f"no such brew arch '{brew_arch}' - cannot translate to golang arch")


def go_suffix_for_arch(arch: str, is_private: bool = False) -> str:
    """
    Imagestreams and namespaces for the release controller indicate
    a CPU architecture and whether the release is part of the private release controller.
    using [-<arch>][-priv] as a suffix. This method calculates that suffix
    based on what arch and privacy you are trying to reach.
    :param arch: The CPU architecture
    :param is_private: True if you are looking for a private release controller
    :return: A suffix to use when address release controller imagestreams/namespaces.
             x86 arch is never included in the suffix (i.e. '' is used).
    """
    arch = go_arch_for_brew_arch(arch)  # translate either incoming arch style
    suffix = GO_ARCH_SUFFIXES[GO_ARCHES.index(arch)]
    if is_private:
        suffix += "-priv"
    return suffix


def brew_arch_for_go_arch(go_arch: str) -> str:
    if go_arch in BREW_ARCHES:
        return go_arch  # allow to already be a brew arch, just keep same
    if go_arch in GO_ARCHES:
        return BREW_ARCHES[GO_ARCHES.index(go_arch)]
    raise Exception(f"no such golang arch '{go_arch}' - cannot translate to brew arch")


def brew_suffix_for_arch(arch: str) -> str:
    arch = brew_arch_for_go_arch(arch)  # translate either incoming arch style
    return BREW_ARCH_SUFFIXES[BREW_ARCHES.index(arch)]
