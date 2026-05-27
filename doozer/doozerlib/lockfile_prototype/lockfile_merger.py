"""
Lockfile merging for multi-stage Dockerfile builds.

Merges multiple per-stage lockfiles into a single lockfile,
deduplicating packages by URL and module metadata by name+stream.
"""

from doozerlib.lockfile_prototype.models import ArchResult, LockfileData, ModuleEntry, PackageEntry


def merge_lockfiles(lockfiles: list[LockfileData]) -> LockfileData:
    """
    Merge multiple stage lockfiles into one, deduplicating packages.

    Arg(s):
        lockfiles (list[LockfileData]): Per-stage lockfiles.
    Return Value(s):
        LockfileData: Merged lockfile.
    """
    all_arches: dict[str, dict[str, PackageEntry]] = {}
    all_module_metadata: dict[str, dict[str, ModuleEntry]] = {}
    all_sources: dict[str, dict[str, PackageEntry]] = {}

    for lockfile in lockfiles:
        for arch_entry in lockfile.arches:
            if arch_entry.arch not in all_arches:
                all_arches[arch_entry.arch] = {}
                all_module_metadata[arch_entry.arch] = {}
                all_sources[arch_entry.arch] = {}
            for pkg in arch_entry.packages:
                key = pkg.url or pkg.name or ""
                if key not in all_arches[arch_entry.arch]:
                    all_arches[arch_entry.arch][key] = pkg
            for src in arch_entry.source:
                src_key = src.url or src.name or ""
                if src_key not in all_sources[arch_entry.arch]:
                    all_sources[arch_entry.arch][src_key] = src
            for mod in arch_entry.module_metadata:
                mod_key = f"{mod.name or ''}:{mod.stream or ''}"
                if mod_key not in all_module_metadata[arch_entry.arch]:
                    all_module_metadata[arch_entry.arch][mod_key] = mod

    arches: list[ArchResult] = []
    for arch in sorted(all_arches.keys()):
        packages = sorted(all_arches[arch].values(), key=lambda p: p.url or p.name or "")
        sources = sorted(all_sources.get(arch, {}).values(), key=lambda s: s.url or s.name or "")
        modules = sorted(all_module_metadata[arch].values(), key=lambda m: m.name or "")
        arches.append(
            ArchResult(
                arch=arch,
                packages=packages,
                source=sources,
                module_metadata=modules,
            )
        )

    return LockfileData(arches=arches)
