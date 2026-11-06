"""Infrastructure adapter that loads RPM inventories from the selected RHCOS images."""

from typing import Any

from artcommonlib import exectools
from artcommonlib.arch_util import brew_arch_for_go_arch, go_arch_for_brew_arch
from artcommonlib.assembly import AssemblyTypes, assembly_basis, assembly_rhcos_config
from artcommonlib.model import Model
from artcommonlib.rhcos import RhcosMissingContainerException, get_container_configs
from artcommonlib.util import oc_image_info_for_arch
from doozerlib.rhcos import RHCOSBuildFinder, RHCOSBuildInspector
from doozerlib.util import get_nightly_pullspec

from elliottlib.rhcos_rpm_evidence.models import InstalledRpm, RHCOSImageInventory
from elliottlib.runtime import Runtime


class RHCOSInventoryProvider:
    """
    Load exact per-architecture RHCOS RPM inventories for the runtime assembly.
    """

    def __init__(self, runtime: Runtime) -> None:
        """
        Store the initialized Elliott runtime used to resolve assembly images.
        """
        self._runtime = runtime

    def collect(self) -> tuple[RHCOSImageInventory, ...]:
        """
        Resolve selected RHCOS images and return their installed RPM inventories.
        """
        assembly_config = assembly_rhcos_config(self._runtime.get_releases_config(), self._runtime.assembly).primitive()
        container_configs = list(get_container_configs(self._runtime))
        primary = next((container for container in container_configs if container.primary), None)
        if primary is None:
            raise ValueError("RHCOS group configuration has no primary payload container")

        basis = assembly_basis(self._runtime.get_releases_config(), self._runtime.assembly).primitive()
        reference_releases = basis.get("reference_releases") or {}
        basis_config = None
        basis_assembly = basis.get("assembly")
        if not reference_releases and basis_assembly:
            basis_config = assembly_rhcos_config(self._runtime.get_releases_config(), basis_assembly).primitive()
        architectures = self._architectures_with_basis(reference_releases, basis_config, primary)

        images: list[RHCOSImageInventory] = []
        for brew_arch in architectures:
            pullspecs = self._selected_pullspecs(assembly_config, container_configs, primary, brew_arch)
            pinned_pullspecs = {
                name: _pin_pullspec(pullspec, brew_arch, self._runtime.registry_config)
                for name, pullspec in pullspecs.items()
            }
            inspector = RHCOSBuildInspector(
                self._runtime,
                pinned_pullspecs,
                brew_arch,
                registry_config=self._runtime.registry_config,
            )
            entries = inspector.get_os_metadata_rpm_list()
            rpms = tuple(_installed_rpm(entry) for entry in entries)
            if not rpms:
                raise ValueError(f"Selected RHCOS image for {brew_arch} has no RPM inventory")
            _validate_kernel_rpm_architecture(rpms, brew_arch, "Selected")
            images.append(
                RHCOSImageInventory(
                    arch=brew_arch,
                    build_id=str(inspector.build_id),
                    pullspecs=tuple(sorted(pinned_pullspecs.items())),
                    rpms=rpms,
                )
            )

        if not images:
            raise ValueError("No selected RHCOS architectures are configured")
        return tuple(images)

    def collect_basis(self) -> tuple[RHCOSImageInventory, ...]:
        """
        Resolve exact basis RHCOS images from reference payloads or assembly pins.

        Cache payload image references because a single multi-architecture basis
        payload can be reused for every architecture in the group.
        """
        releases_config = self._runtime.get_releases_config()
        basis = assembly_basis(releases_config, self._runtime.assembly).primitive()
        basis_config = None
        reference_releases = basis.get("reference_releases") or {}
        basis_assembly = basis.get("assembly")
        if not reference_releases and basis_assembly:
            basis_config = assembly_rhcos_config(releases_config, basis_assembly).primitive()
        if not reference_releases and basis_config is None:
            raise ValueError(
                f"Assembly {self._runtime.assembly} has no exact basis reference releases or basis assembly RHCOS pins"
            )

        container_configs = list(get_container_configs(self._runtime))
        primary = next((container for container in container_configs if container.primary), None)
        if primary is None:
            raise ValueError("RHCOS group configuration has no primary payload container")

        images: list[RHCOSImageInventory] = []
        architectures = self._architectures_with_basis(reference_releases, basis_config, primary)
        pullspecs_by_payload: dict[str, dict[str, str]] = {}
        for brew_arch in architectures:
            if reference_releases:
                reference = self._basis_reference_for_arch(reference_releases, brew_arch)
                payload_pullspec = (
                    reference if "/" in reference else get_nightly_pullspec(reference, self._runtime.build_system)
                )
                pullspecs = pullspecs_by_payload.get(payload_pullspec)
                if pullspecs is None:
                    pullspecs = self._basis_pullspecs_from_payload(payload_pullspec, container_configs, primary)
                    pullspecs_by_payload[payload_pullspec] = pullspecs
            else:
                pullspecs = self._basis_assembly_pullspecs(basis_config, container_configs, primary, brew_arch)

            pinned_pullspecs = {
                name: _pin_pullspec(pullspec, brew_arch, self._runtime.registry_config)
                for name, pullspec in pullspecs.items()
            }
            inspector = RHCOSBuildInspector(
                self._runtime,
                pinned_pullspecs,
                brew_arch,
                registry_config=self._runtime.registry_config,
            )
            entries = inspector.get_os_metadata_rpm_list()
            rpms = tuple(_installed_rpm(entry) for entry in entries)
            if not rpms:
                raise ValueError(f"Basis RHCOS image for {brew_arch} has no RPM inventory")
            _validate_kernel_rpm_architecture(rpms, brew_arch, "Basis")
            images.append(
                RHCOSImageInventory(
                    arch=brew_arch,
                    build_id=str(inspector.build_id),
                    pullspecs=tuple(sorted(pinned_pullspecs.items())),
                    rpms=rpms,
                )
            )

        if not images:
            raise ValueError("No basis RHCOS architectures are configured")
        return tuple(images)

    def _architectures_with_basis(
        self,
        reference_releases: dict[str, str],
        basis_config: dict[str, Any] | None,
        primary: Model,
    ) -> tuple[str, ...]:
        """
        Limit evidence collection to architectures with an exact basis reference or image pin.
        """
        configured_arches = tuple(brew_arch_for_go_arch(arch) for arch in self._runtime.group_config.arches)
        if reference_releases:
            basis_arches = _brew_arches(reference_releases)
        elif basis_config is not None:
            image_pins = basis_config.get(primary.name, {}).get("images", {})
            basis_arches = _brew_arches(image_pins)
        else:
            return configured_arches

        architectures = tuple(arch for arch in configured_arches if arch in basis_arches)
        if not architectures:
            raise ValueError("Assembly basis has no RHCOS references for configured architectures")
        if architectures != configured_arches:
            self._runtime.logger.info(
                "Limiting RHCOS RPM evidence architecture scope to basis-supported architectures: %s",
                ", ".join(architectures),
            )
        return architectures

    def _basis_reference_for_arch(self, reference_releases: dict[str, str], brew_arch: str) -> str:
        """
        Find the configured basis payload for one Brew architecture.
        """
        for arch, reference in reference_releases.items():
            try:
                if brew_arch_for_go_arch(arch) == brew_arch:
                    return str(reference)
            except Exception:
                continue
        raise ValueError(f"No basis reference release is configured for {brew_arch}")

    def _basis_pullspecs_from_payload(
        self,
        payload_pullspec: str,
        container_configs: list[Model],
        primary: Model,
    ) -> dict[str, str]:
        """
        Resolve configured RHCOS image pullspecs from one exact basis payload.
        """
        pullspecs: dict[str, str] = {}
        for container in container_configs:
            command = ["oc", "adm", "release", "info", f"--image-for={container.name}"]
            if self._runtime.registry_config:
                command.append(f"--registry-config={self._runtime.registry_config}")
            command.append(payload_pullspec)
            try:
                output, _ = exectools.cmd_assert(command)
            except Exception:
                if container.primary:
                    raise
                continue
            if output.strip():
                pullspecs[container.name] = output.strip()

        if primary.name not in pullspecs:
            raise ValueError(f"Basis payload {payload_pullspec} has no primary RHCOS image {primary.name}")
        return pullspecs

    def _basis_assembly_pullspecs(
        self,
        basis_config: dict[str, Any],
        container_configs: list[Model],
        primary: Model,
        brew_arch: str,
    ) -> dict[str, str]:
        """
        Read only explicitly pinned basis RHCOS images; never use a moving stream tag.
        """
        pullspecs = {}
        for container in container_configs:
            pullspec = basis_config.get(container.name, {}).get("images", {}).get(brew_arch)
            if pullspec:
                pullspecs[container.name] = pullspec
        if primary.name not in pullspecs:
            raise ValueError(f"Basis assembly has no pinned primary RHCOS image for {brew_arch}")
        return pullspecs

    def _selected_pullspecs(
        self,
        assembly_config: dict[str, Any],
        container_configs: list[Model],
        primary: Model,
        brew_arch: str,
    ) -> dict[str, str]:
        """
        Resolve pinned assembly images, with latest-image fallback only for stream assemblies.
        """
        pullspecs: dict[str, str] = {}
        for container in container_configs:
            container_data = assembly_config.get(container.name, {})
            pullspec = container_data.get("images", {}).get(brew_arch)
            if pullspec:
                pullspecs[container.name] = pullspec
                continue

            if self._runtime.assembly_type != AssemblyTypes.STREAM:
                if container.primary:
                    raise ValueError(
                        f"Assembly {self._runtime.assembly} has no selected RHCOS image for "
                        f"{container.name}/{brew_arch}"
                    )
                continue

            finder = RHCOSBuildFinder(
                self._runtime,
                self._runtime.get_minor_version(),
                brew_arch,
                registry_config=self._runtime.registry_config,
            )
            try:
                _, pullspec = finder.latest_container(container)
            except RhcosMissingContainerException:
                if container.primary:
                    raise
                continue
            if not pullspec:
                if container.primary:
                    raise ValueError(f"No primary RHCOS image found for stream architecture {brew_arch}")
                continue
            pullspecs[container.name] = pullspec

        if primary.name not in pullspecs:
            raise ValueError(f"No primary RHCOS image selected for architecture {brew_arch}")
        return pullspecs


def _pin_pullspec(pullspec: str, brew_arch: str, registry_config: str | None) -> str:
    """
    Resolve mutable image references to architecture-specific manifest digests.
    """
    if "@sha256:" in pullspec:
        return pullspec

    image_info = oc_image_info_for_arch(
        pullspec,
        go_arch_for_brew_arch(brew_arch),
        registry_config=registry_config,
    )
    repository = pullspec.split("@", 1)[0]
    final_component = repository.rsplit("/", 1)[-1]
    if ":" in final_component:
        repository = repository.rsplit(":", 1)[0]
    return f"{repository}@{image_info['digest']}"


def _brew_arches(architectures: dict[str, Any]) -> set[str]:
    """
    Normalize architecture keys from assembly data to Brew architecture names.
    """
    brew_arches = set()
    for architecture in architectures:
        try:
            brew_arches.add(brew_arch_for_go_arch(architecture))
        except Exception:
            continue
    return brew_arches


def _validate_kernel_rpm_architecture(rpms: tuple[InstalledRpm, ...], brew_arch: str, image_role: str) -> None:
    """
    Fail collection if a RHCOS inventory contains a kernel RPM for another architecture.
    """
    mismatches = [
        f"{rpm.name}.{rpm.arch} from {rpm.stream}"
        for rpm in rpms
        if rpm.name == "kernel-core" and rpm.arch != brew_arch
    ]
    if mismatches:
        raise ValueError(
            f"{image_role} RHCOS inventory for {brew_arch} contains kernel RPMs for another architecture: "
            f"{', '.join(mismatches)}"
        )


def _installed_rpm(entry: list[Any]) -> InstalledRpm:
    """
    Convert one RHCOS metadata RPM row into the domain representation.
    """
    if len(entry) < 5:
        raise ValueError(f"Invalid RHCOS RPM metadata row: {entry}")
    name, epoch, version, release, arch = entry[:5]
    stream = entry[5] if len(entry) > 5 else "unknown"
    return InstalledRpm(
        name=str(name),
        epoch=str(epoch or "0"),
        version=str(version),
        release=str(release),
        arch=str(arch),
        stream=str(stream),
    )
