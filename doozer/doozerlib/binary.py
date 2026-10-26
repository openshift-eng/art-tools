from artcommonlib.model import Missing

from doozerlib import util
from doozerlib.image import ImageMetadata


class BinaryMetadata(ImageMetadata):
    """
    A group member from the binaries/ directory of ocp-build-data.

    A binary is a set of client tarballs (for example openshift-install) shipped to the CDN. ART builds it as a
    container image that carries the tarballs under /releases/, so the image rebaser and the Konflux image builder do
    the work. It is a kind of its own next to images and rpms: Runtime loads it into binary_map only for the binaries
    commands, it never joins the image tree, the payload or an image advisory, and its Konflux component and NVR
    names say "binaries".
    """

    kind = "binary"

    def __init__(self, runtime, data_obj, commitish=None, clone_source=False, prevent_cloning=False):
        """
        :param runtime: Doozer runtime.
        :param data_obj: Loaded binaries/<key>.yml data object.
        :param commitish: Upstream commitish override.
        :param clone_source: Clone the source at load time.
        :param prevent_cloning: Never clone the source.
        """
        super().__init__(
            runtime,
            data_obj,
            commitish,
            clone_source=clone_source,
            prevent_cloning=prevent_cloning,
            process_dependents=False,
        )
        if self.config.distgit.component is Missing:
            self._component_name = util.binary_component_name(self.name)

    @property
    def is_payload(self):
        return False

    @property
    def for_release(self):
        return False

    def resolve_parent(self):
        return None

    def get_konflux_component_name(self, app_name: str) -> str:
        """
        Return the Konflux Component name: konflux.component.name_override, else ose-X-Y-<key>-binaries.

        :param app_name: Konflux application name, e.g. openshift-4-22.
        """
        override = self.config.konflux.component.name_override
        if override is not Missing and override:
            return str(override)
        name = util.konflux_binary_component_name(app_name, self.distgit_key)
        if len(name) > 63:
            raise ValueError(
                f"[{self.distgit_key}] Konflux component name '{name}' is {len(name)} chars, "
                f"exceeding the 63-char limit. Set 'konflux.component.name_override' in the binary config."
            )
        return name
