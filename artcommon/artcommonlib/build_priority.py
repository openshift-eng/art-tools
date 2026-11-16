"""Optional central policy for Konflux image build priorities."""

import hashlib
import logging
from typing import Annotated

import aiohttp
import yaml
from artcommonlib.product_catalog import get_product_id_for_product
from artcommonlib.product_ids import ProductId
from artcommonlib.util import product_version_from_group_name
from pydantic import BaseModel, ConfigDict, Field, StrictBool, StringConstraints

LOGGER = logging.getLogger(__name__)
BUILD_PRIORITY_POLICY_URL = (
    'https://raw.githubusercontent.com/openshift-eng/ocp-build-data/main/config/build-priority.yml'
)
Priority = Annotated[int, Field(strict=True, ge=1, le=10)]
ProductVersion = Annotated[str, StringConstraints(pattern=r'^\d+\.\d+$')]


def _version_tuple(version: str) -> tuple[int, int]:
    major, minor = version.split('.')
    return int(major), int(minor)


class BuildPriorityRule(BaseModel):
    model_config = ConfigDict(extra='forbid')

    product: ProductId
    versions: list[ProductVersion] | None = Field(default=None, min_length=1)
    min_version: ProductVersion | None = None
    priority: Priority

    def matches(self, product: ProductId, version: tuple[int, int] | None) -> bool:
        if self.product != product:
            return False
        if self.versions is not None and (
            version is None or version not in [_version_tuple(value) for value in self.versions]
        ):
            return False
        if self.min_version is not None and (version is None or version < _version_tuple(self.min_version)):
            return False
        return True


class BuildPriorityPolicy(BaseModel):
    model_config = ConfigDict(extra='forbid')

    enabled: StrictBool = False
    rules: list[BuildPriorityRule] = Field(default_factory=list)
    default_priority: Priority = 5

    def priority_for(self, product: str, group: str) -> str | None:
        """Return the first matching priority, or None to retain existing behavior."""
        if not self.enabled:
            return None
        product_id = get_product_id_for_product(product)
        version = product_version_from_group_name(group)
        for rule in self.rules:
            if rule.matches(product_id, version):
                LOGGER.info(
                    'Build priority policy matched product=%s group=%s priority=%s', product, group, rule.priority
                )
                return str(rule.priority)
        LOGGER.info('Build priority policy default for product=%s group=%s: %s', product, group, self.default_priority)
        return str(self.default_priority)


async def load_build_priority_policy() -> BuildPriorityPolicy:
    """Read one policy snapshot from canonical main, independently of the build's data ref."""
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        async with session.get(BUILD_PRIORITY_POLICY_URL) as response:
            # Allow the code to land before the configuration file.
            if response.status == 404:
                LOGGER.info('No central build priority policy; retaining existing priorities')
                return BuildPriorityPolicy()
            response.raise_for_status()
            content = await response.text()

    policy = BuildPriorityPolicy.model_validate(yaml.safe_load(content))
    LOGGER.info(
        'Loaded build priority policy from %s (sha256=%s, enabled=%s)',
        BUILD_PRIORITY_POLICY_URL,
        hashlib.sha256(content.encode()).hexdigest(),
        policy.enabled,
    )
    return policy
