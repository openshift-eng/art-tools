from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import List, Literal, Optional, Self

from pydantic import BaseModel, ConfigDict, Field, field_serializer, model_validator
from ruamel.yaml import scalarstring


class StrictBaseModel(BaseModel):
    # do not allow extra fields
    model_config = ConfigDict(extra='forbid')


class Metadata(StrictBaseModel):
    """Defines shipment metadata for a product release"""

    model_config = ConfigDict(
        json_schema_extra={
            'allOf': [
                {
                    'if': {'properties': {'unified': {'const': True}}, 'required': ['unified']},
                    'then': {
                        'properties': {'fbc': {'const': True}},
                        'required': ['fbc'],
                        'not': {'anyOf': [{'required': ['group']}, {'required': ['assembly']}]},
                    },
                    'else': {
                        'required': ['group', 'assembly'],
                        'properties': {
                            'group': {'type': 'string', 'minLength': 1},
                            'assembly': {'type': 'string', 'minLength': 1},
                        },
                    },
                }
            ],
        }
    )

    product: str  # product associated with shipment - see group.yml `product` field
    application: str  # Konflux application to release for
    group: Optional[str] = None  # required for ordinary shipments
    assembly: Optional[str] = None  # required for ordinary shipments
    unified: bool = False
    fbc: Optional[bool] = False  # indicates if shipment is for an FBC release
    advisory_required: Optional[bool] = True  # when False, shipment-ci skips advisory URL check (registry-push-only)
    docs_approval_required: Optional[bool] = (
        True  # when False, elliott verify-docs-approval skips all docs cross-reference / payload SHA checks
    )
    release_notes_in_rpa: Optional[bool] = False  # when True, releaseNotes come from the ReleasePlanAdmission (CDN)

    @model_validator(mode='after')
    def validate_execution_context(self) -> Self:
        """Require one execution context only for ordinary shipments."""
        if self.unified:
            if not self.fbc or self.group is not None or self.assembly is not None:
                raise ValueError('Unified shipments require fbc: true and no group/assembly')
        elif not self.group or not self.assembly:
            raise ValueError('Ordinary shipments require group and assembly')
        return self


class UnifiedAssembly(StrictBaseModel):
    """An assembly and image shipment associated with one unified target."""

    group: str
    assembly: str
    revision: str = Field(pattern=r'^[0-9a-f]{40}$')
    imageShipmentMR: str = Field(pattern=r'^https://[^/]+/.+/merge_requests/[0-9]+$')


class Unified(StrictBaseModel):
    """Contributors; provenance is at the snapshot source commit's standard path."""

    assemblies: List[UnifiedAssembly] = Field(min_length=1)


class GitSource(StrictBaseModel):
    """Defines the git source of a component"""

    url: str
    revision: str


class ComponentSource(StrictBaseModel):
    """Defines the source of a component"""

    git: GitSource


class SnapshotComponent(StrictBaseModel):
    """Defines a component of a Konflux Snapshot"""

    name: str
    containerImage: str
    source: ComponentSource


class SnapshotSpec(StrictBaseModel):
    """Defines a Konflux Snapshot Object for creation"""

    application: str
    components: List[SnapshotComponent]


class Snapshot(StrictBaseModel):
    """Konflux Snapshot definition for release i.e. builds to release"""

    spec: SnapshotSpec
    nvrs: List[str]


class CveAssociation(StrictBaseModel):
    key: str
    component: str


class Issue(StrictBaseModel):
    id: str
    source: str


class Issues(StrictBaseModel):
    fixed: Optional[List[Issue]] = None


class ReleaseNotes(StrictBaseModel):
    """Represents releaseNotes field which contains all advisory metadata, when constructing a Konflux release"""

    type: Literal['RHEA', 'RHBA', 'RHSA']  # Advisory type
    live_id: int = None
    synopsis: str = None
    topic: str = None
    description: str = None
    solution: str = None
    issues: Optional[Issues] = None
    cves: Optional[List[CveAssociation]] = None
    references: Optional[List[str]] = None

    # serialize special text fields, if they contain a new-line char
    # configure them to be LiteralScalarString
    @field_serializer('topic', 'solution', 'description')
    def serialize_text_fields(self, field: str, _info):
        if field and '\n' in field:
            return scalarstring.LiteralScalarString(field)
        return field


class MappingDefaults(StrictBaseModel):
    """Additional image tags supplied by a shipment to the Konflux release mapping."""

    tags: List[str] = Field(min_length=1)

    def resolve_tags(self, assembly: str) -> List[str]:
        """Resolve ART's assembly placeholder while preserving Konflux tag variables."""
        resolved_tags = []
        for tag in self.tags:
            if re.search(r'\{\{\s*assembly\s*\}\}', tag):
                if assembly == 'stream':
                    raise ValueError('Assembly-based release tags require a named assembly, not stream')
                if not re.fullmatch(r'[a-zA-Z0-9_][a-zA-Z0-9_.-]*', assembly):
                    raise ValueError(f'Invalid assembly for release tags: {assembly!r}')
            resolved = re.sub(r'\{\{\s*assembly\s*\}\}', lambda _: assembly, tag)
            # Konflux validates the expanded values of its own variables at release time.
            literal_tag = re.sub(r'\{\{\s*[a-zA-Z0-9_.-]+\s*\}\}', 'x', resolved)
            if not re.fullmatch(r'[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}', literal_tag):
                raise ValueError(f'Invalid release tag after assembly substitution: {resolved!r}')
            resolved_tags.append(resolved)
        return resolved_tags


class Mapping(StrictBaseModel):
    """Shipment-controlled tags; destination repositories remain configured in the RPA."""

    defaults: MappingDefaults


class Data(StrictBaseModel):
    """Represents spec.data field when constructing a Konflux release"""

    releaseNotes: Optional[ReleaseNotes] = None
    mapping: Optional[Mapping] = None


class EnvAdvisory(StrictBaseModel):
    """Information about the advisory that got shipped to the environment"""

    internal_url: Optional[str] = None
    url: Optional[str] = None


class EnvResult(StrictBaseModel):
    """Information about the release result for the environment"""

    pipeline: Optional[str] = None


class ShipmentEnv(StrictBaseModel):
    """Environment specific configuration for a release"""

    releasePlan: str
    advisory: Optional[EnvAdvisory] = None
    result: Optional[EnvResult] = None

    def shipped(self):
        return bool(self.advisory)


class Environments(StrictBaseModel):
    """Environments to release the shipment to"""

    stage: ShipmentEnv = Field(
        ...,
        description='Config for releasing to stage environment',
    )
    prod: ShipmentEnv = Field(
        ...,
        description='Config for releasing to prod environment',
    )


class Tools(StrictBaseModel):
    """Tools to use when releasing shipment to an environment"""

    art_tools: Optional[str] = Field(
        None,
        description='art-tools repo commit to use when releasing shipment to an environment e.g. thegreyd@branch_name. Defaults to openshift-eng@main',
    )
    build_data: Optional[str] = Field(
        None,
        description='ocp-build-data repo commit to use when releasing shipment to an environment e.g. thegreyd@branch_name. Defaults to openshift@openshift-{MAJOR}.{MINOR}',
    )


class Shipment(StrictBaseModel):
    """Config to ship a Konflux release for a product"""

    model_config = ConfigDict(
        json_schema_extra={
            'allOf': [
                {
                    'if': {
                        'properties': {
                            'metadata': {'properties': {'unified': {'const': True}}, 'required': ['unified']}
                        },
                        'required': ['metadata'],
                    },
                    'then': {
                        'required': ['unified', 'snapshot'],
                        'properties': {
                            'unified': {'type': 'object'},
                            'snapshot': {'type': 'object'},
                        },
                    },
                    'else': {'not': {'required': ['unified']}},
                }
            ],
        }
    )

    metadata: Metadata
    unified: Optional[Unified] = None
    tools: Optional[Tools] = None
    environments: Environments
    snapshot: Optional[Snapshot] = None
    data: Optional[Data] = None

    @model_validator(mode='after')
    def validate_unified(self) -> Self:
        """Keep the unified variant complete and separate from ordinary releases."""
        if self.metadata.unified != (self.unified is not None):
            raise ValueError('Unified metadata and contributor list must be supplied together')
        if self.metadata.unified:
            if not self.snapshot or len(self.snapshot.spec.components) != 1:
                raise ValueError('Unified shipments require exactly one snapshot component')
            if len(self.snapshot.nvrs) != 1:
                raise ValueError('Unified shipments require exactly one catalog NVR')
            component = self.snapshot.spec.components[0]
            if not re.fullmatch(r'[^@\s]+@sha256:[0-9a-f]{64}', component.containerImage):
                raise ValueError('Unified catalog image must be digest-pinned')
            if not re.fullmatch(r'[0-9a-f]{40}', component.source.git.revision):
                raise ValueError('Unified catalog source must be commit-pinned')
            identities = [(a.group, a.assembly) for a in self.unified.assemblies]
            if len(identities) != len(set(identities)):
                raise ValueError('Duplicate unified assembly contributors')
        return self

    @model_validator(mode='after')
    def make_sure_data_is_present_unless_fbc(self) -> Self:
        release_notes_present = self.data and self.data.releaseNotes
        if self.metadata.fbc and release_notes_present:
            raise ValueError('FBC shipment is not expected to have data.releaseNotes defined')
        # Only require releaseNotes for OpenShift products (groups starting with 'openshift-')
        if (
            not self.metadata.fbc
            and not self.metadata.release_notes_in_rpa
            and not release_notes_present
            and self.metadata.group.startswith('openshift-')
        ):
            raise ValueError('A regular shipment is expected to have data.releaseNotes defined')
        return self

    @model_validator(mode='after')
    def validate_release_tags(self) -> Self:
        if self.data and self.data.mapping:
            if self.metadata.fbc:
                raise ValueError('FBC shipment is not expected to have data.mapping defined')
            self.data.mapping.defaults.resolve_tags(self.metadata.assembly)
        return self

    @model_validator(mode='after')
    def shipment_and_snapshot_application_must_match(self) -> Self:
        if self.snapshot and self.snapshot.spec.application != self.metadata.application:
            raise ValueError(
                f'shipment.snapshot.spec.application={self.snapshot.spec.application} is expected to be the same as shipment.metadata.application={self.metadata.application}'
            )
        return self


def add_schema_comment(schema: dict):
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M %Z")
    comment = f'Schema generated on {timestamp} from {__name__}'
    schema['$comment'] = comment


class ShipmentConfig(StrictBaseModel):
    """Represents a Shipment Metadata Config file in a product's shipment-data repo"""

    model_config = ConfigDict(json_schema_extra=add_schema_comment)

    shipment: Shipment
