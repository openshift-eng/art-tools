from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Literal, Optional, Self, Union

from pydantic import BaseModel, ConfigDict, Field, field_serializer, model_validator
from ruamel.yaml import scalarstring


class StrictBaseModel(BaseModel):
    # do not allow extra fields
    model_config = ConfigDict(extra='forbid')


class Metadata(StrictBaseModel):
    """Defines shipment metadata for a product release"""

    product: str  # product associated with shipment - see group.yml `product` field
    application: str  # Konflux application to release for
    group: str  # associated build-data group for component metadata
    assembly: str  # associated build-data assembly
    fbc: Optional[bool] = False  # indicates if shipment is for an FBC release


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


class Data(StrictBaseModel):
    """Represents spec.data field when constructing a Konflux release"""

    releaseNotes: ReleaseNotes


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

    metadata: Metadata
    tools: Optional[Tools] = None
    environments: Environments
    snapshot: Optional[Snapshot] = None
    data: Optional[Data] = None

    @model_validator(mode='after')
    def make_sure_data_is_present_unless_fbc(self) -> Self:
        release_notes_present = self.data and self.data.releaseNotes
        if self.metadata.fbc and release_notes_present:
            raise ValueError('FBC shipment is not expected to have data.releaseNotes defined')
        # Only require releaseNotes for OpenShift products (groups starting with 'openshift-')
        if not self.metadata.fbc and not release_notes_present and self.metadata.group.startswith('openshift-'):
            raise ValueError('A regular shipment is expected to have data.releaseNotes defined')
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
