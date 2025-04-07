from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class Metadata(BaseModel):
    product: str
    application: str
    group: str
    assembly: str
    fbc: Optional[bool] = False


class Spec(BaseModel):
    nvrs: List


class Snapshot(BaseModel):
    name: str
    spec: Spec


class ReleaseNotes(BaseModel):
    type: str
    synopsis: str
    topic: str
    description: str
    solution: str
    issues: Optional[Dict[str, Any]] = None
    cves: Optional[List] = None
    references: Optional[List] = None


class Data(BaseModel):
    releaseNotes: ReleaseNotes


class ShipmentEnv(BaseModel):
    releasePlan: str
    releaseName: Optional[str] = None
    advisoryName: Optional[str] = None
    advisoryInternalUrl: Optional[str] = None


class Environments(BaseModel):
    stage: ShipmentEnv = Field(
        ..., description='Config for releasing to stage environment'
    )
    prod: ShipmentEnv = Field(
        ..., description='Config for releasing to prod environment'
    )


class Shipment(BaseModel):
    metadata: Metadata
    environments: Environments
    snapshot: Snapshot
    data: Optional[Data] = None


class ShipmentConfig(BaseModel):
    shipment: Shipment
