from typing import List, Optional

from pydantic import BaseModel, Field, RootModel


class RPMDelivery(BaseModel):
    """An RPMDelivery config"""

    packages: List[str] = Field(min_length=1)
    rhel_tag: Optional[str] = Field(None, min_length=1)
    integration_tag: str = Field(min_length=1)
    stop_ship_tag: Optional[str] = Field(None, min_length=1)
    target_tag: Optional[str] = Field(None, min_length=1)


class RPMDeliveries(RootModel[list[RPMDelivery]]):
    """Represents rpm_deliveries field in group config"""

    def __bool__(self):
        return bool(self.root)

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item):
        return self.root[item]
