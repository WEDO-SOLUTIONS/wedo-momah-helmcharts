from typing import Any

from pydantic import BaseModel, Field, validator

from signs_dashboard.models.interest_zones import InterestZoneType


class AddInterestZoneRequest(BaseModel):
    zone_name: str = Field(min_length=2, max_length=255)
    zone_type: InterestZoneType

    @validator('zone_type', pre=True)
    def validate_zone_type(cls, value: Any) -> InterestZoneType:  # noqa: N805 pylint: disable=E0213
        if value not in {member.name for member in InterestZoneType}:
            raise ValueError(f'Unknown zone type: {value}')
        return InterestZoneType[value]
