from datetime import date
from typing import Optional

from pydantic import BaseModel


class DriversQueryParams(BaseModel):
    emails: Optional[list[str]]
    from_dt: date
    to_dt: Optional[date]
