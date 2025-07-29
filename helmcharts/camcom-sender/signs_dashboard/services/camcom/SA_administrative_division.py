import json
import logging
import os

municipalities_filters_filepath = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    'municipalities.json',
)

logger = logging.getLogger(__name__)


class SAAdministrativeDivisionNamesService:
    def __init__(self):
        with open(municipalities_filters_filepath) as fileobj:
            self.adm_division_filters = json.load(fileobj)

    def get_municipalities_pro_filters(self) -> list[dict]:
        return self.adm_division_filters
