import logging
from dataclasses import dataclass
from typing import Optional

from signs_dashboard.services.twogis_pro.client import TwoGisProAPIClient
from signs_dashboard.services.twogis_pro.filters import TwoGisProFiltersService


@dataclass(frozen=True)
class Asset:
    kind: str
    name: str


class TwoGisProFiltersUpdateService:
    def __init__(
        self,
        assets_config: Optional[dict],
        filters_service: TwoGisProFiltersService,
        api_client: TwoGisProAPIClient,
    ):
        if not assets_config:
            raise ValueError('Pro assets config missing!')
        self._filters_service = filters_service
        self._api_client = api_client
        self._assets_config = assets_config
        self._assets = {
            'frames': self._filters_service.get_frames_filters_update_payload,
            'objects': self._filters_service.get_objects_filters_update_payload,
        }

    def sync(self) -> bool:
        results = []
        for asset, asset_payload in self.get_assets_mapping().items():
            success = self._sync(asset.name, payload=asset_payload)
            results.append(success)
        return all(results)

    def get_assets_mapping(self) -> dict[Asset, dict]:
        return {
            Asset(
                kind=asset_kind,
                name=self._assets_config.get(asset_kind),
            ): asset_payload_generator()
            for asset_kind, asset_payload_generator in self._assets.items()
            if self._assets_config.get(asset_kind)
        }

    def _sync(self, asset_name: str, payload: dict) -> bool:
        success = True
        try:
            status_code, body = self._api_client.update_filters(
                asset_name=asset_name,
                payload=payload,
            )
        except Exception as exc:
            logging.exception(f'Unable to update filters in PRO: {exc}')
            success = False
        else:
            if status_code != 200:
                logging.error(f'Filters update requests failed: {status_code=}, {body=}')
                success = False

        return success
