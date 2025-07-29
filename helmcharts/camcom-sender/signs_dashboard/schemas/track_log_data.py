import json
from dataclasses import dataclass
from types import MappingProxyType
from typing import Set

from signs_dashboard.schemas.fiji.response import FijiStatistics

AUDITED_SIGN_CLASSES = MappingProxyType({
    1773: 'SpeedLimitSigns',
    1774: 'EndSpeedLimitSigns',
    1775: 'EndAllLimitSigns',
    1776: 'BuiltUpAreaSigns',
    1777: 'EndOfBuiltUpAreaSigns',
    1779: 'PedestrianCrossingSigns',
    1780: 'SpeedBumpSigns',
    1781: 'LaneDirectionSigns',
    3264: 'HighwaySigns',
    3265: 'EndOfHighwaySigns',
})
NOT_AUDITED_SIGN_CLASSES = MappingProxyType({
    1778: 'UTurnSigns',
    1859: 'NoEntrySigns',
    2763: 'ProhibitionSigns',
    2764: 'MandatorySigns',
    2765: 'OneWayTrafficStartSign',
    2766: 'OneWayTrafficEndSign',
    2762: 'LaneForRouteVehicles',
    2818: 'EndOfLaneForRouteVehicles',
    3262: 'RoadForRouteVehicles',
    3263: 'EndOfRoadForRouteVehicles',
    3266: 'CarRoadSigns',
    3267: 'EndOfCarRoadSigns',
    5071: 'LivingSectorEndRoadSign',
    5104: 'LivingSectorRoadSign',
})

TRUCK_SIGN_CLASSES = MappingProxyType({
    5015: 'MaximumVehicleLengthRoadSign',
    5016: 'MaximumHeightRoadSign',
    5017: 'MaximumWidthRoadSign',
    5018: 'NoHeavyGoodsVehiclesRoadSign',
    5019: 'MaximumWeightRoadSign',
    5020: 'MaximumWeightPerAxleRoadSign',
    5021: 'NoVehiclesCarryingDangerousGoodsRoadSign',
    5022: 'NoVehiclesCarryingExplosivesRoadSign',
})


def _extract_id(dicts):
    return {dict_['Id'] for dict_ in dicts}


@dataclass
class TrackStatisticsData:
    filtered_ids: Set[int]
    filtered_audited_ids: Set[int]
    filtered_truck_ids: Set[int]
    roads: Set[int]

    @classmethod
    def from_logs(cls, logs):
        filtered = json.loads(logs['FSMR_FilteredSignsOnRoads'])
        filtered = sum([road['Features'] for road in filtered], [])
        filtered_ids = _extract_id(filtered)
        filtered_audited_ids = _extract_id([
            filtered_sing for filtered_sing in filtered if filtered_sing['ClassId'] in AUDITED_SIGN_CLASSES
        ])
        filtered_truck_ids = _extract_id([
            filtered_sign for filtered_sign in filtered if filtered_sign['ClassId'] in TRUCK_SIGN_CLASSES
        ])
        roads = {match['RoadId'] for match in json.loads(logs['Match_MatchedRoads'])}

        return cls(filtered_ids, filtered_audited_ids, filtered_truck_ids, roads)

    @classmethod
    def from_fiji(cls, stats: FijiStatistics):
        return cls(
            {sign.id for sign in stats.filtered},
            {sign.id for sign in stats.filtered if sign.class_id in AUDITED_SIGN_CLASSES},
            {sign.id for sign in stats.filtered if sign.class_id in TRUCK_SIGN_CLASSES},
            set(stats.road_ids),
        )
