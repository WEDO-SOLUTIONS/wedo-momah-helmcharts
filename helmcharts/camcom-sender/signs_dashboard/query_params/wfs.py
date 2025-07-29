import typing as tp
from dataclasses import dataclass


class RequestValidationError(Exception):
    def __init__(self, message):
        super().__init__(self)
        self.message = message


AVAILABLE_COORDINATE_SYSTEM = ('epsg:3395', 'epsg:4326')
AVAILABLE_OUTPUT_FORMAT = ('gml2', 'application/json')


def _get_arg_value(request, value_name, default=''):
    return request.args.get(value_name, default).lower()


@dataclass
class WfsQueryParams:
    service: str
    version: str
    request: str
    srs_name: str
    crs_name: str
    max_features: int
    feature_id: tp.Optional[int]
    type_names: tp.Optional[str]
    bbox: tp.Optional[tp.List[float]]
    output_format: tp.Optional[str]

    @property
    def requested_type_name(self):
        if self.type_names == 'similar_tracks':
            return 'similar_tracks'
        if not self.type_names and self.bbox:
            return 'by_bbox'
        if self.type_names == 'detected_objects_with_detections':
            return 'detected_objects_with_detections'
        return None

    @classmethod
    def from_request(cls, request):
        return cls(
            service=_get_arg_value(request, 'SERVICE'),
            version=_get_arg_value(request, 'VERSION'),
            request=_get_arg_value(request, 'REQUEST'),
            srs_name=_get_arg_value(request, 'srsName'),
            crs_name=_get_arg_value(request, 'crsName'),
            feature_id=_get_arg_value(request, 'featureID'),
            type_names=_get_arg_value(request, 'typeNames'),
            bbox=_get_arg_value(request, 'BBOX').split(',') if _get_arg_value(request, 'BBOX') else [],
            max_features=_get_arg_value(request, 'MaxFeatures', '1000'),
            output_format=_get_arg_value(request, 'outputFormat', 'application/json'),
        ).validate()

    def validate(self):
        self._cast_not_str_types()
        self._validate_base_params()
        self._validate_route_params()
        return self

    def _validate_base_params(self):
        if self.service != 'wfs' or self.version != '2.0.0':
            raise RequestValidationError('Only WFS 2.0.0 is supported')

        if self.request != 'getfeature':
            raise RequestValidationError('WFS methods except "getFeature" are not implemented')

        if self.srs_name not in AVAILABLE_COORDINATE_SYSTEM or self.crs_name not in AVAILABLE_COORDINATE_SYSTEM:
            raise RequestValidationError('Only world mercator projection is supported')

        if self.output_format not in AVAILABLE_OUTPUT_FORMAT:
            raise RequestValidationError('Not supported output format')

    def _validate_route_params(self):
        if not self.requested_type_name:
            raise RequestValidationError('Unknown request type')

        if not self.bbox and self.requested_type_name in {'by_bbox', 'detected_objects_with_detections'}:
            raise RequestValidationError('bbox request without bbox')

        if self.requested_type_name == 'similar_tracks' and not self.feature_id:
            raise RequestValidationError('For similar_tracks should be defined featureID parameter')

        if self.bbox and len(self.bbox) != 4:
            raise RequestValidationError('Wrong BBOX declaration')

    def _cast_not_str_types(self):
        self.max_features = int(self.max_features)
        if self.feature_id:
            self.feature_id = int(self.feature_id)
        if self.bbox:
            self.bbox = list(map(float, self.bbox))
