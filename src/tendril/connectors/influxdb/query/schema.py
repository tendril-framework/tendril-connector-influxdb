

from .builder import InfluxDBFluxQueryBuilderBase


class DistinctTagsFluxQueryBuilder(InfluxDBFluxQueryBuilderBase):
    _strategy = 'DistinctTagsExtraction'
    want_data_frame = True

    def __init__(self, bucket, measurement, field, tag,
                 filters=None, time_span=None):
        super().__init__()
        self._bucket = bucket
        self._measurement = measurement
        self._tag = tag
        self.time_span = time_span
        self.simple_filter('_measurement', measurement)
        for k, v in filters.items():
            self.simple_filter(k, v)
        if field:
            self.simple_filter('_field', field)

    def _build_unfiltered(self):
        rv = 'import "influxdata/influxdb/schema"\n\n'
        rv += 'schema.measurementTagValues(\n'
        rv += f'  bucket: "{self._bucket}",\n'
        rv += f'  measurement: "{self._measurement}",\n'
        rv += f'  tag: "{self._tag}",\n'
        if self.time_span:
            rv += f'  start: {int(self.time_span.start.timestamp())},\n'
            rv += f'  stop: {int(self.time_span.end.timestamp())},\n'
        rv += f')\n\n'
        rv += f' |> distinct(column: "_value")\n'
        return rv

    def _build_filtered(self):
        rv = ''
        rv += self._render_selectors()
        rv += f' |> group(columns: ["{self._tag}"])\n'
        rv += f' |> distinct(column: "{self._tag}")\n'
        return rv

    def build(self):
        if not self._simple_filters:
            return self._build_unfiltered()
        else:
            return self._build_filtered()

    def repacker(self, response):
        return response['_value'].to_list()
