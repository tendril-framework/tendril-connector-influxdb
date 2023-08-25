

import polars
from functools import partial
from typing import List

from tendril import config
from tendril.config import INFLUXDB_BUCKETS
from tendril.core.tsdb.query.models import TimeSeriesQueryItemTModel
from tendril.core.tsdb.query.models import QueryTimeSpanTModel
from tendril.core.tsdb.constants import TimeSeriesExporter


def _get_bucket(domain):
    return getattr(config, f'INFLUXDB_{domain.upper()}_BUCKET')

_buckets = {x: _get_bucket(x) for x in INFLUXDB_BUCKETS}


class InfluxDBFluxQueryBuilder(object):
    _extra_columns = []
    _strategy = None

    def __init__(self, params: TimeSeriesQueryItemTModel,
                 include_open=True, include_close=True,
                 lone_value=False):
        if not lone_value:
            raise NotImplementedError(
                "These query builders presently assume each record holds a single value, "
                "and is present in the field 'value'. This is how tendril interest monitors "
                "currently store data in influxdb. If the data you are querying follows this "
                "pattern, then set lone_value=True when instantiating this builder. However, "
                "this is not the regular way data is stored in influxdb. If the data you are "
                "querying does not do this, then the builders will need to be written to allow "
                "this.")
        self._params = params
        self.bucket = params.domain
        self.time_span = params.time_span
        self._simple_filters = []
        self._subqueries = []
        self.simple_filter('_measurement', params.measurement)

        if include_open:
            self._subqueries = [
                ('openValue', (partial(self._render_selectors, range='before'),
                               ' |> last()\n')),
                ('rangeValues', (self._render_selectors,))
            ]

    @property
    def bucket(self):
        return self._bucket

    @bucket.setter
    def bucket(self, value):
        self._bucket = _buckets[value]

    def _render_bucket(self):
        return f'from(bucket: "{self._bucket}")\n'

    @property
    def time_span(self) -> QueryTimeSpanTModel:
        return self._time_span

    @time_span.setter
    def time_span(self, value):
        self._time_span = value

    def _render_range(self, range=None):
        if range == 'before':
            return f' |> range(start: -inf, stop: {int(self._time_span.start.timestamp())})\n'
        return f' |> range(start: {int(self._time_span.start.timestamp())}, stop: {int(self._time_span.end.timestamp())})\n'

    def simple_filter(self, key, value):
        self._simple_filters.append((key, value))

    def _render_simple_filter(self, key, value):
        return f' |> filter(fn: (r) => r["{key}"] == "{value}")\n'

    def _render_simple_filters(self):
        rv = ""
        for key, value in self._simple_filters:
            rv += self._render_simple_filter(key, value)
        return rv

    def _render_aggregator(self, aggregator):
        rv = f' |> aggregateWindow(every: {int(self.time_span.window_width.total_seconds())}s, fn: {aggregator}, createEmpty: false)\n'
        return rv

    def _reshape_output(self):
        pass

    def _render_selectors(self, range=None):
        rv = self._render_bucket()
        rv += self._render_range(range=range)
        if self._simple_filters:
            rv += self._render_simple_filters()
        return rv

    def _render_union(self):
        return f'union(tables: [{", ".join([x[0] for x in self._subqueries])}])\n'

    def _render_logic(self):
        return ''

    def build(self):
        if not self._subqueries:
            rv = self._render_selectors()
            rv += self._render_logic()
            rv += self._reshape_output()
            return rv

        rv = ''
        for subquery, components in self._subqueries:
            rv += f'{subquery} = '
            for component in components:
                if callable(component):
                    rv += component()
                else:
                    rv += component
            rv += '\n'
        rv += self._render_union()
        rv += self._render_logic()
        rv += self._reshape_output()
        return rv

    want_data_frame = False

    def repacker(self, response):
        return response.to_values(columns=['_time', self._params.measurement])

    @property
    def strategy(self):
        return self._strategy

    @property
    def response_columns(self):
        return []


class SimpleFluxQueryBuilder(InfluxDBFluxQueryBuilder):
    _strategy = TimeSeriesExporter.RAW

    def __init__(self, params, lone_value=False):
        super(SimpleFluxQueryBuilder, self).__init__(params, lone_value=lone_value)
        for key, value in params.tags.items():
            self.simple_filter(key, value)

    def _reshape_output(self):
        # The idea is for _extra_columns to survive in the response. Typically,
        # this would be used as a dataframe for postprocessing in python/polars.
        columns = ["_measurement", "_time", "_value"] + self._extra_columns
        columns_str = ", ".join([f'"{x}"' for x in columns])
        pivot_columns = ["_time"] + self._extra_columns
        pivot_columns_str = ", ".join([f'"{x}"' for x in pivot_columns])
        rv =  f' |> keep(columns: [{columns_str}])\n'
        rv += f' |> pivot(rowKey:[{pivot_columns_str}], columnKey: ["_measurement"], valueColumn: "_value")\n'
        rv += f' |> group()\n'
        rv += f' |> sort(columns: ["_time"], desc: false)\n'
        return rv

    @property
    def response_columns(self):
        return ['_time', self._params.export_name]


class ChangesOnlyFluxQueryBuilder(SimpleFluxQueryBuilder):
    _strategy = TimeSeriesExporter.CHANGES_ONLY
    want_data_frame = True

    def repacker(self, response):
        df = polars.from_pandas(response)
        colname = self._params.measurement
        df = df.with_columns(df[colname].shift(1).alias("prev_value"))
        df = df.with_columns(
            polars.when(polars.col(colname) != polars.col("prev_value"))
            .then(True)
            .otherwise(polars.col("_time") == df["_time"].max()).alias("keep"))
        df = df.filter(polars.col("keep"))
        df = df.drop(["prev_value", "keep", "result", "table"])
        return [row for row in df.rows()]


class DiscontinuitiesOnlyFluxQueryBuilder(SimpleFluxQueryBuilder):
    _strategy = TimeSeriesExporter.DISCONTINUITIES_ONLY
    want_data_frame = True
    step_size = (0, 150)
    _extra_columns = ['difference']

    def _render_logic(self):
        rv = f' |> group()\n'
        rv += f' |> duplicate(column: "_value", as: "difference")\n'
        rv += f' |> difference(columns: ["difference"], keepFirst: true)\n'
        rv += f' |> fill(column: "difference", value: 0)\n'
        return rv
    
    def repacker(self, response):
        df = polars.from_pandas(response)
        df = df.with_columns(polars.col("difference").shift(-1).alias("next_difference"))
        df = df.with_columns(
            polars.when((polars.col("difference") < self.step_size[0]) |
                       (polars.col("difference") > self.step_size[1]) |
                       (polars.col("next_difference") < self.step_size[0]) |
                       (polars.col("next_difference") > self.step_size[1]))
                  .then(True)
                  .otherwise((polars.col("_time") == df["_time"].max()) |
                             (polars.col("_time") == df["_time"].min()))
                  .alias("keep")
        )
        df = df.filter(polars.col("keep"))
        df = df.drop(["keep", "difference", "next_difference", "result", "table"])
        return [row for row in df.rows()]


class WindowedFluxQueryBuilder(InfluxDBFluxQueryBuilder):
    def __init__(self, common_tags):
        self._common_tags = common_tags
        self._inited = False
        self._items: List[TimeSeriesQueryItemTModel] = []
        self._channel_tables = []

    def add_item(self, params: TimeSeriesQueryItemTModel, lone_value=False):
        if not self._inited:
            super().__init__(params, include_open=False, lone_value=lone_value)
        if not params.domain == self._params.domain:
            raise ValueError("We require all windowed queries to have the same domain.")
        if not params.time_span == self._time_span:
            raise ValueError("We require all windowed queries to have the same time span")
        if not lone_value:
            raise ValueError("We require all windowed queries to have lone_value type records")
        self._items.append(params)

    def _render_channel_selectors(self, params: TimeSeriesQueryItemTModel, range=None):
        rv = self._render_bucket()
        rv += self._render_range(range=range)
        rv += self._render_simple_filter('_measurement', params.measurement)
        for key, value in params.tags.items():
            rv += self._render_simple_filter(key, value)
        return rv

    def _render_channel_aggregator(self, params: TimeSeriesQueryItemTModel):
        match params.exporter:
            case TimeSeriesExporter.WINDOWED_MEAN:
                aggregator = 'mean'
            case TimeSeriesExporter.WINDOWED_SUMMATION:
                aggregator = 'sum'
            case _:
                raise NotImplementedError("We only presently support mean and sum aggregators")
        rv = self._render_aggregator(aggregator)
        return rv

    def _render_channel_filler(self, params: TimeSeriesQueryItemTModel):
        rv = ''
        # if params.exporter == TimeSeriesExporter.WINDOWED_MEAN:
        #     rv += " |> fill(usePrevious: true)\n"
        # else:
        #     rv += " |> fill(value: 0)\n"
        return rv

    def _reintegrate_channel_data(self, params: TimeSeriesQueryItemTModel):
        open_value = f'{params.export_name.replace(".", "_")}_openValue'
        range_values = f'{params.export_name.replace(".", "_")}_rangeValues'
        rv = f'{params.export_name.replace(".", "_")} = '
        rv += f'union(tables: [{open_value}, {range_values}])\n'
        rv += ' |> group()\n'
        return rv

    def _prepare_channel_integration(self, params):
        rv = f' |> set(key: "name", value:"{params.export_name}")\n'
        # rv += f' |> rename(columns: {{_value: "{params.export_name}"}})\n'
        rv += f' |> keep(columns: ["_time", "_value", "name"])\n'
        return rv

    def _render_channel(self, params: TimeSeriesQueryItemTModel):
        rv = f'{params.export_name.replace(".", "_")}_openValue = '
        rv += self._render_channel_selectors(params, range='before')
        rv += ' |> last()\n'
        rv += f' |> toFloat()\n\n'

        rv += f'{params.export_name.replace(".", "_")}_rangeValues = '
        rv += self._render_channel_selectors(params)
        rv += self._render_channel_aggregator(params)
        rv += self._render_channel_filler(params)
        rv += f' |> toFloat()\n\n'

        rv += self._reintegrate_channel_data(params)
        rv += self._prepare_channel_integration(params)
        # rv += f' |> toFloat()\n\n'

        self._channel_tables.append(params.export_name.replace(".", "_"))
        return rv

    def _render_channels(self):
        rv = ''
        for item in self._items:
            rv += self._render_channel(item)
        return rv

    def _render_channels_union(self):
        rv = f'union(tables: [{", ".join(self._channel_tables)}])\n'
        return rv

    def _reshape_output(self):
        rv = f' |> group(columns: ["_time"], mode: "by")\n'
        rv += f' |> pivot(rowKey: ["_time"], columnKey: ["name"], valueColumn: "_value")\n'
        rv += f' |> group()\n'
        rv += f' |> sort(columns: ["_time"], desc: false)\n'
        return rv

    def build(self):
        rv = self._render_channels()
        rv += self._render_channels_union()
        rv += self._reshape_output()
        return rv

    def repacker(self, response):
        return response.to_values(columns=self.response_columns)

    @property
    def strategy(self):
        return {x.export_name: x.exporter for x in self._items}

    @property
    def response_columns(self):
        return ['_time'] + [x.export_name for x in self._items]
