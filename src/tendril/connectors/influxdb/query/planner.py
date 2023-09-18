

from tendril.core.tsdb.constants import TimeSeriesExporter
from tendril.core.tsdb.query.models import TimeSeriesQueryItemTModel

from .builder import SimpleFluxQueryBuilder
from .builder import ChangesOnlyFluxQueryBuilder
from .builder import DiscontinuitiesOnlyFluxQueryBuilder
from .builder import WindowedFluxQueryBuilder
from .builder import AggregatedFluxQueryBuilder


def intersect_dicts(a, b):
    matching_keys = a.keys() & b.keys()
    rv = {}
    for key in matching_keys:
        if a[key] == b[key]:
            rv[key] = a[key]
    return rv


def subtract_dicts(a, b):
    # b is guaranteed to have 100% overlap with a
    for key in b.keys():
        a.pop(key)
    return a


class InfluxDBQueryPlanner(object):
    def __init__(self):
        self._items = {}
        self._time_span = None
        self._common_tags = None
        self._windowed_builder = None

    def add_item(self, item: TimeSeriesQueryItemTModel):
        if not self._time_span:
            self._time_span = item.time_span
        else:
            if self._time_span != item.time_span:
                raise ValueError(f"{self.__class__.__name__} does not support different time spans. "
                                 f"Use a separate instance of the planner instead. If there is a "
                                 f"clearly articulated use case for this, implementation may be "
                                 f"considered.")
        if self._common_tags is None:
            self._common_tags = item.tags
        else:
            self._common_tags = intersect_dicts(self._common_tags, item.tags)
        if item.domain not in self._items:
            self._items[item.domain] = {}
        if item.exporter not in self._items[item.domain].keys():
            self._items[item.domain][item.exporter] = []
        self._items[item.domain][item.exporter].append(item)

    def query_domains(self):
        for domain in self._items.keys():
            yield domain

    def _special_tags(self, tags):
        return subtract_dicts(tags, self._common_tags)

    def generate_queries(self, domain):
        windowed_items = []
        for exporter, items in self._items[domain].items():

            if exporter == TimeSeriesExporter.CHANGES_ONLY:
                for item in items:
                    builder = ChangesOnlyFluxQueryBuilder(item, lone_value=True)
                    yield item.export_name, builder

            elif exporter == TimeSeriesExporter.DISCONTINUITIES_ONLY:
                for item in items:
                    builder = DiscontinuitiesOnlyFluxQueryBuilder(item, lone_value=True)
                    yield item.export_name, builder

            elif exporter == TimeSeriesExporter.RAW:
                for item in items:
                    builder = SimpleFluxQueryBuilder(item, lone_value=True)
                    yield item.export_name, builder

            elif exporter in (TimeSeriesExporter.AGGREGATE_MEAN,
                              TimeSeriesExporter.AGGREGATE_SUM,
                              TimeSeriesExporter.AGGREGATE_BAND,
                              TimeSeriesExporter.AGGREGATE_COUNT):
                for item in items:
                    builder = AggregatedFluxQueryBuilder(item, lone_value=True)
                    yield item.export_name, builder

            elif exporter in (TimeSeriesExporter.WINDOWED_MEAN,
                              TimeSeriesExporter.WINDOWED_SUM,
                              TimeSeriesExporter.WINDOWED_BAND,
                              TimeSeriesExporter.WINDOWED_COUNT):
                for item in items:
                    windowed_items.append(item)

        if len(windowed_items):
            windowed_builder = WindowedFluxQueryBuilder(self._common_tags)
            for item in windowed_items:
                windowed_builder.add_item(item, lone_value=True)
            yield "windowed", windowed_builder
