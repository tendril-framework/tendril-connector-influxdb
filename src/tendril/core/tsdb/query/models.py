


from math import ceil
from typing import Dict
from typing import List
from pydantic import root_validator
from datetime import datetime
from datetime import timedelta
from tendril.utils.pydantic import TendrilTBaseModel
from tendril.core.tsdb.constants import TimeSeriesExporter


class QueryTimeSpanTModel(TendrilTBaseModel):
    start: datetime = None
    end: datetime = None
    width: timedelta = None
    window_count: int = 240
    window_width: timedelta = None

    @classmethod
    def _die(cls):
        raise ValueError("Something's wrong with the code. "
                         "Report with input provided.")

    @classmethod
    def _count_fixers(cls, keys, values):
        rv = []
        for key in keys:
            if isinstance(key, tuple):
                defined = True
                for subkey in key:
                    if not values.get(subkey, None):
                        defined = False
                if defined:
                    rv.append(key)
            elif values.get(key, None):
                rv.append(key)
        return rv

    @classmethod
    def _check_fixers(cls, values):
        _period_fixers = cls._count_fixers(
            (('start', 'end'), 'width',
             ('window_count', 'window_width')),
            values
        )

        _anchor_fixers = cls._count_fixers(
            ('start', 'end'), values
        )

        _window_fixers = cls._count_fixers(
            ('window_count', 'window_width'),
            values
        )

        over_constrained = False
        if len(_period_fixers) > 1:
            over_constrained = True
        if len(_anchor_fixers) > 1:
            if ('start', 'end') not in _period_fixers:
                over_constrained = True

        under_constrained = False
        if len(_period_fixers) < 1:
            under_constrained = True
        if len(_anchor_fixers) < 1:
            under_constrained = True
        if len(_window_fixers) < 1:
            under_constrained = True

        return over_constrained, under_constrained

    @classmethod
    def _fill_remainder(cls, values):
        period_key = cls._count_fixers(
            (('start', 'end'), 'width',
             ('window_count', 'window_width')),
            values
        )[0]

        anchor_keys = cls._count_fixers(
            ('start', 'end'), values
        )

        match period_key:
            case ('start', "end"):
                period = values["end"] - values["start"]
            case 'width':
                period = values['width']
            case ('window_count', 'window_width'):
                period = values['window_count'] * values['window_width']
            case _:
                cls._die()

        if period.total_seconds() <= 0:
            raise ValueError("The time span seems to be negative or zero. Ensure start_time is before end_time.")

        if period_key != 'width':
            values['width'] = period

        if len(anchor_keys) < 2:
            if 'end' not in anchor_keys:
                values['end'] = values['start'] + period
            if 'start' not in anchor_keys:
                values['start'] = values['end'] - period

        if not values['window_count']:
            values['window_count'] = int(ceil(period / values['window_width']))
        if not values['window_width']:
            values['window_width'] = period / values['window_count']

        return values

    @root_validator()
    def span_validation(cls, values):
        over_constrained, under_constrained = cls._check_fixers(values)

        if over_constrained:
            raise ValueError("Query Time Span is over-constrained!")

        while under_constrained:
            if not values.get('end', None):
                values['end'] = datetime.now()
            elif not values.get('width', None):
                values['width'] = timedelta(days=1)
            over_constrained, under_constrained = cls._check_fixers(values)
            if over_constrained:
                cls._die()

        values = cls._fill_remainder(values)
        return values


class TimeSeriesQueryItemTModel(TendrilTBaseModel):
    domain: str
    time_span: QueryTimeSpanTModel = QueryTimeSpanTModel()
    export_name: str
    measurement: str
    tags: Dict[str, str]
    fields: List[str]
    exporter: TimeSeriesExporter
