

import asyncio
import importlib
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from .query.planner import InfluxDBQueryPlanner

from tendril import config
from tendril.config import INFLUXDB_SERVER_HOST
from tendril.config import INFLUXDB_SERVER_PORT
from tendril.config import INFLUXDB_ORG
from tendril.config import INFLUXDB_BUCKETS

from tendril.utils import log
logger = log.get_logger(__name__, log.DEBUG)

import warnings
# from influxdb_client.client.warnings import MissingPivotFunction
# warnings.simplefilter("ignore", MissingPivotFunction)


# TODO There is no apparent connection pooling going on here.

_influxdb_url = f"http://{INFLUXDB_SERVER_HOST}:{INFLUXDB_SERVER_PORT}"


def _get_connection_parameters(domain):
    return {
        'url': _influxdb_url,
        'token': getattr(config, f'INFLUXDB_{domain.upper()}_TOKEN'),
        'org': INFLUXDB_ORG
    }

_connection_parameters = {x: _get_connection_parameters(x)
                          for x in INFLUXDB_BUCKETS}


async def influxdb_execute_query(client, query, want_data_frame=False):
    # TODO Investigate Query Profiler.
    #  https://github.com/influxdata/influxdb-client-python#profile-query
    query_api = client.query_api()
    logger.debug(f"Executing query : \n{query}")
    if want_data_frame:
        result = await query_api.query_data_frame(query)
    else:
        result = await query_api.query(query)
    return result


async def influxdb_execute_query_plan(plan: InfluxDBQueryPlanner):
    rv = {}
    for domain in plan.query_domains():
        rv[domain] = {}
        kwargs = _connection_parameters[domain]
        async with (InfluxDBClientAsync(**kwargs) as client):
            for name, builder in plan.generate_queries(domain):
                response = await influxdb_execute_query(
                    client, query=builder.build(),
                    want_data_frame=builder.want_data_frame)
                rv[domain][name] = {'strategy': builder.strategy,
                                    'columns': builder.response_columns,
                                    'data': builder.repacker(response)}
    return rv
