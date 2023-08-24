

import datetime

from influxdb_client import InfluxDBClient
from influxdb_client import Point
from influxdb_client.client.write_api import ASYNCHRONOUS

from tendril.config import INFLUXDB_SERVER_HOST
from tendril.config import INFLUXDB_SERVER_PORT
from tendril.config import INFLUXDB_ORG
from tendril.config import INFLUXDB_DEFAULT_BUCKET
from tendril.config import INFLUXDB_DEFAULT_BUCKET_TOKEN


# This is simply copied over from the earlier implementation and is untested.
# In theory, this should essentially be a fire and forget async writer. In
# practice, not sure. This is probably Influx 1.x code.

class InfluxDBAsyncBurstWriter(object):
    def __init__(self, bucket=INFLUXDB_DEFAULT_BUCKET,
                 token=INFLUXDB_DEFAULT_BUCKET_TOKEN):
        self._url = "http://{0}:{1}".format(INFLUXDB_SERVER_HOST,
                                            INFLUXDB_SERVER_PORT)
        self._token = token
        self._bucket = bucket
        self._write_api = None
        self._points = []

    def write(self, measurement, fields, tags=None, ts=None):
        if not tags:
            tags = {}
        if not ts:
            ts = datetime.datetime.utcnow()

        _point = Point(measurement)

        for tag, value in tags.items():
            _point = _point.tag(tag, value)

        for field, value in fields.items():
            _point = _point.field(field, value)

        _point.time(ts)
        self._points.append(_point)

    def __enter__(self):
        self._client = InfluxDBClient(url=self._url, token=self._token,
                                      org=INFLUXDB_ORG)
        self._write_api = self._client.write_api(write_options=ASYNCHRONOUS)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _async_result = self._write_api.write(bucket=self._bucket,
                                              record=[self._points])
        self._client.close()


TSDBAsyncBurstWriter = InfluxDBAsyncBurstWriter
