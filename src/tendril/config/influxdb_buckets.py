
from tendril.utils.config import ConfigOption
from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)

depends = ['tendril.config.core',
           'tendril.config.influxdb']


def _bucket_config_template(bucket_name):
    return [
        ConfigOption(
            'INFLUXDB_{}_BUCKET'.format(bucket_name.upper()),
            "'{}'".format(bucket_name),
            "InfluxDB Bucket name for {} data".format(bucket_name)
        ),
        ConfigOption(
            'INFLUXDB_{}_TOKEN'.format(bucket_name.upper()),
            "''",
            "InfluxDB Token to with with the {} data bucket".format(bucket_name),
            masked=True
        ),
    ]


def load(manager):
    logger.debug("Loading {0}".format(__name__))
    config_elements_influxdb_buckets = []
    for code in manager.INFLUXDB_BUCKETS:
        config_elements_influxdb_buckets += _bucket_config_template(code)
    manager.load_elements(config_elements_influxdb_buckets,
                          doc="Tendril InfluxDB Buckets Configuration")
