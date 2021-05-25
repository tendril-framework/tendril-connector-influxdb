# Copyright (C) 2019 Chintalagiri Shashank
#
# This file is part of Tendril.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
InfluxDB Configuration Options
==============================
"""


from tendril.utils.config import ConfigOption
from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)

depends = ['tendril.config.core']


config_elements_influxdb = [
    ConfigOption(
        'INFLUXDB_SERVER_HOST',
        "'localhost'",
        "InfluxDB Server Host"
    ),
    ConfigOption(
        'INFLUXDB_SERVER_PORT',
        "8086",
        "InfluxDB Server Port"
    ),
    ConfigOption(
        'INFLUXDB_SERVER_ORG',
        "'tendril'",
        "InfluxDB Organization to use. All InfluxDB Connections from tendril "
        "will use this organization host unless locally overridden in some "
        "as yet unspecified way."
    ),
]


def load(manager):
    logger.debug("Loading {0}".format(__name__))
    manager.load_elements(config_elements_influxdb,
                          doc="Tendril InfluxDB Configuration")
