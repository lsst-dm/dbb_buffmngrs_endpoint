# These are the monitoring routines for DBB

from datetime import datetime
import yaml
import influxdb
from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError
import requests
import random
import logging

logger = logging.getLogger(__name__)


class DbbMonitor:

    def __init__(self, configfile="../../../../../etc/monitoring.yaml"):
        self.dbb_interval = {}
        with open(configfile) as fp:
            configuration = yaml.safe_load(fp)
        self.mon_config = configuration.get("monitoring", None)
        self.username = self.mon_config.get("username", None)
        self.password = self.mon_config.get("password", None)
        self.host = self.mon_config.get("host", None)
        self.port = self.mon_config.get("port", None)
        self.MONFLAG = 1
        try:
            self.client = influxdb.InfluxDBClient(host=self.host, port=self.port,
                                              username=self.username, password=self.password,
                                              ssl=True, verify_ssl=False)

            print(f"self.client.get_list_database = {self.client.get_list_database()}")

            if 'dbbmonitor' not in self.client.get_list_database():
                self.client.create_database('dbbmonitor')
            self.client.switch_database('dbbmonitor')

        except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout,
                    InfluxDBClientError, InfluxDBServerError) as err:
            logger.warning(f"received influxDB error {err}")
            self.MONFLAG = 0
            pass

    def test_data(self):
        if self.MONFLAG:
            json_data = [
                {
                    "measurement": "fileEvent",
                    "tags": {
                        "file_path": "/path/to/test/file",
                        "file_tag": "XXXXX-XX-XXXXX"
                    },
                    "time": datetime.utcnow(),
                    "fields": {
                        "duration": random.randint(0, 999)
                    }
                }
            ]
            self.client.write_points(json_data)

    def read_test_data(self):
        if self.MONFLAG:
            print("query results: {}".format(
                self.client.query('SELECT "duration" from "dbbmonitor"."autogen"."fileEvent"')))

    def send_data(self, json):
        if self.MONFLAG:
            try:
                self.client.write_points(json)
            except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout,
                    InfluxDBClientError, InfluxDBServerError) as err:
                logger.warning(f"received influxDB error {err}")
                self.MONFLAG = 0
                pass

    def register_start(self, path):
        if self.MONFLAG:
            now = datetime.utcnow()
            self.dbb_interval[path] = now

    def report_interval(self, path, mon_key):
        if self.MONFLAG:
            start = self.dbb_interval[path]
            end = datetime.utcnow()
            json_body = [
                {
                    "measurement": mon_key,
                    "tags": {
                        "file_path": path
                    },
                    "time": start,
                    "fields": {
                        "duration": (end-start)
                    }
                }
            ]
            self.send_data(json_body)

    def report_point(self, mon_key, mon_tags, mon_fields):
        if self.MONFLAG:
            now = datetime.utcnow()
            json_body = [
                {
                    "measurement": mon_key,
                    "tags": mon_tags,
                    "time": now,
                    "fields": mon_fields
                }
            ]
            self.send_data(json_body)


DbbM = DbbMonitor()


if __name__ == "__main__":
    DbbM.conf_load("./finder.yaml")
    DbbM.test_data()
    DbbM.read_test_data()
