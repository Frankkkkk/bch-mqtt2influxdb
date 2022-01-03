from abc import ABC, abstractmethod
import logging

import influxdb


class TSDBClient(ABC):
    def __init__(self, config):
        pass

    @abstractmethod
    def create_db(self, dbname: str):
        pass

    @abstractmethod
    def write_points(self, points: list, database: str):
        pass

class InfluxDB(TSDBClient):
    def __init__(self, config):
        self.config = config
        self.influxdb = influxdb.InfluxDBClient(config['influxdb']['host'],
                                                 config['influxdb']['port'],
                                                 config['influxdb'].get('username', 'root'),
                                                 config['influxdb'].get('password', 'root'),
                                                 ssl=config['influxdb'].get('ssl', False))

    def create_db(self, dbname: str):
        logging.debug('InfluxDB create database %s', dbname)
        self.influxdb.create_database(dbname)
        self.influxdb.switch_database(dbname)

    def write_points(self, points: list, database: str):
        self._influxdb.write_points(points, database=database)
