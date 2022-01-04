from abc import ABC, abstractmethod
import logging

import influxdb
import opentsdb


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
        logging.debug('InfluxDB will send %s on %s', points, database)
        self._influxdb.write_points(points, database=database)


class OpenTSDB(TSDBClient):
    def __init__(self, config):
        self.config = config
        self._opentsdb = opentsdb.TSDBClient(
            config['opentsdb']['host'],
            port=config['opentsdb'].get('port', 4242)
        )

    def create_db(self, dbname: str):
        pass

    def write_points(self, points: list, database: str):
        for point in points:
            name = point['measurement']
            value = point['fields']['value']
            tags = point['tags']
            logging.debug('OpenTSDB will %s = %s (with tags %s)', name, value, tags)
            self._opentsdb.send(name, value, **tags)
