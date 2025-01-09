from data_sources.cmems_data import CmemsDataSource
from data_sources.metar_data import MetarDataSource
from data_sources.space_weather_data import SpaceWeatherDataSource
from data_sources.meteostat_data import MeteostatDataSource
from data_sources.aircraft_data import AircraftDataSource
from storage.elasticsearch import ElasticsearchStorage
import multiprocessing
import threading
import logging
import json
import time
import asyncio

class WeatherLab:
    def __init__(self, elasticsearch_storage=None):
        self.queue = multiprocessing.Queue()
        self.cmems_data_source = CmemsDataSource(config_path='src/config/cmems_config.json', queue=self.queue)
        self.metar_data_source = MetarDataSource(config_path='src/config/metar_config.json', queue=self.queue)
        self.space_weather_data_source = SpaceWeatherDataSource(config_path='src/config/space_weather_config.json', queue=self.queue)
        self.meteostat_data_source = MeteostatDataSource(config_path='src/config/meteostat_config.json', queue=self.queue)
        self.aircraft_data_source = AircraftDataSource(config_path='src/config/aircraft_config.json', queue=self.queue)
        
        self.elasticsearch_storage = elasticsearch_storage or ElasticsearchStorage(es_url='http://weather-lab-elasticsearch:9200')
        self.bulk_records = []
        self.bulk_size = 1000

    def fetch_and_process_data(self, data_source):
        data_source.run()

    async def run(self):
        threads = [
            threading.Thread(target=self.fetch_and_process_data, args=(self.cmems_data_source,)),
            threading.Thread(target=self.fetch_and_process_data, args=(self.metar_data_source,)),
            threading.Thread(target=self.fetch_and_process_data, args=(self.space_weather_data_source,)),
            threading.Thread(target=self.fetch_and_process_data, args=(self.meteostat_data_source,)),
            threading.Thread(target=self.fetch_and_process_data, args=(self.aircraft_data_source,))
        ]

        for thread in threads:
            thread.start()

        while True:
            record = self.queue.get()
            if record is None:
                break
            # logging.info(f"Processing record: {record}")
            self.bulk_records.append(record)
            if len(self.bulk_records) >= self.bulk_size:
                await self.elasticsearch_storage.bulk_index_data(self.bulk_records)
                self.bulk_records = []

        # Index any remaining records
        if self.bulk_records:
            await self.elasticsearch_storage.bulk_index_data(self.bulk_records)

        for thread in threads:
            thread.join()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    weather_lab = WeatherLab()
    asyncio.run(weather_lab.run())