import unittest
import os
import json
from main import WeatherLab, AircraftDataSource
from unittest.mock import MagicMock

class TestWeatherLab(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ['CMEMS_USERNAME'] = 'test_username'
        os.environ['CMEMS_PASSWORD'] = 'test_password'

    def test_run(self):
        weather_lab = WeatherLab(elasticsearch_storage=self.mock_elasticsearch_storage())
        weather_lab.run()

    def mock_elasticsearch_storage(self):
        mock_storage = MagicMock()
        mock_storage.index_data = self.mock_index_data
        return mock_storage

    def mock_index_data(self, data):
        print(json.dumps(data, indent=2))

    def test_aircraft_data_source(self):
        data_source = AircraftDataSource(config_path='src/config/aircraft_config.json', queue=MagicMock())
        data = data_source.fetch_data()
        self.assertIsNotNone(data)

if __name__ == '__main__':
    unittest.main()
