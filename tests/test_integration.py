import unittest
import os
from unittest import skipIf
from multiprocessing import Queue

from data_sources.cmems_data import CmemsDataSource
from data_sources.metar_data import MetarDataSource
from data_sources.space_weather_data import SpaceWeatherDataSource
from data_sources.aircraft_data import AircraftDataSource

SKIP_INTEGRATION_TESTS = not os.path.exists("/run/secrets/cmems_username")

@skipIf(SKIP_INTEGRATION_TESTS, "Skipping integration test because Docker secrets are not available.")
class TestIntegration(unittest.TestCase):
    def test_fetch_real_data(self):
        queue = Queue()

        # Adjust config paths below if needed
        cmems_config_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'config', 'cmems_config.json')
        metar_config_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'config', 'metar_config.json')
        space_weather_config_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'config', 'space_weather_config.json')
        aircraft_config_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'config', 'aircraft_config.json')

        # Instantiate data sources with real configs
        cmems_ds = CmemsDataSource(config_path=cmems_config_path, queue=queue)
        metar_ds = MetarDataSource(config_path=metar_config_path, queue=queue)
        space_weather_ds = SpaceWeatherDataSource(config_path=space_weather_config_path, queue=queue)
        aircraft_ds = AircraftDataSource(config_path=aircraft_config_path, queue=queue)

        # Run each data source
        cmems_ds.run()
        metar_ds.run()
        space_weather_ds.run()
        aircraft_ds.run()

        # Collect and print up to 100 records from the queue
        record_count = 0
        while record_count < 100:
            record = queue.get()
            if record is None:
                break
            print(record)
            record_count += 1

        print(f"Fetched and printed {record_count} records.")
        print("Integration test ran successfully.")

if __name__ == '__main__':
    unittest.main()