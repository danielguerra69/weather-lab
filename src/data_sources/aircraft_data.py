import requests
import gzip
import csv
from io import BytesIO
from multiprocessing import Queue
import time
import json
from translators.aircraft_translator import translate_row
import io
import xml.etree.ElementTree as ET
from datetime import datetime

class AircraftDataSource:
    def __init__(self, config_path, queue: Queue):
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
        self.url = config['base_url']
        self.queue = queue

    def fetch_data(self):
        response = requests.get(self.url)
        if response.status_code == 200:
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
                    return gz.read().decode('utf-8')
            except EOFError:
                raise Exception("Compressed file ended before the end-of-stream marker")
        else:
            raise Exception(f"Failed to fetch aircraft data: {response.status_code}")

    def parse_data(self, data):
        root = ET.fromstring(data)
        for aircraft_report in root.findall('.//AircraftReport'):
            try:
                row = {
                    'observation_time': aircraft_report.find('observation_time').text if aircraft_report.find('observation_time') is not None else None,
                    'latitude': aircraft_report.find('latitude').text if aircraft_report.find('latitude') is not None else None,
                    'longitude': aircraft_report.find('longitude').text if aircraft_report.find('longitude') is not None else None,
                    'altitude_ft_msl': aircraft_report.find('altitude_ft_msl').text if aircraft_report.find('altitude_ft_msl') is not None else None,
                    'wind_speed_kt': aircraft_report.find('wind_speed_kt').text if aircraft_report.find('wind_speed_kt') is not None else None,
                    'wind_dir_degrees': aircraft_report.find('wind_dir_degrees').text if aircraft_report.find('wind_dir_degrees') is not None else None,
                    'temp_c': aircraft_report.find('temp_c').text if aircraft_report.find('temp_c') is not None else None,
                    'turbulence_code': aircraft_report.find('turbulence_code').text if aircraft_report.find('turbulence_code') is not None else None,
                    'icing_code': aircraft_report.find('icing_code').text if aircraft_report.find('icing_code') is not None else None,
                    'visibility_statute_mi': aircraft_report.find('visibility_statute_mi').text if aircraft_report.find('visibility_statute_mi') is not None else None,
                    'aircraft_ref': aircraft_report.find('aircraft_ref').text if aircraft_report.find('aircraft_ref') is not None else None
                }
                translated_data = translate_row(row)
                self.queue.put(translated_data)
            except Exception as e:
                print(f"Error parsing aircraft data: {e}")

    def run(self):
        while True:
            try:
                raw_data = self.fetch_data()
                print("Aircraft weather data fetched successfully.")
                if raw_data:
                    self.parse_data(raw_data)
            except Exception as e:
                print(f"Error fetching or parsing aircraft data: {e}")
            time.sleep(60)  # Wait for 60 seconds before fetching data again
