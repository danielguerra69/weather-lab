import logging
import requests
import gzip
import io
import xml.etree.ElementTree as ET
from datetime import datetime
from multiprocessing import Queue
import json
from translators.metar_translator import MetarTranslator
import time
from enrichers.solar_system_influence import SolarSystemInfluence

class MetarDataSource:
    def __init__(self, config_path, queue: Queue):
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
        self.url = config['url']
        self.queue = queue
        self.translator = MetarTranslator()
        self.solar_system_influence = SolarSystemInfluence()
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    def fetch_data(self):
        response = requests.get(self.url)
        if response.status_code == 200:
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
                    return gz.read().decode('utf-8')
            except EOFError:
                raise Exception("Compressed file ended before the end-of-stream marker")
        else:
            raise Exception(f"Failed to fetch METAR data: {response.status_code}")

    def parse_data(self, data):
        root = ET.fromstring(data)
        for metar in root.findall('.//METAR'):
            try:
                station_id = metar.find('station_id').text if metar.find('station_id') is not None else None
                observation_time = metar.find('observation_time').text if metar.find('observation_time') is not None else None
                time_iso = datetime.strptime(observation_time, "%Y-%m-%dT%H:%M:%SZ").isoformat() if observation_time else None
                latitude = self.convert_to_float(metar.find('latitude').text) if metar.find('latitude') is not None else None
                longitude = self.convert_to_float(metar.find('longitude').text) if metar.find('longitude') is not None else None
                
                if latitude is None or longitude is None or not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
                    raise ValueError(f"Invalid latitude or longitude: {latitude}, {longitude}")

                record = {
                    "time": time_iso,
                    "station_id": station_id,
                    "latitude": latitude,
                    "longitude": longitude,
                    "visibility": self.convert_to_float(metar.find('visibility_statute_mi').text) if metar.find('visibility_statute_mi') is not None else None,
                    "wind_speed": self.convert_to_int(metar.find('wind_speed_kt').text) if metar.find('wind_speed_kt') is not None else None,
                    "wind_dir": self.convert_to_int(metar.find('wind_dir_degrees').text) if metar.find('wind_dir_degrees') is not None else None,
                    "pressure": self.convert_to_float(metar.find('altim_in_hg').text) * 33.8639 if metar.find('altim_in_hg') is not None else None,
                    "temperature": self.convert_to_float(metar.find('temp_c').text) if metar.find('temp_c') is not None else None,
                    "dewpoint": self.convert_to_float(metar.find('dewpoint_c').text) if metar.find('dewpoint_c') is not None else None,
                }
                translated_data = self.translator.translate(record)
                # Set light_intensity

                light_intensity = self.solar_system_influence.get_light_intensity_at_location(translated_data)
                
                translated_data["light_intensity"] = float(light_intensity)  # Ensure float

                logging.debug(f"Enriched record: {translated_data}")

                # Add gravitational influence enrichment
                influence_data = self.solar_system_influence.get_influence_at_location(translated_data)
                translated_data.update(influence_data)

                self.queue.put(translated_data)
            except Exception as e:
                logging.debug(f"Error parsing METAR data: {e}")

    def convert_to_float(self, value):
        try:
            return float(value.replace('+', '').replace('M', '-'))
        except ValueError:
            return None

    def convert_to_int(self, value):
        try:
            return int(value.replace('+', '').replace('M', '-'))
        except ValueError:
            return None

    def run(self):
        while True:
            try:
                raw_data = self.fetch_data()
                logging.info("METAR weather data fetched successfully.")
                if raw_data:
                    self.parse_data(raw_data)
            except Exception as e:
                logging.info(f"Error fetching or parsing METAR data: {e}")
            time.sleep(60)  # Wait for 60 seconds before fetching data again