import logging
import requests
import re
from datetime import datetime
from multiprocessing import Queue
import json
from translators.space_weather_translator import SpaceWeatherTranslator
import time
from enrichers.solar_system_influence import SolarSystemInfluence

class SpaceWeatherDataSource:
    def __init__(self, config_path, queue: Queue):
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
        self.url = config['url']
        self.queue = queue
        self.translator = SpaceWeatherTranslator()
        self.solar_system_influence = SolarSystemInfluence() 
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    def fetch_data(self):
        response = requests.get(self.url)
        if response.status_code == 200:
            try:
                return response.json()
            except json.JSONDecodeError:
                return response.text
        else:
            raise Exception(f"Failed to fetch space weather data: {response.status_code}")

    def parse_data(self, data):
        lines = data.splitlines()
        for line in lines:
            if line.startswith("#") or line.startswith(":") or not line.strip():
                continue
            try:
                record = self.decode_space_weather_line(line)
                translated_data = self.translator.translate(record)
                # Set light_intensity

                light_intensity = self.solar_system_influence.get_light_intensity_at_location(translated_data)
                
                translated_data["light_intensity"] = float(light_intensity)  # Ensure float

                logging.debug(f"Enriched record: {translated_data}")

                # Add gravitational influence enrichment
                influence_data = self.solar_system_influence.get_influence_at_location(translated_data)
                translated_data.update(influence_data)

                self.queue.put(translated_data)
            except ValueError as ve:
                logging.debug(f"Skipping line due to invalid data: {line}")
                logging.debug(f"Exception: {ve}")

    def decode_space_weather_line(self, line):
        line = re.sub(r'[ \t]+', ' ', line.strip())
        parts = line.split(' ')
        if len(parts) < 13:
            raise ValueError(f"Insufficient parts or invalid data: {line}")

        time_str = f"{parts[0]}-{parts[1]}-{parts[2]} {parts[3][:2]}:{parts[3][2:]}:00"
        time_iso = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S").isoformat()
        latitude = float(parts[11])
        longitude = float(parts[12]) - 180
        if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
            raise ValueError(f"Invalid latitude or longitude: {latitude}, {longitude}")
        if latitude == -999.9 or longitude == -999.9:
            raise ValueError(f"Invalid latitude or longitude: {latitude}, {longitude}")

        record = {
            "time": time_iso,
            "status": int(parts[6]),
            "bx": float(parts[7]),
            "by": float(parts[8]),
            "bz": float(parts[9]),
            "bt": float(parts[10]),
            "latitude": latitude,
            "longitude": longitude,
            "location": f"{latitude},{longitude}"
        }
        return record

    def run(self):
        while True:
            try:
                raw_data = self.fetch_data()
                logging.info("Space weather data fetched successfully.")
                if raw_data:
                    self.parse_data(raw_data)
            except Exception as e:
                logging.info(f"Error fetching space weather data: {e}")
            time.sleep(60)  # Wait for 60 seconds before fetching data again