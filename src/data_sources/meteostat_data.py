import logging
import requests
import gzip
import csv
import io
import json
import math
import os
from datetime import datetime, timedelta
from multiprocessing import Queue

METEOSTAT_COCO_MAPPING = {
    0: "Clear",
    1: "Partly Cloudy",
    2: "Cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing Rime Fog",
    51: "Drizzle: Light",
    53: "Drizzle: Moderate",
    55: "Drizzle: Dense",
    56: "Freezing Drizzle: Light",
    57: "Freezing Drizzle: Dense",
    61: "Rain: Slight",
    63: "Rain: Moderate",
    65: "Rain: Heavy",
    66: "Freezing Rain: Light",
    67: "Freezing Rain: Heavy",
    71: "Snow: Slight",
    73: "Snow: Moderate",
    75: "Snow: Heavy",
    77: "Snow Grains",
    80: "Rain Showers: Slight",
    81: "Rain Showers: Moderate",
    82: "Rain Showers: Violent",
    85: "Snow Showers: Slight",
    86: "Snow Showers: Heavy",
    95: "Thunderstorm: Slight or Moderate",
    96: "Thunderstorm with Hail: Slight",
    99: "Thunderstorm with Hail: Heavy"
}

def translate_coco(coco):
    return METEOSTAT_COCO_MAPPING.get(int(coco), None)

def download_and_extract_gzip(url, output_dir):
    try:
        file_name = url.split("/")[-1]
        response = requests.get(url)
        response.raise_for_status()
        file_path = os.path.join(output_dir, file_name)
        with open(file_path, 'wb') as f:
            f.write(response.content)
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            headers = ['date', 'hour', 'temp','dewpt','rhum','precipitation','snow','wind_dir','wind_speed','wind_gust','pressure','tsun','coco']
            data = [dict(zip(headers, row)) for row in reader]
        os.remove(file_path)
        return data
    except Exception as e:
        logging.error(f"Error downloading or extracting {url}: {e}")
        return None

class MeteostatDataSource:
    def __init__(self, config_path, queue: Queue):
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
        self.queue = queue
        self.base_url = config.get("base_url", "https://bulk.meteostat.net/v2")
        self.stations_url = config.get("stations_url", f"{self.base_url}/stations/lite.json.gz")
        self.output_directory = config.get("output_directory", "/tmp")
        self.inactive_stations_file = os.path.join(self.output_directory, "inactive_stations.json")
        self.inactive_stations = self.load_inactive_stations()
        logging.basicConfig(level=logging.INFO)

    def load_inactive_stations(self):
        if os.path.exists(self.inactive_stations_file):
            with open(self.inactive_stations_file, 'r') as f:
                return set(json.load(f))
        return set()

    def save_inactive_stations(self):
        with open(self.inactive_stations_file, 'w') as f:
            json.dump(list(self.inactive_stations), f)

    def fetch_station_list(self):
        try:
            response = requests.get(self.stations_url)
            response.raise_for_status()
            with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                stations = json.load(f)
            logging.info(f"Fetched {len(stations)} stations")
            return stations
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching station list: {e}")
            return None

    def fetch_weather_data(self, station):
        station_id = station['id']
        if station_id in self.inactive_stations:
            logging.info(f"Skipping inactive station {station_id}")
            return None
        station_url = f"{self.base_url}/hourly/{station_id}.csv.gz"
        return download_and_extract_gzip(station_url, self.output_directory)

    def compute_dew_point(self, temp, rhum):
        if temp is None or rhum is None:
            return None
        a = 17.27
        b = 237.7
        alpha = ((a * temp) / (b + temp)) + math.log(rhum / 100.0)
        return (b * alpha) / (a - alpha)

    def is_valid_temperature(self, temp):
        return -90 <= temp <= 60

    def is_valid_pressure(self, pressure):
        return 870 <= pressure <= 1085

    def format_record(self, station, record):
        if 'date' not in record or 'hour' not in record:
            return None
        record_datetime = datetime.strptime(f"{record['date']} {record['hour']}", "%Y-%m-%d %H")
        if record_datetime < datetime.utcnow() - timedelta(days=1):
            return None
        formatted_record = {
            "station_id": station["id"],
            "latitude": station["location"]["latitude"],
            "longitude": station["location"]["longitude"],
            "timestamp": f"{record['date']}T{int(record['hour']):02d}:00:00Z",
            "location": f"{station['location']['latitude']},{station['location']['longitude']}"
        }
        optional_fields = ["temp","dewpt","rhum","precipitation","snow","wind_dir","wind_speed","wind_gust","pressure","tsun","coco"]
        for field in optional_fields:
            val = record.get(field)
            if val not in [None, '']:
                try:
                    parsed_val = float(val)
                    if field == "temp":
                        if not self.is_valid_temperature(parsed_val):
                            continue
                        formatted_record["temperature"] = parsed_val
                        continue
                    if field == "dewpt":
                        formatted_record["dewpoint"] = parsed_val
                        continue
                    if field == "pressure" and not self.is_valid_pressure(parsed_val):
                        continue
                    formatted_record[field] = parsed_val
                except ValueError:
                    try:
                        formatted_record[field] = int(val)
                    except ValueError:
                        formatted_record[field] = val
        if "dewpoint" not in formatted_record and "temperature" in formatted_record and "rhum" in formatted_record:
            formatted_record["dewpoint"] = self.compute_dew_point(formatted_record["temperature"], formatted_record["rhum"])
        if "rhum" in formatted_record:
            del formatted_record["rhum"]
        if "coco" in formatted_record:
            weather = translate_coco(formatted_record["coco"])
            if weather:
                formatted_record["weather"] = weather
            del formatted_record["coco"]
        return {k: v for k, v in formatted_record.items() if v is not None}

    def fetch_data(self):
        stations = self.fetch_station_list()
        if not stations:
            logging.error("Failed to fetch station list")
            return

        for station in stations:
            data = self.fetch_weather_data(station)
            if data:
                for record in data:
                    formatted_record = self.format_record(station, record)
                    if formatted_record:
                        formatted_record["source"] = "meteostat"
                        self.queue.put(formatted_record)
            else:
                logging.warning(f"Data unavailable for station {station['id']}")
                self.inactive_stations.add(station['id'])
                self.save_inactive_stations()

    def run(self):
        while True:
            self.fetch_data()