import logging
import os
import time
from datetime import datetime, timedelta
import copernicusmarine as cm
import netCDF4 as nc
import numpy as np
from botocore.exceptions import ClientError
from multiprocessing import Queue
import json
from translators.cmems_translator import CmemsTranslator

class CmemsDataSource:
    def __init__(self, config_path, queue: Queue):
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
        
        self.username = os.environ.get('CMEMS_USERNAME')
        self.password = os.environ.get('CMEMS_PASSWORD')
        
        if not self.username or not self.password:
            raise ValueError("CMEMS credentials not found in environment variables")
        
        self.dataset_ids = config['dataset_ids']
        self.output_directory = config['output_directory']
        self.queue = queue
        self.translator = CmemsTranslator()
        logging.basicConfig(level=logging.INFO)

    def read_secret(self, path):
        with open(path, 'r') as f:
            return f.read().strip()

    def fetch_data(self):
        logging.debug("Logging in to Copernicus Marine")
        cm.login(username=self.username, password=self.password, force_overwrite=True)

        logging.debug("Checking available CMEMS data")
        current_day = datetime.utcnow().strftime("%Y%m%d")
        yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y%m%d")
        date_filters = [f"*{current_day}*", f"*{yesterday}*"]

        for dataset_id in self.dataset_ids:
            retry_attempts = 3
            for attempt in range(retry_attempts):
                try:
                    for date_filter in date_filters:
                        logging.debug(f"Attempting to fetch data for dataset: {dataset_id} with filter {date_filter}, attempt: {attempt + 1}")
                        output_files = cm.get(
                            dataset_id=dataset_id,
                            filter=date_filter,
                            output_directory=self.output_directory,
                            sync=True,
                            dataset_version="202311",
                            dataset_part="latest"
                        )
                        if output_files:
                            logging.info(f"Files available for {dataset_id} with filter {date_filter}: {output_files}")
                            for file_name in output_files:
                                file_path = os.path.join(self.output_directory, file_name)
                                if os.path.exists(file_path):
                                    logging.info(f"File already exists, skipping download: {file_path}")
                                else:
                                    logging.info(f"Downloading file: {file_name}")
                                    cm.get(
                                        dataset_id=dataset_id,
                                        output_directory=self.output_directory,
                                        filter=date_filter,
                                        sync=True,
                                        dataset_version="202311",
                                        dataset_part="latest"
                                    )
                                self.process_data(self.output_directory)
                        else:
                            logging.info(f"No files available for {dataset_id} with filter {date_filter}")
                    break
                except ClientError as e:
                    logging.error(f"ClientError encountered: {e}")
                    if e.response['Error']['Code'] == '408' and attempt < retry_attempts - 1:
                        logging.warning(f"Request Timeout. Retrying... (Attempt {attempt + 1}/{retry_attempts})")
                        time.sleep(5)
                    else:
                        logging.error(f"Failed to fetch data for {dataset_id} after {retry_attempts}")
                        raise
                except Exception as e:
                    logging.error(f"Unexpected error encountered: {e}")
                    raise

    def process_data(self, directory):
        logging.info(f"Processing directory: {directory}")
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith('.nc'):
                    file_path = os.path.join(root, file)
                    logging.info(f"Processing file: {file_path}")
                    data_dict, ds = self.netcdf_to_dict(file_path)
                    if data_dict and ds:
                        translated_data = self.translator.translate(data_dict)
                        combined_results = self.combine_measurements(translated_data, ds)
                        for result in combined_results:
                            self.queue.put(result)
                        ds.close()
                        # Remove the file after processing
                        try:
                            os.remove(file_path)
                            logging.info(f"Removed file: {file_path}")
                        except Exception as e:
                            logging.error(f"Error removing file: {file_path}")
                            logging.error(f"Exception: {e}")

    def save_data(self, processed_data):
        # Implement the logic to save or return processed data
        logging.info("Saving data")
        # ...existing code...

    def netcdf_to_dict(self, file_path):
        logging.info(f"Converting NetCDF file to dictionary: {file_path}")
        try:
            ds = nc.Dataset(file_path, 'r')
            data = {}
            for var_name in ds.variables:
                try:
                    var_obj = ds.variables[var_name]
                    if not var_obj.dimensions or var_obj.size == 0:
                        continue
                    if var_obj.ndim == 0:
                        var_data = var_obj.item()
                    else:
                        var_data = var_obj[:]
                    if np.isscalar(var_data):
                        var_data = [var_data]
                    data[var_name] = self.clean_value_for_json(var_data, var_name)
                except Exception as e:
                    logging.error(f"Error processing variable '{var_name}': {e}")
            return data, ds
        except OSError as e:
            logging.error(f"Error converting NetCDF to dictionary: {e}")
            return None, None

    def combine_measurements(self, data, ds):
        combined_results = []
        keys = list(data.keys())
        length = min(len(data[key]) for key in keys if isinstance(data[key], list))
        fallback_lat, fallback_lon = self.get_fallback_lat_lon(ds)
        fallback_station = self.get_fallback_station(ds)
        for i in range(length):
            combined_result = {}
            for key in keys:
                if key.endswith("_qc"):
                    continue
                if isinstance(data[key], list) and len(data[key]) > 0:
                    if isinstance(data[key][0], list):
                        if i < len(data[key]) and len(data[key][i]) > 0:
                            combined_result[key] = data[key][i][0]
                        else:
                            combined_result[key] = None
                    else:
                        combined_result[key] = data[key][i]
                else:
                    combined_result[key] = data[key]
            if "latitude" not in combined_result and fallback_lat is not None:
                combined_result["latitude"] = fallback_lat
            if "longitude" not in combined_result and fallback_lon is not None:
                combined_result["longitude"] = fallback_lon
            lat_key = "precise_latitude" if "precise_latitude" in combined_result else "latitude"
            lon_key = "precise_longitude" if "precise_longitude" in combined_result else "longitude"
            if lat_key in combined_result and lon_key in combined_result:
                combined_result["location"] = f"{combined_result[lat_key]},{combined_result[lon_key]}"
            if "station" not in combined_result and fallback_station is not None:
                combined_result["station"] = fallback_station
            if "temperature" not in combined_result and "dry_bulb_temperature" in combined_result:
                combined_result["temperature"] = combined_result["dry_bulb_temperature"]
            # Convert time to ISO format
            if "timestamp" in combined_result:
                try:
                    time_units = "days since 1950-01-01T00:00:00Z"  # Replace with actual time units from NetCDF file
                    time_base = datetime.strptime(time_units.split('since')[1].strip(), "%Y-%m-%dT%H:%M:%SZ")
                    combined_result["timestamp"] = (time_base + timedelta(days=float(combined_result["timestamp"]))).isoformat() + "Z"
                except ValueError as e:
                    logging.error(f"Error converting timestamp: {e}")
                    combined_result["timestamp"] = None
            combined_results.append(combined_result)
        return combined_results

    def get_fallback_lat_lon(self, ds):
        lat = ds.getncattr('geospatial_lat_min') if 'geospatial_lat_min' in ds.ncattrs() else None
        lon = ds.getncattr('geospatial_lon_min') if 'geospatial_lon_min' in ds.ncattrs() else None
        return lat, lon

    def get_fallback_station(self, ds):
        station_name = ds.getncattr('station_name') if 'station_name' in ds.ncattrs() else None
        station_id = ds.getncattr('station_id') if 'station_id' in ds.ncattrs() else None
        platform_name = ds.getncattr('platform_name') if 'platform_name' in ds.ncattrs() else None
        platform_id = ds.getncattr('platform_id') if 'platform_id' in ds.ncattrs() else None
        return station_name or station_id or platform_name or platform_id

    def clean_value_for_json(self, value, var_name=""):
        if isinstance(value, (np.int32, np.int64)):
            return int(value)
        elif isinstance(value, (np.float32, np.float64)):
            return float(value)
        elif isinstance(value, np.ndarray):
            value = value.tolist()
            return self.clean_value_for_json(value, var_name)
        elif isinstance(value, bytes):
            try:
                return value.decode('utf-8')
            except UnicodeDecodeError as e:
                logging.error(f"Error decoding bytes: {e}")
                return str(value)
        elif isinstance(value, (datetime, np.datetime64)):
            return value.isoformat()
        elif isinstance(value, np.ma.MaskedArray):
            return value.filled().tolist()
        elif isinstance(value, complex):
            return [value.real, value.imag]
        elif hasattr(value, 'shape'):
            return value.shape
        elif isinstance(value, (int, float, str)):
            return value
        elif isinstance(value, list):
            def decode_bstring(s: str) -> str:
                if s.startswith("b'") and s.endswith("'"):
                    return s[2:-1]
                return s
            cleaned_list = []
            for item in value:
                if isinstance(item, list):
                    subresult = self.clean_value_for_json(item, var_name)
                    if subresult not in [None, "", "None"]:
                        cleaned_list.append(subresult)
                else:
                    decoded = decode_bstring(str(item).strip())
                    if decoded not in ["", "None"]:
                        cleaned_list.append(decoded)
            if not var_name.endswith("_QC") and all(isinstance(x, str) and len(x) == 1 for x in cleaned_list):
                return ''.join(cleaned_list)
            return cleaned_list
        return ""
    
    def run(self):
        while True:
            try:
                self.process_data(self.output_directory)
                self.fetch_data()
            except Exception as e:
                logging.error(f"Error in run method: {e}")