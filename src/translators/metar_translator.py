class MetarTranslator:
    def __init__(self):
        self.translations = {
            "time": "timestamp",
            "station_id": "station_id",
            "latitude": "latitude",
            "longitude": "longitude",
            "temp": "temperature",
            "dewpoint_c": "dew_point_temperature",
            "wind_dir_degrees": "wind_dir",
            "wind_speed": "wind_speed",
            "visibility": "visibility",
            "pressure": "pressure",
            "flight_category": "flight_category",
            "report_type": "report_type",
            "elevation": "elevation"
        }

    def translate(self, metar_data):
        translated_data = {}
        for key, value in metar_data.items():
            if value is not None:
                translated_key = self.translations.get(key, key)
                if translated_key in ["latitude", "longitude", "temperature", "dew_point_temperature", "wind_speed", "visibility", "pressure", "elevation"]:
                    translated_data[translated_key] = float(value)
                else:
                    translated_data[translated_key] = value
        translated_data["source"] = "metar"
        if "latitude" in translated_data and "longitude" in translated_data:
            translated_data["location"] = f"{translated_data['latitude']},{translated_data['longitude']}"
        return translated_data