import re
from datetime import datetime

class SpaceWeatherTranslator:
    def __init__(self):
        self.translations = {
            "time": "timestamp",
            "status": "status",
            "bx": "bx",
            "by": "by",
            "bz": "bz",
            "bt": "bt",
            "latitude": "latitude",
            "longitude": "longitude"
        }

    def translate(self, space_weather_data):
        translated_data = {}
        for key, value in space_weather_data.items():
            translated_key = self.translations.get(key, key)
            if translated_key in ["latitude", "longitude", "bx", "by", "bz", "bt"]:
                translated_data[translated_key] = float(value) if value is not None else None
            else:
                translated_data[translated_key] = value
        translated_data["source"] = "space_weather"
        translated_data["location"] = f"{translated_data['latitude']},{translated_data['longitude']}"
        return translated_data


