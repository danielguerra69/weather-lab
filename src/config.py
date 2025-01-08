import os

class Config:
    DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///data.db")
    ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
    RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
    FLOWISEAI_URL = os.getenv("FLOWISEAI_URL", "http://localhost:5000")
    CMEMS_USERNAME = os.getenv("CMEMS_USERNAME", "your_cmems_username")
    CMEMS_PASSWORD = os.getenv("CMEMS_PASSWORD", "your_cmems_password")
    METAR_DATA_URL = os.getenv("METAR_DATA_URL", "https://aviationweather.gov/data/cache/metars.cache.xml.gz")
    SPACE_WEATHER_URL = os.getenv("SPACE_WEATHER_URL", "https://services.swpc.noaa.gov/text/ace-magnetometer.txt")