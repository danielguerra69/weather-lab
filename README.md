# Weather Lab

Weather Lab is a project that fetches weather data from various sources, processes it, and stores it in Elasticsearch. The data can be visualized using Kibana.

## Prerequisites

- Docker
- Docker Compose

## Setup

1. **Clone the repository**:
   ```sh
   git clone https://github.com/yourusername/weather-lab.git
   cd weather-lab
   ```

2. **Create a `.env` file in the root directory with the following content**:
   ```env
   CMEMS_USERNAME=your_cmems_username
   CMEMS_PASSWORD=your_cmems_password
   ```

3. **Build the Docker image**:
   ```sh
   docker-compose build
   ```

4. **Run the setup script to create Docker secrets**:
   ```sh
   chmod +x bin/setup.sh
   ./bin/setup.sh
   ```

5. **Start the Docker Compose services**:
   ```sh
   docker-compose up
   ```

6. **Import the Kibana configuration**:
   - Open Kibana at `http://localhost:7777`
   - Go to **Management** > **Stack Management** > **Saved Objects**
   - Click **Import** and select the `kibana-config.ndjson` file from the repository

## Services

- **Elasticsearch**: Accessible at `http://localhost:9200`
- **Kibana**: Accessible at `http://localhost:7777`
- **Weather Lab**: Fetches and processes weather data, then stores it in Elasticsearch

## Configuration

### CMEMS Configuration

The CMEMS configuration file is located at `src/config/cmems_config.json`. Update this file with the appropriate dataset IDs and output directory.

```json
{
    "dataset_ids": [
        "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr"
    ],
    "output_directory": "/tmp"
}
```

### Meteostat Configuration

The Meteostat configuration file is located at `src/config/meteostat_config.json`. Update this file with the appropriate base URL and stations URL.

```json
{
    "base_url": "https://bulk.meteostat.net/v2",
    "stations_url": "https://bulk.meteostat.net/v2/stations/lite.json.gz",
    "output_directory": "/tmp"
}
```

### Environment Variables

Create a `.env` file in the root directory with the following content:

```env
CMEMS_USERNAME=your_cmems_username
CMEMS_PASSWORD=your_cmems_password
```

## Running Tests

To run the tests, use the following command:

```sh
docker-compose run weather-lab /venv/bin/python -m unittest discover -s tests
```

## License

This project is licensed under the MIT License.