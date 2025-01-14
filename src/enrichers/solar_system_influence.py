import numpy as np
import matplotlib.pyplot as plt
from shapely.geometry import Polygon, Point, mapping
from datetime import datetime
from skyfield.api import load, Topos, wgs84, load_constellation_map, load_constellation_names
from skyfield.timelib import Time
import json
import logging

class SolarSystemInfluence:
    
    def __init__(self, grid_size=100, field_size=1e7, levels=np.linspace(0, 1e-6, 10)):
        self.grid_size = grid_size
        self.field_size = field_size
        self.levels = levels
        self.masses = [
            1.989e30,   # Sun
            3.3011e23,  # Mercury
            4.8675e24,  # Venus
            5.972e24,   # Earth
            6.39e23,    # Mars
            1.898e27,   # Jupiter
            5.683e26,   # Saturn
            8.681e25,   # Uranus
            1.024e26,   # Neptune
            7.342e22    # Moon
        ]
        # Confirm that only gas giants with significant moon systems use barycenters
        self.planet_names = [
            'sun', 'mercury', 'venus', 'earth', 'mars', 
            'jupiter barycenter',  # Barycenter used due to significant moon system
            'saturn barycenter',   # Barycenter used due to significant moon system
            'uranus barycenter',   # Barycenter used due to significant moon system
            'neptune barycenter',  # Barycenter used due to significant moon system
            'moon'                  # Moon treated as a separate celestial body influencing Earth
        ]
        self.output_names = [
            'Sun', 'Mercury', 'Venus', 'Earth', 'Mars', 'Jupiter', 
            'Saturn', 'Uranus', 'Neptune', 'Moon'
        ]
        self.planets = load('de421.bsp')
        self.earth = self.planets['earth']
        self.ts = load.timescale()
        self.constellation_at = load_constellation_map()  # Load constellation boundaries
        self.constellation_names = dict(load_constellation_names())  # Abbreviation to full name mapping
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def get_distances(self, time: datetime) -> list:
        skyfield_time = self.ts.utc(time.year, time.month, time.day, time.hour, time.minute, time.second)
        distances = []
        try:
            for planet_name in self.planet_names:
                planet = self.planets[planet_name]
                astrometric = self.earth.at(skyfield_time).observe(planet)
                distance = astrometric.distance().m
                self.logger.info(f"Distance to {planet_name}: {distance} meters")  # Replaced print with logging
                distances.append(distance)
        except Exception as e:
            self.logger.error(f"Error in get_distances: {e}")
            return []
        return distances

    def calculate_gravity_influence(self, mass: float, distance: float) -> float:
        G = 6.67430e-11  # gravitational constant
        min_distance = 1e3  # Minimum distance threshold in meters
        if distance < min_distance:
            distance = min_distance
        return float(G * mass / distance**2)

    def calculate_gravity_influence_vector(self, mass: float, distance: float, direction: tuple) -> np.ndarray:
        """
        Calculates the gravitational influence as a vector based on mass, distance, and direction.

        Args:
            mass (float): Mass of the celestial body in kilograms.
            distance (float): Distance from the celestial body in meters.
            direction (tuple): A tuple representing the direction vector (x, y, z).

        Returns:
            np.ndarray: Gravitational influence vector [x, y, z].
        """
        try:
            G = 6.67430e-11  # gravitational constant
            min_distance = 1e3  # Minimum distance threshold in meters
            distance = float(distance)  # Ensure distance is float
            mass = float(mass)          # Ensure mass is float

            if distance < min_distance:
                distance = min_distance
            magnitude = G * mass / (distance**2 + 1e-10)  # Add epsilon to prevent division by zero

            direction_vector = np.array(direction, dtype=float)  # Ensure direction is float
            norm = np.linalg.norm(direction_vector)
            if norm == 0.0:
                return np.array([0.0, 0.0, 0.0], dtype=float)
            direction_unit = direction_vector / norm  # Division should be safe now
            
            # Verify that magnitude is a float
            if not isinstance(magnitude, (float, int)):
                self.logger.error(f"magnitude has invalid type: {type(magnitude)}. Setting to 0.0.")
                magnitude = 0.0

            influence_vector = magnitude * direction_unit  # Multiplication is safe
            
            # Verify that influence_vector contains floats
            if not np.issubdtype(influence_vector.dtype, np.floating):
                self.logger.error(f"influence_vector has invalid dtype: {influence_vector.dtype}. Setting to zeros.")
                influence_vector = np.zeros(3, dtype=float)
                
            return influence_vector
        except Exception as e:
            self.logger.error(f"Error in calculate_gravity_influence_vector: {e}")
            return np.array([0.0, 0.0, 0.0], dtype=float)

    def generate_isobaric_fields(self, mass: float, distance: float) -> tuple:
        distance = float(distance)  # Ensure distance is float
        x = np.linspace(-self.field_size, self.field_size, self.grid_size)
        y = np.linspace(-self.field_size, self.field_size, self.grid_size)
        X, Y = np.meshgrid(x, y)
        denominator = np.sqrt(X**2 + Y**2 + distance**2) + 1e-10  # Add epsilon to prevent division by zero
        
        # Add type checking for gravitational_influence
        gravitational_influence = self.calculate_gravity_influence(mass, distance)
        if not isinstance(gravitational_influence, (float, int)):
            self.logger.error(f"gravitational_influence has invalid type: {type(gravitational_influence)}. Setting to 0.0.")
            gravitational_influence = 0.0
        
        Z = gravitational_influence / denominator  # NumPy will handle broadcasting
        return X, Y, Z

    def convert_xy_to_latlon(self, x: float, y: float, center_lat: float = 0.0, center_lon: float = 0.0) -> list:
        # Approx. conversion: 1° ~ 111,139m at the Equator
        return [
            center_lon + (x / 111139.0),
            center_lat + (y / 111139.0)
        ]

    def create_geo_shapes(self, X: np.ndarray, Y: np.ndarray, Z: np.ndarray) -> list:
        c_filled = plt.contourf(X, Y, Z, levels=self.levels)
        shapes = []
        try:
            for collection in c_filled.collections:
                for path in collection.get_paths():
                    coords = path.vertices
                    if len(coords) > 2:
                        # Convert each (x, y) to approximate lat/lon
                        latlon_coords = [self.convert_xy_to_latlon(x, y) for x, y in coords]
                        polygon = Polygon(latlon_coords)
                        if not polygon.is_empty and polygon.is_valid:
                            shapes.append(polygon)
        except Exception as e:
            self.logger.error(f"Error in create_geo_shapes: {e}")
        return shapes

    def plot_isobaric_fields(self, X: np.ndarray, Y: np.ndarray, Z: np.ndarray, title: str) -> None:
        try:
            plt.contourf(X, Y, Z, levels=self.levels)
            plt.colorbar()
            plt.xlabel('X coordinate')
            plt.ylabel('Y coordinate')
            plt.title(title)
            plt.show()
        except Exception as e:
            self.logger.error(f"Error in plot_isobaric_fields: {e}")

    def check_conjunction(self, time: datetime, observer: Topos = None, threshold: float = 10.0) -> list:
        skyfield_time = self.ts.utc(time.year, time.month, time.day, time.hour, time.minute, time.second)
        conjunctions = []
        # Exclude Earth from conjunction checks by ensuring names are lowercase and not 'earth'
        non_earth_planets = [name for name in self.planet_names if name.lower() != 'earth']
        # self.logger.info(f"Non-Earth Planets for Conjunction Check: {non_earth_planets}")  # Replaced print with logging
        try:
            for i, planet_name1 in enumerate(non_earth_planets):
                for planet_name2 in non_earth_planets[i+1:]:
                    planet1 = self.planets[planet_name1]
                    planet2 = self.planets[planet_name2]
                    if observer:
                        astrometric1 = observer.at(skyfield_time).observe(planet1)
                        astrometric2 = observer.at(skyfield_time).observe(planet2)
                    else:
                        astrometric1 = self.earth.at(skyfield_time).observe(planet1)
                        astrometric2 = self.earth.at(skyfield_time).observe(planet2)
                    separation = astrometric1.separation_from(astrometric2).degrees
                    # self.logger.info(f"Separation between {planet_name1} and {planet_name2}: {separation} degrees")  # Replaced print with logging
                    if separation < threshold:
                        conjunctions.append((planet_name1, planet_name2))
        except Exception as e:
            self.logger.error(f"Error in check_conjunction: {e}")
        # self.logger.info(f"Detected Conjunctions: {conjunctions}")  # Replaced print with logging
        return conjunctions

    def get_known_conjunction_time(self) -> datetime:
        """
        Returns a datetime object for a known conjunction.
        Example: Jupiter and Saturn conjunction on December 21, 2020.
        """
        return datetime(2020, 12, 21, 0, 0, 0)

    def query_gravity_influence_at_time(self, time: datetime, conjunction_threshold: float = 10.0) -> dict:
        distances = self.get_distances(time)
        results = []
        try:
            for mass, distance, planet_name, output_name in zip(self.masses, self.planet_names, self.output_names):
                mass = float(mass)          # Ensure mass is float
                distance = float(distance)  # Ensure distance is float
                
                if distance == 0.0:
                    self.logger.error(f"Distance for {planet_name} is zero. Skipping to avoid division by zero.")
                    continue
                
                influence = self.calculate_gravity_influence(mass, distance)
                
                if not isinstance(influence, float):
                    self.logger.error(f"influence is not a float for {planet_name}: {influence}")
                    influence = 0.0
                
                X, Y, Z = self.generate_isobaric_fields(mass, distance)
                shapes = self.create_geo_shapes(X, Y, Z)
                for shape in shapes:
                    result = {
                        "timestamp": time.isoformat(),
                        "planet_name": output_name,
                        "area": mapping(shape),
                        "bounds": shape.bounds,
                        "influence_value": influence,  # Already ensured to be float
                        "influence_vector": {
                            'x': float(influence / distance**2 * 6.67430e-11),  # Ensure float type
                            'y': 0.0,
                            'z': 0.0,
                            't': float(np.linalg.norm([float(influence / distance**2 * 6.67430e-11), 0.0, 0.0]))
                        }
                    }
                    results.append(result)
        except Exception as e:
            self.logger.error(f"Error in query_gravity_influence_at_time: {e}")
        # Sum all influences excluding the Sun
        total_influence = sum(
            item["influence_value"] 
            for item in results
        )
        return {
            "total_influence": {
                'x': 0.0,
                'y': 0.0,
                'z': 0.0,
                't': float(total_influence)
            },
            "results": results
        }

    def get_influence_at_location(self, location: dict, conjunction_threshold: float = 10.0) -> dict:
        lat = float(location['latitude'])
        lon = float(location['longitude'])
        influence_data = {}
        try:    
            time = datetime.fromisoformat(location['timestamp'])
            skyfield_time = self.ts.utc(time.year, time.month, time.day, time.hour, time.minute, time.second)
            # Combine Earth with the observer's location
            location_topos = self.earth + wgs84.latlon(lat, lon)
            total_influence = np.array([0.0, 0.0, 0.0], dtype=float)  # Ensure float type
            try:
                for mass, planet_name, output_name in zip(self.masses, self.planet_names, self.output_names):
                    planet = self.planets[planet_name]
                    astrometric = location_topos.at(skyfield_time).observe(planet)
                    distance = astrometric.distance().m
                    
                    # Ensure distance is float
                    if not isinstance(distance, (float, int)):
                        self.logger.error(f"distance has invalid type: {type(distance)}. Skipping planet.")
                        continue
                    
                    position = astrometric.apparent().position.au.astype(float)  # Ensure float type
                    norm = np.linalg.norm(position)
                    if norm == 0:
                        direction = (0.0, 0.0, 0.0)
                    else:
                        direction = tuple(position / norm)
                    influence_vector = self.calculate_gravity_influence_vector(mass, distance, direction)
                    
                    # Ensure influence_vector contains floats
                    if not all(isinstance(coord, float) for coord in influence_vector):
                        self.logger.error(f"influence_vector contains non-float types: {influence_vector}. Skipping planet.")
                        continue
                    
                    influence_data[output_name] = {
                        'x': float(influence_vector[0]),  # Ensure float type
                        'y': float(influence_vector[1]),  # Ensure float type
                        'z': float(influence_vector[2]),  # Ensure float type
                        't': float(np.linalg.norm(influence_vector))  # Add 't'
                    }
                    if planet_name.lower() != 'sun':
                        total_influence += influence_vector
                influence_data['total_influence'] = {
                    'x': float(total_influence[0]),  # Ensure float type
                    'y': float(total_influence[1]),  # Ensure float type
                    'z': float(total_influence[2]),  # Ensure float type
                    't': float(np.linalg.norm(total_influence))
                }

                # Check for conjunctions and adjust influence
                conjunctions = self.check_conjunction(time, observer=location_topos, threshold=conjunction_threshold)
                
                # Create a list of conjunctions with planets field
                influence_data['conjunctions'] = [
                    {
                        "planets": [
                            self.output_names[self.planet_names.index(planet1)].capitalize(),
                            self.output_names[self.planet_names.index(planet2)].capitalize()
                        ],
                        "x": float(influence_data[self.output_names[self.planet_names.index(planet1)]]['x'] + 
                                   influence_data[self.output_names[self.planet_names.index(planet2)]]['x']),
                        "y": float(influence_data[self.output_names[self.planet_names.index(planet1)]]['y'] + 
                                   influence_data[self.output_names[self.planet_names.index(planet2)]]['y']),
                        "z": float(influence_data[self.output_names[self.planet_names.index(planet1)]]['z'] + 
                                   influence_data[self.output_names[self.planet_names.index(planet2)]]['z']),
                        "t": float(np.linalg.norm([
                            influence_data[self.output_names[self.planet_names.index(planet1)]]['x'] + 
                            influence_data[self.output_names[self.planet_names.index(planet2)]]['x'],
                            influence_data[self.output_names[self.planet_names.index(planet1)]]['y'] + 
                            influence_data[self.output_names[self.planet_names.index(planet2)]]['y'],
                            influence_data[self.output_names[self.planet_names.index(planet1)]]['z'] + 
                            influence_data[self.output_names[self.planet_names.index(planet2)]]['z']
                        ]))
                    }
                    for planet1, planet2 in conjunctions
                ]

                self.logger.debug(f"Influence data at location: {influence_data}")
                
            except Exception as e:
                self.logger.error(f"Error in get_influence_at_location processing: {e}")

            return influence_data
        except Exception as e:
            self.logger.error(f"Error in get_influence_at_location: {e}")
            return influence_data

    def get_constellations_at_location(self, location: dict) -> list:
        """
        Returns a list of constellations visible at the given location and time.
        
        Args:
            location (dict): A dictionary containing 'latitude', 'longitude', and 'timestamp' keys.
        
        Returns:
            list: List of visible constellation names.
        """
        lat = float(location['latitude'])
        lon = float(location['longitude'])
        # time = datetime.fromisoformat(location['timestamp'])
        time_str = location['timestamp'].rstrip('Z')  # Remove 'Z'
        time = datetime.fromisoformat(time_str)  # Updated parsing
        skyfield_time = self.ts.utc(time.year, time.month, time.day, time.hour, time.minute, time.second)
        
        # Define the observer's location
        observer = wgs84.latlon(lat, lon)
        
        # Define the celestial sphere points with increased step sizes for optimization
        azimuths = np.linspace(0, 360, 180)  # 2-degree steps instead of 1-degree
        altitudes = np.linspace(10, 90, 41)   # 2-degree steps instead of 1-degree; starting from 10° to avoid horizon effects
        
        visible_constellations = set()
        
        try:
            for az in azimuths:
                for alt in altitudes:
                    # Convert azimuth and altitude to RA and Dec
                    sky_position = observer.at(skyfield_time).from_altaz(alt_degrees=alt, az_degrees=az)
                    ra, dec, distance = sky_position.radec()
                    
                    # ...existing code...
                    
                    if constellation:
                        full_name = self.constellation_names.get(constellation, constellation)
                        visible_constellations.add(full_name)
        except Exception as e:
            self.logger.error(f"Error in get_constellations_at_location: {e}")
        
        return list(visible_constellations)

    def get_light_intensity_at_location(self, location: dict) -> float:
        """
        Calculates the light intensity at a given location and time.
        
        Args:
            location (dict): A dictionary containing 'latitude', 'longitude', and 'timestamp' keys.
        
        Returns:
            float: Light intensity ranging from 0.0 (night) to 1.0 (full daylight).
        """
        lat = float(location['latitude'])
        lon = float(location['longitude'])
        # ...existing code...
        
        # Define the observer's location
        observer = self.planets['earth'] + wgs84.latlon(lat, lon)
        
        try:
            # Define skyfield_time
            time_str = location['timestamp'].rstrip('Z')  # Remove 'Z'
            time = datetime.fromisoformat(time_str)
            skyfield_time = self.ts.utc(time.year, time.month, time.day, time.hour, time.minute, time.second)
            
            # Calculate the altitude of the Sun
            astrometric = observer.at(skyfield_time).observe(self.planets['sun'])
            altitude = astrometric.apparent().altaz()[0].degrees
            
            if altitude < 0:
                intensity = 0.0  # Nighttime
                self.logger.debug("It is nighttime at this location.")
            else:
                # Normalize the altitude to get intensity
                intensity = min(1.0, max(0.0, altitude / 90.0))
                self.logger.debug(f"Light intensity at location: {intensity}")
                
            return intensity
        except Exception as e:
            self.logger.error(f"Error in get_light_intensity_at_location: {e}")
            return 0.0

if __name__ == "__main__":
    gravity_influence = SolarSystemInfluence()
    # ...existing code...
    
    # results = gravity_influence.query_gravity_influence_at_time(datetime.fromisoformat(query_time))
    results = gravity_influence.query_gravity_influence_at_time(datetime.fromisoformat(query_time_str))  # Updated parsing
    json_output = json.dumps(results, indent=4)
    print(json_output)

    for result in results["results"]:
        print(result)

    # ...existing code...
    
    # Test get_influence_at_location with locations from the areas produced by query_gravity_influence_at_time
    if results["results"]:
        sample_area = results["results"][0]["area"]
        if sample_area["type"] == "Polygon" and sample_area["coordinates"]:
            sample_location = sample_area["coordinates"][0][0]  # Get the first coordinate of the first polygon
            test_location = {
                "latitude": sample_location[1],
                "longitude": sample_location[0],
                "timestamp": query_time
            }
            gravity_influence.get_influence_at_location(test_location, conjunction_threshold=10.0)
            gravity_influence.get_constellations_at_location(test_location)
            test_location_json_output = json.dumps(test_location, indent=4)
            print(test_location_json_output)