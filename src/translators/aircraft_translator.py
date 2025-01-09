import csv
from datetime import datetime

def translate_wind_speed(wind_speed_kt):
    return float(wind_speed_kt) * 1.852 if wind_speed_kt else None  # Convert knots to km/h

def translate_visibility(visibility_statute_mi):
    return float(visibility_statute_mi) * 1.60934 if visibility_statute_mi else None  # Convert miles to km

def translate_temperature(temp_c):
    return float(temp_c) if temp_c else None  # Assuming temperature is already in Celsius

def translate_timestamp(observation_time):
    try:
        return datetime.strptime(observation_time, '%Y-%m-%dT%H:%M:%SZ').isoformat() if observation_time else None
    except ValueError:
        print(f"Error parsing observation_time: {observation_time}")
        return None

def translate_row(row):
    translated_row = {}
    for key, value in row.items():
        if key == 'observation_time':
            translated_row['timestamp'] = translate_timestamp(value)
        elif key == 'wind_speed_kt':
            translated_row['wind_speed'] = translate_wind_speed(value)
        elif key == 'temp_c':
            translated_row['temperature'] = translate_temperature(value)
        elif key == 'visibility_statute_mi':
            translated_row['visibility'] = translate_visibility(value)
        elif key == 'wind_dir_degrees':
            translated_row['wind_dir'] = float(value) if value else None
        elif key == 'altitude_ft_msl':
            translated_row['altitude'] = float(value) if value else None
        elif key in ['latitude', 'longitude']:
            try:
                translated_row[key] = float(value) if value else None
            except ValueError:
                print(f"Error parsing {key}: {value}")
                translated_row[key] = None
        elif key == 'aircraft_ref':
            translated_row['aircraft_ref'] = value
        else:
            try:
                if isinstance(value, (int, float)):
                    translated_row[key] = value
                else:
                    translated_row[key] = float(value) if value is not None and isinstance(value, str) else value
            except ValueError:
                translated_row[key] = value

    # Remove keys with None values
    translated_row = {k: v for k, v in translated_row.items() if v is not None}

    if 'latitude' in translated_row and 'longitude' in translated_row:
        translated_row['location'] = f"{translated_row['latitude']},{translated_row['longitude']}"
    else:
        translated_row['location'] = None

    # Add source key
    translated_row['source'] = 'aircraft'
    
    return translated_row
