class CmemsTranslator:
    def __init__(self):
        self.translations = {
            "time": "timestamp",
            "latitude": "latitude",
            "longitude": "longitude",
            "dc_reference": "data_center_reference",
            "trajectory": "trajectory",
            "deph": "depth",
            "temp": "temperature",
            "atms": "pressure",
            "slev": "sea_level",
            "psal": "salinity",
            "doxy": "dissolved_oxygen",
            "chla": "chlorophyll",
            "nitr": "nitrate",
            "phos": "phosphate",
            "silic": "silicate",
            "ph": "acidity",
            "turb": "turbidity",
            "dox1": "dissolved_oxygen",
            "osat": "oxygen_saturation",
            "phph": "acidity",
            "cndc": "conductivity",
            "atpt": "air_temperature",
            "ewct": "water_temperature",
            "nsct": "salinity",
            "alts": "altitude",
            "gspd": "ground_speed",
            "wspd": "wind_speed",
            "wdir": "wind_dir",
            "tur4": "turbidity",
            "relh": "relative_humidity",
            "vavh": "wave_height",
            "vped": "peak_energy_direction",
            "vpsp": "peak_spectral_period",
            "vtpk": "peak_wave_period",
            "vtza": "zero_crossing_period",
            "vzmx": "maximum_wave_height",
            "dryt": "dry_bulb_temperature",
            "atmp": "air_temperature",
            "dewt": "dewpoint",
            "amon": "ammonium",
            "bbp470": "backscattering_coefficient_470",
            "bbp532": "backscattering_coefficient_532",
            "bbp700": "backscattering_coefficient_700",
            "bbp700_adjusted": "backscattering_coefficient_700_adjusted",
            "bear": "bearing",
            "ccov": "cloud_cover",
            "cdom": "colored_dissolved_organic_matter",
            "chlt": "chlorophyll_t",
            "cp660": "particulate_organic_carbon_660",
            "cphl": "chlorophyll_a",
            "cphl_adjusted": "chlorophyll_a_adjusted",
            "cphl_adjusted_error": "chlorophyll_a_adjusted_error",
            "dens": "density",
            "down_irradiance380": "downwelling_irradiance_380",
            "down_irradiance412": "downwelling_irradiance_412",
            "down_irradiance443": "downwelling_irradiance_443",
            "down_irradiance490": "downwelling_irradiance_490",
            "down_irradiance555": "downwelling_irradiance_555",
            "dox2": "dissolved_oxygen_2",
            "dox2_adjusted": "dissolved_oxygen_2_adjusted",
            "dox2_adjusted_error": "dissolved_oxygen_2_adjusted_error",
            "drva": "drift_velocity",
            "eacc": "eastward_acceleration",
            "ersc": "earth_rotation_speed",
            "ertc": "earth_rotation_tilt",
            "espc": "earth_surface_pressure",
            "etmp": "earth_temperature",
            "ewcs": "eastward_water_current_speed",
            "flu2": "fluorescence_2",
            "flu3": "fluorescence_3",
            "fluo": "fluorescence",
            "fluo_adjusted": "fluorescence_adjusted",
            "gdir": "ground_direction",
            "gdop": "geometric_dilution_of_precision",
            "hcdt": "heading_course",
            "hcsp": "heading_speed",
            "hcss": "heading_speed_standard_deviation",
            "hmsb": "heading_magnetic_standard_deviation",
            "lgh4": "lightning_4",
            "lght": "lightning",
            "linc": "lightning_count",
            "maxv": "maximum_velocity",
            "minv": "minimum_velocity",
            "narx": "northward_acceleration",
            "natx": "northward_tilt",
            "nscs": "northward_speed",
            "ntaw": "northward_water_current_speed",
            "ntaw_adjusted": "northward_water_current_speed_adjusted",
            "ntaw_adjusted_error": "northward_water_current_speed_adjusted_error",
            "ntra": "northward_transport",
            "ntri": "nitrate",
            "phph_adjusted": "acidity_adjusted",
            "phph_adjusted_error": "acidity_adjusted_error",
            "phyc": "phycocyanin",
            "pres": "water_pressure",
            "pres_adjusted": "water_pressure_adjusted",
            "pres_adjusted_error": "water_pressure_adjusted_error",
            "pres_core": "water_pressure_core",
            "prht": "precipitation_height",
            "prrt": "precipitation_rate",
            "psal_adjusted": "salinity_adjusted",
            "psal_adjusted_error": "salinity_adjusted_error",
            "psal_dm": "salinity_dm",
            "qcflag": "quality_control_flag",
            "rdva": "radial_velocity",
            "rnge": "range",
            "rvfl": "river_flow",
            "rvfl_dm": "river_flow_dm",
            "scdr": "scattering_coefficient",
            "scdt": "scattering_coefficient_tilt",
            "sdn_cruise": "sdn_cruise",
            "sdn_edmo_code": "sdn_edmo_code",
            "sdn_local_cdi_id": "sdn_local_cdi_id",
            "sdn_references": "sdn_references",
            "sdn_station": "sdn_station",
            "sdn_xlink": "sdn_xlink",
            "sigt": "sigma_theta",
            "sinc": "sine_current",
            "slnr": "solar_radiation",
            "slnt": "solar_radiation_tilt",
            "sltr": "solar_radiation_transmittance",
            "sltt": "solar_radiation_transmittance_tilt",
            "sprc": "specific_radiation",
            "ssjt": "sea_surface_temperature",
            "stheta1": "sigma_theta_1",
            "stheta2": "sigma_theta_2",
            "svel": "sound_velocity",
            "swht": "significant_wave_height",
            "temp_cndc": "temperature_conductivity",
            "temp_dm": "temperature_dm",
            "temp_doxy": "temperature_dissolved_oxygen",
            "theta1": "potential_temperature_1",
            "theta2": "potential_temperature_2",
            "tur6": "turbidity_6",
            "turfnu": "turbidity_fnu",
            "uacc": "upward_acceleration",
            "vacc": "vertical_acceleration",
            "vavt": "vertical_average_temperature",
            "vcmx": "vertical_current_maximum",
            "vcsp": "vertical_current_speed",
            "vdir": "vertical_direction",
            "vemh": "vertical_energy_maximum_height",
            "vepk": "vertical_energy_peak",
            "vghs": "vertical_gravity_height_standard_deviation",
            "vgta": "vertical_gravity_tilt_angle",
            "vh110": "vertical_height_110",
            "vhm0": "vertical_height_mean",
            "vhm0_dm": "vertical_height_mean_dm",
            "vhza": "vertical_height_zero_crossing",
            "vmdr": "vertical_mean_direction",
            "vspec1d": "vertical_spectrum_1d",
            "vt110": "vertical_temperature_110",
            "vtm02": "vertical_temperature_mean_02",
            "vtm02_dm": "vertical_temperature_mean_02_dm",
            "vtm10": "vertical_temperature_mean_10",
            "vtmx": "vertical_temperature_maximum",
            "vtzm": "vertical_temperature_zero_crossing",
            "vzmx_dm": "vertical_zero_crossing_maximum_dm",
            "wspe": "wind_speed_error",
            "wspn": "wind_speed_north",
            "wtodir": "wind_to_direction",
            "xdst": "x_distance",
            "ydst": "y_distance",
        }

    def translate(self, cmems_data):
        translated_record = {}
        if hasattr(cmems_data, 'items'):
            for key, value in cmems_data.items():
                translated_key = self.translations.get(key.lower(), key.lower())
                try:
                    translated_record[translated_key] = float(value) if value is not None and isinstance(value, (int, float, str)) else value
                except ValueError:
                    translated_record[translated_key] = value
            translated_record["source"] = "cmems"
            if "latitude" in translated_record and "longitude" in translated_record:
                if translated_record["latitude"] is not None and translated_record["longitude"] is not None:
                    translated_record["location"] = f"{translated_record['latitude']},{translated_record['longitude']}"
        return translated_record