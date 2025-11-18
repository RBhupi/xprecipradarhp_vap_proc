CONFIG = {
    # File patterns
    'input_file_pattern': 'gucxprecipradarcmacppiS2.c1',
    'output_file_pattern': 'gucxprecipradarcmacppihpS2.c1',

    # Fill value matching DOD (_FillValue = -9999)
    'fill_value': -9999,

    # Fields to retain in radar object before gridding
    'filter_fields': [
        'corrected_reflectivity', 'corrected_differential_reflectivity',
        'corrected_specific_diff_phase', 'RHOHV', 'sounding_temperature',
        'hp_semisupervised', 'hp_fhc_summer', 'hp_fhc_winter'
    ],

    # Extra fields to retain in lowest level subset
    'additional_fields': ['corrected_reflectivity'],

    # Grid parameters (used in pyart gridding)
    'x_grid_limits': (-20_000., 20_000.),
    'y_grid_limits': (-20_000., 20_000.),
    'z_grid_limits': (500., 5_000.),
    'grid_resolution': 250,

    # Classification methods mapped to fields (these fields exist in the DOD)
    'classification_fields': {
        'summer': {
            'field_name': 'hp_fhc_summer',  # internal
            'long_name': 'HydroPhase from CSU Summer',
            'function': 'classify_summer'
        },
        'winter': {
            'field_name': 'hp_fhc_winter',
            'long_name': 'HydroPhase from CSU Winter',
            'function': 'classify_winter'
        },
        'pyart': {
            'field_name': 'hp_semisupervised',
            'long_name': 'HydroPhase from Py-ART',
            'function': 'classify_pyart'
        }
    },

    # Mapping xarray variable names to DOD variable names
    'variable_mapping': {
        'corrected_reflectivity': 'corrected_reflectivity',
        'hp_fhc_summer': 'hp_fhc',
        'hp_fhc_winter': 'hp_fhc',
        'hp_semisupervised': 'hp_ssc',
        'lowest_height': 'lowest_height',
        'lat': 'lat',
        'lon': 'lon',
        'x': 'x',
        'y': 'y',
        'radar_lat': 'radar_lat',
        'radar_lon': 'radar_lon',
        'radar_alt': 'radar_alt'
    },

    # Global attributes to fill only if missing in DOD
    'process_version': 'HP-v1.0',
    'dod_version': 'xprecipradarhp-c1-1.3',
    'input_datastream': 'xprecipradarcmacppi',
    'input_datastreams': 'gucxprecipradarcmacppiS2.c1',
    'datastream': 'gucxprecipradarhpS2.c1',
    'platform_id': 'xprecipradar',
    'site_id': 'guc',
    'facility_id': 'X1',
    'data_level': 'c1',
    'location_description': 'Gunnison, Colorado',
    'known_issues': (
        "Issues with SSC method during winter above melting layer, for mixed category."
    ),
    'attributions': (
        "This data is collected by the ARM User Facility. Radar system is operated by the radar engineering team "
        "radar@arm.gov and the data is processed by the precipitation radar products team."
    ),
    'developers': (
        "Bhupendra Raut, ANL., Joseph O'Brien, ANL., Maxwell Grover, ANL., Robert Jackson, ANL., Zachary Sherman, ANL."
    ),
    'doi': '10.5439/2530631'
}

