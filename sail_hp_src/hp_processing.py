# hp_processing.py

import pyart
import numpy as np
import sys
from datetime import datetime
import subprocess
import logging
from csu_radartools import csu_fhc

# Mappings for CSU Summer, Winter, and Py-ART classifications to HydroPhase (hp)
csu_summer_to_hp = np.array([0, 1, 1, 2, 2, 4, 2, 3, 3, 3, 1])
csu_winter_to_hp = np.array([0, 2, 2, 2, 2, 4, 3, 1])
pyart_to_hp = np.array([0, 2, 2, 1, 3, 1, 2, 4, 4, 3])


def read_radar(file, sweep=None):
    radar = pyart.io.read(file)
    return radar.extract_sweeps([sweep]) if sweep is not None else radar

def classify_summer(radar):
    logging.info("Running CSU Summer classification")
    dbz = radar.fields['corrected_reflectivity']['data']
    zdr = radar.fields['corrected_differential_reflectivity']['data']
    kdp = radar.fields['corrected_specific_diff_phase']['data']
    rhv = radar.fields['RHOHV']['data']
    rtemp = radar.fields['sounding_temperature']['data']
    scores = csu_fhc.csu_fhc_summer(dz=dbz, zdr=zdr, rho=rhv, kdp=kdp, use_temp=True, band='X', T=rtemp)
    return csu_summer_to_hp[scores]

def classify_winter(radar):
    logging.info("Running CSU Winter classification")
    dz = np.ma.masked_array(radar.fields['DBZ']['data'])
    zdr = np.ma.masked_array(radar.fields['ZDR']['data'])
    kd = np.ma.masked_array(radar.fields['PHIDP']['data'])
    rh = np.ma.masked_array(radar.fields['RHOHV']['data'])
    sn = np.ma.masked_array(radar.fields['signal_to_noise_ratio']['data'])
    rtemp = radar.fields['sounding_temperature']['data']
    heights_km = radar.fields['height']['data'] / 1000
    azimuths = radar.azimuth['data']
    hcawinter = csu_fhc.run_winter(dz=dz, zdr=zdr, kdp=kd, rho=rh, azimuths=azimuths, sn_thresh=-30,
                                   expected_ML=2.0, sn=sn, T=rtemp, heights=heights_km, nsect=36,
                                   scan_type=radar.scan_type, verbose=False, use_temp=True, band='S',
                                   return_scores=False)
    return csu_winter_to_hp[hcawinter]

def classify_pyart(radar):
    logging.info("Running Py-ART classification")
    radar.instrument_parameters['frequency'] = {'long_name': 'Radar frequency', 'units': 'Hz', 'data': [9.2e9]}
    hydro = pyart.retrieve.hydroclass_semisupervised(radar,
                refl_field="corrected_reflectivity",
                zdr_field="corrected_differential_reflectivity",
                kdp_field="filtered_corrected_specific_diff_phase",
                rhv_field="RHOHV",
                temp_field="sounding_temperature")
    return pyart_to_hp[hydro['data']]

def add_classification_field(classified, radar, field_name, desc, config):
    fill = config["fill_value"]
    masked = np.ma.asanyarray(classified)
    dz_field = 'corrected_reflectivity'
    if hasattr(radar.fields[dz_field]['data'], 'mask'):
        masked.mask = np.logical_or(masked.mask, radar.fields[dz_field]['data'].mask)
    field_dict = {
        'data': masked,
        'units': '', 'long_name': desc, 'standard_name': 'hydrometeor phase',
        '_FillValue': fill, "valid_min": 0, "valid_max": 4,
        "classification_description": "0:Unclassified, 1:Liquid, 2:Frozen, 3:High-Density Frozen, 4:Melting"
    }
    radar.add_field(field_name, field_dict, replace_existing=True)

def filter_fields(radar, config):
    radar.fields = {k: radar.fields[k] for k in config['filter_fields'] if k in radar.fields}
    return radar

def compute_npoints(extent, res): return int((extent[1] - extent[0]) / res)

def grid_radar(radar, config):
    xg = compute_npoints(config['x_grid_limits'], config['grid_resolution'])
    yg = compute_npoints(config['y_grid_limits'], config['grid_resolution'])
    zg = compute_npoints(config['z_grid_limits'], config['grid_resolution'])
    grid = pyart.map.grid_from_radars(radar, grid_shape=(zg, yg, xg),
                                      grid_limits=(config['z_grid_limits'],
                                                   config['y_grid_limits'],
                                                   config['x_grid_limits']),
                                      method='nearest')
    return grid.to_xarray()

def subset_lowest_level(ds, config):
    hp_fields = [v for v in ds.variables if "hp" in v] + config["additional_fields"]
    
    # Create temporary height field masked by reflectivity, fillna with 10km (above grid)
    ds["height_expanded"] = (ds.z * (ds.corrected_reflectivity/ds.corrected_reflectivity)).fillna(10_000)
    
    # Find the lowest valid level for each x,y pixel (2D index array)
    min_index = ds.height_expanded.argmin(dim='z', skipna=True)
    
    # Subset all hp fields and reflectivity at the lowest valid level
    subset_ds = ds[hp_fields].isel(z=min_index)
    
    # Add the actual height values at those lowest levels as a new variable
    subset_ds["lowest_height"] = ds.height_expanded.isel(z=min_index)
    
    return subset_ds

def update_metadata(ds, config):
    ds.attrs['field_names'] = ', '.join(ds.data_vars.keys())
    ds.attrs['radar_name'] = config['radar_name']
    ds.attrs['attributions'] = config['attributions']
    ds.attrs['vap_name'] = config['vap_name']
    ds.attrs['process_version'] = config['process_version']
    ds.attrs['known_issues'] = config['known_issues']
    ds.attrs['input_datastream'] = config['input_datastream']
    ds.attrs['developers'] = config['developers']
    ds.attrs['datastream'] = config['datastream']
    ds.attrs['platform_id'] = config['platform_id']
    ds.attrs['dod_version'] = config['dod_version']
    ds.attrs['doi'] = config['doi']
    ds.attrs['command_line'] = " ".join(sys.argv)
    return ds

def make_squire_grid(radar, config):
    ds = grid_radar(radar, config)
    ds = subset_lowest_level(ds, config)
    return ds

def process_file(file, config, season):
    radar = read_radar(file)
    
    # Run CSU classification based on season
    if season == "summer":
        field = config["classification_fields"]["summer"]["field_name"]
        desc = config["classification_fields"]["summer"]["long_name"]
        add_classification_field(classify_summer(radar), radar, field, desc, config)
    elif season == "winter":
        field = config["classification_fields"]["winter"]["field_name"]
        desc = config["classification_fields"]["winter"]["long_name"]
        add_classification_field(classify_winter(radar), radar, field, desc, config)
    else:
        raise ValueError(f"Invalid season: {season}. Must be 'summer' or 'winter'.")
    
    # Always run PyART classification
    field = config["classification_fields"]["pyart"]["field_name"]
    desc = config["classification_fields"]["pyart"]["long_name"]
    add_classification_field(classify_pyart(radar), radar, field, desc, config)
    
    radar = filter_fields(radar, config)
    return make_squire_grid(radar, config)
