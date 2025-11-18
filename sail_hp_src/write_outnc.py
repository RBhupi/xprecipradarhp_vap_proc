# write_outnc.py

import re
from pathlib import Path
from netCDF4 import Dataset
import numpy as np

def _parse_attribute_value(key, value_str):
    """
    Parse attribute value with correct type based on type hint in key.
    Handles cases like '_FillValue:float = -9999' -> returns float(-9999) and key '_FillValue'
    """
    value = value_str.strip().strip('"')
    
    # Check for type hints like _FillValue:float, flag_values:short
    if ':' in key:
        attr_name, type_hint = key.rsplit(':', 1)
        
        if type_hint == 'float':
            return attr_name, float(value)
        elif type_hint == 'double':
            return attr_name, float(value)
        elif type_hint == 'short' or type_hint == 'int':
            if ',' in value:
                # Parse comma-separated values as tuple of ints
                return attr_name, tuple(int(x.strip()) for x in value.split(','))
            return attr_name, int(value)
        elif type_hint == 'byte':
            return attr_name, int(value)
        else:
            # Unknown type hint, keep as string but remove hint
            return attr_name, value
    
    # No type hint - return as-is
    return key, value


def _parse_dod(dod_path):
    lines = Path(dod_path).read_text().splitlines()
    dims, variables, globals_ = {}, {}, {}
    current_var = None
    in_globals = False

    for raw in lines:
        line = raw.rstrip().replace('\t', ' ')
        if not line.strip():
            continue
        if line.strip().startswith("#"):
            in_globals = True
            current_var = None
            continue
        indent = len(line) - len(line.lstrip())
        if not in_globals and indent == 0:
            current_var = None
            if "=" in line and "(" not in line and ":" not in line:
                key, val = map(str.strip, line.split("=", 1))
                dims[key] = None if val.upper() == "UNLIMITED" else int(val)
                continue
            m = re.match(r"^(\w+)\((.*?)\):(\w+)", line)
            if m:
                name, dims_str, dtype = m.groups()
                dims_tuple = tuple(d.strip() for d in dims_str.split(",")) if dims_str else ()
                variables[name] = {"dtype": dtype, "dims": dims_tuple, "attrs": {}}
                current_var = name
                continue
            m2 = re.match(r"^(\w+)\(\):(\w+)", line)
            if m2:
                name, dtype = m2.groups()
                variables[name] = {"dtype": dtype, "dims": (), "attrs": {}}
                current_var = name
                continue
        elif indent == 4 and current_var:
            if "=" in line:
                ak, av = map(str.strip, line.split("=", 1))
                # Parse with type conversion
                clean_key, typed_value = _parse_attribute_value(ak, av)
                variables[current_var]["attrs"][clean_key] = typed_value
            else:
                # Attribute with no value
                clean_key = line.strip()
                if ':' in clean_key:
                    clean_key = clean_key.split(':')[0]
                variables[current_var]["attrs"][clean_key] = ""
        elif in_globals and indent == 2:
            if "=" in line:
                key, val = map(str.strip, line.split("=", 1))
                globals_[key] = val.strip('"')
            else:
                globals_[line.strip()] = ""

    return {"dimensions": dims, "variables": variables, "globals": globals_}


def _update_dod_globals(dod, config):
    """Fill missing global attributes from config"""
    import sys
    import subprocess
    from datetime import datetime
    
    missing = []

    # Auto-generate command_line
    config["command_line"] = " ".join(sys.argv) if hasattr(sys, "argv") and len(sys.argv) > 0 else "notebook"

    # Auto-generate history with timestamp and system info
    current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    try:
        system_info = subprocess.check_output(['uname', '-n'], encoding='utf-8').strip()
    except:
        system_info = "unknown"
    config["history"] = f"created on {current_time} on {system_info}"

    # For every global attr DOD defines:
    for key, val in dod["globals"].items():
        if val == "":                 # value missing in DOD
            if key in config:         # we can fill from config
                dod["globals"][key] = str(config[key])
            else:                     # cannot fill → error
                missing.append(key)

    if missing:
        raise ValueError(
            f"DOD has empty global attributes not provided in config: {missing}"
        )

    return dod



def _create_nc_structure(path, dod):
    nc = Dataset(path, "w", format="NETCDF4")

    for dim, size in dod["dimensions"].items():
        nc.createDimension(dim, size)

    for gk, gv in dod["globals"].items():
        setattr(nc, gk, gv)

    for varname, vinfo in dod["variables"].items():
        dtype = vinfo["dtype"]
        dims = vinfo["dims"]
        attrs = dict(vinfo["attrs"])

        fill = attrs.pop("_FillValue", None)
        var = nc.createVariable(varname, dtype, dims, fill_value=fill) if fill else nc.createVariable(varname, dtype, dims)

        for ak, av in attrs.items():
            setattr(var, ak, av)

    return nc





from datetime import datetime, timezone


def _get_standardized_times(ds):
    t = ds.time.values[0]
    t64 = np.datetime64(t)
    dt = t64.astype("datetime64[s]").astype(datetime)

    base_time_val = int((t64 - np.datetime64("1970-01-01T00:00:00Z")) /
                        np.timedelta64(1, "s"))

    midnight = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    midnight64 = np.datetime64(midnight)
    time_since_midnight_val = (t64 - midnight64) / np.timedelta64(1, "s")

    # Format scan time string
    scan_time_str = dt.strftime("%Y-%m-%d %H:%M:%S 0:00")

    return {
        "base_time": {
            "value": np.int32(base_time_val),
            "string": scan_time_str,
            "units": "seconds since 1970-1-1 0:00:00 0:00"
        },
        "time_offset": {
            "value": np.array([0.0], dtype="float64"),
            "units": f"seconds since {scan_time_str}"  # time_offset is relative to base_time (scan time)
        },
        "time": {
            "value": np.array([time_since_midnight_val], dtype="float64"),
            "units": f"seconds since {dt.strftime('%Y-%m-%d 00:00:00 0:00')}",
            "string": scan_time_str
        }
    }

def _update_dod_time_attributes(dod, ds):
    times = _get_standardized_times(ds)

    for tname, meta in times.items():
        if tname not in dod["variables"]:
            continue

        attrs = dod["variables"][tname]["attrs"]

        # Fill only attributes present in DOD AND empty
        for attr_key, attr_val in attrs.items():
            if attr_val == "":   # missing in DOD
                if attr_key in meta:
                    attrs[attr_key] = meta[attr_key]

    return dod



def _write_dataset_to_file(ds, nc, dod, config):
    """Write only data values (no attributes). All attrs come from DOD."""
    
    # Standardized numeric time variables
    times = _get_standardized_times(ds)

    # Variables that MUST NOT be written from ds
    time_vars = {"time", "time_offset", "base_time"}

    # Type mapping from DOD dtype to numpy dtype
    dtype_map = {
        'float': np.float32,
        'double': np.float64,
        'short': np.int16,
        'int': np.int32,
        'byte': np.int8
    }

    for xr_name, dod_name in config["variable_mapping"].items():

        # Skip variables not in ds or not in nc
        if xr_name not in ds or dod_name not in nc.variables:
            continue

        # Skip all time-like fields (we write them separately)
        if dod_name in time_vars:
            continue

        data = ds[xr_name].values
        var = nc.variables[dod_name]

        # Get fill value from variable
        dod_fill = getattr(var, "_FillValue", None)

        # Handle masked fields
        if hasattr(data, "mask"):
            data = np.ma.filled(data, fill_value=dod_fill)

        # Replace NaN with DOD fill value
        if dod_fill is not None and np.any(np.isnan(data)):
            data = np.where(np.isnan(data), dod_fill, data)

        # Ensure time is never written from ds
        if len(data) > 0 and "cftime" in str(type(data.flat[0])):
            raise TypeError(
                f"Attempted to write cftime object for variable {xr_name}. "
                "Time variables are not allowed to come from ds."
            )

        # Convert to correct dtype based on DOD specification
        if dod_name in dod["variables"]:
            dod_dtype = dod["variables"][dod_name]["dtype"]
            target_dtype = dtype_map.get(dod_dtype)
            if target_dtype and data.dtype != target_dtype:
                data = data.astype(target_dtype)

        # Now safe to write
        nc.variables[dod_name][:] = data

    # Write standardized numeric time variables only
    for tname, tinfo in times.items():
        if tname in nc.variables:
            nc.variables[tname][:] = tinfo["value"]
    
    # Handle radar metadata from ds.attrs if available
    if 'radar_lat' in nc.variables:
        if 'origin_latitude' in ds.attrs:
            nc.variables['radar_lat'][:] = ds.attrs['origin_latitude']
        else:
            nc.variables['radar_lat'][:] = 0.0
    
    if 'radar_lon' in nc.variables:
        if 'origin_longitude' in ds.attrs:
            nc.variables['radar_lon'][:] = ds.attrs['origin_longitude']
        else:
            nc.variables['radar_lon'][:] = 0.0
    
    if 'radar_alt' in nc.variables:
        if 'origin_altitude' in ds.attrs:
            nc.variables['radar_alt'][:] = ds.attrs['origin_altitude']
        else:
            nc.variables['radar_alt'][:] = 0.0

    nc.close()




def write_ds_to_nc(ds, dod_template_path, output_path, config):
    """Write xarray dataset to NetCDF using DOD template"""
    dod = _parse_dod(dod_template_path)

    dod = _update_dod_globals(dod, config)
    dod = _update_dod_time_attributes(dod, ds)

    nc = _create_nc_structure(output_path, dod)
    
    # Dynamically generate 'fields' global attribute from actual variables
    variable_list = list(nc.variables.keys())
    nc.fields = ', '.join(variable_list)
    
    _write_dataset_to_file(ds, nc, dod, config)



