# HydroPhase Radar Processing Pipeline

Production-ready pipeline for processing SAIL campaign X-band precipitation radar data with hydrometeor phase classification.

## Quick Start

### Process Data with Sequential Script (Recommended)
```bash
python sail_hp_src/run_hp.py 2022 08 \
  --data_dir /gpfs/wolf2/arm/atm124/world-shared/gucxprecipradarcmacS2.c1/ppi/202208 \
  --output_dir /gpfs/wolf2/arm/atm124/proj-shared/HydroPhase202511_test/202208 \
  --dod_template sail_hp_src/dod_v1-3.txt \
  --season summer
```

### Process with Dask (Parallel)
```bash
nohup python sail_hp_src/run_hp_dask.py 2022 08 \
  --season summer \
  --data_dir /gpfs/wolf2/arm/atm124/world-shared/gucxprecipradarcmacS2.c1/ppi/202208 \
  --output_dir /gpfs/wolf2/arm/atm124/proj-shared/HydroPhase202511_test/202208 \
  --dod_template sail_hp_src/dod_v1-3.txt \
  --n_workers 2 \
  --memory_limit 6GB \
  --batch_size 8 \
  > hp_202208.log 2>&1 &
```

## Command Options

### Required Arguments
- `year` - Year (YYYY)
- `month` - Month (MM)
- `--season` - Classification scheme: `summer` or `winter`
- `--data_dir` - Input directory with radar files
- `--output_dir` - Output directory for processed files
- `--dod_template` - DOD template file path

### Optional Arguments
- `--rerun` - Reprocess all files (ignore existing output)
- `--n_workers` - Number of Dask workers (default: auto)
- `--threads_per_worker` - Threads per worker (default: 1)
- `--memory_limit` - Memory per worker (default: 8GB)
- `--batch_size` - Tasks to submit at once (default: 15)

## Monitor Jobs

### Check running processes
```bash
ps aux | grep run_hp
```

### Check SLURM job status
```bash
squeue -u $USER
squeue -u braut | grep 935601
```

### View logs
```bash
tail -f hp_202208.log
```

### Kill running job
```bash
kill <PID>
```

## Output

- **Format**: ARM DOD v1.3 compliant NetCDF
- **Classification**: CSU (summer/winter) + PyART unified to 5 categories:
  - 0: Unclassified
  - 1: Liquid
  - 2: LD_Frozen (low-density)
  - 3: HD_Frozen (high-density)
  - 4: Melting

## Performance

- **Single worker**: ~37s/file, 1.56 files/min
- **Recommended**: 1-2 workers @ 6-8GB memory each
- **Full month (240 files)**: ~2.5 hours

## Files

- `run_hp.py` - Sequential processing (simple, stable)
- `run_hp_dask.py` - Parallel processing with batching
- `hp_processing.py` - Core radar processing functions
- `write_outnc.py` - NetCDF output writer
- `config.py` - Configuration settings
