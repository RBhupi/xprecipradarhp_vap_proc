# run_hp.py

import sys
import argparse
import os
import logging
from pathlib import Path
from hp_processing import process_file
from write_outnc import write_ds_to_nc
from config import CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def unprocessed_files(files, output_dir, config):
    out = []
    for file in files:
        base = os.path.basename(file)
        outfile = base.replace(config["input_file_pattern"], config["output_file_pattern"])
        if not os.path.exists(os.path.join(output_dir, outfile)):
            out.append(file)
    return out

def main():
    p = argparse.ArgumentParser()
    p.add_argument("year")
    p.add_argument("month")
    p.add_argument("--data_dir", required=True)
    p.add_argument("--output_dir", required=True)
    p.add_argument("--dod_template", required=True)
    p.add_argument("--season", required=True)
    p.add_argument("--rerun", action="store_true")
    args = p.parse_args()

    logger.info(f"Starting HP processing for {args.year}-{args.month} ({args.season} season)")
    logger.info(f"Input directory: {args.data_dir}")
    logger.info(f"Output directory: {args.output_dir}")
    logger.info(f"DOD template: {args.dod_template}")
    logger.info(f"Rerun mode: {args.rerun}")

    in_dir = Path(args.data_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory created: {out_dir}")
    
    files = list(in_dir.glob(f"*{CONFIG['input_file_pattern']}*"))
    logger.info(f"Found {len(files)} total files in input directory")
    
    if not args.rerun:
        files = unprocessed_files(files, out_dir, CONFIG)
        logger.info(f"Filtered to {len(files)} unprocessed files")
    
    if len(files) == 0:
        logger.warning("No files to process. Exiting.")
        return

    # Process files sequentially
    logger.info(f"Starting sequential processing of {len(files)} files")
    
    successes = 0
    failures = 0
    
    for file_path in files:
        logger.info(f"Processing: {file_path.name}")
        try:
            ds = process_file(file_path, CONFIG, args.season)
            outname = Path(file_path.name.replace(CONFIG['input_file_pattern'], CONFIG['output_file_pattern']))
            write_ds_to_nc(ds, args.dod_template, out_dir / outname, CONFIG)
            logger.info(f"Completed: {outname}")
            successes += 1
        except Exception as e:
            logger.error(f"Failed to process {file_path.name}: {str(e)}")
            failures += 1
    
    logger.info(f"Processing complete! Success: {successes}, Failed: {failures}")
if __name__ == "__main__":
    main()
