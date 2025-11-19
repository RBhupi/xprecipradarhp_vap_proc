#!/usr/bin/env python
"""
Safer, memory-stable HydroPhase Dask batch processor.
Key improvements:
 - Hard memory caps per worker (prevents worker death)
 - Limit of 1–2 workers unless user overrides
 - Smaller batch submission to avoid long-running heavy graphs
 - Explicit worker timeouts + resilience to retries
 - No change to hp_processing or write_outnc modules
"""

import sys
import os
import argparse
import logging
from pathlib import Path
from datetime import datetime

from dask.distributed import Client, LocalCluster, as_completed

sys.path.insert(0, str(Path(__file__).parent))

from hp_processing import process_file
from write_outnc import write_ds_to_nc
from config import CONFIG


# ---------------------------------------------------
# Logging
# ---------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------
# Helpers
# ---------------------------------------------------
def get_unprocessed_files(input_files, output_dir, config):
    out = []
    for f in input_files:
        base = os.path.basename(f)
        outfile = base.replace(config["input_file_pattern"], config["output_file_pattern"])
        if not (output_dir / outfile).exists():
            out.append(f)
    return out


def process_single_file_wrapper(input_file, output_dir, dod_template, season, config):
    """Runs the heavy processing in one worker safely."""
    try:
        logger.info(f"[START] {input_file}")
        ds = process_file(input_file, config, season)
        name = Path(input_file).name
        outname = name.replace(config["input_file_pattern"], config["output_file_pattern"])
        write_ds_to_nc(ds, dod_template, str(output_dir / outname), config)
        logger.info(f"[DONE]  {outname}")
        return str(outname)
    except Exception as e:
        logger.error(f"[FAIL] {input_file}: {e}")
        return None


def chunked(seq, size):
    buf = []
    for item in seq:
        buf.append(item)
        if len(buf) == size:
            yield buf
            buf = []
    if buf:
        yield buf


# ---------------------------------------------------
# SAFER LOCALCLUSTER
# ---------------------------------------------------
def setup_local_cluster(n_workers=1, memory_limit="6GB"):
    """
    Safer defaults:
      - 1 worker unless user requests more
      - strong memory caps to prevent kill/restart loops
      - threads_per_worker=1 required for Py-ART/CSU
    """
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=1,
        memory_limit=memory_limit,
        processes=True,
        dashboard_address=":8787",
    )
    cli = Client(cluster)
    logger.info(f"Dashboard: {cli.dashboard_link}")
    logger.info(f"Workers={n_workers}, Mem/worker={memory_limit}")
    return cli


# ---------------------------------------------------
# BATCH PROCESSOR (SAFER)
# ---------------------------------------------------
def batch_process_dask(files, output_dir, dod_template, season, config, client,
                       batch_size=8):
    """
    Smaller batches (8) → stable memory use.
    Results streamed as_completed() → no large graphs.
    """
    logger.info(f"Processing {len(files)} files, batch_size={batch_size}")

    results = []
    done = 0
    good = 0
    bad = 0

    for idx, batch in enumerate(chunked(files, batch_size), 1):
        logger.info(f"[BATCH {idx}] {len(batch)} tasks")

        futures = client.map(
            process_single_file_wrapper,
            [str(f) for f in batch],
            output_dir=output_dir,
            dod_template=dod_template,
            season=season,
            config=config,
            retries=2,                     # auto retry on transient failure
            pure=False,                   # avoid unnecessary caching
        )

        for fut in as_completed(futures):
            try:
                res = fut.result(timeout=600)  # 10 min timeout for classification
                results.append(res)
                done += 1
                if res is None:
                    bad += 1
                else:
                    good += 1
            except Exception as e:
                logger.error(f"Future error: {e}")
                results.append(None)
                bad += 1
                done += 1

            if done % 10 == 0:
                logger.info(f"[PROGRESS] {done}/{len(files)}  OK={good}  FAIL={bad}")

    logger.info(f"[FINISHED] OK={good}, FAIL={bad}")
    return results


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------
def main():
    t0 = datetime.now()
    parser = argparse.ArgumentParser(description="Safer HydroPhase Dask runner")

    parser.add_argument("year", type=str)
    parser.add_argument("month", type=str)
    parser.add_argument("--season", required=True, choices=["summer", "winter"])

    parser.add_argument("--data_dir", required=True)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--dod_template", required=True)
    parser.add_argument("--rerun", action="store_true")

    parser.add_argument("--n_workers", type=int, default=1)
    parser.add_argument("--memory_limit", type=str, default="6GB")
    parser.add_argument("--batch_size", type=int, default=8)

    args = parser.parse_args()

    # Paths
    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Files
    pattern = f"{CONFIG['input_file_pattern']}*.nc"
    all_files = sorted(data_dir.glob(pattern))

    if not all_files:
        logger.error(f"No files in {data_dir} matching {pattern}")
        sys.exit(1)

    if args.rerun:
        files_to_process = all_files
    else:
        files_to_process = get_unprocessed_files(all_files, output_dir, CONFIG)

    if not files_to_process:
        logger.info("All files already processed.")
        return

    # Cluster
    client = setup_local_cluster(
        n_workers=args.n_workers,
        memory_limit=args.memory_limit
    )

    try:
        results = batch_process_dask(
            files_to_process,
            output_dir,
            args.dod_template,
            args.season,
            CONFIG,
            client,
            batch_size=args.batch_size,
        )
    finally:
        client.close()

    # Summary
    good = sum(r is not None for r in results)
    bad = len(results) - good

    logger.info("===================================")
    logger.info(f"TOTAL: {len(results)}")
    logger.info(f"OK:    {good}")
    logger.info(f"FAIL:  {bad}")
    logger.info(f"TIME:  {datetime.now() - t0}")


if __name__ == "__main__":
    main()
