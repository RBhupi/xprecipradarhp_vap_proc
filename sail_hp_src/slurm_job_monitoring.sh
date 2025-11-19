#!/bin/bash
# General SLURM job monitor
# Usage:
#   ./monitor_jobs.sh           # monitor current user's jobs with prefix "hp"
#   ./monitor_jobs.sh braut hp  # monitor user "braut" jobs starting with prefix "hp"

set -euo pipefail

USER_NAME=${1:-$USER}
PREFIX=${2:-hp}

echo "=============================================================="
echo " SLURM Job Monitor  (user=$USER_NAME, prefix=${PREFIX}*)"
echo " Time: $(date)"
echo "=============================================================="

echo
echo " Active jobs:"
echo "--------------------------------------------------------------"
squeue -u "$USER_NAME" -o "%.18i %.10T %.12P %.10M %.6D %.20R %.30j" \
    | grep -E "^| ${PREFIX}" || echo "No active ${PREFIX}* jobs."

echo
echo " Summary:"
echo "--------------------------------------------------------------"
total=$(sacct -u "$USER_NAME" --format=JobID,JobName,State --noheader \
        | grep -E "${PREFIX}" | wc -l)

running=$(squeue -u "$USER_NAME" | grep -E "${PREFIX}" | wc -l)

completed=$(sacct -u "$USER_NAME" --format=JobID,JobName,State --noheader \
           | grep -E "${PREFIX}" | grep -E "COMPLETED" | wc -l)

failed=$(sacct -u "$USER_NAME" --format=JobID,JobName,State --noheader \
        | grep -E "${PREFIX}" | grep -E "FAILED|CANCELLED|TIMEOUT" | wc -l)

echo " Total jobs:      $total"
echo " Running/Pending: $running"
echo " Completed:       $completed"
echo " Failed:          $failed"

echo
echo " Recently finished jobs:"
echo "--------------------------------------------------------------"
sacct -u "$USER_NAME" \
      --format=JobID,JobName%30,State,Elapsed,MaxRSS,AllocCPUS%5 \
      --noheader \
      | grep -E "${PREFIX}" \
      | tail -n 15

echo
echo " Done."
