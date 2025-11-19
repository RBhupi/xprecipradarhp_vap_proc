#!/bin/bash
# Submit HydroPhase processing for many months cleanly.
# Each month is submitted as a separate SLURM batch job.

set -euo pipefail

PY_SCRIPT="/ccsopen/home/braut/projects/xprecipradarhp_vap_proc/sail_hp_src/run_hp_dask.py"
BASE_DATA="/gpfs/wolf2/arm/atm124/world-shared/gucxprecipradarcmacS2.c1/ppi"
BASE_OUT="/gpfs/wolf2/arm/atm124/proj-shared/HydroPhase202511_test"
DOD="/ccsopen/home/braut/projects/xprecipradarhp_vap_proc/sail_hp_src/dod_v1-3.txt"
ENV_PATH="/autofs/nccsopen-svm1_home/braut/data-env1"

MONTHS=(
    202111 202112
    202201 202202 202203 202204 202205 202206
    202207 202208 202209 202210 202211 202212
    202301 202302 202303 202304 202305 202306
)

is_winter() {
    m=${1:4:2}
    [[ "$m" == "12" || "$m" == "01" || "$m" == "02" ]]
}

for ym in "${MONTHS[@]}"; do
    year=${ym:0:4}
    month=${ym:4:2}

    # Season logic
    if is_winter "$ym"; then
        season="winter"
    else
        season="summer"
    fi

    # Build paths
    data_dir="$BASE_DATA/$ym"
    out_dir="$BASE_OUT/$ym"

    # SLURM job file
    job="job_hp_${ym}.slurm"

    cat <<EOF > "$job"
#!/bin/bash
#SBATCH --job-name=hp_${ym}
#SBATCH --account=atm124
#SBATCH --output=hp_${ym}.out
#SBATCH --error=hp_${ym}.err
#SBATCH --partition=batch_all
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=40G
#SBATCH --time=12:00:00

source ${ENV_PATH}/bin/activate

python $PY_SCRIPT $year $month \
    --season $season \
    --data_dir $data_dir \
    --output_dir $out_dir \
    --dod_template $DOD \
    --n_workers 4 \
    --memory_limit 10GB \
    --batch_size 8
EOF

    echo "Submitting: $ym  (season=$season)"
    sbatch "$job"
    rm -f "$job"
done

echo "All month jobs submitted."
