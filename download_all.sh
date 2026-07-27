#!/bin/bash
# Download and process all (or selected) datasets.
#
# Usage:
#   bash download_all.sh                  # download everything
#   bash download_all.sh cicids2017 lspr23  # download specific datasets only
#
# Each dataset runs its download.py in its own subdirectory.
# A failed dataset is logged but does not stop the rest.

set -euo pipefail

DATASETS=(
    # Phase 1 — Original Collection
    cicids2017
    cicids2018
    ciciot2023
    ton-iot
    # mirai: pre-processed files included in repo; no download.py
    edge-iiot
    xiiotid
    nf-ton-iot-v3
    wustl-iiot-2021
    lspr23
    # Phase 2 — Expansion
    kddcup1999
    nsl-kdd
    unsw-nb15
    ctu-13
    n-baiot
    cidds-001
    cidds-002
    iotid20
    hikari-2021
    insdn
    bot-iot
    cic-ddos2019
    kitsune
    iot-23
    # Phase 2 — Improved CIC-IDS (Liu et al., 2022)
    cicids2017-imp
    cicids2018-imp
    # Phase 2 — Pending (Kaggle license acceptance required before running)
    awid2
    awid3
)

if [ $# -gt 0 ]; then
    DATASETS=("$@")
fi

FAILED=()
SKIPPED=()
SUCCEEDED=()

for dataset in "${DATASETS[@]}"; do
    # Final artifacts are the only reliable "already done" marker: split_util.py
    # deletes Reformatted_*.csv when CLEAN=true, so a download.py that guards on
    # its own output would re-download everything after a split.
    if [ -f "$dataset/training-flow.csv" ] && [ -f "$dataset/test-flow.csv" ]; then
        echo "[SKIP] $dataset: training-flow.csv / test-flow.csv already exist"
        SKIPPED+=("$dataset")
        continue
    fi

    if [ ! -f "$dataset/download.py" ]; then
        echo "[SKIP] $dataset: no download.py found"
        SKIPPED+=("$dataset")
        continue
    fi

    echo ""
    echo "=============================="
    echo " $dataset"
    echo "=============================="
    if (cd "$dataset" && python3 download.py); then
        SUCCEEDED+=("$dataset")
    else
        echo "[ERROR] $dataset failed — continuing with next dataset"
        FAILED+=("$dataset")
    fi
done

echo ""
echo "=============================="
echo " Summary"
echo "=============================="
echo "  Succeeded : ${#SUCCEEDED[@]}  — ${SUCCEEDED[*]:-none}"
echo "  Failed    : ${#FAILED[@]}  — ${FAILED[*]:-none}"
echo "  Skipped   : ${#SKIPPED[@]}  — ${SKIPPED[*]:-none}"

if [ ${#FAILED[@]} -gt 0 ]; then
    exit 1
fi
