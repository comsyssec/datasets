#!/bin/bash
# Remove regenerable leftovers from datasets that are already finished.
#
# A dataset counts as finished only when BOTH training-flow.csv and test-flow.csv
# exist. Everything else in that directory (raw downloads, the intermediate
# <Name>.csv, Reformatted_*.csv, .zip archives, .partial / .ckpt_* files) is a
# regenerable leftover and gets removed.
#
# Two safety rules:
#   - Datasets not yet split are skipped entirely, so no unsplit work is lost.
#   - git-tracked files are never deleted. mirai/ is our own lab data and is
#     deliberately committed (force-added past the *.csv gitignore rule), so its
#     flow/packet/pcap/label files are protected by this rule.
#
# Usage:
#   bash cleanup.sh            # dry run — only report what would be removed
#   bash cleanup.sh --apply    # actually remove

set -euo pipefail

cd "$(dirname "$0")"

APPLY=false
if [ "${1:-}" = "--apply" ]; then
    APPLY=true
fi

KEEP_BASENAMES=(download.py training-flow.csv test-flow.csv)

declare -A TRACKED
while IFS= read -r f; do
    TRACKED["$f"]=1
done < <(git ls-files)

total_bytes=0
total_files=0
cleaned_datasets=0

for dir in */; do
    dataset="${dir%/}"
    case "$dataset" in
        citations|__pycache__|.*) continue ;;
    esac

    if [ ! -f "$dataset/training-flow.csv" ] || [ ! -f "$dataset/test-flow.csv" ]; then
        echo "[SKIP]  $dataset: not split yet (no training-flow.csv + test-flow.csv)"
        continue
    fi

    victims=()
    while IFS= read -r f; do
        base="$(basename "$f")"
        keep=false
        for k in "${KEEP_BASENAMES[@]}"; do
            [ "$base" = "$k" ] && keep=true && break
        done
        $keep && continue
        [ -n "${TRACKED[$f]:-}" ] && continue
        victims+=("$f")
    done < <(find "$dataset" -mindepth 1 -type f | sort)

    if [ ${#victims[@]} -eq 0 ]; then
        echo "[CLEAN] $dataset: nothing to remove"
        continue
    fi

    dataset_bytes=0
    for f in "${victims[@]}"; do
        size=$(stat -c %s "$f" 2>/dev/null || echo 0)
        dataset_bytes=$((dataset_bytes + size))
    done
    total_bytes=$((total_bytes + dataset_bytes))
    total_files=$((total_files + ${#victims[@]}))
    cleaned_datasets=$((cleaned_datasets + 1))

    printf '[%s] %s: %d file(s), %s\n' \
        "$($APPLY && echo REMOVE || echo DRYRUN)" \
        "$dataset" "${#victims[@]}" \
        "$(numfmt --to=iec --suffix=B "$dataset_bytes" 2>/dev/null || echo "${dataset_bytes}B")"
    for f in "${victims[@]}"; do
        echo "           - ${f#$dataset/}"
        $APPLY && rm -f "$f"
    done
    $APPLY && find "$dataset" -mindepth 1 -type d -empty -delete
done

echo ""
echo "=============================="
echo " Summary"
echo "=============================="
printf '  Datasets affected : %d\n' "$cleaned_datasets"
printf '  Files             : %d\n' "$total_files"
printf '  Space             : %s\n' \
    "$(numfmt --to=iec --suffix=B "$total_bytes" 2>/dev/null || echo "${total_bytes}B")"
if ! $APPLY; then
    echo ""
    echo "  Dry run — nothing was deleted. Re-run with --apply to remove."
fi
