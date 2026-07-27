"""
LSPR23 Dataset Download, Preprocessing, and Cyber Kill Chain Mapping

Steps:
  1. Query Zenodo API to get the download URL for the dataset archive.
  2. Download the archive with wget (supports resume with -c flag).
  3. Extract the archive, preserving the LSPR23 folder structure.
  4. Process the 9.8 GB CSV in chunks to avoid MemoryError.
  5. Map IDS alert category codes to Cyber Kill Chain stages.

Data Cleansing Strategy:
  - "Unknown Attack" anomaly: attack_flag=1 but category='benign'
    -> Renamed to 'outlier' to prevent label noise.
  - "Silent Attack" anomaly: explicit attack category but attack_flag=0
    -> attack_flag forced to 1 for all rows where attack_step > 0.

Cyber Kill Chain Mapping (IDS category codes 1.0–15.0):
  Step 1 (Reconnaissance): 1.0 misc activity, 6.0 network scan, 11.0 device retrieving external ip
  Step 4 (Exploitation):   2.0 protocol command decode, 7.0 potentially bad traffic, 13-15.0 privilege/web attacks
  Step 5 (Installation):   10.0 unwanted program detected
  Step 6 (C&C):            5.0 network trojan detected
  Step 7 (Objectives):     3.0 attempted DoS, 4.0 privacy violation, 8.0 information leak
"""

import json
import os
import subprocess
import urllib.request
import zipfile
from datetime import datetime

import pandas as pd

ZENODO_RECORD_ID = "8042347"
# ls23pr_flows.zip contains a single CSV (ls23pr_v1.csv) at its root, so
# extractall(".") drops it directly into this directory.
INPUT_FILE = "./ls23pr_v1.csv"
OUTPUT_FILE = "./Reformatted_LSPR23.csv"


# ============================================================
# Download
# ============================================================

def get_zenodo_zip_url():
    # Use the /files endpoint: returns {"entries": [...]} with a stable list format.
    # The main /api/records/{id} endpoint returns files as a dict in newer Zenodo (InvenioRDM),
    # which would break iteration, so we avoid it here.
    files_api = f"https://zenodo.org/api/records/{ZENODO_RECORD_ID}/files"
    print(f"[INFO] Querying Zenodo: {files_api}")
    with urllib.request.urlopen(files_api, timeout=30) as resp:
        data = json.loads(resp.read())

    entries = data.get("entries", [])
    if isinstance(entries, dict):
        entries = list(entries.values())

    if not entries:
        raise RuntimeError(
            f"No files found in Zenodo record {ZENODO_RECORD_ID}.\n"
            f"Download manually: https://zenodo.org/records/{ZENODO_RECORD_ID}"
        )

    zip_entries = [e for e in entries if e.get("key", "").endswith(".zip")]
    target = zip_entries[0] if zip_entries else entries[0]
    key = target["key"]

    # Construct the direct download URL — stable regardless of API link format.
    download_url = f"https://zenodo.org/records/{ZENODO_RECORD_ID}/files/{key}"
    size_mb = target.get("size", 0) / (1024 ** 2)
    print(f"[INFO] File: {key}  ({size_mb:.0f} MB)")
    return download_url, key


def download_with_wget(url, output_path):
    if os.path.exists(output_path):
        print(f"[INFO] Partial file detected. Resuming download: {output_path}")
    else:
        print(f"[INFO] Starting download: {url}")
    try:
        subprocess.run(["wget", "-c", "-O", output_path, url], check=True)
    except FileNotFoundError:
        raise RuntimeError(
            "[ERROR] wget not found. Install it or download manually:\n"
            f"  URL: {url}\n"
            f"  Save as: {output_path}"
        )


def extract_zip(archive_path):
    print(f"[INFO] Extracting: {archive_path}")
    with zipfile.ZipFile(archive_path, "r") as z:
        z.extractall(".")
    os.remove(archive_path)
    print("[INFO] Extraction complete. Archive removed.")


# ============================================================
# Checkpoint Utilities
# ============================================================

def _get_checkpoint_path(output_filepath):
    dirpath = os.path.dirname(output_filepath) or "."
    basename = os.path.basename(output_filepath)
    return os.path.join(dirpath, f".ckpt_{basename}.json")


def _load_checkpoint(checkpoint_filepath):
    if not os.path.exists(checkpoint_filepath):
        return None
    try:
        with open(checkpoint_filepath, "r", encoding="utf-8") as f:
            ckpt = json.load(f)
        print(f"[RESUME] Checkpoint detected: '{checkpoint_filepath}'")
        print(f"[RESUME] Last completed chunk : {ckpt['last_completed_chunk']:,}")
        print(f"[RESUME] Rows already processed: {ckpt['rows_processed']:,}")
        print(f"[RESUME] Saved at             : {ckpt['saved_at']}")
        print(f"[RESUME] Skipping to chunk {ckpt['last_completed_chunk'] + 1} ...")
        return ckpt
    except (json.JSONDecodeError, KeyError) as e:
        print(f"[WARNING] Checkpoint file is corrupted ({e}). Starting fresh.")
        os.remove(checkpoint_filepath)
        return None


def _save_checkpoint(checkpoint_filepath, chunk_count, total_rows,
                     total_unmapped, cumulative_steps, output_filepath):
    ckpt = {
        "last_completed_chunk": int(chunk_count),
        "rows_processed": int(total_rows),
        "total_unmapped": int(total_unmapped),
        "cumulative_steps": {str(k): int(v) for k, v in cumulative_steps.items()},
        "output_filepath": output_filepath,
        "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    with open(checkpoint_filepath, "w", encoding="utf-8") as f:
        json.dump(ckpt, f, indent=2, ensure_ascii=False)


# ============================================================
# Processing
# ============================================================

def process_lspr23_dataset(input_filepath, output_filepath, chunk_size=100000):
    print(f"[INFO] Starting processing: {input_filepath}")
    print(f"[INFO] Chunk size: {chunk_size:,} rows")

    category_name_mapping = {
        "1.0": "misc activity", "1": "misc activity",
        "2.0": "generic protocol command decode", "2": "generic protocol command decode",
        "3.0": "attempted denial of service", "3": "attempted denial of service",
        "4.0": "potential corporate privacy violation", "4": "potential corporate privacy violation",
        "5.0": "network trojan detected", "5": "network trojan detected",
        "6.0": "network scan", "6": "network scan",
        "7.0": "potentially bad traffic", "7": "potentially bad traffic",
        "8.0": "attempted information leak", "8": "attempted information leak",
        "9.0": "empty category", "9": "empty category",
        "10.0": "unwanted program detected", "10": "unwanted program detected",
        "11.0": "device retrieving external ip", "11": "device retrieving external ip",
        "12.0": "not suspicious traffic", "12": "not suspicious traffic",
        "13.0": "web application attack", "13": "web application attack",
        "14.0": "attempted user privilege gain", "14": "attempted user privilege gain",
        "15.0": "attempted administrator privilege gain", "15": "attempted administrator privilege gain",
    }

    kill_chain_mapping = {
        "normal": 0, "none": 0, "benign": 0,
        "empty category": 0,
        "not suspicious traffic": 0,
        "outlier": -1,
        "reconn": 1, "comminj": 4, "backdoor": 5,
        "misc activity": 1,
        "network scan": 1,
        "device retrieving external ip": 1,
        "generic protocol command decode": 4,
        "potentially bad traffic": 4,
        "web application attack": 4,
        "attempted user privilege gain": 4,
        "attempted administrator privilege gain": 4,
        "unwanted program detected": 5,
        "network trojan detected": 6,
        "attempted denial of service": 7,
        "potential corporate privacy violation": 7,
        "attempted information leak": 7,
    }

    checkpoint_filepath = _get_checkpoint_path(output_filepath)
    ckpt = _load_checkpoint(checkpoint_filepath)

    if ckpt and not os.path.exists(output_filepath):
        print("[WARNING] Checkpoint found but output file is missing. Starting fresh.")
        ckpt = None

    if ckpt:
        chunk_count = ckpt["last_completed_chunk"] + 1
        total_rows = ckpt["rows_processed"]
        total_unmapped = ckpt["total_unmapped"]
        cumulative_steps = pd.Series(
            {int(k): v for k, v in ckpt["cumulative_steps"].items()}, dtype=int
        )
        rows_to_skip = total_rows
    else:
        if os.path.exists(output_filepath):
            os.remove(output_filepath)
            print(f"[INFO] Removed stale output file: {output_filepath}")
        chunk_count = 1
        total_rows = 0
        total_unmapped = 0
        cumulative_steps = pd.Series(dtype=int)
        rows_to_skip = 0

    unmapped_values_set = set()
    skip_func = (lambda x: 0 < x <= rows_to_skip) if rows_to_skip > 0 else None
    reader = pd.read_csv(input_filepath, chunksize=chunk_size, skiprows=skip_func, low_memory=False)

    for chunk in reader:
        if "Category" in chunk.columns:
            chunk["Category"] = chunk["Category"].fillna("benign")
            chunk["Category"] = chunk["Category"].replace({0: "benign", 0.0: "benign", "0": "benign", "0.0": "benign"})
            chunk.rename(columns={"Category": "attack_name"}, inplace=True)

        if "Label" in chunk.columns:
            chunk.rename(columns={"Label": "attack_flag"}, inplace=True)

        if "attack_flag" in chunk.columns:
            chunk["attack_flag"] = pd.to_numeric(chunk["attack_flag"], errors="coerce").fillna(0).astype(int)

        if "attack_name" in chunk.columns and "attack_flag" in chunk.columns:
            chunk["attack_name"] = chunk["attack_name"].astype(str).str.strip().str.lower()
            conflict_mask = (chunk["attack_flag"] == 1) & (chunk["attack_name"] == "benign")
            chunk.loc[conflict_mask, "attack_name"] = "outlier"
            chunk["attack_name"] = chunk["attack_name"].replace(category_name_mapping)

        if "attack_name" in chunk.columns:
            name_series = chunk["attack_name"]
            unmapped = name_series[~name_series.isin(kill_chain_mapping.keys())].unique()
            unmapped_values_set.update(unmapped)
            chunk["attack_step"] = name_series.map(kill_chain_mapping).fillna(-1).astype(int)

        if "attack_step" in chunk.columns:
            chunk["attack_flag"] = (chunk["attack_step"] != 0).astype(int)

        target_columns = ["attack_name", "attack_flag", "attack_step"]
        target_columns = [c for c in target_columns if c in chunk.columns]
        feature_columns = [c for c in chunk.columns if c not in target_columns]
        chunk = chunk[feature_columns + target_columns]

        total_rows += len(chunk)
        chunk_unmapped_count = (chunk["attack_step"] == -1).sum()
        total_unmapped += chunk_unmapped_count
        mapped_count = total_rows - total_unmapped
        chunk_step_counts = chunk["attack_step"].value_counts()
        cumulative_steps = cumulative_steps.add(chunk_step_counts, fill_value=0)

        print("-" * 50)
        print(f"[CHUNK {chunk_count:04d}]  total={total_rows:,}  mapped={mapped_count:,}  unconfirmed={total_unmapped:,}")
        print("-" * 50)

        write_header = (chunk_count == 1) and (rows_to_skip == 0)
        chunk.to_csv(output_filepath, index=False, mode="w" if write_header else "a", header=write_header)
        _save_checkpoint(checkpoint_filepath, chunk_count, total_rows, total_unmapped, cumulative_steps, output_filepath)
        chunk_count += 1

    print("=" * 60)
    print("[FINAL REPORT] LSPR23 Processing Completed")
    print(f"  Total rows   : {total_rows:,}")
    print(f"  Mapped (0~7) : {total_rows - total_unmapped:,}")
    print(f"  Unmapped (-1): {total_unmapped:,}")
    print("-" * 60)
    for step, count in cumulative_steps.sort_index().items():
        print(f"  Step {int(step):2d} : {int(count):,}")
    print("=" * 60)
    print(f"[INFO] Output: {output_filepath}")

    if unmapped_values_set:
        print(f"[WARNING] Unmapped categories: {list(unmapped_values_set)}")

    if os.path.exists(checkpoint_filepath):
        os.remove(checkpoint_filepath)
        print(f"[INFO] Checkpoint removed: {checkpoint_filepath}")


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    if not os.path.exists(INPUT_FILE):
        url, archive_name = get_zenodo_zip_url()
        download_with_wget(url, archive_name)
        extract_zip(archive_name)
    else:
        print(f"[INFO] Dataset already present at {INPUT_FILE}. Skipping download.")

    if not os.path.exists(OUTPUT_FILE):
        process_lspr23_dataset(INPUT_FILE, OUTPUT_FILE)
    else:
        print(f"[INFO] {OUTPUT_FILE} already exists. Skipping processing.")
