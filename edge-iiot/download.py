import kagglehub
import os
import shutil
import pandas as pd

KAGGLE_DATASET = "mohamedamineferrag/edgeiiotset-cyber-security-dataset-of-iot-iiot"
REQUIRED_COLUMNS = ["Attack_type", "Attack_label"]
# The archive ships the raw traffic split across many per-sensor/per-attack CSVs
# (Normal traffic/*, Attack traffic/*) plus the authors' pre-combined full
# dataset. Target the combined file by name so we don't grab a single raw file.
PREFERRED_FILENAME = "DNN-EdgeIIoT-dataset.csv"
INPUT_FILENAME = "EdgeIIoT.csv"
OUTPUT_FILENAME = "Reformatted_EdgeIIoT.csv"

# Attack_type -> kill-chain step
KILL_CHAIN = {
    "normal": 0,
    # Reconnaissance
    "vulnerability_scanner": 1,
    "port_scanning": 1,
    "fingerprinting": 1,
    # Exploitation
    "sql_injection": 4,
    "xss": 4,
    "uploading": 4,       # malicious file upload
    "password": 4,        # password brute force
    "mitm": 4,            # cf. Kitsune ARP MitM -> 4
    # Installation
    "backdoor": 5,
    # Actions on Objectives
    "ddos_udp": 7,
    "ddos_icmp": 7,
    "ddos_tcp": 7,
    "ddos_http": 7,
    "ransomware": 7,
}


def find_dataset_csv(base_path, preferred_filename, required_columns):
    # Prefer the combined dataset by exact filename.
    for root, _, files in os.walk(base_path):
        if preferred_filename in files:
            return os.path.join(root, preferred_filename)
    # Fall back to the first CSV that has the required columns.
    for root, _, files in os.walk(base_path):
        for f in sorted(files):
            if not f.endswith(".csv"):
                continue
            fpath = os.path.join(root, f)
            try:
                cols = pd.read_csv(fpath, nrows=0).columns.tolist()
                if all(c in cols for c in required_columns):
                    return fpath
            except Exception:
                continue
    return None


def download():
    print(f"[INFO] Downloading from Kaggle: {KAGGLE_DATASET}")
    path = kagglehub.dataset_download(KAGGLE_DATASET)
    src = find_dataset_csv(path, PREFERRED_FILENAME, REQUIRED_COLUMNS)
    if not src:
        raise RuntimeError(
            f"[ERROR] Could not find a CSV with columns {REQUIRED_COLUMNS} in {path}.\n"
            "Column names may differ from the expected format."
        )
    print(f"[INFO] Found dataset file: {src}")
    dst = os.path.join(os.getcwd(), INPUT_FILENAME)
    shutil.copy(src, dst)
    print(f"[INFO] Saved as: {dst}")
    return dst


def process(input_filepath, output_filepath):
    print(f"[INFO] Reading: {input_filepath}")
    df = pd.read_csv(input_filepath, low_memory=False)

    rename_mapping = {}
    if "Attack_type" in df.columns:
        rename_mapping["Attack_type"] = "attack_name"
    if "Attack_label" in df.columns:
        rename_mapping["Attack_label"] = "attack_flag"
    df.rename(columns=rename_mapping, inplace=True)

    name_series = df["attack_name"].astype(str).str.strip().str.lower()
    unmapped = name_series[~name_series.isin(KILL_CHAIN)].unique()
    if len(unmapped) > 0:
        print(f"[WARNING] Unmapped attack types: {unmapped}")
    df["attack_step"] = name_series.map(KILL_CHAIN).fillna(-1).astype(int)

    target_columns = ["attack_name", "attack_flag", "attack_step"]
    target_columns = [c for c in target_columns if c in df.columns]
    feature_columns = [c for c in df.columns if c not in target_columns]
    df = df[feature_columns + target_columns]

    print(f"[INFO] Saving: {output_filepath}")
    df.to_csv(output_filepath, index=False)
    print(f"[INFO] Done. Rows: {len(df):,}")
    vc = df["attack_step"].value_counts().sort_index()
    for step, cnt in vc.items():
        print(f"  Step {step:2d}: {cnt:,}")


if __name__ == "__main__":
    if not os.path.exists(INPUT_FILENAME):
        download()
    else:
        print(f"[INFO] {INPUT_FILENAME} already exists, skipping download.")

    if not os.path.exists(OUTPUT_FILENAME):
        process(INPUT_FILENAME, OUTPUT_FILENAME)
    else:
        print(f"[INFO] {OUTPUT_FILENAME} already exists, skipping processing.")
