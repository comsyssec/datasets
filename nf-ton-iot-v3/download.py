import kagglehub
import os
import shutil
import pandas as pd

KAGGLE_DATASET = "seyhed/nf-ton-iot-v3"
REQUIRED_COLUMNS = ["Label", "Attack"]
INPUT_FILENAME = "NF-ToN-IoT-v3.csv"
OUTPUT_FILENAME = "Reformatted_NF-ToN-IoT-v3.csv"

# Attack -> kill-chain step
KILL_CHAIN = {
    "benign": 0,
    # Reconnaissance
    "scanning": 1,
    # Exploitation
    "injection": 4,
    "xss": 4,
    "password": 4,       # password brute force
    "mitm": 4,           # cf. edge-iiot / Kitsune ARP MitM -> 4
    # Installation
    "backdoor": 5,
    # Actions on Objectives
    "dos": 7,
    "ddos": 7,
    "ransomware": 7,
}


def find_csv_with_columns(base_path, required_columns):
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
    print("[NOTE] The Kaggle source may be the full NF-ToN-IoT-v3 dataset.")
    print("       The previously stored file was a reduced subset.")
    path = kagglehub.dataset_download(KAGGLE_DATASET)
    src = find_csv_with_columns(path, REQUIRED_COLUMNS)
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
    if "Attack" in df.columns:
        rename_mapping["Attack"] = "attack_name"
    if "Label" in df.columns:
        rename_mapping["Label"] = "attack_flag"
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
