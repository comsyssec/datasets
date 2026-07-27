import kagglehub
import os
import shutil
import pandas as pd

KAGGLE_DATASET = "mrwellsdavid/unsw-nb15"
INPUT_FILENAME = "UNSW-NB15.csv"
OUTPUT_FILENAME = "Reformatted_UNSW-NB15.csv"

# attack_cat → kill-chain step
KILL_CHAIN = {
    "normal": 0,
    # Reconnaissance
    "reconnaissance": 1,
    "analysis": 1,       # network analysis / scanning
    "fuzzers": 1,        # probing for vulnerabilities
    # Exploitation
    "exploits": 4,
    "shellcode": 4,      # shellcode delivery
    "generic": 4,        # generic attack patterns
    # Installation
    "backdoor": 5,
    # C&C
    "worms": 6,
    # Actions on Objectives
    "dos": 7,
}


def find_all_csvs(base_path):
    csvs = []
    for root, _, files in os.walk(base_path):
        for f in sorted(files):
            if f.endswith(".csv"):
                csvs.append(os.path.join(root, f))
    return csvs


def download():
    print(f"[INFO] Downloading from Kaggle: {KAGGLE_DATASET}")
    path = kagglehub.dataset_download(KAGGLE_DATASET)

    # Use the official train/test split files
    train_src = os.path.join(path, "UNSW_NB15_training-set.csv")
    test_src = os.path.join(path, "UNSW_NB15_testing-set.csv")

    if not os.path.exists(train_src) or not os.path.exists(test_src):
        csvs = find_all_csvs(path)
        raise RuntimeError(
            f"Expected training/testing CSV not found in {path}.\n"
            f"Available files: {[os.path.basename(c) for c in csvs]}"
        )

    dst = os.path.join(os.getcwd(), INPUT_FILENAME)
    df_train = pd.read_csv(train_src, low_memory=False)
    df_test = pd.read_csv(test_src, low_memory=False)
    # Tag rows so the original train/test split can be preserved downstream.
    df_train["split"] = "train"
    df_test["split"] = "test"
    df = pd.concat([df_train, df_test], ignore_index=True)
    df.to_csv(dst, index=False)
    print(f"[INFO] Saved as: {dst}  ({len(df):,} rows)")
    print(f"[INFO] Columns ({len(df.columns)}): {df.columns.tolist()}")
    print(f"[INFO] Unique attack_cat: {sorted(df['attack_cat'].dropna().unique().tolist())}")


def process(input_filepath, output_filepath):
    print(f"[INFO] Reading: {input_filepath}")
    df = pd.read_csv(input_filepath, low_memory=False)

    # Drop row id (not a feature)
    df.drop(columns=["id"], errors="ignore", inplace=True)

    # attack_cat → attack_name, label → attack_flag
    df.rename(columns={"attack_cat": "attack_name"}, inplace=True)
    df.rename(columns={"label": "attack_flag"}, inplace=True)

    df["attack_flag"] = pd.to_numeric(df["attack_flag"], errors="coerce").fillna(0).astype(int)

    name_series = df["attack_name"].astype(str).str.strip().str.lower()
    unmapped = name_series[~name_series.isin(KILL_CHAIN)].unique()
    if len(unmapped) > 0:
        print(f"[WARNING] Unmapped categories: {unmapped}")
    df["attack_step"] = name_series.map(KILL_CHAIN).fillna(-1).astype(int)

    target_columns = ["attack_name", "attack_flag", "attack_step"]
    # Keep the split provenance column last, out of the feature set.
    provenance_columns = [c for c in ["split"] if c in df.columns]
    feature_columns = [c for c in df.columns if c not in target_columns + provenance_columns]
    df = df[feature_columns + target_columns + provenance_columns]

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
        df = pd.read_csv(INPUT_FILENAME, nrows=0)
        print(f"[INFO] Columns ({len(df.columns)}): {df.columns.tolist()}")

    if not os.path.exists(OUTPUT_FILENAME):
        process(INPUT_FILENAME, OUTPUT_FILENAME)
    else:
        print(f"[INFO] {OUTPUT_FILENAME} already exists, skipping processing.")
