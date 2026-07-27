import kagglehub
import os
import pandas as pd

KAGGLE_DATASET = "vigneshvenkateswaran/bot-iot"
INPUT_FILENAME = "Bot-IoT.csv"
OUTPUT_FILENAME = "Reformatted_Bot-IoT.csv"

# Bot-IoT attack categories → kill-chain step
KILL_CHAIN = {
    "normal": 0,
    "reconnaissance": 1,    # OS fingerprint, service scan
    "ddos": 7,              # DDoS (UDP, TCP, HTTP)
    "dos": 7,               # DoS (UDP, TCP, HTTP)
    "theft": 7,             # Data exfiltration, keylogging
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

    csvs = find_all_csvs(path)
    if not csvs:
        raise RuntimeError(f"No CSV found in {path}. Files: {os.listdir(path)}")

    print(f"[INFO] Found {len(csvs)} CSV file(s)")

    dst = os.path.join(os.getcwd(), INPUT_FILENAME)
    tmp = dst + ".partial"
    first_write = True
    total_rows = 0
    all_cats = set()

    # Write to a .partial file and rename only after every source CSV succeeded.
    # A failure here must abort: a silently skipped file would leave an
    # incomplete Bot-IoT.csv that later runs would accept as complete.
    for i, csv_path in enumerate(csvs, start=1):
        df_part = pd.read_csv(csv_path, low_memory=False)
        n = len(df_part)
        df_part.to_csv(tmp, index=False, mode="w" if first_write else "a", header=first_write)
        first_write = False
        total_rows += n
        if "category" in df_part.columns:
            all_cats.update(df_part["category"].dropna().unique())
        print(f"[INFO] [{i}/{len(csvs)}] {os.path.basename(csv_path)}: {n:,} rows  (total so far: {total_rows:,})")

    os.replace(tmp, dst)
    print(f"[INFO] Saved as: {dst}  (total {total_rows:,} rows)")
    print(f"[INFO] Unique categories: {sorted(all_cats)}")


def process(input_filepath, output_filepath):
    print(f"[INFO] Reading header: {input_filepath}")
    header_df = pd.read_csv(input_filepath, nrows=0)
    rename_map = {}
    if "category" in header_df.columns:
        rename_map["category"] = "attack_name"
    if "attack" in header_df.columns:
        rename_map["attack"] = "attack_flag"
    drop_cols = [c for c in ["subcategory ", "subcategory"] if c in header_df.columns]

    target_columns = ["attack_name", "attack_flag", "attack_step"]
    feature_columns = [c for c in header_df.rename(columns=rename_map).columns
                       if c not in target_columns and c not in drop_cols]

    # ~14 GB in / ~13 GB out: write to .partial and rename only on completion, so
    # an interrupted run never leaves a truncated file that looks finished.
    tmp_filepath = output_filepath + ".partial"
    print(f"[INFO] Writing (chunked): {output_filepath}")
    first_write = True
    total_rows = 0
    unmapped_all = set()

    for chunk_no, chunk in enumerate(
            pd.read_csv(input_filepath, chunksize=500_000, low_memory=False), start=1):
        chunk.rename(columns=rename_map, inplace=True)
        chunk.drop(columns=drop_cols, errors="ignore", inplace=True)

        chunk["attack_flag"] = pd.to_numeric(chunk["attack_flag"], errors="coerce").fillna(0).astype(int)

        name_series = chunk["attack_name"].astype(str).str.strip().str.lower()
        unmapped = set(name_series[~name_series.isin(KILL_CHAIN)].unique())
        unmapped_all.update(unmapped)
        chunk["attack_step"] = name_series.map(KILL_CHAIN).fillna(-1).astype(int)

        col_order = [c for c in feature_columns if c in chunk.columns] + target_columns
        chunk = chunk[col_order]
        chunk.to_csv(tmp_filepath, index=False, mode="w" if first_write else "a", header=first_write)
        first_write = False
        total_rows += len(chunk)
        print(f"[INFO]   chunk {chunk_no:>4}: {total_rows:,} rows written")

    os.replace(tmp_filepath, output_filepath)

    if unmapped_all:
        print(f"[WARNING] Unmapped categories: {unmapped_all}")
    print(f"[INFO] Saving: {output_filepath}")
    print(f"[INFO] Done. Rows: {total_rows:,}")


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
