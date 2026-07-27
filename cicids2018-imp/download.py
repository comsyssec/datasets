"""
CSE-CIC-IDS2018 Improved Dataset (Liu et al., 2022)
Source: https://intrusion-detection.distrinet-research.be/CNS2022/Datasets/CSECICIDS2018_improved.zip
Size: ~9.7 GB compressed, ~32 GB uncompressed

Reference:
  Liu, L., Engelen, G., Lynar, T., Essam, D., Joosen, W. (2022).
  Error Prevalence in NIDS Datasets: A Case Study on CIC-IDS-2017 and CSE-CIC-IDS-2018.
  IEEE CNS 2022. DOI: 10.1109/CNS56114.2022.9947235

This improved version fixes:
  - CICFlowMeter feature extraction bugs
  - Labeling errors and mislabeled flows
  - Uses "Attempted" sub-label for flows intended as attacks with no malicious behavior
    (e.g., "DoS GoldenEye - Attempted") — treated as benign in this mapping.
"""
import os
import subprocess
import zipfile
import pandas as pd

DOWNLOAD_URL = "https://intrusion-detection.distrinet-research.be/CNS2022/Datasets/CSECICIDS2018_improved.zip"
ARCHIVE_NAME = "CSECICIDS2018_improved.zip"
OUTPUT_FILENAME = "Reformatted_CSE-CIC-IDS2018-Imp.csv"

# CSE-CIC-IDS2018 improved label → kill-chain step
# Labels may include "- Attempted" suffix: stripped before mapping, treated same step.
KILL_CHAIN = {
    "benign": 0,
    # Reconnaissance
    "portscan": 1,
    # Exploitation — credential attacks
    "brute force": 4,
    "ftp-bruteforce": 4,
    "ssh-bruteforce": 4,
    # Exploitation — web
    "sql injection": 4,
    "xss": 4,
    "web attacks": 4,
    "web attack": 4,
    # Exploitation — vulnerability
    "heartbleed": 4,
    # C&C
    "bot": 6,
    "botnet": 6,
    # Actions on Objectives — infiltration / exfil
    "infiltration": 7,
    # Actions on Objectives — DoS
    "dos": 7,
    "dos hulk": 7,
    "dos goldeneye": 7,
    "dos slowloris": 7,
    "dos slowhttptest": 7,
    # Actions on Objectives — DDoS
    "ddos": 7,
    "ddos attack-loic-http": 7,
    "ddos attack-loic-udp": 7,
    "ddos attack-hoic": 7,
    "ddos loic-http": 7,
    "ddos loic-udp": 7,
    "ddos hoic": 7,
}


def _map_label(label: str) -> int:
    """Map label string to kill-chain step.
    Strips '- Attempted' suffix and maps to the same step as the actual attack.
    Pure 'Benign' and all 'Attempted' flows with no attack behavior → step 0.
    """
    normalized = label.strip().lower()
    # "DoS GoldenEye - Attempted" → treat as benign (no malicious behavior)
    if normalized.endswith("- attempted") or normalized.endswith("-attempted"):
        return 0
    # Try exact and prefix match
    for key, step in KILL_CHAIN.items():
        if normalized == key or normalized.startswith(key):
            return step
    # Partial substring match as fallback
    for key, step in KILL_CHAIN.items():
        if key in normalized:
            return step
    return -1


def download():
    if os.path.exists(ARCHIVE_NAME):
        print(f"[INFO] Archive already exists, skipping download: {ARCHIVE_NAME}")
    else:
        print(f"[INFO] Downloading (~9.7 GB): {DOWNLOAD_URL}")
        print("[INFO] This will take significant time (~30-60 min depending on connection).")
        try:
            subprocess.run(["wget", "-c", "-O", ARCHIVE_NAME, DOWNLOAD_URL], check=True)
        except FileNotFoundError:
            raise RuntimeError(
                "[ERROR] wget not found. Install it or download manually:\n"
                f"  URL: {DOWNLOAD_URL}\n"
                f"  Save as: {ARCHIVE_NAME}"
            )
        print(f"[INFO] Download complete: {ARCHIVE_NAME}")

    print(f"[INFO] Extracting: {ARCHIVE_NAME}")
    with zipfile.ZipFile(ARCHIVE_NAME, "r") as z:
        names = z.namelist()
        print(f"[INFO] Files in zip: {names}")
        z.extractall(".")
    print("[INFO] Extraction complete.")


def process(output_filepath):
    day_files = sorted([
        f for f in os.listdir(".")
        if f.endswith(".csv")
        and f not in [OUTPUT_FILENAME]
        and not f.startswith("Reformatted")
    ])
    if not day_files:
        raise RuntimeError("No day CSV files found. Run download() first.")

    print(f"[INFO] Processing {len(day_files)} file(s): {day_files}")
    # ~34 GB output: write to .partial and rename only on completion, so an
    # interrupted run never leaves a truncated file that looks finished.
    tmp_filepath = output_filepath + ".partial"
    first_write = True
    total_rows = 0
    unmapped_all = set()

    for fidx, fname in enumerate(day_files, start=1):
        print(f"[INFO] Reading [{fidx}/{len(day_files)}]: {fname}")
        file_rows = 0
        for chunk in pd.read_csv(fname, chunksize=200_000, low_memory=False):
            # Find label column
            label_col = next(
                (c for c in ["Label", "label", "Class", "class"] if c in chunk.columns),
                chunk.columns[-1]
            )
            chunk.rename(columns={label_col: "attack_name"}, inplace=True)
            chunk["attack_name"] = chunk["attack_name"].astype(str).str.strip()

            # attack_flag: 0 for benign and "Attempted" flows, 1 for actual attacks
            chunk["attack_flag"] = chunk["attack_name"].apply(
                lambda x: 0 if x.lower() == "benign" or "attempted" in x.lower() else 1
            )
            chunk["attack_step"] = chunk["attack_name"].apply(_map_label)

            # Track unmapped
            unmapped_mask = chunk["attack_step"] == -1
            unmapped_all.update(chunk.loc[unmapped_mask, "attack_name"].unique())

            target_columns = ["attack_name", "attack_flag", "attack_step"]
            feature_columns = [c for c in chunk.columns if c not in target_columns]
            chunk = chunk[feature_columns + target_columns]

            chunk.to_csv(tmp_filepath, index=False,
                         mode="w" if first_write else "a", header=first_write)
            first_write = False
            total_rows += len(chunk)
            file_rows += len(chunk)
            print(f"[INFO]   {fname}: {file_rows:,} rows  (total {total_rows:,})")

        print(f"[INFO]   → {file_rows:,} rows")

    os.replace(tmp_filepath, output_filepath)

    if unmapped_all:
        print(f"[WARNING] Unmapped labels: {sorted(unmapped_all)}")
    else:
        print("[INFO] All labels mapped successfully (unmapped=0)")

    print(f"[INFO] Total rows: {total_rows:,}")
    print(f"[INFO] Output: {output_filepath}")

    # Step distribution summary (chunked to avoid OOM on large file)
    from collections import Counter
    step_counts = Counter()
    for chunk in pd.read_csv(output_filepath, usecols=["attack_step"],
                             chunksize=500_000, low_memory=False):
        step_counts.update(chunk["attack_step"].value_counts().to_dict())
    print("[INFO] Kill-chain step distribution:")
    for step, cnt in sorted(step_counts.items()):
        print(f"  Step {step:2d}: {cnt:,}")


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    extracted_csvs = [
        f for f in os.listdir(".")
        if f.endswith(".csv")
        and f not in [OUTPUT_FILENAME]
        and not f.startswith("Reformatted")
    ]

    if not extracted_csvs and not os.path.exists(ARCHIVE_NAME):
        download()
        extracted_csvs = [
            f for f in os.listdir(".")
            if f.endswith(".csv")
            and f not in [OUTPUT_FILENAME]
            and not f.startswith("Reformatted")
        ]
    elif not extracted_csvs and os.path.exists(ARCHIVE_NAME):
        print(f"[INFO] Archive found, extracting...")
        with zipfile.ZipFile(ARCHIVE_NAME, "r") as z:
            z.extractall(".")
    else:
        print(f"[INFO] Source files found: {extracted_csvs}")

    if not os.path.exists(OUTPUT_FILENAME):
        process(OUTPUT_FILENAME)
    else:
        print(f"[INFO] {OUTPUT_FILENAME} already exists, skipping processing.")
