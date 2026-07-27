# Cybersecurity & IoT Datasets

A comprehensive collection of publicly available datasets widely used for machine learning and deep learning research in cybersecurity, Intrusion Detection Systems (IDS), and IoT environments.

## Core Concept: Standardized Labeling

The most important feature of this repository is data standardization.
Every dataset is processed so that the final CSV files end with the exact same three columns:

1. `attack_name` — attack type string (lowercase)
2. `attack_flag` — binary label (0 = benign, 1 = attack)
3. `attack_step` — Cyber Kill Chain phase (integer)

Running `python3 download.py` in any dataset directory will download the raw data and produce a standardized output CSV in this format.

### Kill-Chain Step Mapping

| Step | Phase | Examples |
|---|---|---|
| 0 | Benign / Normal | Normal, Benign |
| 1 | Reconnaissance | PortScan, OS Scan, Fuzzing, Probe |
| 2 | Weaponization | *(unused)* |
| 3 | Delivery | TFTP amplification vector |
| 4 | Exploitation | Exploits, Brute Force, SQL Injection, XSS, Shellcode |
| 5 | Installation | U2R, Backdoor, Rootkit |
| 6 | Command & Control | Botnet C&C, Mirai, Torii, CTU-13 botnet |
| 7 | Actions on Objectives | DoS, DDoS, Exfiltration, Theft |
| -1 | Unknown / Unmapped | *(should be 0 for all verified datasets)* |

---

## Available Datasets

> In the tables below, each dataset name links to its original paper's BibTeX file under [`citations/`](./citations). See [Dataset Details](#dataset-details) for the full citation and the file source of each dataset.

### Phase 1 — Original Collection (Verified)

These datasets use the original pipeline (pre-standardized format). Output files are `training-flow.csv` / `test-flow.csv` or dataset-specific names.

| Dataset | Tags | Output file(s) | Notes |
|---|---|---|---|
| **[CICIDS2017](./citations/cicids2017.bib)** | general-purpose, dos-ddos, web-attacks, brute-force, scan, botnet | `training-flow.csv`, `test-flow.csv` | Kaggle `chethuhn/network-intrusion-dataset` |
| **[CSE-CIC-IDS2018](./citations/cicids2018.bib)** | general-purpose, dos-ddos, web-attacks, brute-force, scan, botnet | `training-flow.csv`, `test-flow.csv` | Kaggle `solarmainframe/ids-intrusion-csv` |
| **[CICIoT2023](./citations/ciciot2023.bib)** | iot, general-purpose | `training-flow.csv`, `test-flow.csv` | 105 IoT attack types |
| **[ToN-IoT](./citations/ton-iot.bib)** | iiot-ics, general-purpose | `training-flow.csv`, `test-flow.csv` | Kaggle `dhoogla/nftoniot` |
| **[Mirai Botnet Dataset](./citations/mirai.bib)** | iot, botnet | `training-flow.csv`, `test-flow.csv` | Pre-processed files included; no download.py |
| **[Edge-IIoTset](./citations/edge-iiot.bib)** | iiot-ics, iot | `Reformatted_EdgeIIoT.csv` | Kaggle `mohamedamineferrag/edgeiiotset-...` |
| **[XIIoTID](./citations/xiiotid.bib)** | iiot-ics, iot | `Reformatted_XIIoTID.csv` | Kaggle `munaalhawawreh/xiiotid-...` |
| **[NF-ToN-IoT-v3](./citations/nf-ton-iot-v3.bib)** | iiot-ics | `Reformatted_NF-ToN-IoT-v3.csv` | NetFlow v9, Kaggle `seyhed/nf-ton-iot-v3` |
| **[WUSTL-IIoT-2021](./citations/wustl-iiot-2021.bib)** | iiot-ics | `Reformatted_WUSTL-IIoT-2021.csv` | SCADA, Kaggle `annaamalaiu/wustl-iiot-2021-dataset` |
| **[LSPR23](./citations/lspr23.bib)** | general-purpose, anomaly | `Reformatted_LSPR23.csv` | Zenodo, 9.8 GB CSV, chunked processing |

### Phase 2 — Expansion (Verified, unmapped=0)

All datasets produce `Reformatted_*.csv` with the standardized 3-column format.

| Dataset | Tags | Rows | Steps | Kaggle / Source |
|---|---|---|---|---|
| **[KDDCup 1999](./citations/kddcup1999.bib)** | general-purpose, anomaly | 494k | 0,1,4,5,7 | `galaxyh/kdd-cup-1999-data` |
| **[NSL-KDD](./citations/nsl-kdd.bib)** | general-purpose, anomaly | 148k | 0,1,4,5,6,7 | `hassan06/nslkdd` |
| **[UNSW-NB15](./citations/unsw-nb15.bib)** | general-purpose, anomaly | 258k | 0,1,4,5,6,7 | `mrwellsdavid/unsw-nb15` |
| **[CTU-13](./citations/ctu-13.bib)** | botnet | 1.6M | 0,6 | `dhoogla/ctu13` |
| **[N-BaIoT](./citations/n-baiot.bib)** | iot, botnet | 2.4M+ | 0,1,7 | `mkashifn/nbaiot-dataset` |
| **[CIDDS-001](./citations/cidds-001.bib)** | general-purpose, scan | 204k | 0,1,4 | `dhoogla/cidds001` |
| **[CIDDS-002](./citations/cidds-002.bib)** | general-purpose, scan | 2.6M | 0,1 | `dhoogla/cidds002` |
| **[IoTID20](./citations/iotid20.bib)** | iot | 626k | 0,1,4,6,7 | `rohulaminlabid/iotid20-dataset` |
| **[HIKARI-2021](./citations/hikari-2021.bib)** | general-purpose | 555k | 0,1,4,7 | `kk0105/allflowmeter-hikari2021` |
| **[InSDN](./citations/insdn.bib)** | sdn | 344k | 0,1,4,6,7 | `badcodebuilder/insdn-dataset` |
| **[Bot-IoT](./citations/bot-iot.bib)** | iot, botnet, dos-ddos | 73.4M | 0,1,7 | `vigneshvenkateswaran/bot-iot` |
| **[CIC-DDoS2019](./citations/cic-ddos2019.bib)** | dos-ddos, general-purpose | 431k | 0,3,7 | `dhoogla/cicddos2019` |
| **[Kitsune](./citations/kitsune.bib)** | iot, anomaly | 1.8M | 0,1,4,6,7 | `ymirsky/network-attack-dataset-kitsune` |
| **[IoT-23](./citations/iot-23.bib)** | iot, botnet | 2.6M | 0,1,6,7 | Official CTU: mcfp.felk.cvut.cz |

### Phase 2 — Improved CIC-IDS (Liu et al., 2022)

Fixed CICFlowMeter tool and corrected labeling errors in the original CIC datasets.
Source: https://intrusion-detection.distrinet-research.be/CNS2022/

| Dataset | Tags | Rows | Steps | Archive size |
|---|---|---|---|---|
| **[CIC-IDS2017 Improved](./citations/cicids2017-imp.bib)** | general-purpose, dos-ddos, web-attacks, brute-force, scan, botnet | 2.1M | 0,1,4,6,7 | 328 MB |
| **[CSE-CIC-IDS2018 Improved](./citations/cicids2018-imp.bib)** | general-purpose, dos-ddos, web-attacks, brute-force, scan, botnet | 63.2M | 0,4,6,7 | 9.7 GB |

### Phase 2 — Pending (Kaggle license acceptance required)

| Dataset | Tags | Download |
|---|---|---|
| **[AWID2](./citations/awid2.bib)** | wireless | Kaggle `kolias93/awid2-wifi-intrusion-dataset` **or** `python3 download.py register NAME LAST EMAIL AFFIL` |
| **[AWID3](./citations/awid3.bib)** | wireless | Kaggle `chatzoglou/awid3` **or** `python3 download.py register NAME LAST EMAIL AFFIL` |

---

## Dataset Details

### CICIDS2017
- **Source:** Sharafaldin et al., "Toward Generating a New Intrusion Detection Dataset and Intrusion Traffic Characterization," ICISSP 2018. [[BibTeX]](./citations/cicids2017.bib)
- **File Source:** Kaggle `chethuhn/network-intrusion-dataset`
- **Output:** `training-flow.csv`, `test-flow.csv`

### CSE-CIC-IDS2018
- **Source:** Sharafaldin et al., "Toward Generating a New Intrusion Detection Dataset and Intrusion Traffic Characterization," ICISSP 2018 (CIC-recommended citation). [[BibTeX]](./citations/cicids2018.bib)
- **File Source:** Kaggle `solarmainframe/ids-intrusion-csv`
- **Output:** `training-flow.csv`, `test-flow.csv`

### CICIoT2023
- **Source:** Neto et al., "CICIoT2023: A Real-Time Dataset and Benchmark for Large-Scale Attacks in IoT Environment," Sensors 2023. [[BibTeX]](./citations/ciciot2023.bib)
- **File Source:** Kaggle `akashdogra/cic-iot-2023`
- **Output:** `training-flow.csv`, `test-flow.csv`
- **Note:** 105 IoT attack types

### ToN-IoT
- **Source:** Alsaedi et al., "TON_IoT Telemetry Dataset: A New Generation Dataset of IoT and IIoT for Data-Driven Intrusion Detection Systems," IEEE Access 2020. [[BibTeX]](./citations/ton-iot.bib)
- **File Source:** Kaggle `dhoogla/nftoniot`
- **Output:** `training-flow.csv`, `test-flow.csv`

### Mirai Botnet Dataset
- **Source:** No dedicated dataset paper (proprietary PCAP captures). For the Mirai malware itself: Antonakakis et al., "Understanding the Mirai Botnet," USENIX Security 2017. [[BibTeX]](./citations/mirai.bib)
- **File Source:** Pre-processed files included in the repository; no `download.py`
- **Output:** `training-flow.csv`, `test-flow.csv`

### Edge-IIoTset
- **Source:** Ferrag et al., "Edge-IIoTset: A New Comprehensive Realistic Cyber Security Dataset of IoT and IIoT Applications for Centralized and Federated Learning," IEEE Access 2022. [[BibTeX]](./citations/edge-iiot.bib)
- **File Source:** Kaggle `mohamedamineferrag/edgeiiotset-cyber-security-dataset-of-iot-iiot`
- **Output:** `Reformatted_EdgeIIoT.csv`

### XIIoTID
- **Source:** Al-Hawawreh et al., "X-IIoTID: A Connectivity-Agnostic and Device-Agnostic Intrusion Data Set for Industrial Internet of Things," IEEE IoT Journal 2022. [[BibTeX]](./citations/xiiotid.bib)
- **File Source:** Kaggle `munaalhawawreh/xiiotid-iiot-intrusion-dataset`
- **Output:** `Reformatted_XIIoTID.csv`

### NF-ToN-IoT-v3
- **Source:** Luay et al., "Temporal Analysis of NetFlow Datasets for Network Intrusion Detection Systems," 2025 (v3); Sarhan et al., "NetFlow Datasets for Machine Learning-Based Network Intrusion Detection Systems," BDTA 2020 (original NetFlow set). [[BibTeX]](./citations/nf-ton-iot-v3.bib)
- **File Source:** Kaggle `seyhed/nf-ton-iot-v3`
- **Output:** `Reformatted_NF-ToN-IoT-v3.csv`
- **Note:** NetFlow v9 features

### WUSTL-IIoT-2021
- **Source:** Zolanvari et al., "Machine Learning-Based Network Vulnerability Analysis of Industrial Internet of Things," IEEE IoT Journal 2019 (recommended dataset citation). [[BibTeX]](./citations/wustl-iiot-2021.bib)
- **File Source:** Kaggle `annaamalaiu/wustl-iiot-2021-dataset`
- **Output:** `Reformatted_WUSTL-IIoT-2021.csv`
- **Note:** SCADA testbed

### LSPR23
- **Source:** Dijk et al., "LSPR23: A novel IDS dataset from the largest live-fire cybersecurity exercise," Journal of Information Security and Applications 2024. [[BibTeX]](./citations/lspr23.bib)
- **File Source:** Zenodo record `8042347`
- **Output:** `Reformatted_LSPR23.csv`
- **Note:** 9.8 GB CSV, chunked processing

### KDDCup 1999
- **Source:** Hettich & Bay, "KDD Cup 1999 Data," UCI KDD Archive, 1999 (derived from the 1998 DARPA evaluation). [[BibTeX]](./citations/kddcup1999.bib)
- **File Source:** Kaggle `galaxyh/kdd-cup-1999-data`
- **Output:** `Reformatted_KDDCup1999.csv`
- **Kill-chain:** normal→0, probe attacks→1, R2L→4, U2R→5, DoS→7

### NSL-KDD
- **Source:** Tavallaee et al., "A Detailed Analysis of the KDD CUP 99 Data Set," CISDA 2009. [[BibTeX]](./citations/nsl-kdd.bib)
- **File Source:** Kaggle `hassan06/nslkdd` (KDDTrain+.txt + KDDTest+.txt, no header, 43 cols)
- **Output:** `Reformatted_NSL-KDD.csv`
- **Kill-chain:** same as KDDCup 1999; extended with saint/mscan/apache2/mailbomb/udpstorm etc.

### UNSW-NB15
- **Source:** Moustafa & Slay, "UNSW-NB15: A Comprehensive Data Set for Network Intrusion Detection Systems," MilCIS 2015. [[BibTeX]](./citations/unsw-nb15.bib)
- **File Source:** Kaggle `mrwellsdavid/unsw-nb15`
- **Output:** `Reformatted_UNSW-NB15.csv`
- **Label:** `attack_cat` → `attack_name`, `label` → `attack_flag`
- **Kill-chain:** Normal→0, Reconnaissance/Analysis/Fuzzers→1, Exploits/Shellcode/Generic→4, Backdoor→5, Worms→6, DoS→7

### CTU-13
- **Source:** García et al., "An Empirical Comparison of Botnet Detection Methods," Computers & Security 2014. [[BibTeX]](./citations/ctu-13.bib)
- **File Source:** Kaggle `dhoogla/ctu13` (13 parquet files)
- **Output:** `Reformatted_CTU-13.csv`
- **Label:** `label` with `flow=` prefix stripped; botnet→6, normal/background→0

### N-BaIoT
- **Source:** Meidan et al., "N-BaIoT: Network-Based Detection of IoT Botnet Attacks Using Deep Autoencoders," IEEE Pervasive Computing 2018. [[BibTeX]](./citations/n-baiot.bib)
- **File Source:** Kaggle `mkashifn/nbaiot-dataset`
- **Output:** `Reformatted_N-BaIoT.csv`
- **Label:** Derived from filename — `{device}.benign.csv`→0, `*.mirai.scan`/`*.gafgyt.scan`→1, others→7

### CIDDS-001 / CIDDS-002
- **Source:** Ring et al., "Flow-Based Benchmark Data Sets for Intrusion Detection," ECCWS 2017. [[BibTeX]](./citations/cidds-001.bib)
- **File Source:** Kaggle `dhoogla/cidds001`, `dhoogla/cidds002` (parquet)
- **Output:** `Reformatted_CIDDS-001.csv`, `Reformatted_CIDDS-002.csv`
- **Label:** `attack_type` → `attack_name`

### IoTID20
- **Source:** Ullah & Mahmoud, "A Scheme for Generating a Dataset for Anomalous Activity Detection in IoT Networks," Canadian AI 2020. [[BibTeX]](./citations/iotid20.bib)
- **File Source:** Kaggle `rohulaminlabid/iotid20-dataset`
- **Output:** `Reformatted_IoTID20.csv`
- **Label:** `Cat` → `attack_name`, `Label` → `attack_flag`

### HIKARI-2021
- **Source:** Ferriyan et al., "Generating Network Intrusion Detection Dataset Based on Real and Encrypted Synthetic Attack Traffic," Applied Sciences 2021. [[BibTeX]](./citations/hikari-2021.bib)
- **File Source:** Kaggle `kk0105/allflowmeter-hikari2021`
- **Output:** `Reformatted_HIKARI-2021.csv`
- **Label:** `traffic_category` → `attack_name`, `Label` → `attack_flag`

### InSDN
- **Source:** Elsayed et al., "InSDN: A Novel SDN Intrusion Dataset," IEEE Access 2020. [[BibTeX]](./citations/insdn.bib)
- **File Source:** Kaggle `badcodebuilder/insdn-dataset`
- **Output:** `Reformatted_InSDN.csv`
- **Files:** Normal_data.csv + metasploitable-2.csv + OVS.csv (concatenated, 344k rows)
- **Kill-chain:** Normal→0, Probe→1, BFA/U2R/Web-Attack→4, BOTNET→6, DoS/DDoS→7

### Bot-IoT
- **Source:** Koroniotis et al., "Towards the Development of Realistic Botnet Dataset in the Internet of Things for Network Forensic Analytics: Bot-IoT Dataset," Future Generation Computer Systems 2019. [[BibTeX]](./citations/bot-iot.bib)
- **File Source:** Kaggle `vigneshvenkateswaran/bot-iot` (75 CSV files, 73.4M rows)
- **Output:** `Reformatted_Bot-IoT.csv`
- **Label:** `category` → `attack_name`, `attack` → `attack_flag`
- **Kill-chain:** Normal→0, Reconnaissance→1, DoS/DDoS/Theft→7
- **Note:** `subcategory ` column has trailing space → use `errors="ignore"` when dropping

### CIC-DDoS2019
- **Source:** Sharafaldin et al., "Developing Realistic Distributed Denial of Service (DDoS) Attack Dataset and Taxonomy," ICCST 2019. [[BibTeX]](./citations/cic-ddos2019.bib)
- **File Source:** Kaggle `dhoogla/cicddos2019` (17 parquet files)
- **Output:** `Reformatted_CIC-DDoS2019.csv`
- **Kill-chain:** Benign→0, TFTP→3, all DDoS/DoS variants→7

### Kitsune
- **Source:** Mirsky et al., "Kitsune: An Ensemble of Autoencoders for Online Network Intrusion Detection," NDSS 2018. [[BibTeX]](./citations/kitsune.bib)
- **File Source:** Kaggle `ymirsky/network-attack-dataset-kitsune` (no license required)
- **Output:** `Reformatted_Kitsune.csv`
- **Structure:** 9 scenario directories; `{Name}_dataset.csv` (115 features, no header) + `{name}_labels.csv`
- **Kill-chain:** ARP MitM/Active Wiretap/Video Injection→4, Fuzzing/OS Scan→1, Mirai Botnet→6, SSDP Flood/SSL Renegotiation/SYN DoS→7
- **Sampling:** 200k rows per scenario (total 1.8M)

### IoT-23
- **Source:** Garcia, Parmisano & Erquiaga, "IoT-23: A Labeled Dataset with Malicious and Benign IoT Network Traffic," Stratosphere Lab / Zenodo 2020. [[BibTeX]](./citations/iot-23.bib)
- **File Source:** Official CTU/Stratosphere (`mcfp.felk.cvut.cz/publicDatasets/IoT-23-Dataset/`)
- **Output:** `Reformatted_IoT-23.csv`
- **Format:** Zeek conn.log; last tab-field = `tunnel_parents  label  detailed-label` (space-separated)
- **Kill-chain:** Benign→0, PortScan→1, C&C variants→6, DDoS/Attack→7
- **Sampling:** 200k rows per scenario (total 2.6M)

### CIC-IDS2017 Improved *(Liu et al., 2022)*
- **Source:** Liu et al., "Error Prevalence in NIDS Datasets: A Case Study on CIC-IDS-2017 and CSE-CIC-IDS-2018," IEEE CNS 2022. [[BibTeX]](./citations/cicids2017-imp.bib)
- **File Source:** `https://intrusion-detection.distrinet-research.be/CNS2022/Datasets/CICIDS2017_improved.zip` (328 MB)
- **Output:** `Reformatted_CIC-IDS2017-Imp.csv`
- **Files:** monday.csv ~ friday.csv (5 day files)
- **Note:** "Attempted" flows treated as benign (attack_step=0)

### CSE-CIC-IDS2018 Improved *(Liu et al., 2022)*
- **Source:** Liu et al., "Error Prevalence in NIDS Datasets: A Case Study on CIC-IDS-2017 and CSE-CIC-IDS-2018," IEEE CNS 2022. [[BibTeX]](./citations/cicids2018-imp.bib)
- **File Source:** `https://intrusion-detection.distrinet-research.be/CNS2022/Datasets/CSECICIDS2018_improved.zip` (9.7 GB)
- **Output:** `Reformatted_CSE-CIC-IDS2018-Imp.csv`
- **Files:** 10 day CSV files (~30 GB uncompressed, 63.2M rows)

### AWID2 *(Kolias et al., 2016)*
- **Source:** Kolias et al., "Intrusion Detection in 802.11 Networks: Empirical Evaluation of Threats and a Public Dataset," IEEE Communications Surveys & Tutorials 2016. [[BibTeX]](./citations/awid2.bib)
- **File Source option 1:** Kaggle `kolias93/awid2-wifi-intrusion-dataset` (license acceptance required)
- **File Source option 2:** `python3 download.py register NAME LASTNAME EMAIL AFFIL` → email link in 1-3 days
- **Kill-chain:** Normal→0, Injection/Impersonation→4, Flooding/Deauthentication→7

### AWID3 *(Chatzoglou et al., 2021)*
- **Source:** Chatzoglou et al., "Empirical Evaluation of Attacks Against IEEE 802.11 Enterprise Networks: The AWID3 Dataset," IEEE Access 2021. [[BibTeX]](./citations/awid3.bib)
- **File Source option 1:** Kaggle `chatzoglou/awid3` (license acceptance required)
- **File Source option 2:** `python3 download.py register NAME LASTNAME EMAIL AFFIL` → email link in 1-3 days
- **Kill-chain:** Normal→0, KRACK/Kr00k/ICV/MIC failures→4, Deauth/Disassoc/RTS/CTS flood→7

---

## Prerequisites

| Requirement | Purpose | Install |
|---|---|---|
| Python 3.8+ | All datasets | — |
| Python packages | All datasets | `pip install -r requirements.txt` |
| Kaggle API key | All Kaggle-based datasets | `~/.kaggle/kaggle.json` ([guide](https://www.kaggle.com/docs/api)) |
| `wget` | LSPR23, CIC-IDS improved | Linux/Mac: built-in |

---

## Quick Start

```bash
git clone https://github.com/comsyssec/datasets.git
cd datasets
pip install -r requirements.txt

# Download a specific dataset
cd unsw-nb15
python3 download.py

# Download all at once
bash download_all.sh
```

`download_all.sh` skips any dataset that already has both `training-flow.csv` and
`test-flow.csv`, so re-running it never re-downloads finished datasets.

### Train/test split

Datasets that produce a `Reformatted_*.csv` can be split into `training-flow.csv`
and `test-flow.csv` with `split_util.py`:

```bash
python3 split_util.py cidds-001              # one dataset
python3 split_util.py unsw-nb15 cidds-001    # several at once
python3 split_util.py cidds-001 --keep       # keep the intermediate Reformatted_*.csv
```

The split policy is hardcoded per dataset in **[`split.env`](./split.env)** — one
line each. A dataset is split by a stratified `<ratio>,<seed>` (balanced on
`attack_flag`), the same with a `,lazy` suffix (memory-safe polars scan for very
large files such as `bot-iot`, `cicids2018-imp`, `lspr23`), marked `official` to
preserve its original release split (`nsl-kdd`, `unsw-nb15`), or `pipeline` if its
own `download.py` already emits the two flow files. Edit `split.env` to change
ratios/seed for the whole collection in one place.

### Reclaiming disk space

Once a dataset has been split, its raw downloads and intermediates are
regenerable and can be removed with `cleanup.sh`:

```bash
bash cleanup.sh            # dry run — report what would be removed
bash cleanup.sh --apply    # actually remove
```

Only datasets that already have both flow files are touched, and git-tracked
files are never deleted.

### AWID2/3 — official site registration

```bash
# Submit request (sends download link to email within 1-3 days)
cd awid2
python3 download.py register "Gildong" "Hong" "hong@university.ac.kr" "Seoul Nat'l Univ"

# After receiving email link:
python3 download.py download-link "https://icsdweb.aegean.gr/..."
```

### CIC-IDS2017/2018 Improved

```bash
# CIC-IDS2017 improved (328 MB)
cd cicids2017-imp && python3 download.py

# CSE-CIC-IDS2018 improved (~9.7 GB compressed, ~60 min)
cd cicids2018-imp && python3 download.py
```

---

## Special Notes

### Mirai Botnet Dataset
Pre-processed `training-flow.csv` and `test-flow.csv` are included directly in the repository. No `download.py` exists — the source is proprietary PCAP captures.

### LSPR23
Downloads from Zenodo automatically. Requires `wget`. Processes 9.8 GB CSV in chunks.

### Kitsune
Two Kaggle uploads exist — use the correct one:
- `ymirsky/network-attack-dataset-kitsune` ← **use this** (no license required)
- `ymirsky/kitsune-network-attack-dataset` ← requires license acceptance

### IoT-23
Downloads directly from the official CTU server (no Kaggle account needed). 20 scenarios × 200k rows = 2.6M total.

### Bot-IoT / CSE-CIC-IDS2018 Improved
Very large outputs (14 GB / 34 GB). `process()` uses chunked writes to avoid OOM.

### Cache issues (n-baiot, nf-ton-iot-v3)
If you see "Bad magic number" error, the local Kaggle cache is corrupted:
```bash
rm -rf ~/.cache/kagglehub/datasets/mkashifn/nbaiot-dataset/
rm -rf ~/.cache/kagglehub/datasets/seyhed/nf-ton-iot-v3/
python3 download.py
```

---

## License & Citation

The download and processing scripts are provided for convenience and standardization.
Each dataset's license and citation requirements are governed by their original creators.
Please cite the original papers when using these datasets in your research.
