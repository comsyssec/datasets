# CLAUDE.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

이 파일은 Claude Code가 본 저장소에서 작업할 때 참고하는 가이드입니다.

=======================================================================================

> **팀 프로젝트 주의사항** — 이 파일은 팀과 공유됩니다.
> 작업이 끝난 뒤 **`git push` 하기 전에 항상 이 CLAUDE.md를 최신 상태로 업데이트**해 주세요.
> (새 모듈 추가, CLI 인자 변경, 학습 파이프라인 수정 등 반영)

## 1. Git 커밋/푸쉬 규칙 (팀 합의)

> **❗ 매우 중요 — Claude Code 사용 시**
>
> - 커밋 메시지에 **`Co-Authored-By: Claude ...` 줄(co-author 트레일러)을 절대 포함하지 말 것.**
>   기본 동작에서 추가되는 `Co-Authored-By: Claude Opus ...` 같은 라인은 모두 **제거하고** 커밋한다.
> - 마찬가지로 `🤖 Generated with [Claude Code]` 같은 자동 서명 푸터도 넣지 않는다.
> - 즉, 커밋 메시지는 **사람이 직접 쓴 것처럼 본문(제목 + 설명)만** 남긴다.

### 커밋 예시 (OK)
```
feat(losses): CB-Focal 가중치 정규화 방식 변경

weight.sum() == num_classes 가 되도록 정규화하여
official impl 과 동치가 되게 수정.
```

### 커밋 예시 (NG — 이렇게 쓰지 말 것)
```
feat(losses): CB-Focal 가중치 정규화 방식 변경

...

🤖 Generated with [Claude Code](https://claude.com/claude-code)
Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
```

### 푸쉬 전 체크리스트
1. 변경된 코드/명령/파이프라인을 **CLAUDE.md (이 파일) 와 README.md** 에 반영했는가?
2. 커밋 메시지에 `Co-Authored-By:` 또는 Claude 자동 서명이 들어가 있지 않은가?
3. `outputs/`, `data/`, `*.pth`, 큰 캐시 파일 등을 실수로 커밋하지 않았는가? (필요하면 `.gitignore` 확인)
4. (가능하면) 학습 한 epoch 라도 돌려서 import / dataloader / loss 가 깨지지 않는지 확인.

---

## Train/Test 분할 (`split.env` + `split_util.py`)

`Reformatted_*.csv` 를 `training-flow.csv` / `test-flow.csv` 로 나누는 공용 유틸.

- **정책은 전부 `split.env` 에 데이터셋별 한 줄로 하드코딩.** 비율/시드/보존여부를
  바꾸려면 이 파일 **한 곳만** 고치면 전체에 반영됨. `split_util.py` 는 이 설정을
  읽어 적용만 하는 얕은 러너 — 유틸에 비율/데이터셋명을 하드코딩하지 말 것.
- **값 문법** (우변):
  - `<ratio>,<seed>` — `attack_flag` 기준 stratified 분할 (pandas, 전체 메모리 로드).
  - `<ratio>,<seed>,lazy` — polars 스트리밍(`sink_csv`)으로 디스크에 직접 흘려보냄.
    메모리 사용량이 파일 크기와 무관하게 평평. 초대형 파일용
    (`bot-iot`, `cicids2018-imp`, `lspr23`).
    **행 순서는 원본 그대로 보존**되고, 배정은 행 인덱스 해시 기반이라 클래스별 비율이
    정확히 `test_ratio` 는 아니고 반올림 오차 범위(수천만 행 기준 ±0.1% 미만) 안에 든다.
    시드가 같으면 재현된다. 두 출력 모두 `.partial` 로 쓴 뒤 마지막에 rename 한다.
  - `official` — 원본 배포본의 train/test 분할 보존 (`nsl-kdd`, `unsw-nb15`).
    `split` provenance 컬럼 기준으로 분리. 재분할 시 논문 재현성 깨지므로 보존.
  - `pipeline` — 자체 download.py 가 이미 두 파일 생성 (`cicids2017/2018`, `ciciot2023`,
    `ton-iot`, `mirai`). 러너가 건너뜀.
- **`official` 데이터셋 규칙**: 해당 download.py 는 concat 전에
  `df_train["split"]="train"`, `df_test["split"]="test"` 태그를 남기고, process() 는
  `split` 컬럼을 feature 에서 제외해 맨 끝에 보존해야 함. (예: `unsw-nb15/download.py`)
- **실행**: `python3 split_util.py <dataset> [...]`.
  `--keep`(중간 Reformatted 유지, 기본은 삭제), `--test-ratio` / `--seed` 로 일시 오버라이드.
- 새 데이터셋 추가 시 `split.env` 에 한 줄 등록할 것. `split.env`/`split_util.py` 는
  git 추적, 데이터 CSV 3종(Reformatted/training-flow/test-flow)은 `*.csv` 로 gitignore.

---

## 실행 스킵 & 잔여파일 정리

### `download_all.sh` — 최종 산출물 있으면 건너뜀

`training-flow.csv` + `test-flow.csv` 가 **둘 다** 있으면 그 데이터셋을 통째로 스킵한다.
`download.py` 20개는 이미 `INPUT`/`OUTPUT` 존재 가드를 갖고 있지만, `split_util.py` 가
`CLEAN=true` 로 `Reformatted_*.csv` 를 지우기 때문에 그 가드만으로는 **분할 후 재다운로드를
막을 수 없다.** 분할 후에도 남는 최종 산출물이 유일하게 신뢰할 수 있는 "완료" 마커.

### `cleanup.sh` — 재생성 가능한 잔여파일 제거

- `bash cleanup.sh` (dry run, **기본값**) / `bash cleanup.sh --apply` (실제 삭제)
- **분할 완료된 데이터셋만** 대상. training-flow + test-flow 가 둘 다 있는 디렉토리에서
  `download.py` / 두 flow 파일을 뺀 나머지(원본 CSV, `Reformatted_*.csv`, `.zip`,
  `.partial`, `.ckpt_*.json`)를 삭제한다. 아직 분할 안 된 데이터셋은 건드리지 않으므로
  작업 손실이 없다.
- **git 추적 파일은 절대 삭제하지 않는다.** `mirai/` 8개 파일은 연구실 자체 데이터라
  `*.csv` gitignore 를 뚫고 force-add 되어 있고, 이 규칙으로 보호된다.

### 대용량 `process()` 규약 — `.partial` + rename

`bot-iot`, `cicids2018-imp` 는 청크 쓰기를 `<OUTPUT>.partial` 에 하고 **완주한 뒤에만**
`os.replace()` 로 최종 이름으로 옮긴다. 중단 시 잘린 출력이 남아 다음 실행에서
`already exists` 로 통과되는 사고를 막기 위함. 진행 로그도 청크마다 출력한다 —
14GB 급 쓰기는 20~45분이 걸리므로 로그가 없으면 정지와 구분이 안 된다.

- `bot-iot` 의 `download()` 도 `.partial` 로 concat 후 rename 한다. 개별 CSV 실패는
  WARN 후 continue 가 아니라 **즉시 예외**로 중단 — 조용히 빠진 파일이 불완전한
  `Bot-IoT.csv` 를 만들고, 그게 완성본으로 굳는 것을 막는다.
- `lspr23` 는 체크포인트 재개(`.ckpt_*.json`) 방식으로 같은 문제를 이미 해결하고 있고
  청크 로그도 있다 → **건드리지 말 것.**
- 출력이 이미 있을 때의 `else` 브랜치에서 **결과 파일을 재스캔하지 말 것.** `cicids2018-imp`
  가 "skipping processing" 을 찍고도 통계 출력하려고 34GB 를 전수 스캔하고 있었음(제거됨).
  스킵은 즉시 끝나야 한다.

---

## 데이터셋 작업 현황

> 마지막 업데이트: 2026-07-26

### Phase 1 완료 datasets (원본 컬렉션)

| Dataset | 디렉토리 | 출력 | 소스 | 비고 |
|---|---|---|---|---|
| CICIDS2017 | `cicids2017/` | `training-flow.csv` / `test-flow.csv` | Kaggle `chethuhn/network-intrusion-dataset` | split=pipeline |
| CSE-CIC-IDS2018 | `cicids2018/` | `training-flow.csv` / `test-flow.csv` | Kaggle `solarmainframe/ids-intrusion-csv` | split=pipeline |
| CICIoT2023 | `ciciot2023/` | `training-flow.csv` / `test-flow.csv` | Kaggle `akashdogra/cic-iot-2023` | 105개 IoT 공격, split=pipeline |
| ToN-IoT | `ton-iot/` | `training-flow.csv` / `test-flow.csv` | Kaggle `dhoogla/nftoniot` | split=pipeline |
| Mirai Botnet Dataset | `mirai/` | `training-flow.csv` / `test-flow.csv` | 전처리본이 저장소에 포함 | `download.py` 없음(원본이 사유 pcap), split=pipeline |
| Edge-IIoTset | `edge-iiot/` | `Reformatted_EdgeIIoT.csv` | Kaggle `mohamedamineferrag/edgeiiotset-...` | — |
| X-IIoTID | `xiiotid/` | `Reformatted_XIIoTID.csv` | Kaggle `munaalhawawreh/xiiotid-...` | 820,834행, unmapped=0 |
| NF-ToN-IoT-v3 | `nf-ton-iot-v3/` | `Reformatted_NF-ToN-IoT-v3.csv` | Kaggle `seyhed/nf-ton-iot-v3` | NetFlow v9 |
| WUSTL-IIoT-2021 | `wustl-iiot-2021/` | `Reformatted_WUSTL-IIoT-2021.csv` | Kaggle `annaamalaiu/wustl-iiot-2021-dataset` | SCADA 테스트베드 |
| LSPR23 | `lspr23/` | `Reformatted_LSPR23.csv` | Zenodo record `8042347` | 9.8GB CSV, 청크 처리 |

### Phase 2 완료 datasets (데이터 검증됨, unmapped=0)

| Dataset | 디렉토리 | 행 수 | Kill-chain steps | 소스 | 비고 |
|---|---|---|---|---|---|
| KDDCup 1999 | `kddcup1999/` | 494k | 0,1,4,5,7 | Kaggle `galaxyh/kdd-cup-1999-data` | — |
| CTU-13 | `ctu-13/` | 1.6M | 0,6 | Kaggle `dhoogla/ctu13` | 13개 parquet concat |
| N-BaIoT | `n-baiot/` | 2.4M+ | 0,1,7 | Kaggle `mkashifn/nbaiot-dataset` | 파일명 레이블 추출 |
| CIDDS-001 | `cidds-001/` | 204k | 0,1,4 | Kaggle `dhoogla/cidds001` | parquet |
| CIDDS-002 | `cidds-002/` | 2.6M | 0,1 | Kaggle `dhoogla/cidds002` | parquet |
| IoTID20 | `iotid20/` | 626k | 0,1,4,6,7 | Kaggle `rohulaminlabid/iotid20-dataset` | Cat 컬럼 |
| HIKARI-2021 | `hikari-2021/` | 555k | 0,1,4,7 | Kaggle `kk0105/allflowmeter-hikari2021` | traffic_category 컬럼 |
| NSL-KDD | `nsl-kdd/` | 148k | 0,1,4,5,6,7 | Kaggle `hassan06/nslkdd` | KDDTrain+.txt 파싱 |
| UNSW-NB15 | `unsw-nb15/` | 258k | 0,1,4,5,6,7 | Kaggle `mrwellsdavid/unsw-nb15` | 10개 attack_cat |
| InSDN | `insdn/` | 344k | 0,1,4,6,7 | Kaggle `badcodebuilder/insdn-dataset` | 3 CSV concat |
| CIC-DDoS2019 | `cic-ddos2019/` | 431k | 0,3,7 | Kaggle `dhoogla/cicddos2019` | 17개 parquet, 전체 재다운로드 |
| Bot-IoT | `bot-iot/` | 73.4M | 0,1,7 | Kaggle `vigneshvenkateswaran/bot-iot` | 75 CSV, process() 청크처리 |
| CIC-IDS2017 Imp. | `cicids2017-imp/` | 2.1M | 0,1,4,6,7 | distrinet URL (328MB) | Liu et al. 2022 |
| CSE-CIC-IDS2018 Imp. | `cicids2018-imp/` | 63.2M | 0,4,6,7 | distrinet URL (9.7GB) | Liu et al. 2022 |
| Kitsune | `kitsune/` | 1.8M | 0,1,4,6,7 | Kaggle `ymirsky/network-attack-dataset-kitsune` | 라이선스 불필요 |
| IoT-23 | `iot-23/` | 2.6M | 0,1,6,7 | 공식 CTU 사이트 | Zeek conn.log 파싱 |

### download.py 있음, 데이터 미취득 (Kaggle 라이선스 수락 필요)

| Dataset | 디렉토리 | Kaggle 소스 | 대안 |
|---|---|---|---|
| AWID2 | `awid2/` | `kolias93/awid2-wifi-intrusion-dataset` | `python3 download.py register NAME LAST EMAIL AFFIL` |
| AWID3 | `awid3/` | `chatzoglou/awid3` | `python3 download.py register NAME LAST EMAIL AFFIL` |

### 코드 구조 (모든 download.py 공통 패턴)

```python
KAGGLE_DATASET = "author/dataset-name"
INPUT_FILENAME  = "DatasetName.csv"        # 원본 concat/중간파일
OUTPUT_FILENAME = "Reformatted_DatasetName.csv"  # 최종 표준 포맷

KILL_CHAIN = {"benign": 0, "scan": 1, "exploit": 4, "c&c": 6, "ddos": 7, ...}

def download():   # Kaggle/URL에서 다운로드 → INPUT_FILENAME 생성
def process():    # INPUT_FILENAME → OUTPUT_FILENAME (표준 3컬럼 추가)

# 표준 출력 포맷: feature_columns... | attack_name | attack_flag | attack_step
```

### 인용 / BibTeX (`citations/`)

- 각 데이터셋의 **원본논문 BibTeX** 는 `citations/<dataset>.bib` 에 있음 (파일명 = 디렉토리명, 현재 29개).
- README 연동:
  - `Available Datasets` 표 — 데이터셋 **이름 링크가 곧 `citations/*.bib`** (디렉토리 링크 아님).
  - `Dataset Details` — `Source` = 원본논문명 + `[[BibTeX]]` 링크, `File Source` = 실제 다운로드 소스(Kaggle/URL 등).
- **새 데이터셋 추가 시**: `citations/<dataset>.bib` 생성 + README 표의 이름 링크를 해당 `.bib` 로 연결 + Details 의 Source/File Source 도 같이 추가할 것.
- 저자/연도/venue/DOI 는 **DBLP 기준으로 검증**해서 기입.

### Kaggle 소스 403 현황 (2026-05-30 전수 검증)

| 상태 | Dataset (download.py 실제 ID) | 비고 |
|---|---|---|
| ❌ 403 | awid2: `kolias93/awid2-wifi-intrusion-dataset` | 공식 사이트 이메일 요청 가능 |
| ❌ 403 | awid3: `chatzoglou/awid3` | 동일 |
| ✅ 정상 | 나머지 모두 (17개) | — |

### 알려진 이슈

- **Bot-IoT**: `subcategory ` 컬럼 trailing space → `errors="ignore"`. 14GB CSV라 process() 500k 청크 방식.
- **CTU-13**: attack_name에 `flow=` prefix → `str.replace(r"^flow=", "")`.
- **CIC-DDoS2019**: Kaggle v3 소스에 `netbios/ldap/mssql/portmap/udp/webddos/udplag` 신규 레이블 추가됨 → KILL_CHAIN 확장 완료.
- **NSL-KDD**: KDDTest+.txt에 KDD'99에 없는 공격 타입 포함(saint/mscan/apache2 등) → KILL_CHAIN 확장 완료.
- **CIC-DDoS2019**: KILL_CHAIN에 `netbios`, `ldap`, `mssql`, `portmap`, `udp`, `webddos`, `udplag` 등 신규 레이블 → 확장 완료.
- **IoT-23**: 공식 CTU 사이트 스트리밍. Zeek conn.log 마지막 탭필드 `tunnel_parents  label  detailed-label` (공백구분). okiru/okiru-attack/c&c-heartbeat-filedownload KILL_CHAIN 추가 완료.
- **Kitsune**: `ymirsky/network-attack-dataset-kitsune` (라이선스 불필요). Mirai Botnet labels 파일 소문자(`mirai_labels.csv`) → 대소문자 무시 매칭. labels 형식 2종: 헤더+인덱스컬럼(`x`), 헤더없이 0/1 (Mirai).
- **CIC-IDS2017/2018 Imp.**: `- Attempted` suffix 레이블 → step 0 처리 (공격 의도지만 실제 악성 행동 없음).
- **n-baiot/nf-ton-iot-v3**: 캐시 손상 시 "Bad magic number" → `rm -rf ~/.cache/kagglehub/datasets/{id}` 후 재실행.
- **LSPR23**: Zenodo `ls23pr_flows.zip` 는 루트에 단일 CSV(`ls23pr_v1.csv`, ZIP64 >4GB)만 들어있음 → `INPUT_FILE=./ls23pr_v1.csv`, `OUTPUT_FILE=./Reformatted_LSPR23.csv` (기존 `LSPR23/ls23pr_flows/` 하드코딩 경로는 오류였음).
- **X-IIoTID**: 실제 라벨 컬럼은 `class1`(세부공격 19종)/`class2`(전술 10종)/`class3`(Attack/Normal). `class` 아님 → REQUIRED_COLUMNS·rename 을 `class1→attack_step`, `class2→attack_name`, `class3→attack_flag` 로 수정. KILL_CHAIN 키를 실제 class1 값(`scanning_vulnerability`, `tcp relay`, `crypto-ransomware` 등)에 맞춰 재작성해 unmapped=0 (820,834행 검증).
- **`lazy` 분할 OOM (수정됨)**: 구버전 `_split_stratified_lazy` 는 `filter(...).collect()` 로 **클래스 하나를 통째로** 메모리에 올렸다. `attack_flag` 는 값이 0/1 둘뿐이라 benign 클래스가 파일 대부분 → `cicids2018-imp`(34GB) 에서 `Killed`(OOM). 게다가 첫 클래스는 이미 기록된 뒤라 **공격 트래픽만 든 training/test-flow 가 남았다** — 비율은 8:2 로 맞아 정상처럼 보이는 게 함정. `sink_csv` 스트리밍 + `.partial` rename 으로 교체됨. 구버전으로 만든 flow 파일이 있으면 반드시 지우고 다시 분할할 것.
- **download.py 다운로드 가드 (미수정)**: 다운로드 여부를 `INPUT_FILENAME` 존재만 보고 판단한다. 그래서 원본 CSV 만 수동으로 지우고 `Reformatted_*.csv` 를 남겨두면 원본을 다시 받은 뒤 처리는 스킵한다(예: `bot-iot` 14GB 재다운로드). `cleanup.sh` 는 분할 완료된 디렉토리에서 둘을 함께 지우므로 이 상태를 만들지 않는다 — 수동 삭제로만 걸린다. 고치려면 각 download.py 의 조건을 `not exists(INPUT) and not exists(OUTPUT)` 로 바꾸면 됨(20개 파일, 파일당 1줄).
