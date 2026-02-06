
# ccs-ai-examine
EXAMINE: **Ex**pedient **A**nalysis of **M**anagement **I**nformation to **N**otice **E**rrors

---

## Installation

### Python Virtual Environment

```bash
python3 -m venv venv
```

Activate:

- Linux/macOS: `source venv/bin/activate`
- Windows: `.\venv\Scripts\activate`

Install dependencies:

```bash
pip install -r requirements.txt
```

### SQL Drivers

```bash
bash install_drivers.sh
```

---

## Project Overview

EXAMINE helps CCS identify potential errors in supplier-reported Management Information (MI), particularly under-reporting or missing spend.

A core component of this repository focuses on **entity name matching** (buyer–supplier matching), evaluated using a benchmark dataset and tracked with **MLflow**.

---
## Key Concepts

- **Benchmark Dataset**: Ground-truth buyer and supplier name pairs covering typos, acronyms, semantic equivalents, parent–subsidiary cases, and negative controls.
- **Prompt-driven Evaluation**: Prompts are read from files, not hard-coded.
- **Mock LLM**: Used while Azure OpenAI access is pending.
- **MLflow Tracking**: Captures accuracy, breakdowns and artifacts per prompt version.
- **DVC Pipeline**: Automates data download, matching and summarization with reproducibility.

## Repository Structure

```
CCS-AI-EXAMINE/
├── benchmark_data/
│   └── ccs_combined_buyer_supplier_benchmark.csv
├── evaluation/
│   ├── evaluate_buyer_matching_mlflow.py
│   └── mock_langchain_model.py
├── prompts/
│   ├── buyer_match_v1.txt
│   ├── buyer_match_v2.txt
│   ├── buyer_match_v3.txt
│   └── buyer_match_v4.txt
├── scripts/
│   ├── add_CustomerGroup.py
│   ├── combine_data.py      # Match contracts with MI using LLM 
│   ├── get_data.py          # Download contracts and MI data (dummy + live functions)
│   └── summarise_data.py    # Generate summary statistics
├── tests/
│   ├── test_distractors.py
│   └── test_mock_and_utils.py
├── mlruns/
├── mlflow_outputs/
├── params.yaml              # DVC pipeline configuration
├── dvc.yaml                 # DVC pipeline definition
├── utils.py
├── requirements.txt
└── README.md
```

## DVC Data Pipeline

The data pipeline is fully reproducible using **DVC**:

```
get_data
   ↓
combine_data
   ↓
summarise_data

```

The pipeline supports **dummy** and **live** modes via one parameter.
Live mode requires database credentials, dummy mode runs without external access.

### `params.yaml`

```yaml
data_mode: dummy   # or live based on the data requirement
```

### Run the pipeline

```bash
python -m dvc repro
```

Outputs are written to:

```
data/dummy/
data/live/
```

---

### How it works

DVC templating passes the mode into scripts:

```
--mode ${data_mode}
--outdir data/${data_mode}
--indir data/${data_mode}
```

No Python files contain hard-coded paths.

---

## Pipeline Stages

| Stage | Script | Outputs |
|------|--------|---------|
| get_data | `scripts/get_data.py` | contracts.csv, mi.csv, reg_number_supplier_key.csv |
| combine | `scripts/combine_data.py` | combined.csv, unmatched.csv |
| summarise | `scripts/summarise_data.py` | summary_stats.csv, line_level.csv |

---

## Switching Modes

Change:

```yaml
data_mode: live
```

Re-run:

```bash
python -m dvc repro
```

DVC will re-run only necessary stages automatically.

---

## MLflow Evaluation

Run evaluation:

```bash
python -m evaluation.evaluate_buyer_matching_mlflow
```

Launch MLflow UI:

```bash
python -m mlflow ui --backend-store-uri file:./mlruns --port 5000
```

Open: http://127.0.0.1:5000

Run tests:

```bash
python -m pytest -q
```
---

## Status

- Benchmark dataset completed and reviewed
- MLflow integrated
- Prompt evaluation running
- Unit tests implemented
- DVC pipeline automation (3 stages)

### If Pipeline Not Running, then

1. Ensure DVC is initialized: `python -m dvc init`
2. Check `params.yaml` has correct `data_mode` (dummy or live)
3. Verify all dependencies installed: `pip install -r requirements.txt`