# ccs-ai-examine
EXAMINE: **Ex**pedient **A**nalysis of **M**anagement **I**nformation to **N**otice **E**rrors

## Installation

### Python Virtual Environment

To set up the Python virtual environment, follow these steps:

1.  **Create a Python Virtual Environment:**
    ```bash
    python3 -m venv venv
    ```

2.  **Activate the Virtual Environment:**
    *   On Linux/macOS:
        ```bash
        source venv/bin/activate
        ```
    *   On Windows:
        ```bash
        .\\venv\\Scripts\\activate
        ```

3.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

### SQL Drivers

To install the SQL drivers, run the `install_drivers.sh` script:

```
bash install_drivers.sh
```

# CCS-AI-EXAMINE

EXAMINE (**Ex**pedient **A**nalysis of **M**anagement **I**nformation to **N**otice **E**rrors) is a data science–driven project that helps CCS identify potential errors in supplier-reported Management Information (MI), particularly under-reporting or missing spend.

A core component of this repository focuses on **entity name matching** (buyer–supplier matching), evaluated using a benchmark dataset and tracked with **MLflow**.

---

## Repository Structure

```
CCS-AI-EXAMINE/
├── data/
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
│   ├── download_data.py     # Download contracts and MI data (has both dummy and live data functions)
│   ├── combine_data.py      # Match contracts with MI using LLM
│   └── summarise_data.py    # Generate summary statistics
├── tests/
│   ├── test_distractors.py
│   └── test_mock_and_utils.py
├── mlruns/
├── mlflow_outputs/
├── params.yaml              # Pipeline configuration
├── dvc.yaml                 # Pipeline definition
├── run_pipeline.py          # Pipeline execution script
├── utils.py
├── requirements.txt
└── README.md

```

## Key Concepts

- **Benchmark Dataset**: Ground-truth buyer and supplier name pairs covering typos, acronyms, semantic equivalents, parent–subsidiary cases, and negative controls.
- **Prompt-driven Evaluation**: Prompts are read from files, not hard-coded.
- **Mock LLM**: Used while Azure OpenAI access is pending.
- **MLflow Tracking**: Captures accuracy, breakdowns and artifacts per prompt version.
- **DVC Pipeline**: Automates data download, matching and summarization with reproducibility.

---

## DVC Pipeline

### Overview

The DVC pipeline automates the revenue assurance workflow in three stages:

1. **Download Data**: Retrieves contracts and MI data
2. **Combine Data**: Matches contracts with MI using LLM-based name matching
3. **Summarize Data**: Generates statistics on matched/unmatched entries

### Pipeline Modes

The pipeline supports two modes:
- **Dummy Mode**: Uses generated test data for development and testing
- **Live Mode**: Uses real database data (requires credentials)

**Note**: Currently using mock data for live mode as database credentials are pending. This allows verification that all pipeline stages work correctly.

### Running the Pipeline

#### 1. Configure Mode

Edit `params.yaml` to set the mode:

```yaml
data_mode: dummy  # or 'live'
```

#### 2. Run Pipeline

```bash
python run_pipeline.py
```

This automatically:
- Reads the mode from `params.yaml`
- Executes the appropriate pipeline stages
- Generates outputs in `dummy_data/` or `data/` folder

#### 3. View Pipeline DAG

```bash
# View all stages
python -m dvc dag

# View specific mode
python -m dvc dag download_data_dummy combine_data_dummy summarise_data_dummy
```

### Pipeline Outputs

**Dummy Mode** (`dummy_data/`):
- `dummy_contracts.csv` - Test contract data
- `dummy_mi.csv` - Test MI data
- `dummy_combined.csv` - Matched data
- `dummy_unmatched_mi.csv` - Unmatched MI entries
- `dummy_summary_stats.csv` - Summary statistics
- `dummy_line_level.csv` - Per buyer-supplier pair details

**Live Mode** (`data/`):
- `contracts.csv` - Real contract data
- `mi.csv` - Real MI data
- `combined.csv` - Matched data
- `unmatched.csv` - Unmatched MI entries
- `summary_stats.csv` - Summary statistics
- `line_level.csv` - Per buyer-supplier pair details

---

## Running the Evaluation

```bash
python -m evaluation.evaluate_buyer_matching_mlflow
```

---

## Viewing Results in MLflow

```bash
python -m mlflow ui --backend-store-uri file:./mlruns --port 5000
```

Open: http://127.0.0.1:5000

---

## Testing

Run unit tests:

```bash
python -m pytest -q
```

---

## Azure Readiness

The evaluation pipeline is designed so that once Azure OpenAI access is available, the mock LLM can be replaced with a real Azure client with minimal changes.

---

## Status

- Benchmark dataset completed and reviewed
- MLflow integrated
- Prompt evaluation running
- Unit tests implemented
- DVC pipeline automated (dummy mode and mock live mode working)

### If Pipeline Not Running, then

1. Ensure DVC is initialized: `python -m dvc init`
2. Check `params.yaml` has correct `data_mode` setting
3. Verify all dependencies installed: `pip install -r requirements.txt`