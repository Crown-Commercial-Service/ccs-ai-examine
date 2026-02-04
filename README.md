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

The pipeline uses a **single unified output folder** (`data/`) for both dummy and live modes, controlled by parameters in `params.yaml`.

### Pipeline Modes

The pipeline supports two modes:
- **Dummy Mode**: Uses generated test data for development and testing
- **Live Mode**: Uses real database data (requires credentials)

**Note**: Currently using mock data for live mode as database credentials are pending. This allows verification that all pipeline stages work correctly.

### Running the Pipeline

#### 1. Configure Mode

Configure the pipeline mode in `params.yaml`:

```yaml
data_mode: dummy          # Options: 'dummy' or 'live'
force_erase_live: false   # Safety flag for Live data protection. Set true to enable the live data override
```

#### 2. Run Pipeline

```bash
# 1. Set mode in params.yaml
data_mode: dummy  # or 'live'

# 2. Run the pipeline
python -m dvc repro
```
That's it! DVC will:
- Read configuration from `params.yaml`
- Determine which stages need to run
- Execute stages in dependency order
- Use cached results when nothing changed
- Output all results to `data/` folder

#### 3. View Pipeline DAG

```bash
# Force re-run all stages (ignore cache)
python -m dvc repro --force

# Force re-run from specific stage onwards
python -m dvc repro --force download_data

# View pipeline structure
python -m dvc dag

# Check what will run without executing
python -m dvc status

# View pipeline metrics
python -m dvc metrics show
```
### Pipeline Safety Mechanisms

The pipeline includes **production-grade safety checks** to prevent accidental data loss:

#### Stamp File Protection

When live data is downloaded, a hidden file `.is_live` is created in the `data/` folder to mark it as containing production data.

**Safety Rules:**

1. **Live → Dummy Switch**: Blocked unless `force_erase_live: true`
   ```
   CRITICAL: 'data/' folder currently contains LIVE DATA.
   Running DUMMY will erase LIVE data.
   ACTION REQUIRED: Set 'force_erase_live: true' in params.yaml
   ```
2. **Live → Live Re-run**: Also requires `force_erase_live: true`
   ```
   CRITICAL: 'data/' folder currently contains LIVE DATA.
   Re-running LIVE will overwrite existing LIVE data.
   ACTION REQUIRED: Set 'force_erase_live: true' in params.yaml
   ```
3. **Dummy → Dummy/live**: No restrictions, runs freely

### Pipeline Outputs

All outputs are saved to the **unified `data/` folder** regardless of mode:
**Dummy Mode** (`dummy_data/`):
- `contracts.csv` - contract data
- `mi.csv` - Management Information data
- `combined.csv` - Matched data
- `unmatched.csv` - Unmatched MI entries
- `summary_stats.csv` - Summary statistics
- `line_level.csv` - Per buyer-supplier pair details
- `.is_live` - Stamp file (only present when folder contains live data not for dummy data) 

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
- DVC pipeline automation (3 stages)
- Production-grade safety mechanisms

### If Pipeline Not Running, then

1. Ensure DVC is initialized: `python -m dvc init`
2. Check `params.yaml` has correct `data_mode` setting also check the force_erase_live to override the Live data
3. Verify all dependencies installed: `pip install -r requirements.txt`