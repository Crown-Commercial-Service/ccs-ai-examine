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

### Environment variables

Create a local `.env` file (not committed) based on `env.example`:

```bash
cp env.example .env
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
├── tests/
│   ├── test_distractors.py
│   └── test_mock_and_utils.py
├── mlruns/
├── mlflow_outputs/
├── utils.py
├── utils_old.py
├── requirements.txt
└── README.md
```

---

## Key Concepts

- **Benchmark Dataset**: Ground-truth buyer and supplier name pairs covering typos, acronyms, semantic equivalents, parent–subsidiary cases, and negative controls.
- **Prompt-driven Evaluation**: Prompts are read from files, not hard-coded.
- **Mock LLM**: Used while Azure OpenAI access is pending.
- **MLflow Tracking**: Captures accuracy, breakdowns and artifacts per prompt version.

---

## Running the Evaluation

```bash
python -m evaluation.evaluate_buyer_matching_mlflow
```

### Matching API (required)

Entity name matching is performed via an **external API** (rather than calling `match_string_with_langchain` locally).

- **Set**: `MATCH_STRING_API_URL` to the full URL of the external matcher endpoint.
- **Endpoint**: expects a `GET /match` route with query params:
  - `input_string` (string)
  - `candidates` (repeat this param once per candidate)
  - `prompt_path` (optional)
- **Response**: JSON like:
  - `{"input_string": "...", "match": "<candidate>|null", "raw": "..."}`

Example:

```bash
export MATCH_STRING_API_URL="http://localhost:8000/match"
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
- Azure integration pending
