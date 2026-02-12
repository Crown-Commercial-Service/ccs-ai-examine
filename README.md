# EXAMINE

EXAMINE (**Ex**pedient **A**nalysis of **M**anagement **I**nformation to **N**otice **E**rrors) is a system that helps CCS identify potential errors in supplier-reported Management Information (MI), particularly under-reported or missing spend.

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

### Environment Variables

Create a local `.env` file (not committed) based on `env.example`:

```bash
cp env.example .env
```

### SQL Drivers

To install the SQL drivers, run the `install_drivers.sh` script:

```
bash install_drivers.sh
```

## Developer Tooling (Pre-commit, Ruff, pytest)

This project uses:

- [pre-commit](https://pre-commit.com/) for running checks automatically before each commit.
- [Ruff](https://docs.astral.sh/ruff/) for fast linting.
- [pytest](https://docs.pytest.org/) for unit testing.

### Install tooling

If you already installed dependencies from `requirements.txt`, install the remaining developer tools:

```bash
python -m pip install pre-commit ruff
```

Or install all at once:

```bash
python -m pip install -r requirements.txt pre-commit ruff
```

### Set up pre-commit hooks

Install hooks locally:

```bash
pre-commit install
```

Run all hooks manually across the repository:

```bash
pre-commit run --all-files
```

### Run Ruff and pytest manually

Run Ruff:

```bash
ruff check .
```

Run tests:

```bash
pytest -q
```

CI also runs Ruff and pytest on every push.
