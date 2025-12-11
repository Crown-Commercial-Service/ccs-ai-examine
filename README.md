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

### Verification

To verify that you have installed the repo correctly, run the tests:
```
python -m pytest
```