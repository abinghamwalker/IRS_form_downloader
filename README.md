# IRS Form 990 ETL Pipeline

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

This project provides a robust, scalable ETL (Extract, Transform, Load) pipeline for processing IRS Form 990 XML data. It automates the download of annual bulk ZIP archives, extracts and parses the XML filings, cleans the data, and loads it into a user-friendly SQLite database and a series of CSV files.

This tool is designed for researchers, data scientists, and analysts who need a clean, structured dataset of nonprofit financial and grant-making information.

## How It Works

The pipeline is designed to be resilient to the inconsistencies in the IRS's data distribution. For any given year, it performs the following steps:

1.  **Discover:** Intelligently probes for available ZIP archive URLs based on the year, automatically handling the different naming conventions used by the IRS for pre- and post-2021 data.
2.  **Download:** Efficiently downloads all found ZIP archives for the year in parallel, skipping any that already exist locally.
3.  **Extract:** Unzips all archives into a temporary directory for processing.
4.  **Parse & Clean:** Processes hundreds of thousands of XML files in parallel, extracting key data points and normalizing them into a consistent schema.
5.  **Load:** Saves the clean, structured data into yearly CSV files and appends it to a persistent SQLite database.
6.  **Cleanup (Optional):** Automatically deletes the large source ZIP files to save disk space if configured to do so.

## Features

- **Intelligent Data Sourcing:** Automatically discovers and downloads the correct multi-part ZIP archives, handling different IRS naming conventions across years.
- **Parallel Processing:** Utilizes multiprocessing and multithreading for efficient downloading, extraction, and parsing.
- **Data Caching:** By default, downloaded ZIP archives are kept, allowing for rapid re-runs without re-downloading gigabytes of data.
- **Data Cleaning & Normalization:** Standardizes text fields, handles missing values gracefully, and normalizes financial data from different 990 form types (990, 990EZ, 990PF).
- **Relational Output:** Creates a structured SQLite database with indexed tables for `filings` and `grants`, enabling powerful SQL queries.
- **Accessible Formats:** Generates clean, ready-to-use CSV files for users who prefer spreadsheet-based tools.
- **Comprehensive Testing:** Includes a `pytest` suite to validate parsing logic, data cleaning rules, and I/O operations.
- **Data Quality Evaluation:** An accompanying script (`evaluate_data.py`) provides a summary report of the final dataset.

## Project Structure

```
.
├── .venv/                  # Virtual environment (ignored by Git)
├── irs_990_data/           # All generated data (ignored by Git)
│   ├── output/             # Final CSVs and data dictionary
│   ├── raw_zips/           # Downloaded yearly ZIP archives (can be auto-deleted)
│   ├── etl.log             # Log file for the pipeline
│   └── irs_990.sqlite      # The final SQLite database
├── tests/
│   ├── fixtures/           # Sample XML files for testing
│   └── test_comprehensive.py # The main pytest test suite
├── .gitignore              # Specifies files for Git to ignore
├── config.json             # User-configurable settings for the pipeline
├── etl_pipeline.py         # The main ETL script
├── evaluate_data.py        # Script to analyze the output data
├── pyproject.toml          # Project definition and dependencies
└── README.md               # This file
```

## Setup & Installation

**Prerequisites:**

- Git
- Python 3.11+
- [uv](https://github.com/astral-sh/uv) (for environment and package management)

**Steps:**

1.  **Clone the repository:**

    ```bash
    git clone <your-repo-url>
    cd <your-repo-name>
    ```

2.  **Create a virtual environment using `uv`:**

    ```bash
    uv venv
    ```

3.  **Activate the virtual environment:**

    - **macOS / Linux (zsh/bash):**
      ```bash
      source .venv/bin/activate
      ```
    - **Windows (PowerShell):**
      ```powershell
      .venv\Scripts\Activate.ps1
      ```

4.  **Install project dependencies (including test dependencies):**
    `uv` will read the `pyproject.toml` file and install all necessary packages.
    ```bash
    uv pip install -e ".[test]"
    ```

## Usage

### 1. Configure the Pipeline

Before running, edit the `config.json` file to specify your desired settings.

```json
{
  "start_year": 2020,
  "end_year": 2021,
  "base_url": "https://apps.irs.gov/pub/epostcard/990/xml",
  "base_dir": "./irs_990_data",
  "db_name": "irs_990.sqlite",
  "max_download_workers": 4,
  "max_parsing_workers": null,
  "filings_to_process_per_year": null,
  "cleanup_raw_zips": false
}
```

- `filings_to_process_per_year`: Set to an integer (e.g., `1000`) for a small sample run, or `null` to process all files.
- `max_parsing_workers`: Set to `null` to automatically use all available CPU cores.
- `cleanup_raw_zips`: Set to `true` to automatically delete source ZIP files after successful processing.

### 2. Run the ETL Pipeline

Execute the main script from your terminal. You can either rely on the `config.json` file or override the years with command-line arguments.

```bash
# Run for the years specified in config.json
python etl_pipeline.py

# Override the config and run for a specific range
python etl_pipeline.py --start_year 2021 --end_year 2022
```

### 3. Run the Tests

To verify that the code logic is correct and robust, run the `pytest` suite:

```bash
pytest -v
```

All tests should pass, confirming the reliability of the parser and cleaning functions.

### 4. Evaluate the Output Data

After the ETL pipeline has successfully run, you can generate a data quality report for a specific year using the `evaluate_data.py` script.

```bash
# Generate a report for the 2020 dataset
python evaluate_data.py 2020
```

This will print a summary of the processed data, including record counts, missing value analysis, and summary statistics.

## Output Description

- **`irs_990_data/output/`**:
  - `filings_{year}.csv`: One row per tax filing, containing organization info and summary financials.
  - `grants_{year}.csv`: One row per grant made, extracted from Form 990-PF and Schedule I.
  - `data_dictionary.md`: Auto-generated documentation explaining the columns in each table.
- **`irs_990_data/irs_990.sqlite`**:
  - A single-file database containing two tables:
    - `filings`: All data from the `filings_*.csv` files, combined.
    - `grants`: All data from the `grants_*.csv` files, combined.
  - Key columns are indexed for fast query performance.

---
