"""
IRS Form 990 XML Data ETL Pipeline (ZIP Archive Version).

Transforms IRS Form 990 XML data into a clean, user-friendly database and CSVs
by downloading and extracting annual multi-part ZIP archives from the IRS.
"""

import argparse
import json
import logging
import os
import shutil
import sqlite3
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
)
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

# --- 1. Load Configuration ---
with open("config.json", "r") as f:
    CONFIG = json.load(f)

CONFIG["zip_base_url"] = "https://apps.irs.gov/pub/epostcard/990/xml"
CONFIG["base_dir"] = Path(CONFIG["base_dir"])

if not CONFIG.get("max_parsing_workers"):
    CONFIG["max_parsing_workers"] = os.cpu_count()

CONFIG["base_dir"].mkdir(parents=True, exist_ok=True)
(CONFIG["base_dir"] / "raw_zips").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(CONFIG["base_dir"] / "etl.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# --- 2. Data Schema and Parser ---
class Form990Schema:
    """Defines the normalized schema and maps XML paths for 990 forms."""

    NAMESPACES = {"efile": "http://www.irs.gov/efile"}
    FIELD_XPATH_MAP = {
        "ein": ".//efile:Filer/efile:EIN",
        "tax_year": ".//efile:ReturnHeader/efile:TaxYr",
        "org_name": ".//efile:Filer/efile:BusinessName/efile:BusinessNameLine1Txt",
        "city": ".//efile:Filer/efile:USAddress/efile:CityNm",
        "state": ".//efile:Filer/efile:USAddress/efile:StateAbbreviationCd",
        "zip_code": ".//efile:Filer/efile:USAddress/efile:ZIPCd",
        "total_revenue": {
            "IRS990": ".//efile:CYTotalRevenueAmt",
            "IRS990EZ": ".//efile:TotalRevenueAmt",
            "IRS990PF": ".//efile:TotalRevAndExpnssAmt",
        },
        "total_expenses": {
            "IRS990": ".//efile:CYTotalExpensesAmt",
            "IRS990EZ": ".//efile:TotalExpensesAmt",
            "IRS990PF": ".//efile:TotalExpensesRevAndExpnssAmt",
        },
        "total_assets_eoy": {
            "IRS990": ".//efile:TotalAssetsEOYAmt",
            "IRS990EZ": ".//efile:TotalAssetsEOYAmt",
            "IRS990PF": ".//efile:TotalAssetsEOYAmt",
        },
        "contributions_grants": {
            "IRS990": ".//efile:CYContributionsGrantsAmt",
            "IRS990EZ": ".//efile:ContributionsGiftsGrantsEtcAmt",
            "IRS990PF": ".//efile:ContriRcvdRevAndExpnssAmt",
        },
    }

    @classmethod
    def get_xpath(cls, field: str, form_type: str) -> Optional[str]:
        """Get the correct XPath for a field given the form type."""
        mapping = cls.FIELD_XPATH_MAP.get(field)
        if isinstance(mapping, str):
            return mapping
        if isinstance(mapping, dict):
            return mapping.get(form_type)
        return None


class Form990Parser:
    """Parses a single Form 990 XML file into structured data."""

    def __init__(self, xml_content: str):
        try:
            self.root = ET.fromstring(xml_content)
            self.ns = Form990Schema.NAMESPACES
            self.form_type = self._get_form_type()
        except ET.ParseError as e:
            raise ValueError(f"XML Parse Error: {e}")

    def _get_form_type(self) -> str:
        """Determine the form type (990, 990EZ, 990PF) from the XML."""
        for form in ["IRS990", "IRS990EZ", "IRS990PF"]:
            if self.root.find(f".//efile:{form}", self.ns) is not None:
                return form
        return "UNKNOWN"

    def _find_text(self, xpath: str, default: Any = None) -> Optional[str]:
        """Safely find text in the XML, returning a default if not found."""
        if not xpath:
            return default
        try:
            node = self.root.find(xpath, self.ns)
            return node.text.strip() if node is not None and node.text else default
        except AttributeError:
            return default

    def _find_numeric(self, xpath: str, default: float = 0.0) -> float:
        """Safely find and convert a numeric value."""
        text_val = self._find_text(xpath)
        if text_val:
            try:
                return float(text_val)
            except (ValueError, TypeError):
                return default
        return default

    def parse_filing(self) -> Dict[str, Any]:
        """Parse the main filing data based on the defined schema."""
        data = {"form_type": self.form_type}
        for field in Form990Schema.FIELD_XPATH_MAP:
            xpath = Form990Schema.get_xpath(field, self.form_type)
            if "total_" in field or "contributions" in field:
                data[field] = self._find_numeric(xpath)
            else:
                data[field] = self._find_text(xpath)
        return data

    def parse_grants(self) -> List[Dict[str, Any]]:
        """Parse grant information from Schedule I or Part XV."""
        grants = []
        if self.form_type == "IRS990PF":
            elements = self.root.findall(
                ".//efile:GrantOrContributionPdDurYrGrp", self.ns
            )
            for elem in elements:
                address = elem.find(".//efile:RecipientUSAddress", self.ns)
                grants.append(
                    {
                        "recipient_name": self._find_text_from_node(
                            elem,
                            ".//efile:RecipientBusinessName/efile:BusinessNameLine1Txt",
                        ),
                        "recipient_city": self._find_text_from_node(
                            address, ".//efile:CityNm"
                        ),
                        "recipient_state": self._find_text_from_node(
                            address, ".//efile:StateAbbreviationCd"
                        ),
                        "grant_amount": self._find_numeric_from_node(
                            elem, ".//efile:Amt"
                        ),
                        "grant_purpose": self._find_text_from_node(
                            elem, ".//efile:PurposeOfGrantTxt"
                        ),
                    }
                )
        elif self.form_type in ["IRS990", "IRS990EZ"]:
            elements = self.root.findall(
                ".//efile:IRS990ScheduleI//efile:RecipientTable", self.ns
            )
            for elem in elements:
                address = elem.find(".//efile:USAddress", self.ns)
                grants.append(
                    {
                        "recipient_name": self._find_text_from_node(
                            elem,
                            ".//efile:RecipientBusinessName/efile:BusinessNameLine1Txt",
                        ),
                        "recipient_city": self._find_text_from_node(
                            address, ".//efile:CityNm"
                        ),
                        "recipient_state": self._find_text_from_node(
                            address, ".//efile:StateAbbreviationCd"
                        ),
                        "grant_amount": self._find_numeric_from_node(
                            elem, ".//efile:CashGrantAmt"
                        ),
                        "grant_purpose": self._find_text_from_node(
                            elem, ".//efile:PurposeOfGrantTxt"
                        ),
                    }
                )
        return grants

    def _find_text_from_node(
        self, node: Optional[ET.Element], xpath: str
    ) -> Optional[str]:
        """Safely find text from a specific XML sub-element."""
        if node is None:
            return None
        try:
            found_node = node.find(xpath, self.ns)
            return (
                found_node.text.strip()
                if found_node is not None and found_node.text
                else None
            )
        except AttributeError:
            return None

    def _find_numeric_from_node(self, node: ET.Element, xpath: str) -> float:
        """Safely find and convert a numeric value from a sub-element."""
        text_val = self._find_text_from_node(node, xpath)
        if text_val:
            try:
                return float(text_val)
            except (ValueError, TypeError):
                return 0.0
        return 0.0


# --- 3. Top-Level Worker Functions ---
def parse_xml_content(
    object_id: str, xml_content: str, filing_year: int
) -> Optional[Dict]:
    """Parse XML content into structured data. Can be used by any process."""
    try:
        parser = Form990Parser(xml_content)
        filing_data, grants_data = parser.parse_filing(), parser.parse_grants()
        filing_data.update({"object_id": object_id, "filing_year": filing_year})
        for grant in grants_data:
            grant.update(
                {
                    "filer_ein": filing_data.get("ein"),
                    "filing_year": filing_year,
                }
            )
        return {"filing": filing_data, "grants": grants_data}
    except Exception as e:
        logger.error(f"Failed to parse content for object_id {object_id}: {e}")
        return None


def parse_local_xml_file_worker(args: Tuple[Path, int]) -> Optional[Dict]:
    """Top-level wrapper for ProcessPoolExecutor to read a file and parse."""
    xml_path, year = args
    try:
        content = xml_path.read_text(encoding="utf-8")
        object_id = xml_path.stem.replace("_public", "")
        return parse_xml_content(object_id, content, year)
    except Exception as e:
        logger.error(f"Could not read or parse {xml_path.name}: {e}")
        return None


# --- 4. ZIP Archive Processing ---
def process_year(year: int) -> bool:
    """Process a single year using the ZIP archive method."""
    logger.info(f"--- Starting processing for year {year} using ZIP archive method ---")
    zip_paths = _discover_and_download_zips(year)
    if not zip_paths:
        logger.warning(f"No ZIP archives found for year {year}. This may be expected.")
        return True

    all_filings_data, all_grants_data = [], []
    with tempfile.TemporaryDirectory(prefix=f"irs990_{year}_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        logger.info(f"Extracting {len(zip_paths)} ZIPs to temporary directory...")
        with ThreadPoolExecutor(max_workers=CONFIG["max_parsing_workers"]) as executor:
            list(
                tqdm(
                    executor.map(lambda p: _extract_zip(p, temp_dir), zip_paths),
                    total=len(zip_paths),
                    desc=f"Extracting ZIPs {year}",
                )
            )

        xml_files = list(temp_dir.rglob("*.xml"))
        if not xml_files:
            logger.warning("No XML files found after extraction.")
            return True

        if CONFIG["filings_to_process_per_year"]:
            xml_files = xml_files[: CONFIG["filings_to_process_per_year"]]
        logger.info(f"Found {len(xml_files)} XML files to parse.")

        tasks = [(path, year) for path in xml_files]
        with ProcessPoolExecutor(max_workers=CONFIG["max_parsing_workers"]) as executor:
            results = list(
                tqdm(
                    executor.map(parse_local_xml_file_worker, tasks),
                    total=len(tasks),
                    desc=f"Parsing XMLs {year}",
                )
            )
        for result in results:
            if result:
                all_filings_data.append(result["filing"])
                all_grants_data.extend(result["grants"])

    if not all_filings_data:
        logger.warning(f"No data was successfully parsed for {year}.")
        return True

    _finalize_and_save(all_filings_data, all_grants_data, year)

    if CONFIG.get("cleanup_raw_zips", False):
        logger.info(f"Cleaning up raw ZIP archives for year {year}...")
        try:
            shutil.rmtree(CONFIG["base_dir"] / "raw_zips" / str(year))
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    return True


def _get_zip_urls_for_year(year: int) -> List[str]:
    """Generate list of possible ZIP URLs based on IRS naming conventions."""
    urls = []
    base_url = CONFIG["zip_base_url"]
    if year <= 2020:
        for i in range(1, 20):
            urls.append(f"{base_url}/{year}/download990xml_{year}_{i}.zip")
    else:
        for month in range(1, 13):
            for suffix in ["A", "B", "C", "D", "E"]:
                urls.append(
                    f"{base_url}/{year}/{year}_TEOS_XML_{month:02d}{suffix}.zip"
                )
    return urls


def _discover_and_download_zips(year: int) -> List[Path]:
    """Find, download, and verify all ZIP archives for a given year."""
    zip_dir = CONFIG["base_dir"] / "raw_zips" / str(year)
    zip_dir.mkdir(parents=True, exist_ok=True)
    possible_urls = _get_zip_urls_for_year(year)

    logger.info(f"Checking for available ZIP archives for year {year}...")

    def check_url(url):
        try:
            if requests.head(url, timeout=5).status_code == 200:
                return url
        except requests.RequestException:
            return None
        return None

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(check_url, possible_urls))
    valid_urls = [url for url in results if url]

    if not valid_urls:
        return []

    logger.info(f"Found {len(valid_urls)} ZIP archives to download for {year}.")

    def download_worker(url: str) -> Optional[Path]:
        filepath = zip_dir / url.split("/")[-1]
        if filepath.exists():
            try:
                with zipfile.ZipFile(filepath, "r") as _:
                    logger.info(f"Skipping {filepath.name}, already exists.")
                    return filepath
            except zipfile.BadZipFile:
                logger.warning(f"File {filepath.name} is corrupted, re-downloading...")
                filepath.unlink()
        try:
            with requests.get(url, stream=True, timeout=300) as r:
                r.raise_for_status()
                with open(filepath, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            with zipfile.ZipFile(filepath, "r") as _:
                pass
            return filepath
        except Exception as e:
            logger.error(f"Failed to download or verify {url}: {e}")
            if filepath.exists():
                filepath.unlink()
            return None

    with ThreadPoolExecutor(max_workers=CONFIG["max_download_workers"]) as executor:
        futures = {executor.submit(download_worker, url): url for url in valid_urls}
        return [
            f.result()
            for f in tqdm(
                as_completed(futures),
                total=len(futures),
                desc=f"Downloading ZIPs {year}",
            )
            if f.result()
        ]


def _extract_zip(zip_path: Path, extract_dir: Path):
    """Extract a single zip file."""
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(extract_dir)
    except Exception as e:
        logger.error(f"Failed to extract {zip_path.name}: {e}")


# --- 5. Common Finalizing Functions ---
def _finalize_and_save(filings_data: List[Dict], grants_data: List[Dict], year: int):
    """Take parsed data, clean it, and save it."""
    filings_df = pd.DataFrame(filings_data)
    grants_df = pd.DataFrame(grants_data)
    total_filings = len(filings_df)
    total_grants = len(grants_df)
    logger.info(f"Parsed {total_filings} filings and {total_grants} grants for {year}.")
    filings_df, grants_df = clean_dataframes(filings_df, grants_df)
    save_outputs(filings_df, grants_df, year)


def clean_dataframes(
    filings_df: pd.DataFrame, grants_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Apply cleaning and standardization rules to the extracted data."""
    logger.info("Starting data cleaning and standardization...")
    if not filings_df.empty:
        string_cols = ["org_name", "city", "state", "zip_code"]
        for col in string_cols:
            if col in filings_df.columns and pd.api.types.is_string_dtype(
                filings_df[col]
            ):
                filings_df[col] = filings_df[col].str.strip()
                filings_df.loc[filings_df[col] == "", col] = np.nan
                filings_df[col] = filings_df[col].str.upper()
        if "state" in filings_df.columns:
            filings_df["state"] = filings_df["state"].str.slice(0, 2)

    if not grants_df.empty:
        string_cols = [
            "recipient_name",
            "recipient_city",
            "recipient_state",
            "grant_purpose",
        ]
        for col in string_cols:
            if col in grants_df.columns and pd.api.types.is_string_dtype(
                grants_df[col]
            ):
                grants_df[col] = grants_df[col].str.strip()
                grants_df.loc[grants_df[col] == "", col] = np.nan
        if "recipient_name" in grants_df.columns:
            grants_df["recipient_name"] = grants_df["recipient_name"].str.upper()
        if "recipient_state" in grants_df.columns:
            grants_df["recipient_state"] = (
                grants_df["recipient_state"].str.upper().str.slice(0, 2)
            )
    logger.info("Data cleaning finished.")
    return filings_df, grants_df


def save_outputs(filings_df: pd.DataFrame, grants_df: pd.DataFrame, year: int):
    """Save DataFrames to a SQLite database and CSV files."""
    output_dir = CONFIG["base_dir"] / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    filings_df.to_csv(output_dir / f"filings_{year}.csv", index=False)
    if not grants_df.empty:
        grants_df.to_csv(output_dir / f"grants_{year}.csv", index=False)
    logger.info(f"CSV files saved to {output_dir}")

    db_path = CONFIG["base_dir"] / CONFIG["db_name"]
    try:
        with sqlite3.connect(db_path) as conn:
            filings_df.to_sql("filings", conn, if_exists="append", index=False)
            if not grants_df.empty:
                grants_df.to_sql("grants", conn, if_exists="append", index=False)
            index_statements = [
                "CREATE INDEX IF NOT EXISTS idx_filings_ein ON filings (ein);",
                "CREATE INDEX IF NOT EXISTS idx_filings_year ON filings (filing_year);",
                "CREATE INDEX IF NOT EXISTS idx_grants_filer_ein ON grants "
                "(filer_ein);",
            ]
            # Execute each statement
            for sql in index_statements:
                conn.execute(sql)
        logger.info(f"Data appended to SQLite database: {db_path}")
    except Exception as e:
        logger.error(f"Failed to write to database: {e}")


def generate_data_dictionary():
    """Generate a markdown data dictionary from the schema."""
    output_dir = CONFIG["base_dir"] / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    dict_path = output_dir / "data_dictionary.md"
    with open(dict_path, "w") as f:
        f.write("# IRS 990 Data Dictionary\n\n")
        f.write("This file describes the columns in the generated datasets.\n\n")
        f.write("## `filings` table/csv\n\n| Column | Description |\n|---|---|\n")
        f.write("| object_id | The unique ID for the filing (from XML filename). |\n")
        f.write("| filing_year | The year the filing was published by the IRS. |\n")
        for field in Form990Schema.FIELD_XPATH_MAP:
            f.write(f"| {field} | {field.replace('_', ' ').title()} from the XML. |\n")
        f.write("\n## `grants` table/csv\n\n| Column | Description |\n|---|---|\n")
        f.write("| filer_ein | Filer's EIN. Foreign key to `filings` table. |\n")
        f.write("| filing_year | Year of the filer's tax return. |\n")
        f.write("| recipient_name | Name of the grant recipient. |\n")
        f.write("| recipient_city | City of the grant recipient. |\n")
        f.write("| recipient_state | State of the grant recipient. |\n")
        f.write("| grant_amount | Monetary value of the grant. |\n")
        f.write("| grant_purpose | Stated purpose of the grant. |\n")
    logger.info(f"Data dictionary generated at {dict_path}")


# --- 6. Main Execution Block ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="IRS 990 Data ETL Pipeline (ZIP Version)."
    )
    parser.add_argument(
        "--start_year", type=int, help="Override start_year from config.json."
    )
    parser.add_argument(
        "--end_year", type=int, help="Override end_year from config.json."
    )
    args = parser.parse_args()
    start_year = (
        args.start_year if args.start_year is not None else CONFIG["start_year"]
    )
    end_year = args.end_year if args.end_year is not None else CONFIG["end_year"]

    logger.info(f"Starting IRS 990 ETL pipeline for years {start_year} to {end_year}")

    overall_success = all(
        process_year(year) for year in range(start_year, end_year + 1)
    )

    generate_data_dictionary()

    if overall_success:
        logger.info("Pipeline finished successfully.")
    else:
        logger.error("Pipeline finished with one or more errors. Check the log file.")
