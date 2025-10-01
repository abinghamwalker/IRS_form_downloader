"""
IRS Form 990 XML Data ETL Pipeline (ZIP Archive Version)
Transforms IRS Form 990 XML data (2020–2025) into a clean, 
user-friendly database and CSV datasets by downloading and extracting
the annual multi-part ZIP archives from the IRS website.
"""

import os
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from tqdm import tqdm
import sqlite3
import argparse
import zipfile
import tempfile

# --- 1. Configuration & Setup ---
CONFIG = {
    "start_year": 2020,
    "end_year": 2020,
    "base_url": "https://apps.irs.gov/pub/epostcard/990/xml",
    "base_dir": Path("./irs_990_data"),
    "db_name": "irs_990.sqlite",
    "max_download_workers": 4,
    "max_parsing_workers": os.cpu_count(),
    "filings_to_process_per_year": None,
}

# Create base directories required for the script to run
CONFIG["base_dir"].mkdir(parents=True, exist_ok=True)
(CONFIG["base_dir"] / "raw_zips").mkdir(exist_ok=True)

# Setup logging to file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(CONFIG["base_dir"] / "etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# --- 2. Normalized Data Schema ---
class Form990Schema:
    """Defines the normalized schema and maps XML paths for different 990 forms."""
    NAMESPACES = {'efile': 'http://www.irs.gov/efile'}
    FIELD_XPATH_MAP = {
        'ein': './/efile:Filer/efile:EIN',
        'tax_year': './/efile:ReturnHeader/efile:TaxYr',
        'org_name': './/efile:Filer/efile:BusinessName/efile:BusinessNameLine1Txt',
        'city': './/efile:Filer/efile:USAddress/efile:CityNm',
        'state': './/efile:Filer/efile:USAddress/efile:StateAbbreviationCd',
        'zip_code': './/efile:Filer/efile:USAddress/efile:ZIPCd',
        'total_revenue': {
            'IRS990': './/efile:CYTotalRevenueAmt',
            'IRS990EZ': './/efile:TotalRevenueAmt',
            'IRS990PF': './/efile:TotalRevAndExpnssAmt',
        },
        'total_expenses': {
            'IRS990': './/efile:CYTotalExpensesAmt',
            'IRS990EZ': './/efile:TotalExpensesAmt',
            'IRS990PF': './/efile:TotalExpensesRevAndExpnssAmt',
        },
        'total_assets_eoy': {
            'IRS990': './/efile:TotalAssetsEOYAmt',
            'IRS990EZ': './/efile:TotalAssetsEOYAmt',
            'IRS990PF': './/efile:TotalAssetsEOYAmt',
        },
        'contributions_grants': {
            'IRS990': './/efile:CYContributionsGrantsAmt',
            'IRS990EZ': './/efile:ContributionsGiftsGrantsEtcAmt',
            'IRS990PF': './/efile:ContriRcvdRevAndExpnssAmt',
        }
    }

    @classmethod
    def get_xpath(cls, field: str, form_type: str) -> Optional[str]:
        """Gets the correct XPath for a field given the form type."""
        mapping = cls.FIELD_XPATH_MAP.get(field)
        if isinstance(mapping, str): return mapping
        if isinstance(mapping, dict): return mapping.get(form_type)
        return None


# --- 3. Robust XML Parser ---
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
        """Determines the form type (990, 990EZ, 990PF) from the XML."""
        for form in ['IRS990', 'IRS990EZ', 'IRS990PF']:
            if self.root.find(f".//efile:{form}", self.ns) is not None:
                return form
        return "UNKNOWN"
        
    def _find_text(self, xpath: str, default: Any = None) -> Optional[str]:
        """Safely finds text in the XML, returning a default if not found."""
        if not xpath: return default
        try:
            node = self.root.find(xpath, self.ns)
            return node.text.strip() if node is not None and node.text else default
        except AttributeError:
            return default

    def _find_numeric(self, xpath: str, default: float = 0.0) -> float:
        """Safely finds and converts a numeric value."""
        text_val = self._find_text(xpath)
        if text_val:
            try: return float(text_val)
            except (ValueError, TypeError): return default
        return default

    def parse_filing(self) -> Dict[str, Any]:
        """Parses the main filing data based on the defined schema."""
        data = {'form_type': self.form_type}
        for field in Form990Schema.FIELD_XPATH_MAP:
            xpath = Form990Schema.get_xpath(field, self.form_type)
            if 'total_' in field or 'contributions' in field:
                data[field] = self._find_numeric(xpath)
            else:
                data[field] = self._find_text(xpath)
        return data

    def parse_grants(self) -> List[Dict[str, Any]]:
        """Parses grant information from Schedule I or Part XV."""
        grants = []
        if self.form_type == 'IRS990PF':
            elements = self.root.findall('.//efile:GrantOrContributionPdDurYrGrp', self.ns)
            for elem in elements:
                grants.append({
                    'recipient_name': self._find_text_from_node(elem, './/efile:RecipientBusinessName/efile:BusinessNameLine1Txt'),
                    'recipient_city': self._find_text_from_node(elem, './/efile:RecipientUSAddress/efile:CityNm'),
                    'recipient_state': self._find_text_from_node(elem, './/efile:RecipientUSAddress/efile:StateAbbreviationCd'),
                    'grant_amount': self._find_numeric_from_node(elem, './/efile:Amt'),
                    'grant_purpose': self._find_text_from_node(elem, './/efile:PurposeOfGrantTxt'),
                })
        elif self.form_type in ['IRS990', 'IRS990EZ']:
            elements = self.root.findall('.//efile:IRS990ScheduleI//efile:RecipientTable', self.ns)
            for elem in elements:
                grants.append({
                    'recipient_name': self._find_text_from_node(elem, './/efile:RecipientBusinessName/efile:BusinessNameLine1Txt'),
                    'recipient_city': self._find_text_from_node(elem, './/efile:USAddress/efile:CityNm'),
                    'recipient_state': self._find_text_from_node(elem, './/efile:USAddress/efile:StateAbbreviationCd'),
                    'grant_amount': self._find_numeric_from_node(elem, './/efile:CashGrantAmt'),
                    'grant_purpose': self._find_text_from_node(elem, './/efile:PurposeOfGrantTxt'),
                })
        return grants

    def _find_text_from_node(self, node: ET.Element, xpath: str) -> Optional[str]:
        try:
            found_node = node.find(xpath, self.ns)
            return found_node.text.strip() if found_node is not None and found_node.text else None
        except AttributeError: return None

    def _find_numeric_from_node(self, node: ET.Element, xpath: str) -> float:
        text_val = self._find_text_from_node(node, xpath)
        if text_val:
            try: return float(text_val)
            except (ValueError, TypeError): return 0.0
        return 0.0


# --- 4. Data Cleaning ---
def clean_dataframes(filings_df: pd.DataFrame, grants_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Applies cleaning and standardization rules to the extracted data."""
    logger.info("Starting data cleaning and standardization...")

    if not filings_df.empty:
        for col in ['org_name', 'city', 'state', 'zip_code']:
            if col in filings_df.columns and pd.api.types.is_string_dtype(filings_df[col]):
                filings_df[col] = filings_df[col].str.strip().str.upper()
        if 'state' in filings_df.columns:
            filings_df['state'] = filings_df['state'].str.slice(0, 2)
        filings_df.replace("", np.nan, inplace=True)

    if not grants_df.empty:
        for col in ['recipient_name', 'recipient_city', 'recipient_state', 'grant_purpose']:
            if col in grants_df.columns and pd.api.types.is_string_dtype(grants_df[col]):
                grants_df[col] = grants_df[col].str.strip()
        if 'recipient_name' in grants_df.columns:
             grants_df['recipient_name'] = grants_df['recipient_name'].str.upper()
        if 'recipient_state' in grants_df.columns:
             grants_df['recipient_state'] = grants_df['recipient_state'].str.upper().str.slice(0, 2)
        grants_df.replace("", np.nan, inplace=True)

    logger.info("Data cleaning finished.")
    return filings_df, grants_df


# --- 5. Worker and Helper Functions ---
def parse_local_xml_file(xml_path: Path, filing_year: int) -> Optional[Dict]:
    """Worker function to parse a single local XML file."""
    try:
        xml_content = xml_path.read_text(encoding='utf-8')
        object_id = xml_path.stem.replace("_public", "")
        parser = Form990Parser(xml_content)
        filing_data, grants_data = parser.parse_filing(), parser.parse_grants()
        filing_data.update({'object_id': object_id, 'filing_year': filing_year})
        for grant in grants_data:
            grant.update({'filer_ein': filing_data.get('ein'), 'filing_year': filing_year})
        return {'filing': filing_data, 'grants': grants_data}
    except Exception as e:
        logger.error(f"Could not parse {xml_path.name}: {e}")
        return None

def discover_and_download_zips(year: int) -> List[Path]:
    """Finds all sequential ZIP files for a year, downloads them, and returns their local paths."""
    zip_dir = CONFIG["base_dir"] / "raw_zips" / str(year)
    zip_dir.mkdir(parents=True, exist_ok=True)
    
    urls_to_download = []
    for i in range(1, 20):
        url = f"{CONFIG['base_url']}/{year}/download990xml_{year}_{i}.zip"
        try:
            response = requests.head(url, timeout=10)
            if response.status_code == 200:
                urls_to_download.append(url)
            else:
                break
        except requests.RequestException:
            break
    
    if not urls_to_download:
        logger.warning(f"No ZIP archives found for year {year}.")
        return []

    logger.info(f"Found {len(urls_to_download)} ZIP archives to download for {year}.")

    downloaded_paths = []
    def download_worker(url: str):
        filename = url.split('/')[-1]
        filepath = zip_dir / filename
        if filepath.exists():
            logger.info(f"Skipping download, {filename} already exists.")
            return filepath
        try:
            with requests.get(url, stream=True, timeout=300) as r:
                r.raise_for_status()
                with open(filepath, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            return filepath
        except Exception as e:
            logger.error(f"Failed to download {url}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=CONFIG['max_download_workers']) as executor:
        futures = {executor.submit(download_worker, url): url for url in urls_to_download}
        for future in tqdm(as_completed(futures), total=len(urls_to_download), desc=f"Downloading ZIPs {year}"):
            if result := future.result():
                downloaded_paths.append(result)

    return downloaded_paths

def extract_zip(zip_path: Path, extract_dir: Path):
    """Worker function to extract a single zip file."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        return True
    except Exception as e:
        logger.error(f"Failed to extract {zip_path.name}: {e}")
        return False


# --- 6. Main ETL Orchestration ---
def process_year(year: int) -> bool:
    """Orchestrates the download, extraction, parsing, and loading for a single year."""
    logger.info(f"--- Starting processing for year {year} using ZIP archive method ---")
    
    zip_paths = discover_and_download_zips(year)
    if not zip_paths:
        return True

    all_filings_data, all_grants_data = [], []
    with tempfile.TemporaryDirectory(prefix=f"irs990_{year}_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        logger.info(f"Extracting {len(zip_paths)} ZIPs to temporary directory: {temp_dir}")
        
        with ThreadPoolExecutor(max_workers=CONFIG['max_parsing_workers']) as executor:
            list(tqdm(executor.map(lambda p: extract_zip(p, temp_dir), zip_paths), total=len(zip_paths), desc=f"Extracting ZIPs {year}"))

        xml_files = list(temp_dir.glob('*.xml'))
        if CONFIG["filings_to_process_per_year"]:
            xml_files = xml_files[:CONFIG["filings_to_process_per_year"]]
        logger.info(f"Found {len(xml_files)} XML files to parse.")

        with ProcessPoolExecutor(max_workers=CONFIG['max_parsing_workers']) as executor:
            futures = {executor.submit(parse_local_xml_file, xml_path, year): xml_path for xml_path in xml_files}
            for future in tqdm(as_completed(futures), total=len(xml_files), desc=f"Parsing XMLs {year}"):
                if result := future.result():
                    all_filings_data.append(result['filing'])
                    all_grants_data.extend(result['grants'])
        
        logger.info("Temporary directory will be cleaned up automatically.")

    if not all_filings_data:
        logger.warning(f"No data was successfully parsed for {year}.")
        return True

    filings_df = pd.DataFrame(all_filings_data)
    grants_df = pd.DataFrame(all_grants_data)
    logger.info(f"Parsed {len(filings_df)} filings and {len(grants_df)} grants for {year}.")

    filings_df, grants_df = clean_dataframes(filings_df, grants_df)
    
    save_outputs(filings_df, grants_df, year)
    return True
    

# --- 7. Output and Documentation ---
def save_outputs(filings_df: pd.DataFrame, grants_df: pd.DataFrame, year: int):
    """Saves DataFrames to a SQLite database and CSV files."""
    output_dir = CONFIG['base_dir'] / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    filings_df.to_csv(output_dir / f"filings_{year}.csv", index=False)
    if not grants_df.empty:
        grants_df.to_csv(output_dir / f"grants_{year}.csv", index=False)
    logger.info(f"CSV files saved to {output_dir}")
    
    db_path = CONFIG['base_dir'] / CONFIG['db_name']
    try:
        with sqlite3.connect(db_path) as conn:
            filings_df.to_sql('filings', conn, if_exists='append', index=False)
            if not grants_df.empty:
                grants_df.to_sql('grants', conn, if_exists='append', index=False)
            for index_sql in ["CREATE INDEX IF NOT EXISTS idx_filings_ein ON filings (ein);", "CREATE INDEX IF NOT EXISTS idx_filings_year ON filings (filing_year);", "CREATE INDEX IF NOT EXISTS idx_grants_filer_ein ON grants (filer_ein);"]:
                conn.execute(index_sql)
        logger.info(f"Data appended to SQLite database: {db_path}")
    except Exception as e:
        logger.error(f"Failed to write to database: {e}")

def generate_data_dictionary():
    """Generates a markdown data dictionary from the schema."""
    output_dir = CONFIG['base_dir'] / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    dict_path = output_dir / "data_dictionary.md"
    
    with open(dict_path, "w") as f:
        f.write("# IRS 990 Data Dictionary\n\nThis file describes the columns in the generated datasets.\n\n")
        f.write("## `filings` table/csv\n\n| Column | Description |\n|---|---|\n")
        f.write("| object_id | The unique ID for the filing, derived from the XML filename. |\n")
        f.write("| filing_year | The year the filing was published by the IRS. |\n")
        for field in Form990Schema.FIELD_XPATH_MAP:
            f.write(f"| {field} | {field.replace('_', ' ').title()} extracted from the XML. |\n")
            
        f.write("\n## `grants` table/csv\n\n| Column | Description |\n|---|---|\n")
        f.write("| filer_ein | EIN of the organization that made the grant. Foreign key to the `filings` table. |\n")
        f.write("| filing_year | Year of the filer's tax return. |\n")
        f.write("| recipient_name | Name of the grant recipient. |\n")
        f.write("| recipient_city | City of the grant recipient. |\n")
        f.write("| recipient_state | State of the grant recipient. |\n")
        f.write("| grant_amount | Monetary value of the grant. |\n")
        f.write("| grant_purpose | Stated purpose of the grant. |\n")
        
    logger.info(f"Data dictionary generated at {dict_path}")


# --- 8. Main Execution Block ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IRS 990 Data ETL Pipeline (ZIP Archive Version).")
    parser.add_argument("--start_year", type=int, default=CONFIG['start_year'], help="First year to process.")
    parser.add_argument("--end_year", type=int, default=CONFIG['end_year'], help="Last year to process.")
    args = parser.parse_args()

    logger.info("Starting IRS 990 ETL pipeline.")
    
    overall_success = all(process_year(year) for year in range(args.start_year, args.end_year + 1))
    
    generate_data_dictionary()
    
    if overall_success:
        logger.info("Pipeline finished successfully.")
    else:
        logger.error("Pipeline finished with one or more errors. Please check the log file for details.")