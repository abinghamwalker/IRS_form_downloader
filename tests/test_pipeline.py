# Important: This makes the main script importable
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl_pipeline import Form990Parser, clean_dataframes

# Define paths to our test data
FIXTURES_DIR = Path(__file__).parent / "fixtures"
SAMPLE_990PF_PATH = FIXTURES_DIR / "sample_990pf.xml"
SAMPLE_990_PATH = FIXTURES_DIR / "sample_990.xml"

# --- Test the Form990Parser Class ---


def test_parse_990pf_filing():
    """Verify basic financial parsing from a 990PF form."""
    content = SAMPLE_990PF_PATH.read_text()
    parser = Form990Parser(content)
    filing_data = parser.parse_filing()

    assert parser.form_type == "IRS990PF"
    assert filing_data["ein"] == "123456789"
    assert filing_data["org_name"] == "TEST FOUNDATION PF"
    assert filing_data["total_revenue"] == 100000.0


def test_parse_990pf_grants():
    """Verify grant parsing from a 990PF form."""
    content = SAMPLE_990PF_PATH.read_text()
    parser = Form990Parser(content)
    grants = parser.parse_grants()

    assert len(grants) == 1
    assert grants[0]["recipient_name"] == "CHARITY ONE"
    assert grants[0]["grant_amount"] == 5000.0


def test_parse_990_schedule_i_grants():
    """Verify grant parsing from a 990 with Schedule I."""
    content = SAMPLE_990_PATH.read_text()
    parser = Form990Parser(content)
    grants = parser.parse_grants()

    assert len(grants) == 1
    assert grants[0]["recipient_name"] == "HOMELESS SHELTER"
    assert grants[0]["grant_amount"] == 25000.0


# --- Test the clean_dataframes Function ---


def test_cleaning_whitespace_and_case():
    """Verify that text fields are stripped and uppercased."""
    messy_filings = pd.DataFrame(
        [{"org_name": "  test charity ", "city": "anytown", "state": " fl "}]
    )
    empty_grants = pd.DataFrame()

    cleaned_filings, _ = clean_dataframes(messy_filings, empty_grants)
    result = cleaned_filings.iloc[0]

    assert result["org_name"] == "TEST CHARITY"
    assert result["city"] == "ANYTOWN"
    assert result["state"] == "FL"


def test_cleaning_empty_strings_to_nan():
    """Verify that empty strings are converted to NumPy NaN for null representation."""
    messy_filings = pd.DataFrame([{"org_name": "GOOD ORG", "city": ""}])
    empty_grants = pd.DataFrame()

    cleaned_filings, _ = clean_dataframes(messy_filings, empty_grants)
    result = cleaned_filings.iloc[0]

    assert result["org_name"] == "GOOD ORG"
    assert pd.isna(result["city"])
