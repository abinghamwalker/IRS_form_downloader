import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sqlite3

# Make the main script and its components importable
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl_pipeline import (
    Form990Parser, 
    clean_dataframes,
    parse_local_xml_file_worker, # Use the correct top-level worker
    save_outputs,
    CONFIG,
    Form990Schema
)

# --- Test Classes ---

class TestForm990Parser:
    """Groups all unit tests for the Form990Parser class."""

    def test_malformed_xml_raises_error(self):
        with pytest.raises(ValueError, match="XML Parse Error"):
            Form990Parser("<Return><Filer></Filer>")

    def test_unknown_form_type(self):
        xml_content = '<Return xmlns="http://www.irs.gov/efile"><ReturnData><IRSUnknownForm/></ReturnData></Return>'
        parser = Form990Parser(xml_content)
        assert parser.form_type == "UNKNOWN"

    @pytest.mark.parametrize("invalid_amount", ["N/A", "###", "invalid text"])
    def test_various_invalid_numeric_values(self, invalid_amount):
        xml = f"""<Return xmlns="http://www.irs.gov/efile">
          <ReturnData><IRS990PF><TotalRevAndExpnssAmt>{invalid_amount}</TotalRevAndExpnssAmt></IRS990PF></ReturnData>
        </Return>"""
        parser = Form990Parser(xml)
        assert parser.parse_filing()['total_revenue'] == 0.0

    def test_very_large_financial_values(self):
        xml = """<Return xmlns="http://www.irs.gov/efile">
          <ReturnData><IRS990PF><TotalRevAndExpnssAmt>5000000000</TotalRevAndExpnssAmt></IRS990PF></ReturnData>
        </Return>"""
        parser = Form990Parser(xml)
        assert parser.parse_filing()['total_revenue'] == 5_000_000_000.0

    def test_negative_financial_values(self):
        xml = """<Return xmlns="http://www.irs.gov/efile">
          <ReturnData><IRS990PF><TotalRevAndExpnssAmt>-50000</TotalRevAndExpnssAmt></IRS990PF></ReturnData>
        </Return>"""
        parser = Form990Parser(xml)
        assert parser.parse_filing()['total_revenue'] == -50000.0

    def test_missing_critical_fields(self):
        xml = """<Return xmlns="http://www.irs.gov/efile">
          <ReturnHeader><Filer></Filer></ReturnHeader><ReturnData><IRS990PF/></ReturnData>
        </Return>"""
        parser = Form990Parser(xml)
        data = parser.parse_filing()
        assert data['ein'] is None


class TestGrantParsing:
    """Dedicated tests for grant extraction logic."""
    
    def test_grants_with_foreign_addresses(self):
        xml = """<Return xmlns="http://www.irs.gov/efile">
          <ReturnData><IRS990PF>
            <GrantOrContributionPdDurYrGrp>
              <RecipientBusinessName><BusinessNameLine1Txt>INTL ORG</BusinessNameLine1Txt></RecipientBusinessName>
              <RecipientForeignAddress><CityNm>London</CityNm></RecipientForeignAddress>
              <Amt>10000</Amt>
            </GrantOrContributionPdDurYrGrp>
          </IRS990PF></ReturnData>
        </Return>"""
        parser = Form990Parser(xml)
        grants = parser.parse_grants()
        assert len(grants) == 1
        assert grants[0]['recipient_name'] == 'INTL ORG'
        assert grants[0]['recipient_state'] is None

    def test_multiple_grants_same_form(self):
        xml = """<Return xmlns="http://www.irs.gov/efile">
          <ReturnData><IRS990PF>
            <GrantOrContributionPdDurYrGrp><Amt>100</Amt></GrantOrContributionPdDurYrGrp>
            <GrantOrContributionPdDurYrGrp><Amt>200</Amt></GrantOrContributionPdDurYrGrp>
          </IRS990PF></ReturnData>
        </Return>"""
        parser = Form990Parser(xml)
        grants = parser.parse_grants()
        assert len(grants) == 2

    def test_grants_missing_optional_fields(self):
        xml = """<Return xmlns="http://www.irs.gov/efile">
          <ReturnData><IRS990PF><GrantOrContributionPdDurYrGrp><Amt>5000</Amt></GrantOrContributionPdDurYrGrp></IRS990PF></ReturnData>
        </Return>"""
        parser = Form990Parser(xml)
        grants = parser.parse_grants()
        assert len(grants) == 1
        assert grants[0]['grant_purpose'] is None

    def test_parse_form_with_no_grants(self):
        xml = '<Return xmlns="http://www.irs.gov/efile"><ReturnData><IRS990PF/></ReturnData></Return>'
        parser = Form990Parser(xml)
        grants = parser.parse_grants()
        assert grants == []


class TestDataCleaning:
    """Dedicated tests for the clean_dataframes function."""

    @pytest.mark.parametrize("input_val, expected_is_na", [
        ("", True), ("   ", True), ("VALID", False), (None, True),
    ])
    def test_empty_string_to_nan_conversion(self, input_val, expected_is_na):
        df = pd.DataFrame([{'org_name': input_val}])
        cleaned_df, _ = clean_dataframes(df, pd.DataFrame())
        result = cleaned_df.iloc[0]['org_name']
        assert pd.isna(result) == expected_is_na

    def test_cleaning_grants_dataframe(self):
        grants_df = pd.DataFrame([{'recipient_name': ' lower case org ', 'recipient_state': 'california', 'grant_purpose': '  education  '}])
        _, cleaned_grants = clean_dataframes(pd.DataFrame(), grants_df)
        result = cleaned_grants.iloc[0]
        assert result['recipient_name'] == 'LOWER CASE ORG'
        assert result['recipient_state'] == 'CA'
        assert result['grant_purpose'] == 'education'

    def test_cleaning_preserves_existing_nan_values(self):
        df = pd.DataFrame([{'org_name': 'Valid', 'city': np.nan}])
        cleaned_df, _ = clean_dataframes(df, pd.DataFrame())
        assert pd.isna(cleaned_df.iloc[0]['city'])


class TestSchemaValidation:
    """Ensure output data has the expected structure and columns."""
    
    def test_filing_dataframe_has_required_columns(self):
        fixture_path = Path(__file__).parent / "fixtures" / "sample_990.xml"
        result = parse_local_xml_file_worker((fixture_path, 2021))
        filings_df, _ = clean_dataframes(pd.DataFrame([result['filing']]), pd.DataFrame())
        expected_cols = set(Form990Schema.FIELD_XPATH_MAP.keys())
        expected_cols.update(['object_id', 'filing_year', 'form_type'])
        assert expected_cols.issubset(filings_df.columns)

    def test_grants_dataframe_has_required_columns(self):
        fixture_path = Path(__file__).parent / "fixtures" / "sample_990pf.xml"
        result = parse_local_xml_file_worker((fixture_path, 2020))
        _, grants_df = clean_dataframes(pd.DataFrame(), pd.DataFrame(result['grants']))
        expected_cols = {'filer_ein', 'filing_year', 'recipient_name', 'recipient_city', 'recipient_state', 'grant_amount', 'grant_purpose'}
        assert expected_cols.issubset(grants_df.columns)


class TestFileIOAndDatabase:
    """Tests for file writing, database interactions, and unicode handling."""

    def test_parse_local_xml_file_corrupt(self, tmp_path):
        xml_file = tmp_path / "corrupt.xml"
        xml_file.write_text("<badxml>")
        result = parse_local_xml_file_worker((xml_file, 2022))
        assert result is None

    def test_parse_xml_file_with_unicode_content(self, tmp_path):
        xml_content = """<Return xmlns="http://www.irs.gov/efile">
          <ReturnHeader><Filer><EIN>123</EIN>
          <BusinessName><BusinessNameLine1Txt>Café München</BusinessNameLine1Txt></BusinessName>
          </Filer></ReturnHeader><ReturnData><IRS990PF/></ReturnData></Return>"""
        xml_file = tmp_path / "unicode.xml"
        xml_file.write_text(xml_content, encoding='utf-8')
        result = parse_local_xml_file_worker((xml_file, 2022))
        assert result is not None
        assert result['filing']['org_name'] == 'Café München'

    def test_database_append_not_replace(self, tmp_path):
        db_path = tmp_path / "append_test.sqlite"
        original_db_name = CONFIG['db_name']
        CONFIG['db_name'] = str(db_path)
        try:
            df1 = pd.DataFrame([{'ein': '1', 'filing_year': 2020}])
            save_outputs(df1, pd.DataFrame(), 2020)
            df2 = pd.DataFrame([{'ein': '2', 'filing_year': 2021}])
            save_outputs(df2, pd.DataFrame(), 2021)
            with sqlite3.connect(db_path) as conn:
                count = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
                assert count == 2
        finally:
            CONFIG['db_name'] = original_db_name

    def test_database_index_creation(self, tmp_path):
        db_path = tmp_path / "test_indexes.sqlite"
        original_db_name = CONFIG['db_name']
        CONFIG['db_name'] = str(db_path)
        try:
            df = pd.DataFrame([{'ein': '1', 'filing_year': 2020}])
            grants = pd.DataFrame([{'filer_ein': '1', 'filing_year': 2020}])
            save_outputs(df, grants, 2020)
            with sqlite3.connect(db_path) as conn:
                filings_indexes = conn.execute("PRAGMA index_list('filings');").fetchall()
                assert 'idx_filings_ein' in {idx[1] for idx in filings_indexes}
                grants_indexes = conn.execute("PRAGMA index_list('grants');").fetchall()
                assert 'idx_grants_filer_ein' in {idx[1] for idx in grants_indexes}
        finally:
            CONFIG['db_name'] = original_db_name


class TestWithRealFixtures:
    """Tests using the actual sample XML fixture files."""
    
    def test_sample_990pf_complete_parsing(self):
        fixture_path = Path(__file__).parent / "fixtures" / "sample_990pf.xml"
        result = parse_local_xml_file_worker((fixture_path, 2020))
        assert result is not None
        assert result['filing']['ein'] == "123456789"
        assert result['filing']['org_name'] == "TEST FOUNDATION PF"
        assert len(result['grants']) == 1
        assert result['grants'][0]['recipient_name'] == "CHARITY ONE"
    
    def test_sample_990_complete_parsing(self):
        fixture_path = Path(__file__).parent / "fixtures" / "sample_990.xml"
        result = parse_local_xml_file_worker((fixture_path, 2021))
        assert result is not None
        assert result['filing']['ein'] == "987654321"
        assert result['filing']['org_name'] == "PUBLIC CHARITY TEST"