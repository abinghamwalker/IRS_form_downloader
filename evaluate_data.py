import pandas as pd
from pathlib import Path
import sqlite3
import argparse

# Define the location of the generated data
BASE_DIR = Path("./irs_990_data")
OUTPUT_DIR = BASE_DIR / "output"
DB_PATH = BASE_DIR / "irs_990.sqlite"

def generate_profile_report(filings_df: pd.DataFrame, grants_df: pd.DataFrame, year: int):
    """Prints a data quality and summary report to the console."""
    
    print("\n" + "="*80)
    print(f"      DATA QUALITY AND EVALUATION REPORT FOR YEAR {year}")
    print("="*80)

    # --- Filings Report ---
    print("\n--- Filings Data ---")
    if filings_df.empty:
        print("No filings data found.")
    else:
        print(f"Total Filings Processed: {len(filings_df):,}")
        print(f"Unique EINs: {filings_df['ein'].nunique():,}")

        # Missing Value Analysis
        print("\nMissing Value Counts (Filings):")
        missing_counts = filings_df.isnull().sum()
        missing_percent = (missing_counts / len(filings_df) * 100).round(2)
        missing_df = pd.DataFrame({'count': missing_counts, 'percent': missing_percent})
        print(missing_df[missing_df['count'] > 0])
        
        # Summary Statistics for Financials
        print("\nSummary Statistics (Financials):")
        print(filings_df[['total_revenue', 'total_expenses', 'total_assets_eoy']].describe().apply(lambda s: s.apply('{:,.2f}'.format)))

        # Categorical Value Distribution
        print("\nTop 10 States by Filing Count:")
        print(filings_df['state'].value_counts().nlargest(10))

    # --- Grants Report ---
    print("\n--- Grants Data ---")
    if grants_df.empty:
        print("No grants data found.")
    else:
        print(f"Total Grants Extracted: {len(grants_df):,}")
        
        # Missing Value Analysis
        print("\nMissing Value Counts (Grants):")
        missing_counts = grants_df.isnull().sum()
        missing_percent = (missing_counts / len(grants_df) * 100).round(2)
        missing_df = pd.DataFrame({'count': missing_counts, 'percent': missing_percent})
        print(missing_df[missing_df['count'] > 0])
        
        # Summary Statistics for Grant Amounts
        print("\nSummary Statistics (Grant Amount):")
        # Filter out potential zero or negative amounts for a more meaningful summary
        positive_grants = grants_df[grants_df['grant_amount'] > 0]
        if not positive_grants.empty:
            print(positive_grants[['grant_amount']].describe().apply(lambda s: s.apply('{:,.2f}'.format)))
        else:
            print("No positive grant amounts found to describe.")

def main():
    parser = argparse.ArgumentParser(description="Data Quality Evaluation Tool for IRS 990 ETL.")
    parser.add_argument("year", type=int, help="The year of the dataset to evaluate.")
    parser.add_argument("--source", type=str, choices=['csv', 'sqlite'], default='csv', help="Source to load data from (csv or sqlite).")
    args = parser.parse_args()

    print(f"Loading data for year {args.year} from {args.source}...")
    
    try:
        if args.source == 'csv':
            filings_path = OUTPUT_DIR / f"filings_{args.year}.csv"
            grants_path = OUTPUT_DIR / f"grants_{args.year}.csv"
            filings_df = pd.read_csv(filings_path)
            grants_df = pd.read_csv(grants_path) if grants_path.exists() else pd.DataFrame()
        else: # sqlite
            with sqlite3.connect(DB_PATH) as conn:
                filings_df = pd.read_sql_query(f"SELECT * FROM filings WHERE filing_year = {args.year}", conn)
                grants_df = pd.read_sql_query(f"SELECT * FROM grants WHERE filing_year = {args.year}", conn)
    except FileNotFoundError:
        print(f"ERROR: Data files for year {args.year} not found. Please run the ETL script first.")
        return
    except Exception as e:
        print(f"An error occurred while loading data: {e}")
        return

    generate_profile_report(filings_df, grants_df, args.year)

if __name__ == "__main__":
    main()