import argparse
import sqlite3
from pathlib import Path

import pandas as pd

BASE_DIR = Path("./irs_990_data")
OUTPUT_DIR = BASE_DIR / "output"
DB_PATH = BASE_DIR / "irs_990.sqlite"


def print_header(title):
    print("\n" + "=" * 80)
    print(f" {title.upper()} ")
    print("=" * 80)


def generate_profile_report(
    filings_df: pd.DataFrame, grants_df: pd.DataFrame, year: int
):
    print_header(f"Data Quality & Evaluation Report for Year {year}")

    print_header("Filings Data Profile")
    if filings_df.empty:
        print("No filings data found.")
        return

    print(f"Total Filings Processed: {len(filings_df):,}")
    
    unique_objects = filings_df['object_id'].nunique()
    print(f"Unique Filings (by object_id): {unique_objects:,}")
    if len(filings_df) > unique_objects:
        print(f"WARNING: Found {len(filings_df) - unique_objects} duplicate filings!")
    else:
        print("✓ Uniqueness check passed.")

    print("\n--- Completeness: Missing Values (Filings) ---")
    missing = filings_df.isnull().sum()
    missing_pct = (missing / len(filings_df) * 100).round(2)
    missing_df = pd.DataFrame({"count": missing, "percent": missing_pct})
    print(missing_df[missing_df["count"] > 0])
    print("\nNOTE: Missing values for EIN, name, or address are rare but possible.")

    print("\n--- Distribution: Financial Summary (Filings) ---")
    financial_cols = ["total_revenue", "total_expenses", "total_assets_eoy"]
    print(filings_df[financial_cols].describe().apply(lambda s: s.apply("{:,.2f}".format)))
    neg_revenue = (filings_df['total_revenue'] < 0).sum()
    print(f"\nFilings with negative total revenue (losses): {neg_revenue:,} (This is normal)")

    print("\n--- Validity: Top 10 States & Form Types ---")
    print("Top 10 States by Filing Count:")
    print(filings_df["state"].value_counts().nlargest(10))
    print("\nForm Type Distribution:")
    print(filings_df["form_type"].value_counts())

    print_header("Grants Data Profile")
    if grants_df.empty:
        print("No grants data found.")
        return

    print(f"Total Grants Extracted: {len(grants_df):,}")

    print("\n--- Completeness: Missing Values (Grants) ---")
    missing_g = grants_df.isnull().sum()
    missing_g_pct = (missing_g / len(grants_df) * 100).round(2)
    missing_g_df = pd.DataFrame({"count": missing_g, "percent": missing_g_pct})
    print(missing_g_df[missing_g_df["count"] > 0])
    print("\nNOTE: High missing count for 'grant_purpose' is expected behavior.")

    print("\n--- Distribution: Grant Amount Summary ---")
    positive_grants = grants_df[grants_df["grant_amount"] > 0]
    print(positive_grants[["grant_amount"]].describe().apply(
        lambda s: s.apply("{:,.2f}".format)
    ))
    zero_or_neg_grants = (grants_df['grant_amount'] <= 0).sum()
    if zero_or_neg_grants > 0:
        print(f"\nWARNING: Found {zero_or_neg_grants:,} grants with a value of $0 or less.")
    else:
        print("\n✓ All grant amounts are positive.")

    print("\n--- Referential Integrity: Grants to Filings Link ---")
    grant_eins = set(grants_df['filer_ein'].dropna())
    filing_eins = set(filings_df['ein'].dropna())
    orphan_grants = grant_eins - filing_eins
    
    if not orphan_grants:
        print("✓ All grants successfully link to a filing in the dataset.")
    else:
        print(f"WARNING: Found {len(orphan_grants)} 'orphan' filer EINs in the grants data")
        print("that do not have a corresponding record in the filings data.")
        print(f"Sample orphans: {list(orphan_grants)[:5]}")

def main():
    parser = argparse.ArgumentParser(
        description="Data Quality Evaluation Tool for IRS 990 ETL."
    )
    parser.add_argument("year", type=int, help="The year of the dataset to evaluate.")
    parser.add_argument(
        "--source",
        type=str,
        choices=["csv", "sqlite"],
        default="csv",
        help="Source to load data from (csv or sqlite).",
    )
    args = parser.parse_args()

    print(f"Loading data for year {args.year} from {args.source}...")
    try:
        # Define expected data types for ID columns to prevent warnings
        filing_dtypes = {'object_id': str, 'ein': str, 'zip_code': str}
        grant_dtypes = {'filer_ein': str}

        if args.source == "csv":
            filings_df = pd.read_csv(OUTPUT_DIR / f"filings_{args.year}.csv", dtype=filing_dtypes)
            grants_path = OUTPUT_DIR / f"grants_{args.year}.csv"
            grants_df = pd.read_csv(grants_path, dtype=grant_dtypes) if grants_path.exists() else pd.DataFrame()
        else:
            with sqlite3.connect(DB_PATH) as conn:
                filings_df = pd.read_sql_query(f"SELECT * FROM filings WHERE filing_year = {args.year}", conn)
                grants_df = pd.read_sql_query(f"SELECT * FROM grants WHERE filing_year = {args.year}", conn)
    except FileNotFoundError:
        print(f"ERROR: Data files for year {args.year} not found. Please run the ETL first.")
        return
    except Exception as e:
        print(f"An error occurred while loading data: {e}")
        return

    generate_profile_report(filings_df, grants_df, args.year)

if __name__ == "__main__":
    main()