# IRS_form_downloader

Download and Transform IRS Form 990 XML data

# 1. Setup project

mkdir irs_990_project
cd irs_990_project

# (Create etl_pipeline.py and requirements.txt here)

# 2. Create and activate venv

uv venv
source .venv/bin/activate

# 3. Install dependencies

uv pip install -r requirements.txt

# 4. Run the script

python etl_pipeline.py --start_year 2022 --end_year 2022

# 5. Deactivate when done

deactivate
