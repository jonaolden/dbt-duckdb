## ./dlt_ddb_dbt/pipelines/_get_hospital_beds.py
import os
import dlt
import requests  # ✅ Import correct requests module
from datetime import datetime

# ✅ Fetch MotherDuck token from environment
motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
if not motherduck_token:
    raise ValueError("❌ MOTHERDUCK_TOKEN is not set in the environment!")

# ✅ Define MotherDuck destination
database_name = os.getenv("MOTHERDUCK_DB", "source")  # Default to "source"
destination = dlt.destinations.motherduck(f"md:{database_name}?motherduck_token={motherduck_token}")

# ✅ Initialize DLT pipeline with schema `source.raw`
pipeline = dlt.pipeline(
    pipeline_name="get_hospital_beds_pipeline",
    destination=destination,
    dataset_name="raw"
)

# ✅ API Parameters
API_BASE_URL = "http://api.kolada.se/v2/data/kpi/N70845/year"
CURRENT_YEAR = datetime.now().year  # Dynamically get the current year

# ✅ DLT Resource for Incremental Loading (Correct Primary Key)
@dlt.resource(primary_key=["municipality_id", "year"], write_disposition="append")
def kolada_hospital_beds(
    year: dlt.sources.incremental[int] = dlt.sources.incremental("year", initial_value=1992)
):
    """
    Fetches hospital bed data incrementally.
    
    - First run: Loads all years from 1992 onward.
    - Future runs: Loads only new years.
    """

    # ✅ Only fetch years that haven't been processed yet
    years_to_fetch = range(year.last_value, CURRENT_YEAR + 1)

    for target_year in years_to_fetch:
        api_url = f"{API_BASE_URL}/{target_year}"
        try:
            response = requests.get(api_url)  # ✅ Uses standard requests module
            response.raise_for_status()
            data = response.json()
            records = data.get("values", [])

            if not records:
                print(f"⚠️ No data found for {target_year}. Skipping...")
                continue

            # ✅ Flatten response and generate a proper primary key
            expanded_data = []
            for record in records:
                for value in record.get("values", []):
                    flattened_record = {**record, **value, "year": target_year}

                    # ✅ Assign a proper primary key (combining municipality_id + year)
                    flattened_record["municipality_id"] = record.get("municipality", "unknown")

                    flattened_record.pop("values", None)  # Remove nested field
                    expanded_data.append(flattened_record)

            yield expanded_data  # ✅ Yields data to DLT, which tracks incremental progress

        except requests.RequestException as e:  # ✅ Correct API error handling
            print(f"❌ Error fetching data for {target_year}: {e}")

# ✅ Run the pipeline
if __name__ == "__main__":
    print(f"🚀 Writing Kolada data to MotherDuck (DB: {database_name})...")
    load_info = pipeline.run(kolada_hospital_beds)
    print(f"✅ Kolada API data successfully loaded! Summary: {load_info}")
