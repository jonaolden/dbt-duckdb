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
    pipeline_name="get_inhabitants_pipeline",
    destination=destination,
    dataset_name="raw"
)

# ✅ API Parameters
API_BASE_URL = "http://api.kolada.se/v2/data/kpi/N01951/year"
CURRENT_YEAR = datetime.now().year  # Dynamically get the current year

# ✅ Generic DLT Resource for Incremental Loading with Flattening
@dlt.resource(primary_key=["municipality_id", "period", "gender"], write_disposition="append")
def kolada_inhabitants(
    period: dlt.sources.incremental[int] = dlt.sources.incremental("period", initial_value=1992)
):
    """
    Fetches inhabitants data incrementally, flattening response like `hospital_beds`.

    - First run: Loads all years from 2023 onward.
    - Future runs: Loads only new years.
    - Flattened structure makes it reusable for future datasets.
    """

    # ✅ Fetch only new years based on the last processed period
    years_to_fetch = range(period.last_value, CURRENT_YEAR + 1)

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
                    flattened_record = {**record, **value, "period": target_year}

                    # ✅ Assign a proper primary key (combining municipality_id + period)
                    flattened_record["municipality_id"] = record.get("municipality", "unknown")

                    # ✅ Remove nested fields to keep structure clean
                    flattened_record.pop("values", None)  

                    expanded_data.append(flattened_record)

            # ✅ Yield flattened data
            yield expanded_data  

        except requests.RequestException as e:
            print(f"❌ Error fetching data for {target_year}: {e}")

# ✅ Run the pipeline
if __name__ == "__main__":
    print(f"🚀 Writing Kolada inhabitants data to MotherDuck (DB: {database_name})...")
    load_info = pipeline.run(kolada_inhabitants)
    print(f"✅ Kolada inhabitants data successfully loaded! Summary: {load_info}")
