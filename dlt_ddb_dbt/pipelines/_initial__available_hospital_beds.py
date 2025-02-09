import os
import dlt
import pandas as pd
import requests
import time

# ✅ Fetch the MotherDuck token from the environment
motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
if not motherduck_token:
    raise ValueError("❌ MOTHERDUCK_TOKEN is not set in the environment! Set it before running.")

# ✅ Use a valid MotherDuck database (update with a real name from SHOW DATABASES)
database_name = "dlt_data"  # Change this to an actual database you own in MotherDuck

# ✅ Define the MotherDuck connection using the token
destination = dlt.destinations.motherduck(f"md:{database_name}?motherduck_token={motherduck_token}")

# ✅ Initialize the DLT pipeline
pipeline = dlt.pipeline(
    pipeline_name="_initial_available_hospital_beds_pipeline",
    destination=destination,
    dataset_name="available_hospital_beds"
)

# ✅ API Query Function
def fetch_kolada_data(year):
    api_url = f"http://api.kolada.se/v2/data/kpi/N70845/year/{year}"
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        records = data.get("values", [])
        
        if not records:
            print(f"⚠️ No data found for {year}. Skipping...")
            return None

        expanded_data = []
        for record in records:
            for value in record.get("values", []):
                flattened_record = {**record, **value, "year": year}
                flattened_record.pop("values", None)
                expanded_data.append(flattened_record)

        return pd.DataFrame(expanded_data)

    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching data for {year}: {e}")
        return None

# ✅ Loop over years from 1992 to 2024
all_data = []
for year in range(1992, 2025):
    print(f"📡 Fetching data for {year}...")
    df = fetch_kolada_data(year)
    if df is not None:
        all_data.append(df)
    time.sleep(0.5)  # Add a small delay to avoid rate limiting

# ✅ Combine all data into a single DataFrame
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)

    # ✅ Load the DataFrame into MotherDuck
    print(f"🚀 Writing Kolada yearly data to MotherDuck (DB: {database_name})...")
    pipeline.run(final_df, table_name="kolada_data", write_disposition="append")
    print("✅ Kolada API data successfully appended to MotherDuck!")

else:
    print("⚠️ No data retrieved. Nothing to load.")

