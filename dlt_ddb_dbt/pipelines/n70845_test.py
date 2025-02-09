import os
import dlt
import pandas as pd
import requests

# ✅ Set the API endpoint
api_url = "http://api.kolada.se/v2/data/kpi/N70845/year/2023"

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
    pipeline_name="kolada_pipeline",
    destination=destination,
    dataset_name="kolada_dataset"
)

try:
    # Fetch data from the API
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()

    # Extract the main 'values' key
    records = data.get("values", [])
    if not records:
        raise ValueError("No data found in 'values' key.")

    # Flatten the JSON structure
    expanded_data = []
    for record in records:
        for value in record.get("values", []):
            flattened_record = {**record, **value}
            flattened_record.pop("values", None)  # Remove nested 'values' field
            expanded_data.append(flattened_record)

    # ✅ Convert to DataFrame
    df = pd.DataFrame(expanded_data)

    # ✅ Load the dataframe into MotherDuck
    print(f"🚀 Writing Kolada API data to MotherDuck (DB: {database_name})...")
    pipeline.run(df, table_name="kolada_data", write_disposition="replace")
    print("✅ Kolada API data successfully written to MotherDuck!")

except requests.exceptions.RequestException as e:
    print(f"❌ Error fetching data from API: {e}")
except ValueError as e:
    print(f"❌ Data processing error: {e}")
except Exception as e:
    print(f"❌ An unexpected error occurred: {e}")
