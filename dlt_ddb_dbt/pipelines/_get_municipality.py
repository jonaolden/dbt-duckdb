import os
import dlt
import requests

# ✅ Fetch MotherDuck token from environment
motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
if not motherduck_token:
    raise ValueError("❌ MOTHERDUCK_TOKEN is not set in the environment!")

# ✅ Define MotherDuck destination
database_name = os.getenv("MOTHERDUCK_DB", "source")  # Default to "source"
destination = dlt.destinations.motherduck(f"md:{database_name}?motherduck_token={motherduck_token}")

# ✅ Initialize DLT pipeline with schema `source.raw`
pipeline = dlt.pipeline(
    pipeline_name="get_municipality_pipeline",
    destination=destination,
    dataset_name="raw"
)

# ✅ API Endpoint
API_URL = "http://api.kolada.se/v2/municipality"

# ✅ DLT Resource for Incremental Loading
@dlt.resource(primary_key="municipality_id", write_disposition="merge")
def kolada_municipalities(
    municipality_id: dlt.sources.incremental[int] = dlt.sources.incremental("municipality_id", initial_value=0)
):
    """
    Fetches municipality data from Kolada API.
    
    - First run: Loads all municipalities.
    - Future runs: Loads only new municipalities with IDs greater than last processed.
    """

    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()

        # ✅ Extract correct key
        records = data.get("values", [])
        if not records:
            print("⚠️ No new data found. Skipping update.")
            return

        # ✅ Process records & ensure unique primary keys
        for record in records:
            yield {
                "municipality_id": int(record["id"]),
                "municipality_name": record["title"],
                "municipality_type": record["type"]
            }

    except requests.RequestException as e:
        print(f"❌ Error fetching data from API: {e}")

# ✅ Run the pipeline
if __name__ == "__main__":
    print(f"🚀 Writing Kolada municipality data to MotherDuck (DB: {database_name})...")
    load_info = pipeline.run(kolada_municipalities)
    print(f"✅ Kolada municipality data successfully loaded! Summary: {load_info}")
