import os
import json
import logging
from src.etl import ETL

# Environment variables
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
DATASET = os.getenv("DATASET")
API_URL = os.getenv("API_URL", "https://api.publibike.ch/v1/public/partner/stations")


def extract_transform_load(request: dict) -> tuple[dict, int]:
    """
    Cloud Function entry point for the ETL process.

    Args:
        request (dict): The HTTP request object containing the mode.

    Returns:
        tuple[dict, int]: A response indicating the status of the ETL process and HTTP status code.
    """
    # Extract mode from the request
    mode = json.loads(request.data.decode()).get("mode")
    logging.info(f"Received request with mode: {mode}")

    if mode not in ["stations", "capacity"]:
        logging.error("Invalid mode specified.")
        return {
            "status": "error",
            "message": "Invalid mode specified. Please use 'stations' or 'capacity'.",
        }, 400

    # Validate environment variables
    if not GCS_BUCKET_NAME or not GCP_PROJECT_ID or not DATASET:
        logging.error(
            "Missing required environment variables. You have to set GCS_BUCKET_NAME, GCP_PROJECT_ID, and DATASET."
        )
        return {
            "status": "error",
            "message": "Missing required environment variables.",
        }, 500

    # Initialize the ETL process
    etl = ETL(
        mode=mode,
        url=API_URL,
        bucket_name=GCS_BUCKET_NAME,
        project_id=GCP_PROJECT_ID,
        dataset=DATASET,
    )

    # Run the ETL process
    try:
        etl.run()
        logging.info(f"ETL process for mode {mode} completed successfully.")
        return {
            "status": "success",
            "message": f"ETL process for mode {mode} completed successfully.",
        }, 200
    except Exception as e:
        logging.error(f"ETL process failed: {str(e)}")
        return {"status": "error", "message": f"ETL process failed: {str(e)}"}, 500


if __name__ == "__main__":
    # Example request for local testing
    test_request = {"data": json.dumps({"mode": "stations"}).encode("utf-8")}
    response, status_code = extract_transform_load(test_request)
    print(f"Response: {response}, Status Code: {status_code}")
