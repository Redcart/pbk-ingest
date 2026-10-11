import os
import json
import logging
from datetime import datetime
import pytz
from google.cloud import pubsub_v1

from src.extract import Extract

# Environment variables
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
API_URL = os.getenv("API_URL", "https://api.publibike.ch/v1/public/partner/stations")
TOPIC_NAME = os.getenv("PUBSUB_TOPIC_TRANSFORM")


def extract(request: dict) -> tuple[dict, int]:
    """
    Cloud Function entry point for the ETL process.

    Args:
        request (dict): The HTTP request object containing the mode.

    Returns:
        tuple[dict, int]: A response indicating the status of the ETL process and HTTP status code.
    """
    # Extract mode from the request
    mode = json.loads(s=request.data.decode()).get("mode")
    logging.info(f"Received request with mode: {mode}")

    if mode not in ["stations", "capacity"]:
        logging.error("Invalid mode specified.")
        return {
            "status": "error",
            "message": "Invalid mode specified. Please use 'stations' or 'capacity'.",
        }, 400

    # Validate environment variables
    if not GCS_BUCKET_NAME or not GCP_PROJECT_ID:
        logging.error(
            "Missing required environment variables. You have to set GCS_BUCKET_NAME, GCP_PROJECT_ID."
        )
        return {
            "status": "error",
            "message": "Missing required environment variables.",
        }, 500

    now = datetime.now(pytz.timezone("UTC"))
    current_ymd = now.strftime("%Y-%m-%d")
    current_minute = now.strftime("%H:%M:00")

    # Initialize the ETL process
    extractor = Extract(
        url=API_URL,
        bucket_name=GCS_BUCKET_NAME,
        mode=mode,
        output_path=f"{mode}/raw_data/{current_ymd}/{current_minute}/data.json",
    )

    data = json.dumps(
        {"mode": mode, "timestamp": now.strftime("%Y-%m-%d %H:%M:%S")}
    ).encode("utf-8")

    # Initialize Pub/Sub client
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(GCP_PROJECT_ID, TOPIC_NAME)

    # Run the ETL process
    try:
        extractor.get_data()
        future = publisher.publish(topic=topic_path, data=data)
        message_id = future.result()

        logging.info(f"Extract process for mode {mode} completed successfully.")
        logging.info(f"Message published with ID: {message_id}")
        return {
            "status": "success",
            "message": f"Extract process for mode {mode} completed successfully.",
        }, 200
    except Exception as e:
        logging.error(f"Extract process failed: {str(e)}")
        return {"status": "error", "message": f"Extract process failed: {str(e)}"}, 500


if __name__ == "__main__":
    # Example request for local testing
    test_request = {"data": json.dumps({"mode": "stations"}).encode("utf-8")}
    response, status_code = extract(test_request)
    print(f"Response: {response}, Status Code: {status_code}")
