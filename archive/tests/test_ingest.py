from src.ingest import Ingest


def test_ingest_instance():
    # Provide an instance of Ingest for each test
    ingestor = Ingest(
        project_id="test_project",
        dataset="test_dataset",
        table="test_table",
        bucket_name="test_bucket",
    )
    assert ingestor is not None
