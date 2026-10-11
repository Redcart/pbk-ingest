from src.etl import ETL


def test_etl_instance():
    # Provide an instance of ETL for each test
    ETL(
        mode="stations",
        url="http://example.com",
        bucket_name="test_bucket",
        project_id="test_project",
        dataset="test_dataset",
    )

    assert ETL is not None
