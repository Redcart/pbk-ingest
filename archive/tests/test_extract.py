from src.extract import Extract


def test_extract_instance():
    # Provide an instance of Extract for each test
    extractor = Extract(
        url="http://example.com/api/data",
        bucket_name="test_bucket",
        mode="stations",
        output_path="test_output_path/data.json",
    )
    assert extractor is not None
