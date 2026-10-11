from src.transform import Transform


def test_transform_instance():
    # Provide an instance of Transform for each test
    transfomer = Transform(
        bucket_name="test_bucket",
        input_path="test_input_path/data.csv",
        output_path="test_output_path/transformed_data.csv",
        mode="stations",
        date_time="2023-10-01T00:00:00Z",
    )
    assert transfomer is not None
