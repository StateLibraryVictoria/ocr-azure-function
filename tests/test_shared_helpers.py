import pytest

from src.shared_helpers import get_file_id, convert_df_column_to_string
from src.shared_azure_dl import read_df_from_data_lake


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ("image-pipeline/image-capture/101/file.jpg", "101/file"),
        ("image-pipeline/image-capture", False),
        ("image-pipeline/ocr/101/file.txt", "101/file"),
        ("image-pipeline/ocr/101/b/file.txt", "101/b/file"),
        ("image-pipeline/ocr/101/b/file", "101/b/file"),
        ("image-pipeline/ocr/10.1/b/file.txt", "10.1/b/file"),
    ],
)
def test_get_file_id(test_input, expected):
    assert get_file_id(test_input) == expected


sample_df = read_df_from_data_lake(
    "image-pipeline", "ocr", "018-POC/WIN_20240318_09_32_44_Pro", add_column_names=False
)


@pytest.mark.parametrize(
    "test_input, expected", [({"df": sample_df, "column_name": 11}, str)]
)
def test_convert_df_column_to_string(test_input, expected):
    assert isinstance(convert_df_column_to_string(**test_input), expected)
