import pytest

from src.shared_helpers import get_file_id


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
