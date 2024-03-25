import pytest

from src.op_ner import format_named_entity


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (
            'Span[87:89]: "JO FIRESTONE" → PERSON (0.8275)',
            {
                "span": "87:89",
                "named_entity": "JO FIRESTONE",
                "category": "PERSON",
                "score": "0.8275",
            },
        ),
        (
            'Span[90:91]: "usa" → GPE (0.9498)',
            {
                "span": "90:91",
                "named_entity": "usa",
                "category": "GPE",
                "score": "0.9498",
            },
        ),
        (
            'Span[11:14]: "TUESDAY 26 MARCH" → DATE (0.8084)',
            {
                "span": "11:14",
                "named_entity": "TUESDAY 26 MARCH",
                "category": "DATE",
                "score": "0.8084",
            },
        ),
        (
            'Span[0:3]: "ABC RADIO MELBOURNE" → ORG (0.6695)',
            {
                "span": "0:3",
                "named_entity": "ABC RADIO MELBOURNE",
                "category": "ORG",
                "score": "0.6695",
            },
        ),
    ],
)
def test_format_named_entity(test_input, expected):

    assert format_named_entity(test_input) == expected
