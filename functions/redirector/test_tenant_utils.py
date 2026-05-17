import pytest

from tenant_utils import normalize_original_host


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("", ""),
        ("   ", ""),
        ("Go.Example.DE", "go.example.de"),
        ("GO.HOST.DE:443", "go.host.de"),
        ("  Host.Name:8080  ", "host.name"),
    ],
)
def test_normalize_original_host(raw, expected):
    assert normalize_original_host(raw) == expected
