from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from campaign_pipeline.drive.sync import DriveSync, MANIFEST_NAME, _should_push_file


@pytest.fixture(autouse=True)
def _drive_cache_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CAMPAIGN_DRIVE_CACHE", str(tmp_path / "drive-cache"))


@pytest.mark.parametrize(
    "name,expected",
    [
        ("data.csv", True),
        ("__pycache__", False),
        (MANIFEST_NAME, False),
        ("module.pyc", False),
    ],
)
def test_should_push_file(name: str, expected: bool, tmp_path: Path) -> None:
    path = tmp_path / name
    path.write_text("x", encoding="utf-8")
    assert _should_push_file(path) is expected


def test_manifest_roundtrip(tmp_path: Path) -> None:
    mock_creds = MagicMock()
    sync = DriveSync(mock_creds, "root-folder-id")
    sync.local_root = tmp_path / "drive-root"
    campaign = sync.local_root / "003-test"
    campaign.mkdir(parents=True)

    sync.save_manifest(campaign, {"files": {"lists/a.csv": {"id": "abc"}}})
    loaded = sync.load_manifest(campaign)
    assert loaded["files"]["lists/a.csv"]["id"] == "abc"
    assert "updated_at" in loaded


def test_push_campaign_uploads_new_files(tmp_path: Path) -> None:
    root_id = "drive-root-id"
    local_root = tmp_path / "cache" / root_id
    campaign = local_root / "003-test"
    lists = campaign / "lists"
    lists.mkdir(parents=True)
    csv_path = lists / "003-test_raw.csv"
    csv_path.write_text("domain\nexample.com\n", encoding="utf-8")

    mock_client = MagicMock()
    mock_client.ensure_folder.return_value = "campaign-drive-id"
    mock_client.upload_file.return_value = "file-id-1"
    mock_client.get_file.return_value = {
        "modifiedTime": "2026-01-01T00:00:00.000Z",
        "md5Checksum": "abc",
    }

    sync = DriveSync(MagicMock(), root_id)
    sync.local_root = local_root
    sync.client = mock_client

    stats = sync.push_campaign(campaign)

    assert stats["uploaded"] == 1
    mock_client.ensure_folder.assert_any_call(root_id, "003-test")
    mock_client.upload_file.assert_called_once()
    manifest = json.loads((campaign / ".pipeline" / MANIFEST_NAME).read_text(encoding="utf-8"))
    assert "lists/003-test_raw.csv" in manifest["files"]


def test_pull_campaign_downloads_when_remote_newer(tmp_path: Path) -> None:
    root_id = "drive-root-id"
    local_root = tmp_path / "cache" / root_id
    campaign_name = "003-test"
    local_campaign = local_root / campaign_name
    local_campaign.mkdir(parents=True)

    mock_client = MagicMock()
    mock_client.find_child.return_value = {"id": "campaign-drive-id", "name": campaign_name}
    mock_client.list_children.return_value = [
        {
            "id": "file-1",
            "name": "003-test_raw.csv",
            "mimeType": "text/csv",
            "modifiedTime": "2026-02-01T00:00:00.000Z",
        }
    ]
    mock_client.is_syncable_file.return_value = True

    def fake_download(file_id: str, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("domain\nnew.com\n", encoding="utf-8")

    mock_client.download_file.side_effect = fake_download

    sync = DriveSync(MagicMock(), root_id)
    sync.local_root = local_root
    sync.client = mock_client

    stats = sync.pull_campaign(campaign_name)

    assert stats["downloaded"] == 1
    assert (local_campaign / "003-test_raw.csv").read_text(encoding="utf-8").startswith("domain")
    manifest = sync.load_manifest(local_campaign)
    assert manifest["files"]["003-test_raw.csv"]["id"] == "file-1"


def test_pull_campaign_skips_unchanged(tmp_path: Path) -> None:
    root_id = "drive-root-id"
    local_root = tmp_path / "cache" / root_id
    campaign_name = "003-test"
    local_campaign = local_root / campaign_name
    local_campaign.mkdir(parents=True)
    csv_path = local_campaign / "003-test_raw.csv"
    csv_path.write_text("domain\nexample.com\n", encoding="utf-8")

    sync = DriveSync(MagicMock(), root_id)
    sync.local_root = local_root
    sync.save_manifest(
        local_campaign,
        {
            "files": {
                "003-test_raw.csv": {
                    "id": "file-1",
                    "modifiedTime": "2026-02-01T00:00:00.000Z",
                }
            }
        },
    )

    mock_client = MagicMock()
    mock_client.find_child.return_value = {"id": "campaign-drive-id"}
    mock_client.list_children.return_value = [
        {
            "id": "file-1",
            "name": "003-test_raw.csv",
            "mimeType": "text/csv",
            "modifiedTime": "2026-02-01T00:00:00.000Z",
        }
    ]
    mock_client.is_syncable_file.return_value = True
    sync.client = mock_client

    stats = sync.pull_campaign(campaign_name)

    assert stats["skipped"] == 1
    assert stats["downloaded"] == 0
    mock_client.download_file.assert_not_called()
