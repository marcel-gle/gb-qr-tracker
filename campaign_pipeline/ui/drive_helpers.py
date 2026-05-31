from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import streamlit as st

from campaign_pipeline.drive.config import client_secret_path, local_root_for_drive_folder
from campaign_pipeline.drive.oauth import disconnect, get_credentials, is_connected, run_connect_flow
from campaign_pipeline.drive.settings import load_drive_settings, save_drive_settings
from campaign_pipeline.drive.sync import DriveSync


def init_drive_session() -> None:
    settings = load_drive_settings()
    if settings.get("use_google_drive") is not None:
        st.session_state.setdefault("use_google_drive", bool(settings.get("use_google_drive")))
    else:
        st.session_state.setdefault("use_google_drive", False)
    if settings.get("drive_root_id"):
        st.session_state.setdefault("drive_root_id", settings["drive_root_id"])
        st.session_state.setdefault("drive_root_name", settings.get("drive_root_name", ""))
        root_id = str(settings["drive_root_id"])
        st.session_state.setdefault("campaign_base_parent", str(local_root_for_drive_folder(root_id)))


def use_google_drive() -> bool:
    return bool(st.session_state.get("use_google_drive"))


def get_drive_sync() -> Optional[DriveSync]:
    root_id = str(st.session_state.get("drive_root_id") or "").strip()
    if not root_id:
        return None
    creds = get_credentials(refresh=True)
    if not creds:
        return None
    return DriveSync(creds, root_id)


def maybe_push_active_campaign(*, show_message: bool = True) -> Optional[dict[str, int]]:
    if not use_google_drive():
        return None
    sync = get_drive_sync()
    campaign_dir = str(st.session_state.get("active_campaign_dir") or "").strip()
    if not sync or not campaign_dir:
        return None
    try:
        stats = sync.push_campaign(Path(campaign_dir))
        if show_message:
            st.caption(
                f"Synced to Google Drive: **{stats.get('uploaded', 0)}** uploaded, "
                f"**{stats.get('skipped', 0)}** unchanged"
            )
        return stats
    except Exception as exc:
        if show_message:
            st.warning(f"Google Drive sync failed: {exc}")
        return None


def pull_active_campaign(*, show_message: bool = True) -> Optional[dict[str, int]]:
    if not use_google_drive():
        return None
    sync = get_drive_sync()
    base_name = str(st.session_state.get("active_base_name") or "").strip()
    if not sync or not base_name:
        return None
    try:
        stats = sync.pull_campaign(base_name)
        if show_message:
            st.caption(
                f"Pulled from Google Drive: **{stats.get('downloaded', 0)}** files, "
                f"**{stats.get('skipped', 0)}** unchanged"
            )
        return stats
    except Exception as exc:
        if show_message:
            st.warning(f"Google Drive pull failed: {exc}")
        return None


def render_drive_connection_panel() -> None:
    st.markdown("#### Google Drive")
    secret = client_secret_path()
    if not secret.exists():
        st.warning(
            f"Place OAuth **Desktop client** JSON at `{secret}` "
            "(Google Cloud Console → Credentials → OAuth client ID → Desktop app). "
            "Or set `CAMPAIGN_DRIVE_CLIENT_SECRET` to its path."
        )

    connected = is_connected()
    if connected:
        st.success("Connected to Google Drive")
        if st.button("Disconnect Google Drive", key="drive_disconnect"):
            disconnect()
            st.session_state.pop("drive_root_id", None)
            st.rerun()
    else:
        if st.button("Connect Google Drive", key="drive_connect"):
            try:
                run_connect_flow()
                st.success("Connected. Set your Briefversand folder below.")
                st.rerun()
            except Exception as exc:
                st.error(str(exc))

    if not is_connected():
        return

    root_id = st.text_input(
        "Drive folder ID (Briefversand root)",
        value=str(st.session_state.get("drive_root_id") or load_drive_settings().get("drive_root_id") or ""),
        help="From the folder URL: drive.google.com/drive/folders/FOLDER_ID",
        key="drive_root_id_input",
    )
    if st.button("Save Drive folder", key="drive_save_folder"):
        folder_id = root_id.strip()
        if not folder_id:
            st.error("Enter a folder ID.")
            return
        sync = get_drive_sync() if folder_id == st.session_state.get("drive_root_id") else None
        if sync is None:
            creds = get_credentials(refresh=True)
            if not creds:
                st.error("Not connected.")
                return
            sync = DriveSync(creds, folder_id)
        try:
            meta = sync.client.get_file(folder_id, fields="id,name")
            name = str(meta.get("name") or folder_id)
            save_drive_settings(
                use_google_drive=True,
                drive_root_id=folder_id,
                drive_root_name=name,
            )
            st.session_state["use_google_drive"] = True
            st.session_state["drive_root_id"] = folder_id
            st.session_state["drive_root_name"] = name
            st.session_state["campaign_base_parent"] = str(local_root_for_drive_folder(folder_id))
            st.success(f"Using Drive folder **{name}**")
            st.rerun()
        except Exception as exc:
            st.error(f"Could not access folder: {exc}")

    if st.session_state.get("drive_root_name"):
        st.caption(
            f"Drive root: **{st.session_state['drive_root_name']}** · "
            f"Local cache: `{st.session_state.get('campaign_base_parent', '')}`"
        )

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Pull all campaigns from Drive", key="drive_pull_all"):
            sync = get_drive_sync()
            if sync:
                with st.spinner("Downloading from Drive..."):
                    stats = sync.pull_root()
                st.success(f"Pulled {stats.get('downloaded', 0)} file(s)")
    with col2:
        if st.button("Push active campaign to Drive", key="drive_push_active"):
            maybe_push_active_campaign(show_message=True)


def list_campaign_dirs() -> list[str]:
    if use_google_drive():
        sync = get_drive_sync()
        if sync:
            return sync.list_campaign_names()
    parent = Path(str(st.session_state.get("campaign_base_parent") or ""))
    if parent.is_dir():
        return sorted(p.name for p in parent.iterdir() if p.is_dir() and not p.name.startswith("."))
    return []
