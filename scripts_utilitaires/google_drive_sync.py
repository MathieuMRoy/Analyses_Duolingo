from __future__ import annotations

import argparse
import io
import json
import os
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

BASE_DIR = Path(__file__).resolve().parent.parent
SCOPES = ["https://www.googleapis.com/auth/drive"]
FILES_TO_SYNC = {
    "daily_streaks_log.csv": BASE_DIR / "daily_streaks_log.csv",
    "rapport_historique.xlsx": BASE_DIR / "rapports_donnees" / "rapport_historique.xlsx",
}
MIME_TYPES = {
    ".csv": "text/csv",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"La variable d'environnement {name} est requise.")
    return value


def _build_drive_service():
    raw_credentials = _require_env("GDRIVE_SERVICE_ACCOUNT_JSON")
    credentials_info = json.loads(raw_credentials)
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=SCOPES,
    )
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


def _folder_id() -> str:
    return _require_env("GDRIVE_FOLDER_ID")


def _query_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _find_file(service, folder_id: str, remote_name: str) -> dict | None:
    response = service.files().list(
        q=(
            f"name = '{_query_escape(remote_name)}' "
            f"and '{folder_id}' in parents and trashed = false"
        ),
        orderBy="modifiedTime desc",
        pageSize=10,
        fields="files(id, name, modifiedTime)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    files = response.get("files", [])
    return files[0] if files else None


def _download_file(service, file_id: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    destination.write_bytes(buffer.getvalue())


def _upload_file(service, folder_id: str, local_path: Path, remote_name: str) -> None:
    metadata = _find_file(service, folder_id, remote_name)
    media = MediaFileUpload(
        str(local_path),
        mimetype=MIME_TYPES.get(local_path.suffix.lower()),
        resumable=False,
    )

    if metadata:
        service.files().update(
            fileId=metadata["id"],
            media_body=media,
            supportsAllDrives=True,
        ).execute()
        print(f"[Drive] Mise a jour : {remote_name}")
        return

    service.files().create(
        body={"name": remote_name, "parents": [folder_id]},
        media_body=media,
        fields="id",
        supportsAllDrives=True,
    ).execute()
    print(f"[Drive] Creation : {remote_name}")


def pull_files() -> None:
    service = _build_drive_service()
    folder_id = _folder_id()

    for remote_name, local_path in FILES_TO_SYNC.items():
        metadata = _find_file(service, folder_id, remote_name)
        if not metadata:
            print(f"[Drive] Absent, on continue : {remote_name}")
            continue
        _download_file(service, metadata["id"], local_path)
        print(f"[Drive] Telecharge : {remote_name} -> {local_path}")


def push_files() -> None:
    service = _build_drive_service()
    folder_id = _folder_id()

    for remote_name, local_path in FILES_TO_SYNC.items():
        if not local_path.exists():
            print(f"[Drive] Introuvable localement, on saute : {local_path}")
            continue
        _upload_file(service, folder_id, local_path, remote_name)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Synchronise les fichiers persistants Duolingo avec Google Drive."
    )
    parser.add_argument(
        "action",
        choices=("pull", "push"),
        help="pull = telecharger l'historique, push = renvoyer les fichiers mis a jour",
    )
    args = parser.parse_args()

    if args.action == "pull":
        pull_files()
        return

    push_files()


if __name__ == "__main__":
    main()
