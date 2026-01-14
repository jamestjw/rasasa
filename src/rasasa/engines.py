from __future__ import annotations

import json
import os
import platform
import re
import shutil
import tarfile
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from rasasa.config import EngineConfig

GITHUB_RELEASES_BASE = (
    "https://github.com/official-stockfish/Stockfish/releases/download"
)


@dataclass(frozen=True)
class EngineInstallResult:
    version: str
    url: str
    path: Path
    skipped: bool


def _stockfish_version_token(version: str) -> str:
    match = re.search(r"(\d+(?:\.\d+)*)", version)
    if not match:
        raise ValueError(f"Unrecognized Stockfish version: {version}")
    return match.group(1)


def _stockfish_candidate_urls(version_token: str) -> list[str]:
    base = f"{GITHUB_RELEASES_BASE}/sf_{version_token}/"
    names = [
        "stockfish-ubuntu-x86-64-avx2.tar",
        "stockfish-ubuntu-x86-64-bmi2.tar",
        "stockfish-ubuntu-x86-64-sse41-popcnt.tar",
        "stockfish-ubuntu-x86-64.tar",
    ]
    return [base + name for name in names]


def _first_available_url(urls: list[str]) -> str:
    for url in urls:
        request = Request(url, headers={"User-Agent": "rasasa/0.1"}, method="HEAD")
        try:
            with urlopen(request, timeout=60) as response:
                if getattr(response, "status", 200) == 200:
                    return url
        except HTTPError:
            continue
    raise RuntimeError("No suitable Stockfish assets found in release")


def _select_stockfish_asset(version: str) -> tuple[str, str]:
    token = _stockfish_version_token(version)
    system = platform.system().lower()
    machine = platform.machine().lower()
    if system != "linux":
        raise ValueError("Stockfish downloader only supports Linux for now")
    if machine not in {"x86_64", "amd64"}:
        raise ValueError("Stockfish downloader only supports x86_64 Linux for now")
    candidates: list[tuple[str, str]] = []
    for url in _stockfish_candidate_urls(token):
        candidates.append((Path(url).name.lower(), url))

    selected_url = _first_available_url([url for _, url in candidates])
    return Path(selected_url).name.lower(), selected_url


def _download_file(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    request = Request(url, headers={"User-Agent": "rasasa/0.1"})
    with urlopen(request, timeout=60) as response:
        if getattr(response, "status", 200) != 200:
            raise RuntimeError(f"Unexpected response status for {url}")
        with destination.open("wb") as handle:
            shutil.copyfileobj(response, handle)


def _find_largest_file(members: list[zipfile.ZipInfo]) -> zipfile.ZipInfo:
    candidates = [member for member in members if not member.is_dir()]
    if not candidates:
        raise RuntimeError("No files found in Stockfish archive")
    return max(candidates, key=lambda member: member.file_size)


def _write_manifest(path: Path, url: str, version: str) -> None:
    payload = {
        "url": url,
        "version": version,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _stockfish_install_dir(version: str, tools_dir: Path) -> Path:
    token = _stockfish_version_token(version)
    return tools_dir / "stockfish" / token


def stockfish_binary_path(version: str, tools_dir: Path) -> Path:
    install_dir = _stockfish_install_dir(version, tools_dir)
    return install_dir / "stockfish"


def install_stockfish(version: str, tools_dir: Path) -> EngineInstallResult:
    _, url = _select_stockfish_asset(version)
    install_dir = _stockfish_install_dir(version, tools_dir)
    binary_path = stockfish_binary_path(version, tools_dir)
    manifest_path = install_dir / "manifest.json"
    if binary_path.exists():
        return EngineInstallResult(
            version=version, url=url, path=binary_path, skipped=True
        )

    install_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        archive_path = temp_path / "stockfish.archive"
        _download_file(url, archive_path)
        if zipfile.is_zipfile(archive_path):
            with zipfile.ZipFile(archive_path) as archive:
                selected = _find_largest_file(archive.infolist())
                archive.extract(selected, path=temp_path)
                extracted_path = temp_path / selected.filename
                shutil.move(str(extracted_path), binary_path)
        else:
            with tarfile.open(archive_path) as archive:
                members = [member for member in archive.getmembers() if member.isfile()]
                if not members:
                    raise RuntimeError("No files found in Stockfish archive")
                selected = max(members, key=lambda member: member.size)
                archive.extract(selected, path=temp_path)
                extracted_path = temp_path / selected.name
                shutil.move(str(extracted_path), binary_path)

    os.chmod(binary_path, 0o755)
    _write_manifest(manifest_path, url=url, version=version)
    return EngineInstallResult(
        version=version, url=url, path=binary_path, skipped=False
    )


def resolve_engine_path(engine: EngineConfig, tools_dir: Path) -> str:
    if engine.name == "stockfish":
        candidate = stockfish_binary_path(engine.version, tools_dir)
        if candidate.exists():
            return str(candidate)
    return engine.name
