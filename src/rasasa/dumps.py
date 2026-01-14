from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from urllib.request import Request, urlopen

DEFAULT_BASE_URL = "https://database.lichess.org"


@dataclass(frozen=True)
class DumpDownloadResult:
    url: str
    output_path: Path
    bytes_written: int
    skipped: bool
    existing_bytes: int
    remote_bytes: int | None


def build_dump_url(
    *,
    year: int,
    month: int,
    variant: str,
    rated: bool,
    base_url: str = DEFAULT_BASE_URL,
) -> str:
    rated_tag = "rated" if rated else "unrated"
    return (
        f"{base_url}/{variant}/"
        f"lichess_db_{variant}_{rated_tag}_{year}-{month:02d}.pgn.zst"
    )


def download_dump(
    *,
    url: str,
    output_path: Path,
    chunk_size: int = 1024 * 1024,
    timeout: int = 60,
) -> DumpDownloadResult:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    existing_bytes = output_path.stat().st_size if output_path.exists() else 0
    head_request = Request(url, headers={"User-Agent": "rasasa/0.1"}, method="HEAD")
    remote_bytes: int | None = None
    with urlopen(head_request, timeout=timeout) as response:
        if getattr(response, "status", 200) != 200:
            raise RuntimeError(f"Unexpected response status for {url}")
        length = response.headers.get("Content-Length")
        if length is not None:
            try:
                remote_bytes = int(length)
            except ValueError:
                remote_bytes = None
    if (
        existing_bytes > 0
        and remote_bytes is not None
        and existing_bytes == remote_bytes
    ):
        return DumpDownloadResult(
            url=url,
            output_path=output_path,
            bytes_written=0,
            skipped=True,
            existing_bytes=existing_bytes,
            remote_bytes=remote_bytes,
        )

    bytes_written = 0
    request = Request(url, headers={"User-Agent": "rasasa/0.1"})

    with urlopen(request, timeout=timeout) as response:
        if getattr(response, "status", 200) != 200:
            raise RuntimeError(f"Unexpected response status for {url}")
        with output_path.open("wb") as handle:
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                handle.write(chunk)
                bytes_written += len(chunk)

    return DumpDownloadResult(
        url=url,
        output_path=output_path,
        bytes_written=bytes_written,
        skipped=False,
        existing_bytes=0,
        remote_bytes=remote_bytes,
    )


def compute_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()
