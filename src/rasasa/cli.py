from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, cast

from rasasa.dumps import (
    DumpDownloadResult,
    build_dump_url,
    compute_sha256,
    download_dump,
)
from rasasa.pgn import FilterStats, Speed, filter_games_with_clocks


def _write_metadata(
    meta_path: Path,
    username: str,
    output_path: Path,
    since: Optional[int],
    until: Optional[int],
    max_games: Optional[int],
    stats: DumpDownloadResult,
    source_url: Optional[str],
) -> None:
    payload = {
        "username": username,
        "output_path": str(output_path),
        "since": since,
        "until": until,
        "max_games": max_games,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "stats": asdict(stats),
        "source_url": source_url,
    }
    meta_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _write_manifest(
    manifest_path: Path, output_path: Path, source_url: str, stats: DumpDownloadResult
) -> None:
    file_size = output_path.stat().st_size
    payload = {
        "source_url": source_url,
        "file_path": str(output_path),
        "file_size": file_size,
        "remote_size": stats.remote_bytes,
        "sha256": compute_sha256(output_path),
        "downloaded": not stats.skipped,
    }
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_extract_metadata(
    meta_path: Path,
    input_path: Path,
    output_path: Path,
    max_games: Optional[int],
    stats: FilterStats,
    speed: str,
) -> None:
    payload = {
        "input_path": str(input_path),
        "output_path": str(output_path),
        "max_games": max_games,
        "speed": speed,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "stats": asdict(stats),
    }
    meta_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


class DownloadDumpArgs(argparse.Namespace):
    command: str
    output: Optional[str]
    year: int
    month: int
    variant: str
    rated: bool
    base_url: str


class ExtractArgs(argparse.Namespace):
    command: str
    input: str
    output: Optional[str]
    max_games: Optional[int]
    speed: str


def _parse_speed(raw: str) -> Speed:
    for speed in Speed:
        if speed.value == raw:
            return speed
    raise ValueError(f"Unknown speed: {raw}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="rasasa utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    download = subparsers.add_parser(
        "dump", help="Download monthly Lichess database dump"
    )
    download.add_argument("--year", type=int, required=True)
    download.add_argument("--month", type=int, required=True)
    download.add_argument("--variant", default="standard")
    download.add_argument("--rated", dest="rated", action="store_true", default=True)
    download.add_argument("--unrated", dest="rated", action="store_false")
    download.add_argument("--base-url", default="https://database.lichess.org")
    download.add_argument("--output")

    extract = subparsers.add_parser(
        "extract", help="Extract games with complete clock data"
    )
    extract.add_argument("--input", required=True)
    extract.add_argument("--output")
    extract.add_argument("--max", dest="max_games", type=int)
    extract.add_argument(
        "--speed", default=Speed.BULLET.value, choices=[s.value for s in Speed]
    )

    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "dump":
        dump_args = cast(DownloadDumpArgs, args)
        output = (
            Path(dump_args.output)
            if dump_args.output
            else Path("data")
            / "raw"
            / f"lichess_db_{dump_args.variant}_{'rated' if dump_args.rated else 'unrated'}"
            f"_{dump_args.year}-{dump_args.month:02d}.pgn.zst"
        )
        url = build_dump_url(
            year=dump_args.year,
            month=dump_args.month,
            variant=dump_args.variant,
            rated=dump_args.rated,
            base_url=dump_args.base_url,
        )
        result = download_dump(url=url, output_path=output)
        meta_path = output.with_suffix(output.suffix + ".meta.json")
        _write_metadata(
            meta_path=meta_path,
            username="lichess-dump",
            output_path=output,
            since=None,
            until=None,
            max_games=None,
            stats=result,
            source_url=url,
        )
        manifest_path = output.with_suffix(output.suffix + ".manifest.json")
        _write_manifest(manifest_path, output, url, result)
        if result.skipped:
            print(
                f"Skipped download; using existing {output} ({result.existing_bytes} bytes)"
            )
        else:
            print(f"Wrote {output} ({result.bytes_written} bytes)")
        print(f"Metadata: {meta_path}")
        print(f"Manifest: {manifest_path}")
        return 0

    if args.command == "extract":
        extract_args = cast(ExtractArgs, args)
        input_path = Path(extract_args.input)
        output_path = (
            Path(extract_args.output)
            if extract_args.output
            else Path("data") / "processed" / (input_path.stem + ".ndjson")
        )
        parsed_speed = _parse_speed(extract_args.speed)
        stats = filter_games_with_clocks(
            input_path=input_path,
            output_path=output_path,
            max_games=extract_args.max_games,
            speed=parsed_speed,
        )
        meta_path = output_path.with_suffix(output_path.suffix + ".meta.json")
        _write_extract_metadata(
            meta_path=meta_path,
            input_path=input_path,
            output_path=output_path,
            max_games=extract_args.max_games,
            stats=stats,
            speed=parsed_speed.value,
        )
        print(f"Wrote {output_path} ({stats.kept_games} games kept)")
        print(f"Metadata: {meta_path}")
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
