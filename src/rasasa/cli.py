from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from rasasa.config import EngineConfig, load_engine_config
from rasasa.dumps import (
    DumpDownloadResult,
    build_dump_url,
    compute_sha256,
    download_dump,
)
from rasasa.engines import install_stockfish, resolve_engine_path
from rasasa.evaluation import EvaluationStats, evaluate_games, evaluate_games_parallel
from rasasa.pgn import FilterStats, Speed, filter_games_with_clocks

LOGGER = logging.getLogger(__name__)


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


def _write_evaluation_metadata(
    meta_path: Path,
    input_path: Path,
    output_path: Path,
    max_games: Optional[int],
    stats: EvaluationStats,
    engine: EngineConfig,
    engine_version: str,
) -> None:
    payload = {
        "input_path": str(input_path),
        "output_path": str(output_path),
        "max_games": max_games,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "stats": asdict(stats),
        "engine": {
            "name": engine.name,
            "version": engine_version,
            "depth": engine.depth,
            "threads": engine.threads,
            "hash_mb": engine.hash_mb,
        },
    }
    meta_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _parse_speed(raw: str) -> Speed:
    for speed in Speed:
        if speed.value == raw:
            return speed
    raise ValueError(f"Unknown speed: {raw}")


def _optional_int(value: object) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value:
        return int(value)
    return None


def _optional_str(value: object) -> Optional[str]:
    if isinstance(value, str) and value:
        return value
    return None


def _required_str(args: argparse.Namespace, name: str) -> str:
    value = getattr(args, name, None)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Missing required argument: {name}")
    return value


def _required_str_value(value: Optional[str], name: str) -> str:
    if value:
        return value
    raise ValueError(f"Missing required argument: {name}")


def _required_int(args: argparse.Namespace, name: str) -> int:
    value = getattr(args, name, None)
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value:
        return int(value)
    raise ValueError(f"Missing required argument: {name}")


def _optional_int_from_obj(value: object) -> Optional[int]:
    if isinstance(value, int):
        return value
    return None


def _optional_str_from_obj(value: object) -> Optional[str]:
    if isinstance(value, str) and value:
        return value
    return None


def _load_json_dict(path: Path) -> Optional[dict[str, object]]:
    try:
        payload_any: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return _as_dict(payload_any)


def _as_dict(value: Any) -> Optional[dict[str, object]]:
    value_any: Any = value
    if not hasattr(value_any, "items"):
        return None
    items_any: Any = value_any.items()
    payload: dict[str, object] = {}
    for key, item in items_any:
        if not isinstance(key, str):
            return None
        payload[key] = item
    return payload


def _meta_matches_evaluation(
    meta: dict[str, object],
    input_path: Path,
    output_path: Path,
    max_games: Optional[int],
    engine: EngineConfig,
) -> bool:
    if _optional_str_from_obj(meta.get("input_path")) != str(input_path):
        return False
    if _optional_str_from_obj(meta.get("output_path")) != str(output_path):
        return False
    if _optional_int_from_obj(meta.get("max_games")) != max_games:
        return False
    engine_meta = _as_dict(meta.get("engine"))
    if engine_meta is None:
        return False
    engine_name = _optional_str_from_obj(engine_meta.get("name"))
    engine_version = _optional_str_from_obj(engine_meta.get("version"))
    engine_depth = _optional_int_from_obj(engine_meta.get("depth"))
    engine_threads = _optional_int_from_obj(engine_meta.get("threads"))
    engine_hash = _optional_int_from_obj(engine_meta.get("hash_mb"))
    return (
        engine_name == engine.name
        and engine_version == engine.version
        and engine_depth == engine.depth
        and engine_threads == engine.threads
        and engine_hash == engine.hash_mb
    )


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

    evaluate = subparsers.add_parser(
        "evaluate", help="Evaluate games with a chess engine"
    )
    evaluate.add_argument("--input", required=True)
    evaluate.add_argument("--output")
    evaluate.add_argument("--max", dest="max_games", type=int)
    evaluate.add_argument("--engine")
    evaluate.add_argument("--engine-version", dest="engine_version")
    evaluate.add_argument("--depth", type=int)
    evaluate.add_argument("--threads", type=int)
    evaluate.add_argument("--hash-mb", dest="hash_mb", type=int)
    evaluate.add_argument("--workers", type=int, default=1)
    evaluate.add_argument("--config", default="config.toml")

    engine = subparsers.add_parser("engine", help="Manage chess engines")
    engine_subparsers = engine.add_subparsers(dest="engine_command", required=True)
    engine_install = engine_subparsers.add_parser(
        "install", help="Download a chess engine"
    )
    engine_install.add_argument("--config", default="config.toml")

    return parser


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "dump":
        year = _required_int(args, "year")
        month = _required_int(args, "month")
        variant = _required_str(args, "variant")
        rated = bool(getattr(args, "rated"))
        base_url = _required_str(args, "base_url")
        output_raw = _optional_str(getattr(args, "output", None))
        output = (
            Path(output_raw)
            if output_raw
            else Path("data")
            / "raw"
            / f"lichess_db_{variant}_{'rated' if rated else 'unrated'}"
            f"_{year}-{month:02d}.pgn.zst"
        )
        url = build_dump_url(
            year=year,
            month=month,
            variant=variant,
            rated=rated,
            base_url=base_url,
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
            LOGGER.info(
                f"Skipped download; using existing {output} ({result.existing_bytes} bytes)"
            )
        else:
            LOGGER.info(f"Wrote {output} ({result.bytes_written} bytes)")
        LOGGER.info("Metadata: %s", meta_path)
        LOGGER.info("Manifest: %s", manifest_path)
        return 0

    if args.command == "extract":
        input_path = Path(_required_str(args, "input"))
        output_raw = _optional_str(getattr(args, "output", None))
        max_games = _optional_int(getattr(args, "max_games", None))
        speed_raw = _required_str(args, "speed")
        output_path = (
            Path(output_raw)
            if output_raw
            else Path("data") / "processed" / (input_path.stem + ".ndjson")
        )
        parsed_speed = _parse_speed(speed_raw)
        stats = filter_games_with_clocks(
            input_path=input_path,
            output_path=output_path,
            max_games=max_games,
            speed=parsed_speed,
        )
        meta_path = output_path.with_suffix(output_path.suffix + ".meta.json")
        _write_extract_metadata(
            meta_path=meta_path,
            input_path=input_path,
            output_path=output_path,
            max_games=max_games,
            stats=stats,
            speed=parsed_speed.value,
        )
        LOGGER.info("Wrote %s (%s games kept)", output_path, stats.kept_games)
        LOGGER.info("Metadata: %s", meta_path)
        return 0

    if args.command == "evaluate":
        input_path = Path(_required_str(args, "input"))
        output_raw = _optional_str(getattr(args, "output", None))
        max_games = _optional_int(getattr(args, "max_games", None))
        engine_override = _optional_str(getattr(args, "engine", None))
        engine_version_override = _optional_str(getattr(args, "engine_version", None))
        depth_override = _optional_int(getattr(args, "depth", None))
        threads_override = _optional_int(getattr(args, "threads", None))
        hash_override = _optional_int(getattr(args, "hash_mb", None))
        workers = _optional_int(getattr(args, "workers", None)) or 1
        config_path = _optional_str(getattr(args, "config", None)) or "config.toml"
        output_path = (
            Path(output_raw)
            if output_raw
            else Path("data") / "processed" / f"{input_path.stem}.evals.ndjson"
        )
        config = load_engine_config(Path(config_path))
        engine = EngineConfig(
            name=engine_override or config.name,
            version=_required_str_value(
                engine_version_override or config.version, "engine_version"
            ),
            depth=depth_override if depth_override is not None else config.depth,
            threads=(
                threads_override if threads_override is not None else config.threads
            ),
            hash_mb=hash_override if hash_override is not None else config.hash_mb,
        )
        meta_path = output_path.with_suffix(output_path.suffix + ".meta.json")
        if output_path.exists() and meta_path.exists():
            meta = _load_json_dict(meta_path)
            if meta and _meta_matches_evaluation(
                meta=meta,
                input_path=input_path,
                output_path=output_path,
                max_games=max_games,
                engine=engine,
            ):
                LOGGER.info("Skipped evaluation; using existing %s", output_path)
                LOGGER.info("Metadata: %s", meta_path)
                return 0
        engine_path = resolve_engine_path(engine, Path("tools"))
        if workers > 1:
            shard_dir = (
                Path("data")
                / "processed"
                / "shards"
                / input_path.stem
                / f"workers-{workers}"
                / (f"max-{max_games}" if max_games is not None else "all")
            )
            result = evaluate_games_parallel(
                input_path=input_path,
                output_path=output_path,
                max_games=max_games,
                engine_path=engine_path,
                engine_version=engine.version,
                depth=engine.depth,
                threads=engine.threads,
                hash_mb=engine.hash_mb,
                workers=workers,
                shard_dir=shard_dir,
            )
        else:
            result = evaluate_games(
                input_path=input_path,
                output_path=output_path,
                max_games=max_games,
                engine_path=engine_path,
                engine_version=engine.version,
                depth=engine.depth,
                threads=engine.threads,
                hash_mb=engine.hash_mb,
            )
        _write_evaluation_metadata(
            meta_path=meta_path,
            input_path=input_path,
            output_path=output_path,
            max_games=max_games,
            stats=result.stats,
            engine=engine,
            engine_version=result.engine_version,
        )
        LOGGER.info(
            "Wrote %s (%s games evaluated)",
            output_path,
            result.stats.evaluated_games,
        )
        LOGGER.info("Metadata: %s", meta_path)
        return 0

    if args.command == "engine":
        engine_command = _required_str(args, "engine_command")
        if engine_command == "install":
            config_path = _optional_str(getattr(args, "config", None)) or "config.toml"
            config = load_engine_config(Path(config_path))
            if config.name != "stockfish":
                raise ValueError(f"Only stockfish is supported, got {config.name}")
            result = install_stockfish(config.version, Path("tools"))
            if result.skipped:
                LOGGER.info("Skipped download; using existing %s", result.path)
            else:
                LOGGER.info("Downloaded %s to %s", result.version, result.path)
            return 0
        raise ValueError(f"Unknown engine command: {engine_command}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
