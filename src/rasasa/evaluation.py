from __future__ import annotations

import json
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import chess
import chess.engine


@dataclass(frozen=True)
class EvalScore:
    cp: Optional[int]
    mate: Optional[int]

    def to_dict(self) -> dict[str, Optional[int]]:
        return {"cp": self.cp, "mate": self.mate}


@dataclass(frozen=True)
class EvaluationStats:
    total_games: int
    evaluated_games: int
    skipped_illegal_games: int
    skipped_engine_errors: int


@dataclass(frozen=True)
class EvaluationResult:
    stats: EvaluationStats
    engine_version: str


@dataclass(frozen=True)
class ShardManifest:
    input_path: Path
    output_dir: Path
    max_games: Optional[int]
    workers: int
    total_lines: int
    shard_paths: list[Path]


def _score_from_info(info: chess.engine.InfoDict, board: chess.Board) -> EvalScore:
    score = info.get("score")
    if score is None:
        return EvalScore(cp=None, mate=None)
    pov_score = score.pov(board.turn)
    return EvalScore(cp=pov_score.score(mate_score=None), mate=pov_score.mate())


def _configure_engine(
    engine: chess.engine.SimpleEngine, threads: int, hash_mb: int
) -> None:
    options: dict[str, int] = {}
    if threads > 0:
        options["Threads"] = threads
    if hash_mb > 0:
        options["Hash"] = hash_mb
    if options:
        engine.configure(options)


def evaluate_games(
    *,
    input_path: Path,
    output_path: Path,
    max_games: Optional[int],
    engine_path: str,
    engine_version: str,
    depth: int,
    threads: int,
    hash_mb: int,
) -> EvaluationResult:
    total_games = 0
    evaluated_games = 0
    skipped_illegal_games = 0
    skipped_engine_errors = 0
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with input_path.open("r", encoding="utf-8") as in_handle:
        with output_path.open("w", encoding="utf-8") as out_handle:
            with chess.engine.SimpleEngine.popen_uci(engine_path) as engine:
                _configure_engine(engine, threads=threads, hash_mb=hash_mb)
                resolved_version = engine_version
                for line in in_handle:
                    total_games += 1
                    payload = json.loads(line)
                    headers = payload["headers"]
                    moves = payload["moves"]
                    clocks = payload["clocks"]

                    board = chess.Board()
                    evals: list[dict[str, Optional[int]]] = []
                    illegal = False
                    engine_failed = False
                    for move in moves:
                        try:
                            info = engine.analyse(
                                board, chess.engine.Limit(depth=depth)
                            )
                        except chess.engine.EngineError:
                            engine_failed = True
                            break
                        evals.append(_score_from_info(info, board).to_dict())
                        try:
                            board.push_uci(move)
                        except ValueError:
                            illegal = True
                            break

                    if engine_failed:
                        skipped_engine_errors += 1
                    elif illegal:
                        skipped_illegal_games += 1
                    else:
                        output_record = {
                            "headers": headers,
                            "moves": moves,
                            "clocks": clocks,
                            "evals": evals,
                            "engine": {
                                "path": engine_path,
                                "version": resolved_version,
                                "depth": depth,
                                "threads": threads,
                                "hash_mb": hash_mb,
                            },
                        }
                        out_handle.write(json.dumps(output_record))
                        out_handle.write("\n")
                        evaluated_games += 1

                    if max_games is not None and total_games >= max_games:
                        break

    stats = EvaluationStats(
        total_games=total_games,
        evaluated_games=evaluated_games,
        skipped_illegal_games=skipped_illegal_games,
        skipped_engine_errors=skipped_engine_errors,
    )
    return EvaluationResult(stats=stats, engine_version=resolved_version)


def _write_part_meta(path: Path, stats: EvaluationStats) -> None:
    lines = [
        f"total_games={stats.total_games}",
        f"evaluated_games={stats.evaluated_games}",
        f"skipped_illegal_games={stats.skipped_illegal_games}",
        f"skipped_engine_errors={stats.skipped_engine_errors}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _read_part_meta(path: Path) -> Optional[EvaluationStats]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    values: dict[str, int] = {}
    for line in lines:
        if "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        if not raw_value:
            continue
        try:
            values[key] = int(raw_value)
        except ValueError:
            return None
    required = {
        "total_games",
        "evaluated_games",
        "skipped_illegal_games",
        "skipped_engine_errors",
    }
    if not required.issubset(values):
        return None
    return EvaluationStats(
        total_games=values["total_games"],
        evaluated_games=values["evaluated_games"],
        skipped_illegal_games=values["skipped_illegal_games"],
        skipped_engine_errors=values["skipped_engine_errors"],
    )


def _write_manifest(path: Path, manifest: ShardManifest) -> None:
    lines = [
        f"input_path={manifest.input_path}",
        f"output_dir={manifest.output_dir}",
        f"max_games={manifest.max_games if manifest.max_games is not None else ''}",
        f"workers={manifest.workers}",
        f"total_lines={manifest.total_lines}",
    ]
    for shard_path in manifest.shard_paths:
        lines.append(f"shard_path={shard_path}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _load_manifest(path: Path) -> Optional[ShardManifest]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    values: dict[str, str] = {}
    shard_paths: list[Path] = []
    for line in lines:
        if "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        if key == "shard_path" and raw_value:
            shard_paths.append(Path(raw_value))
            continue
        values[key] = raw_value
    input_raw = values.get("input_path")
    output_raw = values.get("output_dir")
    workers_raw = values.get("workers")
    total_raw = values.get("total_lines")
    max_raw = values.get("max_games")
    if not input_raw or not output_raw or not workers_raw or not total_raw:
        return None
    try:
        workers = int(workers_raw)
        total_lines = int(total_raw)
    except ValueError:
        return None
    max_games: Optional[int]
    if max_raw == "":
        max_games = None
    elif max_raw is None:
        max_games = None
    else:
        try:
            max_games = int(max_raw)
        except ValueError:
            return None
    if not shard_paths:
        return None
    return ShardManifest(
        input_path=Path(input_raw),
        output_dir=Path(output_raw),
        max_games=max_games,
        workers=workers,
        total_lines=total_lines,
        shard_paths=shard_paths,
    )


def _prepare_shards(
    *,
    input_path: Path,
    shard_dir: Path,
    workers: int,
    max_games: Optional[int],
) -> ShardManifest:
    shard_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = shard_dir / "manifest.txt"
    if manifest_path.exists():
        manifest = _load_manifest(manifest_path)
        if (
            manifest
            and manifest.input_path == input_path
            and manifest.output_dir == shard_dir
            and manifest.max_games == max_games
            and manifest.workers == workers
            and all(path.exists() for path in manifest.shard_paths)
        ):
            return manifest

    shard_paths = [shard_dir / f"shard-{idx:03d}.ndjson" for idx in range(workers)]
    handles = [path.open("w", encoding="utf-8") for path in shard_paths]
    total_lines = 0
    try:
        with input_path.open("r", encoding="utf-8") as in_handle:
            for line in in_handle:
                handles[total_lines % workers].write(line)
                total_lines += 1
                if max_games is not None and total_lines >= max_games:
                    break
    finally:
        for handle in handles:
            handle.close()

    manifest = ShardManifest(
        input_path=input_path,
        output_dir=shard_dir,
        max_games=max_games,
        workers=workers,
        total_lines=total_lines,
        shard_paths=shard_paths,
    )
    _write_manifest(manifest_path, manifest)
    return manifest


def _evaluate_shard(
    input_path: Path,
    output_path: Path,
    meta_path: Path,
    engine_path: str,
    engine_version: str,
    depth: int,
    threads: int,
    hash_mb: int,
) -> EvaluationStats:
    result = evaluate_games(
        input_path=input_path,
        output_path=output_path,
        max_games=None,
        engine_path=engine_path,
        engine_version=engine_version,
        depth=depth,
        threads=threads,
        hash_mb=hash_mb,
    )
    _write_part_meta(meta_path, result.stats)
    return result.stats


def _merge_parts(output_path: Path, part_paths: list[Path]) -> None:
    with output_path.open("w", encoding="utf-8") as out_handle:
        for part_path in part_paths:
            with part_path.open("r", encoding="utf-8") as in_handle:
                for line in in_handle:
                    out_handle.write(line)


def evaluate_games_parallel(
    *,
    input_path: Path,
    output_path: Path,
    max_games: Optional[int],
    engine_path: str,
    engine_version: str,
    depth: int,
    threads: int,
    hash_mb: int,
    workers: int,
    shard_dir: Path,
) -> EvaluationResult:
    manifest = _prepare_shards(
        input_path=input_path,
        shard_dir=shard_dir,
        workers=workers,
        max_games=max_games,
    )
    part_paths = [
        output_path.with_name(f"{output_path.stem}.part-{idx:03d}{output_path.suffix}")
        for idx in range(workers)
    ]
    part_meta_paths = [
        path.with_suffix(path.suffix + ".meta.txt") for path in part_paths
    ]
    stats_list: list[EvaluationStats] = []
    futures: list[Future[EvaluationStats]] = []
    with ProcessPoolExecutor(max_workers=workers) as executor:
        for shard_path, part_path, meta_path in zip(
            manifest.shard_paths, part_paths, part_meta_paths
        ):
            if part_path.exists() and meta_path.exists():
                meta_stats = _read_part_meta(meta_path)
                if meta_stats is not None:
                    stats_list.append(meta_stats)
                    continue
            futures.append(
                executor.submit(
                    _evaluate_shard,
                    shard_path,
                    part_path,
                    meta_path,
                    engine_path,
                    engine_version,
                    depth,
                    threads,
                    hash_mb,
                )
            )
        for future in as_completed(futures):
            stats_list.append(future.result())

    total = sum(item.total_games for item in stats_list)
    evaluated = sum(item.evaluated_games for item in stats_list)
    illegal = sum(item.skipped_illegal_games for item in stats_list)
    engine_errors = sum(item.skipped_engine_errors for item in stats_list)
    stats = EvaluationStats(
        total_games=total,
        evaluated_games=evaluated,
        skipped_illegal_games=illegal,
        skipped_engine_errors=engine_errors,
    )

    if not output_path.exists():
        _merge_parts(output_path, part_paths)

    return EvaluationResult(stats=stats, engine_version=engine_version)
