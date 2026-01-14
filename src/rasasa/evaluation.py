from __future__ import annotations

import json
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
