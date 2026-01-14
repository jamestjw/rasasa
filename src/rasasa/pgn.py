from __future__ import annotations

import io
import json
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Iterator, Optional, TextIO

import chess.pgn
import zstandard

CLOCK_RE = re.compile(r"%clk\s+([0-9:.]+)")


class Speed(Enum):
    BULLET = "bullet"
    BLITZ = "blitz"
    RAPID = "rapid"
    CLASSICAL = "classical"


@dataclass(frozen=True)
class FilterStats:
    total_games: int
    kept_games: int
    skipped_missing_clocks: int
    skipped_non_matching_speed: int


def parse_clock_value(value: str) -> Optional[float]:
    parts = value.split(":")
    if not parts:
        return None
    try:
        if len(parts) == 3:
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = float(parts[2])
        elif len(parts) == 2:
            hours = 0
            minutes = int(parts[0])
            seconds = float(parts[1])
        elif len(parts) == 1:
            hours = 0
            minutes = 0
            seconds = float(parts[0])
        else:
            return None
    except ValueError:
        return None
    return hours * 3600 + minutes * 60 + seconds


def _extract_clocks(game: chess.pgn.Game) -> Optional[list[float]]:
    clocks: list[float] = []
    for node in game.mainline():
        match = CLOCK_RE.search(node.comment)
        if not match:
            return None
        parsed = parse_clock_value(match.group(1))
        if parsed is None:
            return None
        clocks.append(parsed)
    return clocks


def _game_to_record(game: chess.pgn.Game, clocks: list[float]) -> dict[str, object]:
    headers = dict(game.headers)
    moves = [move.uci() for move in game.mainline_moves()]
    return {
        "headers": headers,
        "moves": moves,
        "clocks": clocks,
    }


def _estimated_duration_seconds(time_control: str) -> Optional[float]:
    if time_control in {"-", ""}:
        return None
    if "+" not in time_control:
        return None
    base_str, inc_str = time_control.split("+", 1)
    try:
        base = int(base_str)
        increment = int(inc_str)
    except ValueError:
        return None
    return base + 40 * increment


def _speed_from_headers(headers: dict[str, str]) -> Optional[Speed]:
    event = headers.get("Event", "")
    if "Bullet" in event:
        return Speed.BULLET
    if "Blitz" in event:
        return Speed.BLITZ
    if "Rapid" in event:
        return Speed.RAPID
    if "Classical" in event:
        return Speed.CLASSICAL
    duration = _estimated_duration_seconds(headers.get("TimeControl", ""))
    if duration is None:
        return None
    if duration < 180:
        return Speed.BULLET
    if duration < 480:
        return Speed.BLITZ
    if duration < 1500:
        return Speed.RAPID
    return Speed.CLASSICAL


def _iter_games(handle: TextIO) -> Iterator[chess.pgn.Game]:
    while True:
        game = chess.pgn.read_game(handle)
        if game is None:
            break
        yield game


def iter_games_from_zst(path: Path) -> Iterator[chess.pgn.Game]:
    with path.open("rb") as raw_handle:
        decompressor = zstandard.ZstdDecompressor()
        with decompressor.stream_reader(raw_handle) as reader:
            with io.TextIOWrapper(reader, encoding="utf-8") as text_stream:
                for game in _iter_games(text_stream):
                    yield game


def filter_games_with_clocks(
    input_path: Path,
    output_path: Path,
    max_games: Optional[int],
    speed: Speed,
) -> FilterStats:
    total_games = 0
    kept_games = 0
    skipped_missing_clocks = 0
    skipped_non_matching_speed = 0
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as out_handle:
        for game in iter_games_from_zst(input_path):
            total_games += 1
            headers = dict(game.headers)
            detected_speed = _speed_from_headers(headers)
            if detected_speed != speed:
                skipped_non_matching_speed += 1
                if max_games is not None and total_games >= max_games:
                    break
                continue
            clocks = _extract_clocks(game)
            if clocks is None:
                skipped_missing_clocks += 1
            else:
                record = _game_to_record(game, clocks)
                out_handle.write(json.dumps(record))
                out_handle.write("\n")
                kept_games += 1
            if max_games is not None and total_games >= max_games:
                break

    return FilterStats(
        total_games=total_games,
        kept_games=kept_games,
        skipped_missing_clocks=skipped_missing_clocks,
        skipped_non_matching_speed=skipped_non_matching_speed,
    )
