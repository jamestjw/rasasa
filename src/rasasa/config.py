from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class EngineConfig:
    name: str
    version: str
    depth: int
    threads: int
    hash_mb: int


DEFAULT_ENGINE_CONFIG = EngineConfig(
    name="stockfish",
    version="",
    depth=16,
    threads=2,
    hash_mb=256,
)


def _require_int(value: object, name: str) -> int:
    if isinstance(value, int):
        return value
    raise ValueError(f"Invalid {name}; expected int")


def _require_str(value: object, name: str) -> str:
    if isinstance(value, str) and value:
        return value
    raise ValueError(f"Invalid {name}; expected non-empty string")


def load_engine_config(path: Path) -> EngineConfig:
    if not path.exists():
        return DEFAULT_ENGINE_CONFIG
    payload: dict[str, object] = tomllib.loads(path.read_text(encoding="utf-8"))
    engine_section = payload.get("engine")
    if not isinstance(engine_section, dict):
        return DEFAULT_ENGINE_CONFIG
    engine_data: dict[str, object] = {}
    for key in ("name", "version", "depth", "threads", "hash_mb"):
        if key in engine_section:
            engine_data[key] = engine_section[key]
    return EngineConfig(
        name=_require_str(engine_data.get("name", DEFAULT_ENGINE_CONFIG.name), "name"),
        version=_require_str(
            engine_data.get("version", DEFAULT_ENGINE_CONFIG.version), "version"
        ),
        depth=_require_int(
            engine_data.get("depth", DEFAULT_ENGINE_CONFIG.depth), "depth"
        ),
        threads=_require_int(
            engine_data.get("threads", DEFAULT_ENGINE_CONFIG.threads), "threads"
        ),
        hash_mb=_require_int(
            engine_data.get("hash_mb", DEFAULT_ENGINE_CONFIG.hash_mb), "hash_mb"
        ),
    )
