# Project Agent Instructions

## Goal
Download bullet games from Lichess and analyze how move accuracy changes as time pressure evolves.

## Tech Stack
- Language: Python
- Tooling: `uv` for environment/deps and a `pyproject.toml`
- Formatting: `black` required
- Type checking: `pyright` required

## Coding Standards
- All function parameters must be type annotated.
- Fix all type errors reported by `pyright`.
- Run `pyright` after every change and address any issues.
- Run `isort` and then `black` after every change.

## Data Source
- Lichess monthly database dumps (rate limits and ToS must be respected).

## Notes
- Prefer small, composable modules with clear data models.
