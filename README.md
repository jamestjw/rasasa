# rasasa

Analyze Lichess bullet game accuracy vs time pressure using Lichess monthly database dumps.

## Accuracy definition (planned)
- Use Stockfish to evaluate each position in centipawns.
- Convert centipawns to win probability with a logistic curve.
- Define move accuracy from win-probability drop (or centipawn loss vs best move).

## Data handling
- Omit games that lack complete move clock data.
- Treat Lichess `%clk` values as time remaining after a move; analysis should convert to time at turn start.
- Config parsing is strict; invalid types or empty required values will raise errors.

## Data source
- Monthly standard variant dumps from https://database.lichess.org, filtered to bullet by speed.

## Download (planned workflow)
```bash
uv run python -m rasasa.cli dump --year 2024 --month 1 --variant standard
```

## Extract clocked games (planned workflow)
```bash
uv run python -m rasasa.cli extract --input data/raw/lichess_db_standard_rated_2024-01.pgn.zst --speed bullet
```

## Evaluate games (planned workflow)
```bash
uv run python -m rasasa.cli evaluate --input data/processed/lichess_db_standard_rated_2024-01.pgn.ndjson
```

## Install Stockfish (planned workflow)
```bash
uv run python -m rasasa.cli engine install
```
