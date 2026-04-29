# Datasets

## Historical HandHQ source

The supported historical lane accepts HandHQ `.phhs` data only. `PHH_DATASET_SOURCE` may point at a local `.phhs` file, a directory of `.phhs` files, an archive, or an HTTP(S) URL that resolves to HandHQ data.

The checked-in HandHQ fixture remains available for deterministic tests and demos:

- `sample_data/historical/handhq_6max_sample.phhs`

Use the fixture for smoke tests, and use `PHH_DATASET_SOURCE` plus `PHH_CACHE_DIR` for larger HandHQ corpora.

## Commands

- `python3 -m historical_ingest.fetch_phh --source "$PHH_DATASET_SOURCE" --cache-dir "$PHH_CACHE_DIR"`
- `python3 -m historical_ingest.handhq_backfill --input "$PHH_DATASET_SOURCE" --cache-dir "$PHH_CACHE_DIR" --write`

The HandHQ backfill accepts only strict 6-max no-limit hold'em hands and rejects unsupported shapes before writing warehouse rows.
