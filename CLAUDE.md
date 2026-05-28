# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Streamlit app for inspecting Environment Agency Water Quality Archive data. Personal/work-in-progress project.

## Commands

- Run the app: `pixi run streamlit` (equivalent to `streamlit run src/app.py`)
- Install/sync env: `pixi install` (pixi is the primary env manager; `environment.yml` mirrors `pixi.toml` for conda-based deployment)

There are no tests or lint configurations in this repo.

## Code Conventions

- Stick strictly to PEP8, including 79 character line length maximum.

## Architecture

- **Entry point** — `src/app.py` wires up Streamlit multi-page navigation via `st.navigation` over files in `src/pages/`. Adding a page means adding both the file under `src/pages/` and a `st.Page(...)` entry in `app.py`.
- **Data source** — All data is read from an S3-compatible bucket (`s3://ea-water-quality/...`) using Polars with `fsspec`/`s3fs`. Credentials and `endpoint_url` come from `.streamlit/secrets.toml` under `[storage]` (gitignored). The app passes the fsspec-style spelling (`key`/`secret`/`endpoint_url`). Polars' alternative `object_store` path (rare in this app) expects `aws_access_key_id`/`aws_secret_access_key`/`aws_endpoint_url`/`aws_region='auto'` instead — relevant if you ever hit `s3fs` errors from an `IMDS` lookup.
- **Data layout in the bucket**:
  - `EA_WQA_determinands_by-sampleMaterialType.csv` — lookup of determinands (`determinand.notation`, `determinand.prefLabel`, `unit`) joined with `sampleMaterialType`. Drives the UI selectors. `get_determinands` returns both a `prefLabel (notation) → notation` map and a `notation → [sampleMaterialType]` map; the sample-material multiselect is scoped to the currently-selected determinand(s) via the latter.
  - `determinand_{notation}.parquet` — one parquet file per determinand, scanned lazily with `pl.scan_parquet`. Pages typically `concat` several of these based on user selection, then filter on `sampleMaterialType` before `.collect()`.
- **Result parsing** — the `result` column is a string. `<X` means "below the limit of detection X". Standard parse is `is_censored = result.str.starts_with('<')`, `result_value = result.str.strip_prefix('<').cast(Float64)` — so for censored rows `result_value` is the *detection limit*, not the true value. Three strategies for the censored rows live in `determinand.py`: exclude, substitute LOD/2, or `ros_impute` (lognormal regression-on-order-statistics, requires `scipy.stats`).
- **Unit canonicalisation** — `UNIT_CONVERSIONS` at the top of `determinand.py` maps recognised concentration units (mass/L, mass/kg, mass/m³ families) to `(canonical_family, factor_to_canonical, pretty_label)`. Anything not in the table passes through as its own canonical family with factor 1.0, so single-unit datasets are untouched. When a selection spans more than one canonical family the distribution panel errors out instead of plotting; when it's a single family with mixed prefixes the most common original unit is picked as the display unit and values are converted accordingly.
- **Charting** — distribution plots use `plotly.express` (`st.plotly_chart`); historic altair code has been removed.
- **Caching** — expensive bucket reads should be wrapped in `@st.cache_data` (e.g. `get_determinands` in `determinand.py`).
- **`src/generate_metadata.py`** — offline utility to pre-compute metadata over the full parquet dataset so the app doesn't have to scan everything on startup. Not invoked by the app itself.
