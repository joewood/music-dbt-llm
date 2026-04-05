# 05. Decisions And Troubleshooting

## Key decisions captured

1. Use DuckDB + dbt for local analytical modeling.
2. Use Spotify ISRC as primary bridge key into MusicBrainz.
3. Centralize Spotify auth callback logic in `scripts/spotify_auth.py`.
4. Ensure raw tables exist via explicit dbt `run-operation init_raw_spotify_tables` bootstrap.
5. Decouple enrichment fetcher from database concerns:
   - dbt exports queue CSV.
   - enrichment script is file-based.
  - dbt run-operation macro handles DB writes.

## Notable issues fixed

### dbt build failed due to missing raw source tables

- Symptom: source tables missing in `raw` schema.
- Fix: `macros/init_raw_tables.sql` and `on-run-start` hook in `dbt_project.yml`.

### dbt extension SQL analyzer LSP ENOENT

- Symptom: `dbt-lsp.exe` missing, spawn ENOENT.
- Root cause: failed extension binary download and stale version.
- Fix:
  - upgrade extension to `dbtlabsinc.dbt@0.51.4`
  - clear `globalStorage/dbtlabsinc.dbt`
  - reload window.

### PowerShell/CLI reliability on Windows

- `code.exe` path invocation may not support extension CLI flags reliably.
- Use `code.cmd` from VS Code `bin` for extension install/remove/version commands.

## Current known behavior

- MusicBrainz API may return 404 for some ISRCs; script handles this and continues.
- Enrichment outputs still generate CSVs even when row counts are zero.
- Loader run-operation safely skips missing CSV files and handles zero-row files.

## Suggested next improvements

1. Add confidence scoring for ISRC candidate disambiguation.
2. Add explicit monitoring for queue export emptiness and stale queues.
3. Keep the Python orchestrator as the single cross-platform entry point.
4. Add incremental state tracking for MusicBrainz retries across runs.
