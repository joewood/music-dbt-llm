# Music DBT Spec

This directory captures the agreed implementation and decisions from the build-out of this repository.

## Contents

- `spec/01-project-scope.md`
  - Goals, non-goals, and constraints.
- `spec/02-architecture-and-flow.md`
  - End-to-end architecture and separation of concerns.
- `spec/03-data-contracts.md`
  - Raw tables, staging/marts, and CSV contracts between dbt and enrichment scripts.
- `spec/04-runbook-and-commands.md`
  - Operational commands for local runs and full pipeline execution.
- `spec/05-decisions-and-troubleshooting.md`
  - Key decisions, fixes, and known operational notes.

## Current design summary

- Spotify ingestion script writes saved library data to DuckDB `raw.*`.
- dbt builds queue model for MusicBrainz enrichment candidates.
- dbt exports queue to CSV (`exports/musicbrainz/enrichment_queue.csv`).
- MusicBrainz enrichment script reads CSV and writes CSV outputs only.
- Loader script imports enrichment output CSV files into DuckDB `raw.*` MusicBrainz tables.
- dbt staging/marts consume those raw tables for playlist-ready analytics.
