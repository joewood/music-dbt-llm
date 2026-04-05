import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path

from .enrich_musicbrainz import run_enrichment
from .get_max_added_at import get_max_added_at_value
from .ingest_spotify_library import run_ingestion


REPO_ROOT = Path(__file__).resolve().parents[2]


def resolve_dbt_cmd(explicit_cmd: str | None) -> str:
    if explicit_cmd:
        return explicit_cmd

    env_cmd = os.getenv("DBT_CMD", "").strip()
    if env_cmd:
        return env_cmd

    windows_fusion = Path.home() / ".local" / "bin" / "dbt.exe"
    if windows_fusion.exists():
        return str(windows_fusion)

    unix_fusion = Path.home() / ".local" / "bin" / "dbt"
    if unix_fusion.exists():
        return str(unix_fusion)

    dbtf = shutil.which("dbtf")
    if dbtf:
        return dbtf

    raise FileNotFoundError(
        "Fusion CLI not found. Set DBT_CMD, pass --dbt-cmd, install to ~/.local/bin/dbt(.exe), "
        "or add dbtf to PATH."
    )


def run_cmd(cmd: list[str], env: dict[str, str]) -> None:
    subprocess.run(cmd, check=True, cwd=REPO_ROOT, env=env)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Spotify ingest + MusicBrainz enrichment + dbt build/test in one command."
    )
    parser.add_argument("--max-tracks", type=int, default=1000)
    parser.add_argument("--max-unenriched", type=int, default=1000)
    parser.add_argument("--dbt-cmd", default=None)
    parser.add_argument("--profiles-dir", default="profiles")
    args = parser.parse_args()

    dbt_cmd = resolve_dbt_cmd(args.dbt_cmd)
    profiles_dir = Path(args.profiles_dir)
    if not profiles_dir.is_absolute():
        profiles_dir = REPO_ROOT / profiles_dir

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(profiles_dir)

    max_added_at = get_max_added_at_value()

    run_ingestion(max_tracks=args.max_tracks, stop_before_added_at=max_added_at or None)

    run_cmd(
        [
            dbt_cmd,
            "run",
            "--select",
            "stg_musicbrainz_enrichment_queue",
            "--profiles-dir",
            str(profiles_dir),
        ],
        env,
    )

    queue_args = json.dumps(
        {
            "output_path": "exports/musicbrainz/enrichment_queue.csv",
            "max_unenriched": args.max_unenriched,
        }
    )
    run_cmd(
        [
            dbt_cmd,
            "run-operation",
            "export_musicbrainz_enrichment_queue",
            "--args",
            queue_args,
            "--profiles-dir",
            str(profiles_dir),
        ],
        env,
    )

    run_enrichment(
        input_csv="exports/musicbrainz/enrichment_queue.csv",
        output_dir="exports/musicbrainz/results",
        max_unenriched=args.max_unenriched,
    )

    load_args = json.dumps({"input_dir": "exports/musicbrainz/results"})
    run_cmd(
        [
            dbt_cmd,
            "run-operation",
            "load_musicbrainz_enrichment_results",
            "--args",
            load_args,
            "--profiles-dir",
            str(profiles_dir),
        ],
        env,
    )

    run_cmd([dbt_cmd, "run", "--profiles-dir", str(profiles_dir)], env)
    run_cmd([dbt_cmd, "test", "--profiles-dir", str(profiles_dir)], env)


if __name__ == "__main__":
    main()
