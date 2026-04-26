import argparse
import json
import os
import sys
from pathlib import Path
from .config import STAGES
from .state import (
    ensure_db, get_run, get_stage_rows, set_run_fields, set_stage, refresh_run_progress, append_log
)
from .stages import STAGE_FUNCS

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    ensure_db()
    run = get_run(args.run_id)
    if not run:
        print(f"Run not found: {args.run_id}", file=sys.stderr)
        sys.exit(1)

    set_run_fields(args.run_id, status="running", pid=os.getpid())

    def log(msg: str):
        append_log(args.run_id, msg)

    log(f"=== RUN START {args.run_id} ===")
    log(json.dumps({"run": run}, ensure_ascii=False))

    for stage in STAGES:
        stage_key = stage["key"]
        stage_rows = {r["stage_key"]: r for r in get_stage_rows(args.run_id)}
        row = stage_rows[stage_key]

        if stage_key == "deploy_ru" and not run["deploy_enabled"]:
            set_stage(args.run_id, stage_key, status="skipped", progress=1.0, started_at="", finished_at="", meta_json='{"skipped": true}')
            refresh_run_progress(args.run_id)
            continue

        if row["status"] in ("done", "skipped"):
            continue

        def stage_progress(p: float):
            set_stage(args.run_id, stage_key, status="running", progress=float(p))
            set_run_fields(args.run_id, current_stage=stage_key, status="running")
            refresh_run_progress(args.run_id)

        try:
            log(f"--- STAGE START: {stage_key} ---")
            set_stage(args.run_id, stage_key, status="running", progress=0.0, started_at=__import__("datetime").datetime.now().isoformat(timespec="seconds"), error_text="")
            refresh_run_progress(args.run_id)

            result = STAGE_FUNCS[stage_key](run, stage_progress, log)

            set_stage(
                args.run_id,
                stage_key,
                status="done",
                progress=1.0,
                finished_at=__import__("datetime").datetime.now().isoformat(timespec="seconds"),
                meta_json=json.dumps(result, ensure_ascii=False)
            )
            refresh_run_progress(args.run_id)
            log(f"--- STAGE DONE: {stage_key} ---")
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            set_stage(
                args.run_id,
                stage_key,
                status="failed",
                error_text=err,
                finished_at=__import__("datetime").datetime.now().isoformat(timespec="seconds")
            )
            set_run_fields(args.run_id, status="failed", current_stage=stage_key, last_error=err)
            refresh_run_progress(args.run_id)
            log(f"!!! STAGE FAILED: {stage_key} -> {err}")
            sys.exit(1)

    set_run_fields(args.run_id, status="done", current_stage="", last_error="")
    refresh_run_progress(args.run_id)
    log(f"=== RUN DONE {args.run_id} ===")

if __name__ == "__main__":
    main()
