import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from .config import (
    KB_PIPELINE_DIR, KB_PIPELINE_PYTHON, KB_INPUT_EXPECTED, KB_OUTPUT_DIR,
    DEFAULT_BATCH_SIZE, DEFAULT_MIN_COUNT,
    RUN2_DIR, RUN3_DIR, MERGED_DIR, CLEAN_DIR, RATED_DIR, FINAL_DIR, PROD_DIR, RETRIEVAL_DIR,
    REMOTE_RU_HOST, REMOTE_RU_USER, REMOTE_RU_PATH, REMOTE_RU_SERVICE
)

def latest_json_in_dir(path: Path):
    if not path.exists():
        return None
    files = sorted(path.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return str(files[0]) if files else None

def run_command(cmd, cwd, log, env=None):
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env or os.environ.copy()
    )
    for line in proc.stdout:
        log(line.rstrip())
    proc.wait()
    if proc.returncode != 0:
        raise RuntimeError(f"Команда завершилась с кодом {proc.returncode}: {' '.join(cmd)}")

def stage_prepare_input(run, stage_progress, log):
    source = Path(run["source_txt"]).expanduser().resolve()
    if not source.exists():
        raise FileNotFoundError(f"Не найден исходный TXT: {source}")

    KB_INPUT_EXPECTED.parent.mkdir(parents=True, exist_ok=True)
    stage_progress(0.2)
    shutil.copy2(source, KB_INPUT_EXPECTED)
    log(f"[prepare_input] copied: {source} -> {KB_INPUT_EXPECTED}")
    stage_progress(1.0)
    return {"copied_to": str(KB_INPUT_EXPECTED)}

def stage_base_pipeline(run, stage_progress, log):
    stage_progress(0.05)
    run1 = KB_OUTPUT_DIR / "run1"
    run1.mkdir(parents=True, exist_ok=True)
    run_command(
        [
            str(KB_PIPELINE_PYTHON),
            "run_pipeline.py",
            "--input", str(KB_INPUT_EXPECTED),
            "--output", str(run1),
        ],
        KB_PIPELINE_DIR,
        log
    )
    stage_progress(1.0)
    return {
        "run1_dir": str(run1),
        "summary_guess": latest_json_in_dir(run1)
    }

def stage_top200(run, stage_progress, log):
    stage_progress(0.05)
    run_command([str(KB_PIPELINE_PYTHON), "rebuild_top_clusters.py"], KB_PIPELINE_DIR, log)
    stage_progress(1.0)
    return {"summary": str(RUN2_DIR / "summary_top200.json")}

def stage_remaining(run, stage_progress, log):
    batch_size = int(run["batch_size"] or DEFAULT_BATCH_SIZE)
    min_count = int(run["min_count"] or DEFAULT_MIN_COUNT)

    done = False
    loop_idx = 0
    last_summary = {}

    while not done:
        loop_idx += 1
        log(f"[remaining] loop={loop_idx} batch_size={batch_size} min_count={min_count}")
        run_command(
            [str(KB_PIPELINE_PYTHON), "process_remaining_clusters.py", "--batch-size", str(batch_size), "--min-count", str(min_count)],
            KB_PIPELINE_DIR,
            log
        )

        summary_path = RUN3_DIR / "summary_remaining.json"
        if not summary_path.exists():
            raise RuntimeError("Не найден summary_remaining.json после process_remaining_clusters.py")

        last_summary = json.loads(summary_path.read_text(encoding="utf-8"))
        all_clusters = int(last_summary.get("all_clusters", 0))
        already_done_before_run = int(last_summary.get("already_done_before_run", 0))
        processed_in_this_run = int(last_summary.get("processed_in_this_run", 0))
        skipped_in_this_run = int(last_summary.get("skipped_in_this_run", 0))
        remaining_before_run = int(last_summary.get("remaining_before_run", 0))

        total_done = already_done_before_run + processed_in_this_run + skipped_in_this_run
        progress = (total_done / all_clusters) if all_clusters else 1.0
        stage_progress(min(progress, 0.999 if remaining_before_run > 0 else 1.0))

        log(f"[remaining] summary={json.dumps(last_summary, ensure_ascii=False)}")

        if remaining_before_run <= 0:
            done = True
        elif processed_in_this_run + skipped_in_this_run == 0:
            done = True

    stage_progress(1.0)
    return last_summary

def stage_merge(run, stage_progress, log):
    stage_progress(0.05)
    run_command([str(KB_PIPELINE_PYTHON), "merge_kb_results.py"], KB_PIPELINE_DIR, log)
    stage_progress(1.0)
    return {"summary": str(MERGED_DIR / "summary_merged.json")}

def stage_clean_v2(run, stage_progress, log):
    stage_progress(0.05)
    run_command([str(KB_PIPELINE_PYTHON), "clean_merged_kb_v2.py"], KB_PIPELINE_DIR, log)
    stage_progress(1.0)
    return {"summary": str((KB_OUTPUT_DIR / "kb_clean_v2" / "summary_clean_v2.json"))}

def stage_rate(run, stage_progress, log):
    stage_progress(0.05)
    run_command([str(KB_PIPELINE_PYTHON), "rate_cards.py"], KB_PIPELINE_DIR, log)
    stage_progress(1.0)
    return {"summary": str((KB_OUTPUT_DIR / "kb_rated" / "summary_rated.json"))}

def stage_categorize(run, stage_progress, log):
    stage_progress(0.05)
    run_command([str(KB_PIPELINE_PYTHON), "categorize_good_cards.py"], KB_PIPELINE_DIR, log)
    stage_progress(1.0)
    return {"summary": str((KB_OUTPUT_DIR / "kb_final" / "summary_final.json"))}

def stage_finalize(run, stage_progress, log):
    stage_progress(0.05)
    run_command([str(KB_PIPELINE_PYTHON), "finalize_good_cards.py"], KB_PIPELINE_DIR, log)
    stage_progress(1.0)
    return {"summary": str((KB_OUTPUT_DIR / "kb_prod" / "summary_prod.json"))}

def stage_retrieval_pack(run, stage_progress, log):
    stage_progress(0.05)
    run_command([str(KB_PIPELINE_PYTHON), "build_retrieval_pack.py"], KB_PIPELINE_DIR, log)
    stage_progress(1.0)
    return {"summary": str((KB_OUTPUT_DIR / "kb_retrieval" / "summary_retrieval.json"))}

def stage_deploy_ru(run, stage_progress, log):
    if not run["deploy_enabled"]:
        return {"skipped": True, "reason": "deploy_disabled"}

    if not REMOTE_RU_HOST:
        raise RuntimeError("Не задан KB_REMOTE_RU_HOST")

    src = RETRIEVAL_DIR / "knowledge_retrieval.jsonl"
    if not src.exists():
        raise FileNotFoundError(f"Не найден retrieval-файл: {src}")

    stage_progress(0.2)
    scp_target = f"{REMOTE_RU_USER}@{REMOTE_RU_HOST}:{REMOTE_RU_PATH}"
    run_command(["scp", str(src), scp_target], KB_PIPELINE_DIR, log)

    stage_progress(0.6)
    remote_cmd = f"systemctl restart {REMOTE_RU_SERVICE} && sleep 2 && curl -s http://127.0.0.1:8787/health"
    run_command(["ssh", f"{REMOTE_RU_USER}@{REMOTE_RU_HOST}", remote_cmd], KB_PIPELINE_DIR, log)

    stage_progress(1.0)
    return {"deployed_to": scp_target, "service": REMOTE_RU_SERVICE}

STAGE_FUNCS = {
    "prepare_input": stage_prepare_input,
    "base_pipeline": stage_base_pipeline,
    "top200": stage_top200,
    "remaining": stage_remaining,
    "merge": stage_merge,
    "clean_v2": stage_clean_v2,
    "rate": stage_rate,
    "categorize": stage_categorize,
    "finalize": stage_finalize,
    "retrieval_pack": stage_retrieval_pack,
    "deploy_ru": stage_deploy_ru,
}
