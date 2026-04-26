import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from .config import DB_PATH, RUNTIME_DIR, RUNS_DIR, STAGES

RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
RUNS_DIR.mkdir(parents=True, exist_ok=True)

def now_iso():
    return datetime.now().isoformat(timespec="seconds")

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS runs (
        run_id TEXT PRIMARY KEY,
        run_name TEXT,
        source_txt TEXT,
        deploy_enabled INTEGER DEFAULT 0,
        batch_size INTEGER DEFAULT 300,
        min_count INTEGER DEFAULT 5,
        status TEXT,
        current_stage TEXT,
        progress REAL DEFAULT 0,
        pid INTEGER,
        log_path TEXT,
        created_at TEXT,
        updated_at TEXT,
        last_error TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS stage_runs (
        run_id TEXT,
        stage_key TEXT,
        title TEXT,
        weight REAL,
        status TEXT,
        progress REAL DEFAULT 0,
        started_at TEXT,
        finished_at TEXT,
        meta_json TEXT,
        error_text TEXT,
        PRIMARY KEY (run_id, stage_key)
    )
    """)

    conn.commit()
    conn.close()

def create_run(run_name: str, source_txt: str, deploy_enabled: bool, batch_size: int, min_count: int):
    ensure_db()
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
    log_path = str((RUNS_DIR / f"{run_id}.log").resolve())

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO runs (run_id, run_name, source_txt, deploy_enabled, batch_size, min_count, status, current_stage, progress, pid, log_path, created_at, updated_at, last_error)
    VALUES (?, ?, ?, ?, ?, ?, 'pending', '', 0, NULL, ?, ?, ?, '')
    """, (run_id, run_name, source_txt, int(deploy_enabled), batch_size, min_count, log_path, now_iso(), now_iso()))

    for stage in STAGES:
        cur.execute("""
        INSERT INTO stage_runs (run_id, stage_key, title, weight, status, progress, started_at, finished_at, meta_json, error_text)
        VALUES (?, ?, ?, ?, 'pending', 0, '', '', '', '')
        """, (run_id, stage["key"], stage["title"], stage["weight"]))

    conn.commit()
    conn.close()
    Path(log_path).write_text("", encoding="utf-8")
    return run_id, log_path

def set_run_fields(run_id: str, **fields):
    if not fields:
        return
    fields["updated_at"] = now_iso()
    keys = list(fields.keys())
    vals = [fields[k] for k in keys]
    sql = f"UPDATE runs SET {', '.join([f'{k}=?' for k in keys])} WHERE run_id=?"
    conn = get_conn()
    conn.execute(sql, vals + [run_id])
    conn.commit()
    conn.close()

def set_stage(run_id: str, stage_key: str, **fields):
    if not fields:
        return
    keys = list(fields.keys())
    vals = [fields[k] for k in keys]
    sql = f"UPDATE stage_runs SET {', '.join([f'{k}=?' for k in keys])} WHERE run_id=? AND stage_key=?"
    conn = get_conn()
    conn.execute(sql, vals + [run_id, stage_key])
    conn.commit()
    conn.close()

def get_run(run_id: str):
    ensure_db()
    conn = get_conn()
    row = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
    conn.close()
    return dict(row) if row else None

def get_stage_rows(run_id: str):
    ensure_db()
    conn = get_conn()
    rows = conn.execute("SELECT * FROM stage_runs WHERE run_id=? ORDER BY rowid", (run_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def list_runs(limit: int = 50):
    ensure_db()
    conn = get_conn()
    rows = conn.execute("SELECT * FROM runs ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def calc_overall_progress(run_id: str) -> float:
    rows = get_stage_rows(run_id)
    if not rows:
        return 0.0
    total_weight = sum(float(r["weight"]) for r in rows)
    acc = 0.0
    for r in rows:
        status = r["status"]
        weight = float(r["weight"])
        progress = float(r["progress"] or 0)
        if status == "done":
            acc += weight
        elif status == "skipped":
            acc += weight
        elif status == "running":
            acc += weight * max(0.0, min(progress, 1.0))
    return round((acc / total_weight) * 100, 2) if total_weight else 0.0

def refresh_run_progress(run_id: str):
    stages = get_stage_rows(run_id)
    progress = calc_overall_progress(run_id)
    current_stage = ""
    status = "pending"

    if any(s["status"] == "failed" for s in stages):
        status = "failed"
    elif all(s["status"] in ("done", "skipped") for s in stages):
        status = "done"
    elif any(s["status"] == "running" for s in stages):
        status = "running"
    elif any(s["status"] == "done" for s in stages):
        status = "running"

    for s in stages:
        if s["status"] == "running":
            current_stage = s["stage_key"]
            break

    set_run_fields(run_id, progress=progress, current_stage=current_stage, status=status)

def append_log(run_id: str, text: str):
    run = get_run(run_id)
    if not run:
        return
    log_path = Path(run["log_path"])
    with log_path.open("a", encoding="utf-8") as f:
        f.write(text.rstrip() + "\n")
