import subprocess
import sys
from pathlib import Path
import streamlit as st

from app.config import BASE_DIR, KB_PIPELINE_DIR
from app.state import ensure_db, create_run, list_runs, get_stage_rows

ensure_db()

UPLOADS_DIR = BASE_DIR / "uploads"
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

st.set_page_config(page_title="KB Orchestrator", layout="wide")
st.title("KB Orchestrator")

def discover_txt_files():
    candidates = []
    roots = [
        KB_PIPELINE_DIR / "data" / "input",
        KB_PIPELINE_DIR / "exports",
        Path.home() / "Downloads",
        Path.home() / "Documents",
    ]
    for root in roots:
        if not root.exists():
            continue
        for p in root.glob("*.txt"):
            candidates.append(str(p.resolve()))
    return sorted(set(candidates))

def launch_run(run_id: str):
    subprocess.Popen(
        [sys.executable, "-m", "app.runner", "--run-id", run_id],
        cwd=str(BASE_DIR),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )

def save_uploaded_file(uploaded_file) -> str:
    target = UPLOADS_DIR / uploaded_file.name
    target.write_bytes(uploaded_file.getbuffer())
    return str(target.resolve())

with st.sidebar:
    st.header("Новый запуск")

    uploaded = st.file_uploader("Загрузить TXT", type=["txt"])

    st.caption("Или выбрать уже существующий файл")
    discovered = discover_txt_files()
    selected = st.selectbox("Выбери TXT из найденных", [""] + discovered)

    manual_path = st.text_input("Или вставь путь вручную", value="")
    run_name = st.text_input("Имя запуска", value="snapshot_run")
    deploy_enabled = st.checkbox("После сборки задеплоить на RU", value=False)
    batch_size = st.number_input("Batch size remaining", min_value=50, max_value=1000, value=300, step=50)
    min_count = st.number_input("Min cluster count", min_value=1, max_value=100, value=5, step=1)

    chosen_path = ""
    source_label = ""

    if uploaded is not None:
        chosen_path = save_uploaded_file(uploaded)
        source_label = f"Загруженный файл: {chosen_path}"
    elif manual_path.strip():
        chosen_path = manual_path.strip()
        source_label = f"Ручной путь: {chosen_path}"
    elif selected:
        chosen_path = selected
        source_label = f"Выбранный найденный файл: {chosen_path}"

    if source_label:
        st.success(source_label)

    if st.button("Старт"):
        if not chosen_path:
            st.error("Загрузи TXT, выбери найденный файл или вставь путь вручную")
        else:
            run_id, _ = create_run(
                run_name=run_name.strip() or "snapshot_run",
                source_txt=chosen_path,
                deploy_enabled=deploy_enabled,
                batch_size=int(batch_size),
                min_count=int(min_count),
            )
            launch_run(run_id)
            st.success(f"Запуск создан: {run_id}")

    st.divider()
    st.caption("После падения можно просто нажать Resume у нужного run")

st.subheader("История запусков")
runs = list_runs(50)

if not runs:
    st.info("Пока нет запусков")
    st.stop()

run_options = [f"{r['run_id']} | {r['status']} | {r['run_name']}" for r in runs]
selected_idx = st.selectbox("Выбери run", range(len(run_options)), format_func=lambda i: run_options[i])
run = runs[selected_idx]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Run ID", run["run_id"])
col2.metric("Статус", run["status"])
col3.metric("Прогресс", f"{run['progress']}%")
col4.metric("Current stage", run["current_stage"] or "-")

c1, c2 = st.columns([1, 1])
with c1:
    if st.button("Resume выбранный run"):
        launch_run(run["run_id"])
        st.success("Resume запущен")
with c2:
    st.button("Обновить")

st.write(f"Источник: `{run['source_txt']}`")
st.write(f"Лог: `{run['log_path']}`")
if run.get("last_error"):
    st.error(run["last_error"])

st.subheader("Этапы")
stages = get_stage_rows(run["run_id"])
for s in stages:
    with st.expander(f"{s['stage_key']} | {s['status']} | {round((s['progress'] or 0)*100,1)}%"):
        st.write(f"Title: {s['title']}")
        st.write(f"Started: {s['started_at'] or '-'}")
        st.write(f"Finished: {s['finished_at'] or '-'}")
        if s.get("error_text"):
            st.error(s["error_text"])
        if s.get("meta_json"):
            st.code(s["meta_json"], language="json")

st.subheader("Последние строки лога")
log_path = Path(run["log_path"])
if log_path.exists():
    lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()[-200:]
    st.text("\n".join(lines))
else:
    st.info("Лог пока пустой")
