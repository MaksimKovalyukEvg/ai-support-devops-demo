import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
from app.pipeline import run_all

st.set_page_config(page_title="KB Pipeline", layout="wide")
st.title("Локальная обработка TXT в базу знаний")

input_file = st.text_input(
    "Путь к TXT-файлу",
    str(Path.home() / "Downloads" / "dialog_messages_snapshot.txt")
)
output_dir = st.text_input(
    "Папка результата",
    str(Path.home() / "kb_pipeline" / "data" / "output" / "run1")
)

progress_bar = st.progress(0)
status_box = st.empty()

def progress_cb(value: int, text: str):
    progress_bar.progress(max(0, min(100, value)))
    status_box.info(f"{value}% — {text}")

if st.button("Запустить обработку"):
    with st.spinner("Идёт обработка..."):
        result = run_all(input_file, output_dir, progress_cb=progress_cb)
    st.success("Готово")
    st.json(result)

summary_path = Path(output_dir) / "run_summary.json"
if summary_path.exists():
    st.subheader("Последний результат")
    st.json(json.loads(summary_path.read_text(encoding="utf-8")))
