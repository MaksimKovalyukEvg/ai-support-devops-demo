from pathlib import Path
import os

BASE_DIR = Path.home() / "kb_orchestrator"
RUNTIME_DIR = BASE_DIR / "runtime"
RUNS_DIR = BASE_DIR / "runs"
DB_PATH = RUNTIME_DIR / "orchestrator.db"

KB_PIPELINE_DIR = Path.home() / "kb_pipeline"
KB_PIPELINE_PYTHON = KB_PIPELINE_DIR / ".venv" / "bin" / "python"
KB_INPUT_EXPECTED = KB_PIPELINE_DIR / "data" / "input" / "dialog_messages_snapshot.txt"
KB_OUTPUT_DIR = KB_PIPELINE_DIR / "data" / "output"

DEFAULT_BATCH_SIZE = int(os.getenv("KB_ORCH_BATCH_SIZE", "300"))
DEFAULT_MIN_COUNT = int(os.getenv("KB_ORCH_MIN_COUNT", "5"))

RUN2_DIR = KB_OUTPUT_DIR / "run2_top200"
RUN3_DIR = KB_OUTPUT_DIR / "run3_remaining"
MERGED_DIR = KB_OUTPUT_DIR / "kb_merged"
CLEAN_DIR = KB_OUTPUT_DIR / "kb_clean_v2"
RATED_DIR = KB_OUTPUT_DIR / "kb_rated"
FINAL_DIR = KB_OUTPUT_DIR / "kb_final"
PROD_DIR = KB_OUTPUT_DIR / "kb_prod"
RETRIEVAL_DIR = KB_OUTPUT_DIR / "kb_retrieval"

REMOTE_RU_HOST = os.getenv("KB_REMOTE_RU_HOST", "")
REMOTE_RU_USER = os.getenv("KB_REMOTE_RU_USER", "deploy")
REMOTE_RU_PATH = os.getenv("KB_REMOTE_RU_PATH", "/opt/demo-backend/data/knowledge_retrieval.jsonl")
REMOTE_RU_SERVICE = os.getenv("KB_REMOTE_RU_SERVICE", "demo-backend")

STAGES = [
    {"key": "prepare_input",      "title": "Подготовка входного TXT",                 "weight": 4},
    {"key": "base_pipeline",      "title": "Базовый parse / qa_pairs / cluster",      "weight": 34},
    {"key": "top200",             "title": "Top-200 карточек",                        "weight": 8},
    {"key": "remaining",          "title": "Оставшиеся кластеры",                     "weight": 24},
    {"key": "merge",              "title": "Merge",                                   "weight": 4},
    {"key": "clean_v2",           "title": "Чистка v2",                               "weight": 4},
    {"key": "rate",               "title": "Оценка качества",                         "weight": 8},
    {"key": "categorize",         "title": "Категоризация GOOD",                      "weight": 4},
    {"key": "finalize",           "title": "Финализация GOOD",                        "weight": 4},
    {"key": "retrieval_pack",     "title": "Сбор retrieval-пака",                     "weight": 4},
    {"key": "deploy_ru",          "title": "Деплой на RU и health-check",             "weight": 6},
]
