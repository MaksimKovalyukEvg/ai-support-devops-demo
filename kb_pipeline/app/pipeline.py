from __future__ import annotations
import json
import hashlib
from pathlib import Path
from datetime import datetime

import duckdb
import pandas as pd
import requests
from tqdm import tqdm
from sklearn.feature_extraction.text import TfidfVectorizer
import hdbscan

FIELDS = {
    "Диалог:": "dialog_identity",
    "Ссылка:": "thread_url",
    "Клиент:": "client_name",
    "Менеджер:": "manager_name",
    "Роль:": "sender_role",
    "Отправитель:": "sender_name",
    "Время:": "sent_at",
    "Канал:": "source_channel",
    "Текст:": "message_text",
}

OLLAMA_URL = "http://127.0.0.1:11434"
GEN_MODEL = "qwen2.5:14b"


def clean(s: str | None) -> str:
    s = (s or "").replace("\xa0", " ").strip()
    while "  " in s:
        s = s.replace("  ", " ")
    return s


def h(*parts: str) -> str:
    return hashlib.sha256("||".join(parts).encode("utf-8", errors="ignore")).hexdigest()


def report(progress_cb, value: int, text: str):
    if progress_cb:
        progress_cb(value, text)


def parse_txt_to_messages(txt_path: str, out_dir: str, progress_cb=None):
    txt_path = Path(txt_path)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    messages_jsonl = out_dir / "messages.jsonl"

    current = {v: "" for v in FIELDS.values()}
    in_text = False
    record_index = 0

    def flush(fh, rec):
        nonlocal record_index
        rec = {k: clean(v) for k, v in rec.items()}
        if not rec.get("message_text"):
            return
        rec["record_index"] = record_index
        record_index += 1
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    report(progress_cb, 1, "Разбираю TXT в сообщения")

    with txt_path.open("r", encoding="utf-8", errors="ignore") as f, messages_jsonl.open("w", encoding="utf-8") as out:
        for raw in f:
            line = raw.rstrip("\n").rstrip("\r")
            stripped = line.strip()

            if stripped and set(stripped) == {"-"} and len(stripped) >= 10:
                flush(out, current)
                current = {v: "" for v in FIELDS.values()}
                in_text = False
                continue

            matched = False
            for prefix, field in FIELDS.items():
                if line.startswith(prefix):
                    matched = True
                    value = line[len(prefix):].strip()
                    if field == "message_text":
                        current[field] = value
                        in_text = True
                    else:
                        current[field] = value
                    break

            if matched:
                continue

            if in_text:
                current["message_text"] += ("\n" if current["message_text"] else "") + line

        flush(out, current)

    db_path = out_dir / "knowledge.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(f"CREATE OR REPLACE TABLE messages AS SELECT * FROM read_json_auto('{str(messages_jsonl)}')")
    con.execute(f"COPY (SELECT * FROM messages ORDER BY record_index) TO '{str(out_dir / 'messages.parquet')}' (FORMAT PARQUET)")
    rows = con.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    dialogs = con.execute("SELECT COUNT(DISTINCT dialog_identity) FROM messages").fetchone()[0]
    con.close()

    report(progress_cb, 10, f"TXT разобран: {rows} сообщений, {dialogs} диалогов")

    return {
        "messages_jsonl": str(messages_jsonl),
        "db_path": str(db_path),
        "messages_count": rows,
        "dialogs_count": dialogs,
    }


def build_qa_pairs(db_path: str, out_dir: str, progress_cb=None):
    out_dir = Path(out_dir)
    con = duckdb.connect(db_path)

    msgs = con.execute("""
        SELECT
            dialog_identity, thread_url, client_name, manager_name,
            sender_role, sender_name, sent_at, source_channel,
            message_text, record_index
        FROM messages
        ORDER BY dialog_identity, record_index
    """).df()

    report(progress_cb, 12, "Собираю пары клиент → менеджер")

    pairs = []

    for dialog_id, grp in msgs.groupby("dialog_identity", sort=False):
        rows = grp.to_dict("records")
        for i, row in enumerate(rows):
            if row["sender_role"] != "user":
                continue

            client_msg = clean(row["message_text"])
            if not client_msg:
                continue

            reply = None
            for j in range(i + 1, len(rows)):
                if rows[j]["sender_role"] == "manager":
                    reply = rows[j]
                    break
                if rows[j]["sender_role"] == "user":
                    break

            if not reply:
                continue

            context_before = []
            for c in rows[max(0, i - 3):i]:
                context_before.append(f"{c['sender_role']}: {clean(c['message_text'])}")

            pairs.append({
                "pair_id": h(dialog_id, str(row["record_index"]), str(reply["record_index"])),
                "dialog_identity": row["dialog_identity"],
                "thread_url": row["thread_url"],
                "client_name": row["client_name"],
                "manager_name": row["manager_name"],
                "client_sender_name": row["sender_name"],
                "manager_sender_name": reply["sender_name"],
                "client_sent_at": row["sent_at"],
                "manager_sent_at": reply["sent_at"],
                "client_channel": row["source_channel"],
                "manager_channel": reply["source_channel"],
                "client_message": client_msg,
                "manager_reply": clean(reply["message_text"]),
                "context_before": "\n".join(context_before),
            })

    pairs_df = pd.DataFrame(pairs)
    pairs_jsonl = out_dir / "qa_pairs.jsonl"
    pairs_parquet = out_dir / "qa_pairs.parquet"

    with pairs_jsonl.open("w", encoding="utf-8") as f:
        for rec in pairs:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    con.execute("CREATE OR REPLACE TABLE qa_pairs AS SELECT * FROM pairs_df")
    con.execute(f"COPY qa_pairs TO '{str(pairs_parquet)}' (FORMAT PARQUET)")
    con.close()

    report(progress_cb, 25, f"Пары собраны: {len(pairs)}")

    return {
        "qa_pairs_jsonl": str(pairs_jsonl),
        "qa_pairs_parquet": str(pairs_parquet),
        "pairs_count": len(pairs),
    }


def cluster_issues(db_path: str, out_dir: str, min_cluster_size: int = 15, progress_cb=None):
    out_dir = Path(out_dir)
    con = duckdb.connect(db_path)

    report(progress_cb, 30, "Кластеризую клиентские проблемы")

    qa = con.execute("""
        SELECT pair_id, dialog_identity, thread_url, client_message, manager_reply
        FROM qa_pairs
        WHERE length(trim(client_message)) > 8
    """).df()

    texts = qa["client_message"].tolist()

    vectorizer = TfidfVectorizer(
        max_features=12000,
        ngram_range=(1, 2),
        min_df=2,
        max_df=0.9,
    )
    X = vectorizer.fit_transform(texts)

    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=5,
        metric="euclidean",
        cluster_selection_method="eom",
    )
    labels = clusterer.fit_predict(X.toarray())

    qa["cluster_id"] = labels
    con.execute("CREATE OR REPLACE TABLE issue_candidates AS SELECT * FROM qa")
    con.execute(f"COPY issue_candidates TO '{str(out_dir / 'issue_candidates.parquet')}' (FORMAT PARQUET)")

    clusters_total = int(len(set([x for x in labels if x != -1])))
    noise_count = int((labels == -1).sum())
    rows_count = int(len(qa))

    con.close()

    report(progress_cb, 45, f"Кластеризация завершена: кластеров {clusters_total}, шумовых {noise_count}")

    return {
        "clusters_total": clusters_total,
        "noise_count": noise_count,
        "rows_count": rows_count,
    }


def llm_json(prompt: str) -> dict:
    resp = requests.post(
        f"{OLLAMA_URL}/api/generate",
        json={
            "model": GEN_MODEL,
            "prompt": prompt,
            "stream": False,
            "format": "json"
        },
        timeout=1800,
    )
    resp.raise_for_status()
    text = resp.json()["response"]
    return json.loads(text)


def build_solution_cards(db_path: str, out_dir: str, max_examples_per_cluster: int = 25, progress_cb=None):
    out_dir = Path(out_dir)
    con = duckdb.connect(db_path)

    clusters = con.execute("""
        SELECT cluster_id, COUNT(*) AS cnt
        FROM issue_candidates
        WHERE cluster_id != -1
        GROUP BY cluster_id
        ORDER BY cnt DESC
    """).fetchall()

    total_clusters = len(clusters)
    cards = []

    if total_clusters == 0:
        report(progress_cb, 95, "Кластеров для карточек решений не найдено")

    for n, (cluster_id, cnt) in enumerate(clusters, start=1):
        percent = 45 + int((n / total_clusters) * 50)
        report(progress_cb, min(percent, 95), f"Строю карточки решений: {n}/{total_clusters}")

        examples = con.execute(f"""
            SELECT client_message, manager_reply, thread_url
            FROM issue_candidates
            WHERE cluster_id = {int(cluster_id)}
            LIMIT {int(max_examples_per_cluster)}
        """).fetchall()

        example_block = []
        for i, (client_msg, manager_reply, thread_url) in enumerate(examples, 1):
            example_block.append(
                f"Пример {i}\n"
                f"Вопрос клиента: {client_msg}\n"
                f"Ответ менеджера: {manager_reply}\n"
                f"Источник: {thread_url}\n"
            )

        prompt = f"""
Ты аналитик базы поддержки онлайн-школы.
Ниже группа похожих обращений клиентов и ответы менеджеров.

Нужно вернуть JSON строго такого формата:
{{
  "problem_title": "...",
  "problem_summary": "...",
  "client_signals": ["...", "..."],
  "solution_steps": ["...", "..."],
  "canonical_manager_answer": "...",
  "confidence": 0.0
}}

Правила:
- problem_title короткий и понятный
- problem_summary 1-3 предложения
- client_signals это типовые формулировки клиента
- solution_steps это конкретные шаги решения
- canonical_manager_answer это хороший универсальный ответ клиенту
- confidence от 0 до 1
- никакого текста вне JSON

Данные:
{chr(10).join(example_block)}
""".strip()

        try:
            obj = llm_json(prompt)
        except Exception as e:
            obj = {
                "problem_title": f"cluster_{cluster_id}",
                "problem_summary": f"LLM summary failed: {e}",
                "client_signals": [],
                "solution_steps": [],
                "canonical_manager_answer": "",
                "confidence": 0.0,
            }

        obj["cluster_id"] = int(cluster_id)
        obj["examples_count"] = int(cnt)
        obj["source_examples"] = [
            {"client_message": a, "manager_reply": b, "thread_url": c}
            for a, b, c in examples[:5]
        ]
        cards.append(obj)

    solution_jsonl = out_dir / "solution_cards.jsonl"
    with solution_jsonl.open("w", encoding="utf-8") as f:
        for card in cards:
            f.write(json.dumps(card, ensure_ascii=False) + "\n")

    con.execute(f"CREATE OR REPLACE TABLE solution_cards AS SELECT * FROM read_json_auto('{str(solution_jsonl)}')")
    con.execute(f"COPY solution_cards TO '{str(out_dir / 'solution_cards.parquet')}' (FORMAT PARQUET)")

    kb_md = out_dir / "knowledge_base_for_gpt.md"
    with kb_md.open("w", encoding="utf-8") as f:
        for card in cards:
            f.write(f"# {card.get('problem_title', '')}\n\n")
            f.write(f"Описание: {card.get('problem_summary', '')}\n\n")
            f.write("Сигналы клиента:\n")
            for x in card.get("client_signals", []):
                f.write(f"- {x}\n")
            f.write("\nШаги решения:\n")
            for x in card.get("solution_steps", []):
                f.write(f"- {x}\n")
            f.write(f"\nКанонический ответ:\n{card.get('canonical_manager_answer', '')}\n\n")
            f.write(f"Уверенность: {card.get('confidence', 0)}\n")
            f.write("\n---\n\n")

    con.close()

    report(progress_cb, 95, f"Карточки решений готовы: {len(cards)}")

    return {
        "solution_cards_jsonl": str(solution_jsonl),
        "knowledge_base_md": str(kb_md),
        "cards_count": len(cards),
    }


def run_all(input_txt: str, out_dir: str, progress_cb=None):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    report(progress_cb, 0, "Запуск pipeline")

    r1 = parse_txt_to_messages(input_txt, out_dir, progress_cb=progress_cb)
    r2 = build_qa_pairs(r1["db_path"], out_dir, progress_cb=progress_cb)
    r3 = cluster_issues(r1["db_path"], out_dir, progress_cb=progress_cb)
    r4 = build_solution_cards(r1["db_path"], out_dir, progress_cb=progress_cb)

    summary = {
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "input_txt": str(input_txt),
        **r1,
        **r2,
        **r3,
        **r4,
    }

    with (out_dir / "run_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    report(progress_cb, 100, "Готово")

    return summary
