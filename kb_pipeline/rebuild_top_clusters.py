from __future__ import annotations
import json
from pathlib import Path
import duckdb
import requests

OLLAMA_URL = "http://127.0.0.1:11434"
GEN_MODEL = "qwen2.5:14b"

RUN_DIR = Path.home() / "kb_pipeline/data/output/run1"
ISSUE_PARQUET = RUN_DIR / "issue_candidates.parquet"
OUT_DIR = Path.home() / "kb_pipeline/data/output/run2_top200"

OUT_DIR.mkdir(parents=True, exist_ok=True)

def llm_text(prompt: str) -> str:
    r = requests.post(
        f"{OLLAMA_URL}/api/generate",
        json={
            "model": GEN_MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.1,
                "num_predict": 1200
            }
        },
        timeout=1800,
    )
    r.raise_for_status()
    return r.json()["response"].strip()

def parse_sections(text: str) -> dict:
    sections = {
        "problem_title": "",
        "problem_summary": "",
        "client_signals": [],
        "solution_steps": [],
        "canonical_manager_answer": "",
    }

    current = None
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue

        low = line.lower()

        if low.startswith("название проблемы:"):
            current = "problem_title"
            sections[current] = line.split(":", 1)[1].strip()
            continue
        if low.startswith("описание:"):
            current = "problem_summary"
            sections[current] = line.split(":", 1)[1].strip()
            continue
        if low.startswith("сигналы клиента:"):
            current = "client_signals"
            continue
        if low.startswith("шаги решения:"):
            current = "solution_steps"
            continue
        if low.startswith("канонический ответ:"):
            current = "canonical_manager_answer"
            sections[current] = line.split(":", 1)[1].strip()
            continue

        if current in ("client_signals", "solution_steps"):
            if line.startswith("-"):
                sections[current].append(line[1:].strip())
            else:
                sections[current].append(line.strip())
        elif current in ("problem_summary", "canonical_manager_answer"):
            if sections[current]:
                sections[current] += " " + line
            else:
                sections[current] = line

    return sections

def is_garbage_cluster(client_examples: list[str], manager_examples: list[str]) -> bool:
    text = " ".join(client_examples[:10] + manager_examples[:10]).lower()
    garbage_markers = [
        "спасибо", "благодарю", "благодарность",
        "хорошо", "поняла", "понял", "ок", "👍", "❤", "💐", "💝",
        "замечательно", "прекрасно", "хорошего дня", "приятного творчества"
    ]
    score = sum(1 for x in garbage_markers if x in text)
    if score >= 4:
        return True
    if len(text.strip()) < 120:
        return True
    return False

con = duckdb.connect()
con.execute(f"""
CREATE OR REPLACE VIEW issue_candidates AS
SELECT * FROM read_parquet('{ISSUE_PARQUET}')
""")

top_clusters = con.execute("""
SELECT cluster_id, COUNT(*) AS cnt
FROM issue_candidates
WHERE cluster_id != -1
GROUP BY cluster_id
ORDER BY cnt DESC
LIMIT 200
""").fetchall()

cards = []
skipped = []

for idx, (cluster_id, cnt) in enumerate(top_clusters, start=1):
    rows = con.execute(f"""
    SELECT client_message, manager_reply, thread_url
    FROM issue_candidates
    WHERE cluster_id = {int(cluster_id)}
    LIMIT 12
    """).fetchall()

    client_examples = [r[0] for r in rows if r[0]]
    manager_examples = [r[1] for r in rows if r[1]]

    if is_garbage_cluster(client_examples, manager_examples):
        skipped.append({
            "cluster_id": int(cluster_id),
            "examples_count": int(cnt),
            "reason": "garbage_cluster"
        })
        print(f"[{idx}/200] skip garbage cluster {cluster_id}")
        continue

    example_block = []
    for n, (client_msg, manager_reply, thread_url) in enumerate(rows[:8], start=1):
        example_block.append(
            f"Пример {n}\n"
            f"Вопрос клиента: {client_msg}\n"
            f"Ответ менеджера: {manager_reply}\n"
            f"Источник: {thread_url}\n"
        )

    prompt = f"""
Ты анализируешь переписки поддержки онлайн-школы.

Ниже группа похожих обращений клиентов и ответы менеджеров.
Нужно выделить только полезную support-проблему и способ её решения.

Верни ответ СТРОГО в таком текстовом формате:

Название проблемы: ...
Описание: ...
Сигналы клиента:
- ...
- ...
Шаги решения:
- ...
- ...
Канонический ответ: ...

Требования:
- не пиши JSON
- не пиши пояснений вне шаблона
- если кластер не про support, а про благодарности, пустые ответы, смайлы, завершение разговора или обычную вежливость, напиши:
Название проблемы: SKIP
Описание: not_support
Сигналы клиента:
- ...
Шаги решения:
- ...
Канонический ответ: ...

Данные:
{chr(10).join(example_block)}
""".strip()

    try:
        raw = llm_text(prompt)
        parsed = parse_sections(raw)

        if parsed["problem_title"].strip().upper() == "SKIP":
            skipped.append({
                "cluster_id": int(cluster_id),
                "examples_count": int(cnt),
                "reason": "llm_skip"
            })
            print(f"[{idx}/200] llm skip cluster {cluster_id}")
            continue

        card = {
            "cluster_id": int(cluster_id),
            "examples_count": int(cnt),
            "problem_title": parsed["problem_title"],
            "problem_summary": parsed["problem_summary"],
            "client_signals": parsed["client_signals"][:8],
            "solution_steps": parsed["solution_steps"][:8],
            "canonical_manager_answer": parsed["canonical_manager_answer"],
            "source_examples": [
                {
                    "client_message": a,
                    "manager_reply": b,
                    "thread_url": c
                }
                for a, b, c in rows[:5]
            ]
        }
        cards.append(card)
        print(f"[{idx}/200] ok cluster {cluster_id} -> {parsed['problem_title'][:80]}")

    except Exception as e:
        skipped.append({
            "cluster_id": int(cluster_id),
            "examples_count": int(cnt),
            "reason": f"error: {e}"
        })
        print(f"[{idx}/200] error cluster {cluster_id}: {e}")

jsonl_path = OUT_DIR / "solution_cards_top200.jsonl"
with jsonl_path.open("w", encoding="utf-8") as f:
    for card in cards:
        f.write(json.dumps(card, ensure_ascii=False) + "\n")

skip_path = OUT_DIR / "skipped_top200.jsonl"
with skip_path.open("w", encoding="utf-8") as f:
    for rec in skipped:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

md_path = OUT_DIR / "knowledge_base_top200.md"
with md_path.open("w", encoding="utf-8") as f:
    for card in cards:
        f.write(f"# {card['problem_title']}\n\n")
        f.write(f"Описание: {card['problem_summary']}\n\n")
        f.write("Сигналы клиента:\n")
        for x in card["client_signals"]:
            f.write(f"- {x}\n")
        f.write("\nШаги решения:\n")
        for x in card["solution_steps"]:
            f.write(f"- {x}\n")
        f.write(f"\nКанонический ответ:\n{card['canonical_manager_answer']}\n\n")
        f.write("---\n\n")

summary = {
    "top_clusters_processed": 200,
    "cards_created": len(cards),
    "clusters_skipped": len(skipped),
    "solution_cards_jsonl": str(jsonl_path),
    "knowledge_base_md": str(md_path),
    "skipped_jsonl": str(skip_path),
}

with (OUT_DIR / "summary_top200.json").open("w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)

print(json.dumps(summary, ensure_ascii=False, indent=2))
