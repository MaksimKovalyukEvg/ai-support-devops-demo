from __future__ import annotations
import json
import argparse
from pathlib import Path

import duckdb
import requests

OLLAMA_URL = "http://127.0.0.1:11434"
GEN_MODEL = "qwen2.5:14b"

BASE_DIR = Path.home() / "kb_pipeline/data/output"
RUN1_DIR = BASE_DIR / "run1"
RUN2_DIR = BASE_DIR / "run2_top200"


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
        "замечательно", "прекрасно", "хорошего дня", "приятного творчества",
        "благодарю вас", "очень приятно", "всего доброго"
    ]
    score = sum(1 for x in garbage_markers if x in text)
    if score >= 4:
        return True
    if len(text.strip()) < 120:
        return True
    return False


def load_jsonl_cluster_ids(path: Path, key: str) -> set[int]:
    result = set()
    if not path.exists():
        return result
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if key in obj and obj[key] is not None:
                    result.add(int(obj[key]))
            except Exception:
                pass
    return result


def append_jsonl(path: Path, obj: dict) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--min-count", type=int, default=5)
    parser.add_argument("--output-dir", default=str(BASE_DIR / "run3_remaining"))
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    issue_parquet = RUN1_DIR / "issue_candidates.parquet"
    processed_old = RUN2_DIR / "solution_cards_top200.jsonl"
    skipped_old = RUN2_DIR / "skipped_top200.jsonl"

    processed_new = out_dir / "solution_cards_remaining.jsonl"
    skipped_new = out_dir / "skipped_remaining.jsonl"
    summary_json = out_dir / "summary_remaining.json"

    already_done = set()
    already_done |= load_jsonl_cluster_ids(processed_old, "cluster_id")
    already_done |= load_jsonl_cluster_ids(skipped_old, "cluster_id")
    already_done |= load_jsonl_cluster_ids(processed_new, "cluster_id")
    already_done |= load_jsonl_cluster_ids(skipped_new, "cluster_id")

    con = duckdb.connect()
    con.execute(f"CREATE OR REPLACE VIEW issue_candidates AS SELECT * FROM read_parquet('{issue_parquet}')")

    all_clusters = con.execute(f"""
        SELECT cluster_id, COUNT(*) AS cnt
        FROM issue_candidates
        WHERE cluster_id != -1
        GROUP BY cluster_id
        HAVING COUNT(*) >= {int(args.min_count)}
        ORDER BY cnt DESC
    """).fetchall()

    remaining = [(cid, cnt) for cid, cnt in all_clusters if int(cid) not in already_done]
    batch = remaining[:args.batch_size]

    print(f"all_clusters={len(all_clusters)}")
    print(f"already_done={len(already_done)}")
    print(f"remaining={len(remaining)}")
    print(f"processing_now={len(batch)}")

    created = 0
    skipped = 0

    for idx, (cluster_id, cnt) in enumerate(batch, start=1):
        rows = con.execute(f"""
            SELECT client_message, manager_reply, thread_url
            FROM issue_candidates
            WHERE cluster_id = {int(cluster_id)}
            LIMIT 12
        """).fetchall()

        client_examples = [r[0] for r in rows if r[0]]
        manager_examples = [r[1] for r in rows if r[1]]

        if is_garbage_cluster(client_examples, manager_examples):
            rec = {
                "cluster_id": int(cluster_id),
                "examples_count": int(cnt),
                "reason": "garbage_cluster"
            }
            append_jsonl(skipped_new, rec)
            skipped += 1
            print(f"[{idx}/{len(batch)}] skip garbage cluster={cluster_id}")
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

Правила:
- не JSON
- не поясняй вне шаблона
- если кластер не про support, а про благодарности, смайлы, завершение разговора, подтверждение без проблемы или просто вежливость, напиши:
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
                rec = {
                    "cluster_id": int(cluster_id),
                    "examples_count": int(cnt),
                    "reason": "llm_skip"
                }
                append_jsonl(skipped_new, rec)
                skipped += 1
                print(f"[{idx}/{len(batch)}] llm skip cluster={cluster_id}")
                continue

            if not parsed["problem_title"] or parsed["problem_title"] in ("...", ".", ".."):
                rec = {
                    "cluster_id": int(cluster_id),
                    "examples_count": int(cnt),
                    "reason": "bad_title"
                }
                append_jsonl(skipped_new, rec)
                skipped += 1
                print(f"[{idx}/{len(batch)}] bad title cluster={cluster_id}")
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
            append_jsonl(processed_new, card)
            created += 1
            print(f"[{idx}/{len(batch)}] ok cluster={cluster_id} title={parsed['problem_title'][:80]}")

        except Exception as e:
            rec = {
                "cluster_id": int(cluster_id),
                "examples_count": int(cnt),
                "reason": f"error: {e}"
            }
            append_jsonl(skipped_new, rec)
            skipped += 1
            print(f"[{idx}/{len(batch)}] error cluster={cluster_id} {e}")

    processed_total = len(load_jsonl_cluster_ids(processed_new, "cluster_id"))
    skipped_total = len(load_jsonl_cluster_ids(skipped_new, "cluster_id"))

    summary = {
        "batch_size": args.batch_size,
        "min_count": args.min_count,
        "all_clusters": len(all_clusters),
        "already_done_before_run": len(already_done),
        "remaining_before_run": len(remaining),
        "processed_in_this_run": created,
        "skipped_in_this_run": skipped,
        "processed_total_in_run3": processed_total,
        "skipped_total_in_run3": skipped_total,
        "processed_jsonl": str(processed_new),
        "skipped_jsonl": str(skipped_new),
    }

    with summary_json.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
