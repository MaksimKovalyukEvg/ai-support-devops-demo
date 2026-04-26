from __future__ import annotations
import json
import re
from pathlib import Path

BASE = Path.home() / "kb_pipeline/data/output"
SRC = BASE / "kb_prod" / "knowledge_cards_prod.jsonl"
OUTDIR = BASE / "kb_retrieval"
OUTDIR.mkdir(parents=True, exist_ok=True)

OUT_JSONL = OUTDIR / "knowledge_retrieval.jsonl"
OUT_MD = OUTDIR / "knowledge_retrieval.md"
SUMMARY = OUTDIR / "summary_retrieval.json"

def clean_text(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r'\s+', ' ', s)
    s = s.replace("Здравствуйте! .", "Здравствуйте!")
    s = s.replace("Здравствуйте, .", "Здравствуйте!")
    s = s.replace("Здравствуйте,  .", "Здравствуйте!")
    s = s.replace("Добрый день! .", "Добрый день!")
    s = s.replace("  ", " ")
    s = s.replace(" ,", ",")
    s = s.replace(" .", ".")
    s = s.replace(" !", "!")
    s = s.replace(" ?", "?")
    s = s.strip()
    return s

cards = []
with SRC.open("r", encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        obj = json.loads(line)

        packed = {
            "category": clean_text(obj.get("category", "")),
            "problem_title": clean_text(obj.get("problem_title", "")),
            "client_signals": [clean_text(x) for x in obj.get("client_signals", []) if clean_text(x)],
            "solution_steps": [clean_text(x) for x in obj.get("solution_steps", []) if clean_text(x)],
            "canonical_manager_answer": clean_text(obj.get("canonical_manager_answer", "")),
        }

        packed["retrieval_text"] = clean_text(
            f"Категория: {packed['category']}. "
            f"Проблема: {packed['problem_title']}. "
            f"Сигналы клиента: {'; '.join(packed['client_signals'])}. "
            f"Шаги решения: {'; '.join(packed['solution_steps'])}. "
            f"Ответ: {packed['canonical_manager_answer']}"
        )

        cards.append(packed)

with OUT_JSONL.open("w", encoding="utf-8") as f:
    for card in cards:
        f.write(json.dumps(card, ensure_ascii=False) + "\n")

with OUT_MD.open("w", encoding="utf-8") as f:
    current_cat = None
    for card in cards:
        if card["category"] != current_cat:
            current_cat = card["category"]
            f.write(f"# CATEGORY: {current_cat}\n\n")
        f.write(f"## {card['problem_title']}\n\n")
        f.write("Сигналы клиента:\n")
        for x in card["client_signals"]:
            f.write(f"- {x}\n")
        f.write("\nШаги решения:\n")
        for x in card["solution_steps"]:
            f.write(f"- {x}\n")
        f.write(f"\nКанонический ответ:\n{card['canonical_manager_answer']}\n\n")
        f.write("---\n\n")

summary = {
    "cards_total": len(cards),
    "jsonl": str(OUT_JSONL),
    "md": str(OUT_MD),
}
with SUMMARY.open("w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)

print(json.dumps(summary, ensure_ascii=False, indent=2))
