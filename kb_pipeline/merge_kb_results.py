from __future__ import annotations
import json
from pathlib import Path

BASE = Path.home() / "kb_pipeline/data/output"
RUN2 = BASE / "run2_top200" / "solution_cards_top200.jsonl"
RUN3 = BASE / "run3_remaining" / "solution_cards_remaining.jsonl"
OUT = BASE / "kb_merged"

OUT.mkdir(parents=True, exist_ok=True)

cards = []
seen = set()

for path in [RUN2, RUN3]:
    if not path.exists():
        continue
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            cid = int(obj["cluster_id"])
            if cid in seen:
                continue
            seen.add(cid)
            cards.append(obj)

cards.sort(key=lambda x: (-int(x.get("examples_count", 0)), x.get("problem_title", "")))

jsonl_path = OUT / "knowledge_cards_merged.jsonl"
with jsonl_path.open("w", encoding="utf-8") as f:
    for card in cards:
        f.write(json.dumps(card, ensure_ascii=False) + "\n")

md_path = OUT / "knowledge_cards_merged.md"
with md_path.open("w", encoding="utf-8") as f:
    for card in cards:
        f.write(f"# {card.get('problem_title','')}\n\n")
        f.write(f"Описание: {card.get('problem_summary','')}\n\n")
        f.write("Сигналы клиента:\n")
        for x in card.get("client_signals", []):
            f.write(f"- {x}\n")
        f.write("\nШаги решения:\n")
        for x in card.get("solution_steps", []):
            f.write(f"- {x}\n")
        f.write(f"\nКанонический ответ:\n{card.get('canonical_manager_answer','')}\n\n")
        f.write("---\n\n")

summary = {
    "cards_total": len(cards),
    "jsonl": str(jsonl_path),
    "md": str(md_path),
}
with (OUT / "summary_merged.json").open("w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)

print(json.dumps(summary, ensure_ascii=False, indent=2))
