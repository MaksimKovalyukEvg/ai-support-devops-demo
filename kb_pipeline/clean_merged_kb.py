from __future__ import annotations
import json
from pathlib import Path

BASE = Path.home() / "kb_pipeline/data/output"
MERGED = BASE / "kb_merged" / "knowledge_cards_merged.jsonl"
OUTDIR = BASE / "kb_clean"
OUTDIR.mkdir(parents=True, exist_ok=True)

CLEAN_JSONL = OUTDIR / "knowledge_cards_clean.jsonl"
CLEAN_MD = OUTDIR / "knowledge_cards_clean.md"
DROPPED_JSONL = OUTDIR / "dropped_cards.jsonl"
SUMMARY_JSON = OUTDIR / "summary_clean.json"

BAD_TITLES = {"", ".", "..", "...", "skip", "skipped"}
BAD_PHRASES = {
    "...",
    "не относится к проблеме",
    "общий сигнал для всех сообщений",
    "получены файлы",
}
GARBAGE_KEYWORDS = [
    "благодарность",
    "спасибо",
    "закрытие обращения",
    "пустое сообщение",
    "подтверждение успешного подключения",
]

def norm(s: str) -> str:
    return " ".join((s or "").lower().strip().split())

def is_bad_card(card: dict) -> tuple[bool, str]:
    title = norm(card.get("problem_title", ""))
    summary = norm(card.get("problem_summary", ""))
    answer = norm(card.get("canonical_manager_answer", ""))

    if title in BAD_TITLES:
        return True, "bad_title"

    if any(x in title for x in GARBAGE_KEYWORDS):
        return True, "garbage_title"

    if not summary or len(summary) < 20:
        return True, "weak_summary"

    if not answer or len(answer) < 30:
        return True, "weak_answer"

    signals = card.get("client_signals", []) or []
    steps = card.get("solution_steps", []) or []

    good_signals = []
    for s in signals:
        ns = norm(s)
        if not ns:
            continue
        if ns in BAD_PHRASES:
            continue
        if "не относится к проблеме" in ns:
            continue
        if len(ns) < 4:
            continue
        good_signals.append(s)

    good_steps = []
    for s in steps:
        ns = norm(s)
        if not ns:
            continue
        if ns in BAD_PHRASES:
            continue
        if ns == "...":
            continue
        if len(ns) < 6:
            continue
        good_steps.append(s)

    card["client_signals"] = good_signals[:8]
    card["solution_steps"] = good_steps[:8]

    if len(card["solution_steps"]) == 0:
        return True, "no_steps"

    return False, "ok"

def dedupe_key(card: dict) -> str:
    title = norm(card.get("problem_title", ""))
    steps = " | ".join(norm(x) for x in card.get("solution_steps", [])[:3])
    return f"{title}__{steps}"

cards = []
if MERGED.exists():
    with MERGED.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            cards.append(json.loads(line))

clean = []
dropped = []
seen = set()

for card in cards:
    bad, reason = is_bad_card(card)
    if bad:
        dropped.append({"reason": reason, "card": card})
        continue

    key = dedupe_key(card)
    if key in seen:
        dropped.append({"reason": "duplicate", "card": card})
        continue
    seen.add(key)
    clean.append(card)

clean.sort(key=lambda x: (-int(x.get("examples_count", 0)), x.get("problem_title", "")))

with CLEAN_JSONL.open("w", encoding="utf-8") as f:
    for card in clean:
        f.write(json.dumps(card, ensure_ascii=False) + "\n")

with DROPPED_JSONL.open("w", encoding="utf-8") as f:
    for rec in dropped:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

with CLEAN_MD.open("w", encoding="utf-8") as f:
    for card in clean:
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
    "source_cards": len(cards),
    "clean_cards": len(clean),
    "dropped_cards": len(dropped),
    "clean_jsonl": str(CLEAN_JSONL),
    "clean_md": str(CLEAN_MD),
    "dropped_jsonl": str(DROPPED_JSONL),
}
with SUMMARY_JSON.open("w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)

print(json.dumps(summary, ensure_ascii=False, indent=2))
