from __future__ import annotations
import json
from pathlib import Path

BASE = Path.home() / "kb_pipeline/data/output"
MERGED = BASE / "kb_merged" / "knowledge_cards_merged.jsonl"
OUTDIR = BASE / "kb_clean_v2"
OUTDIR.mkdir(parents=True, exist_ok=True)

CLEAN_JSONL = OUTDIR / "knowledge_cards_clean_v2.jsonl"
CLEAN_MD = OUTDIR / "knowledge_cards_clean_v2.md"
DROPPED_JSONL = OUTDIR / "dropped_cards_v2.jsonl"
SUMMARY_JSON = OUTDIR / "summary_clean_v2.json"

BAD_TITLES = {
    "", ".", "..", "...", "skip", "skipped",
    "благодарность клиента", "пустое сообщение клиента",
    "благодарность за поддержку", "закрытие обращения"
}

BAD_SIGNAL_SUBSTRINGS = [
    "не относится к проблеме",
    "общий сигнал для всех сообщений",
    "получен файл",
    "получены файлы",
    "❤", "💝", "💐", "👍", "🙏", "✨"
]

BAD_SUMMARY_SUBSTRINGS = [
    "не относится к проблеме",
    "общий сигнал",
    "llm summary failed",
]

SUPPORT_KEYWORDS = [
    "доступ", "курс", "урок", "уроки", "мастер-класс", "мастер-классы",
    "войти", "вход", "личный кабинет", "геткурс", "письмо", "ссылка",
    "оплата", "оплат", "рассроч", "вебинар", "эфир", "трансляц",
    "видео", "файл", "бот", "vpn", "браузер", "профиль", "аккаунт",
    "загрузка", "ошибка", "кабинет"
]

def norm(s: str) -> str:
    return " ".join((s or "").lower().strip().split())

def has_support_meaning(card: dict) -> bool:
    blob = " ".join([
        card.get("problem_title", ""),
        card.get("problem_summary", ""),
        " ".join(card.get("client_signals", []) or []),
        " ".join(card.get("solution_steps", []) or []),
        card.get("canonical_manager_answer", "")
    ]).lower()
    return any(k in blob for k in SUPPORT_KEYWORDS)

def clean_signals(signals: list[str]) -> list[str]:
    out = []
    for s in signals or []:
        ns = norm(s)
        if not ns:
            continue
        if ns in {"...", ".", ".."}:
            continue
        if len(ns) < 6:
            continue
        if any(x in ns for x in BAD_SIGNAL_SUBSTRINGS):
            continue
        out.append(s.strip())
    return out[:8]

def clean_steps(steps: list[str]) -> list[str]:
    out = []
    for s in steps or []:
        ns = norm(s)
        if not ns:
            continue
        if ns in {"...", ".", ".."}:
            continue
        if len(ns) < 10:
            continue
        out.append(s.strip())
    return out[:8]

def is_bad_answer(answer: str) -> bool:
    a = norm(answer)
    if not a or len(a) < 40:
        return True
    if a in {"...", ".", ".."}:
        return True
    return False

def is_bad_card(card: dict) -> tuple[bool, str]:
    title = norm(card.get("problem_title", ""))
    summary = norm(card.get("problem_summary", ""))

    if title in BAD_TITLES:
        return True, "bad_title"
    if len(title) < 8:
        return True, "short_title"
    if title.startswith("..."):
        return True, "dots_title"
    if any(x in summary for x in BAD_SUMMARY_SUBSTRINGS):
        return True, "bad_summary"
    if len(summary) < 35:
        return True, "weak_summary"

    card["client_signals"] = clean_signals(card.get("client_signals", []))
    card["solution_steps"] = clean_steps(card.get("solution_steps", []))

    if len(card["solution_steps"]) < 1:
        return True, "no_steps"
    if is_bad_answer(card.get("canonical_manager_answer", "")):
        return True, "bad_answer"
    if not has_support_meaning(card):
        return True, "not_support"

    return False, "ok"

def dedupe_key(card: dict) -> str:
    title = norm(card.get("problem_title", ""))
    steps = " | ".join(norm(x) for x in card.get("solution_steps", [])[:3])
    return f"{title}__{steps}"

cards = []
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
