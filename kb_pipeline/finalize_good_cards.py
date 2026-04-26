from __future__ import annotations
import json
import re
from pathlib import Path

BASE = Path.home() / "kb_pipeline/data/output"
SRC = BASE / "kb_final" / "knowledge_cards_final.jsonl"
OUTDIR = BASE / "kb_prod"
OUTDIR.mkdir(parents=True, exist_ok=True)

OUT_JSONL = OUTDIR / "knowledge_cards_prod.jsonl"
OUT_MD = OUTDIR / "knowledge_cards_prod.md"
SUMMARY = OUTDIR / "summary_prod.json"

EMOJI_RE = re.compile(
    "["
    "\U0001F300-\U0001FAD6"
    "\U0001F600-\U0001F64F"
    "\U0001F680-\U0001F6FF"
    "\U00002600-\U000026FF"
    "\U00002700-\U000027BF"
    "]+",
    flags=re.UNICODE,
)

PHONE_RE = re.compile(r"\+?\d[\d\-\s\(\)]{8,}\d")
EMAIL_RE = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')
AMOUNT_RE = re.compile(r'(?<!\w)(\d{1,3}(?:[ \u00A0]?\d{3})+|\d+)\s?(?:руб\.?|р\.?)(?!\w)', re.I)
LINK_RE = re.compile(r'https?://\S+|\[ссылка[^\]]*\]', re.I)
NAME_PREFIX_RE = re.compile(
    r'^(Здравствуйте|Добрый день|Добрый вечер|Привет),?\s+[А-ЯЁ][а-яё]+!?[\s,]*',
    re.U
)

GENERIC_REPLACEMENTS = [
    (r'Катя из отдела заботы', 'менеджер службы поддержки'),
    (r'Ольга, помощница Анастасии Астафьевой', 'менеджер службы поддержки'),
    (r'помощница Анастасии Астафьевой', 'менеджер службы поддержки'),
    (r'помощник Анастасии Астафьевой', 'менеджер службы поддержки'),
    (r'Анастасии Астафьевой', '{brand_owner}'),
    (r'Геткурс', 'GetCourse'),
]

def clean_text(s: str) -> str:
    s = s or ""
    s = EMOJI_RE.sub("", s)
    s = PHONE_RE.sub("{phone}", s)
    s = EMAIL_RE.sub("{client_email}", s)
    s = AMOUNT_RE.sub("{amount}", s)
    s = LINK_RE.sub("{link}", s)
    for old, new in GENERIC_REPLACEMENTS:
        s = re.sub(old, new, s, flags=re.I)
    s = s.replace("Людмила,", "{client_name},")
    s = s.replace("Любовь,", "{client_name},")
    s = s.replace("Римма,", "{client_name},")
    s = s.replace("Юлия!", "{client_name}!")
    s = s.replace("Юлия,", "{client_name},")
    s = s.replace("Мария,", "{client_name},")
    s = s.replace("Оксана,", "{client_name},")
    s = s.replace("Анна,", "{client_name},")
    s = NAME_PREFIX_RE.sub(r'\1, ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    s = s.replace("  ", " ")
    return s

def clean_signal(s: str) -> str:
    s = clean_text(s)
    s = s.strip(' "\'')
    return s

def clean_step(s: str) -> str:
    s = clean_text(s)
    s = s[0].upper() + s[1:] if s else s
    return s

def clean_answer(s: str) -> str:
    s = clean_text(s)
    s = s.replace("Здравствуйте, {client_name}!", "Здравствуйте!")
    s = s.replace("Здравствуйте, {client_name}.", "Здравствуйте.")
    s = s.replace("{client_name}, здравствуйте!", "Здравствуйте!")
    s = s.replace("{client_name}, добрый день!", "Здравствуйте!")
    s = s.replace("{client_name}, добрый вечер!", "Здравствуйте!")
    s = re.sub(r'Отвечает.*?(?=[\.\!])', '', s, flags=re.I)
    s = re.sub(r'Пишет вам.*?(?=[\.\!])', '', s, flags=re.I)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

cards = []
with SRC.open("r", encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        obj = json.loads(line)
        out = {
            "category": obj.get("category", "other"),
            "problem_title": clean_text(obj.get("problem_title", "")),
            "problem_summary": clean_text(obj.get("problem_summary", "")),
            "client_signals": [clean_signal(x) for x in (obj.get("client_signals") or []) if clean_signal(x)],
            "solution_steps": [clean_step(x) for x in (obj.get("solution_steps") or []) if clean_step(x)],
            "canonical_manager_answer": clean_answer(obj.get("canonical_manager_answer", "")),
            "quality_score": obj.get("quality_score", 0),
        }
        cards.append(out)

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
    "cards_total": len(cards),
    "jsonl": str(OUT_JSONL),
    "md": str(OUT_MD),
}
with SUMMARY.open("w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)

print(json.dumps(summary, ensure_ascii=False, indent=2))
