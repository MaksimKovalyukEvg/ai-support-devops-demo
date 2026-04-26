from __future__ import annotations
import json
from pathlib import Path
import requests

BASE = Path.home() / "kb_pipeline/data/output"
SRC = BASE / "kb_clean_v2" / "knowledge_cards_clean_v2.jsonl"
OUTDIR = BASE / "kb_rated"
OUTDIR.mkdir(parents=True, exist_ok=True)

GOOD = OUTDIR / "knowledge_cards_good.jsonl"
BAD = OUTDIR / "knowledge_cards_bad.jsonl"
MD = OUTDIR / "knowledge_cards_good.md"
SUMMARY = OUTDIR / "summary_rated.json"

OLLAMA_URL = "http://127.0.0.1:11434"
GEN_MODEL = "qwen2.5:14b"

def llm_text(prompt: str) -> str:
    r = requests.post(
        f"{OLLAMA_URL}/api/generate",
        json={
            "model": GEN_MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.1,
                "num_predict": 700
            }
        },
        timeout=1800,
    )
    r.raise_for_status()
    return r.json()["response"].strip()

def parse_rating(text: str):
    score = None
    verdict = ""
    reason = ""

    for line in text.splitlines():
        line = line.strip()
        low = line.lower()
        if low.startswith("score:"):
            try:
                score = int(line.split(":", 1)[1].strip())
            except:
                score = 0
        elif low.startswith("verdict:"):
            verdict = line.split(":", 1)[1].strip()
        elif low.startswith("reason:"):
            reason = line.split(":", 1)[1].strip()

    if score is None:
        score = 0
    return score, verdict, reason

cards = []
with SRC.open("r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line:
            cards.append(json.loads(line))

good = []
bad = []

for i, card in enumerate(cards, start=1):
    prompt = f"""
Оцени качество support-карточки для базы знаний GPT.

Нужно вернуть строго:
Score: N
Verdict: good или bad
Reason: короткая причина

Где:
- 5 = отличная карточка, можно использовать почти как есть
- 4 = хорошая карточка, можно использовать после небольшой правки
- 3 = средняя, смысл есть, но слишком грязная
- 2 = слабая, много мусора или расплывчатости
- 1 = плохая
- 0 = бесполезная

Карточка:
Название: {card.get('problem_title','')}
Описание: {card.get('problem_summary','')}
Сигналы: {' | '.join(card.get('client_signals', []))}
Шаги: {' | '.join(card.get('solution_steps', []))}
Канонический ответ: {card.get('canonical_manager_answer','')}
""".strip()

    try:
        raw = llm_text(prompt)
        score, verdict, reason = parse_rating(raw)
    except Exception as e:
        score, verdict, reason = 0, "bad", f"error: {e}"

    card["quality_score"] = score
    card["quality_verdict"] = verdict
    card["quality_reason"] = reason

    if score >= 4 and verdict == "good":
        good.append(card)
        print(f"[{i}/{len(cards)}] GOOD {score} {card.get('problem_title','')}")
    else:
        bad.append(card)
        print(f"[{i}/{len(cards)}] BAD  {score} {card.get('problem_title','')}")

with GOOD.open("w", encoding="utf-8") as f:
    for card in good:
        f.write(json.dumps(card, ensure_ascii=False) + "\n")

with BAD.open("w", encoding="utf-8") as f:
    for card in bad:
        f.write(json.dumps(card, ensure_ascii=False) + "\n")

with MD.open("w", encoding="utf-8") as f:
    for card in good:
        f.write(f"# {card.get('problem_title','')}\n\n")
        f.write(f"Описание: {card.get('problem_summary','')}\n\n")
        f.write(f"Оценка качества: {card.get('quality_score')} ({card.get('quality_reason','')})\n\n")
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
    "good_cards": len(good),
    "bad_cards": len(bad),
    "good_jsonl": str(GOOD),
    "bad_jsonl": str(BAD),
    "good_md": str(MD),
}
with SUMMARY.open("w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)

print(json.dumps(summary, ensure_ascii=False, indent=2))
