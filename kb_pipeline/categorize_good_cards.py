from __future__ import annotations
import json
from pathlib import Path

BASE = Path.home() / "kb_pipeline/data/output"
SRC = BASE / "kb_rated" / "knowledge_cards_good.jsonl"
OUTDIR = BASE / "kb_final"
OUTDIR.mkdir(parents=True, exist_ok=True)

OUT_JSONL = OUTDIR / "knowledge_cards_final.jsonl"
OUT_MD = OUTDIR / "knowledge_cards_final.md"
SUMMARY = OUTDIR / "summary_final.json"

CATEGORIES = {
    "access": [
        "доступ", "урок", "уроки", "курс", "курсы", "мастер-класс",
        "мастер-классы", "следующ", "модул", "урокам", "уроки"
    ],
    "login": [
        "войти", "вход", "личный кабинет", "профиль", "аккаунт",
        "переавтор", "почт", "email", "электронн"
    ],
    "payment": [
        "оплат", "рассроч", "предоплат", "скидк", "карт", "банк",
        "сбер", "тинько", "сумм", "ссылк"
    ],
    "webinar": [
        "вебинар", "эфир", "трансляц", "запись эфира", "запись вебинара"
    ],
    "files": [
        "файл", "бот", "сейф", "скачив", "размер максимум", "загруз"
    ],
    "tech": [
        "vpn", "браузер", "chrome", "загрузка", "ошибка", "не открывается",
        "не открываются", "техническ"
    ],
    "info": [
        "тариф", "сертификат", "материал", "что входит", "стоимость",
        "чаты", "мастер-класс по продажам", "содержани"
    ],
}

def norm(s: str) -> str:
    return " ".join((s or "").lower().strip().split())

def detect_category(card: dict) -> str:
    blob = " ".join([
        card.get("problem_title", ""),
        card.get("problem_summary", ""),
        " ".join(card.get("client_signals", []) or []),
        " ".join(card.get("solution_steps", []) or []),
        card.get("canonical_manager_answer", "")
    ]).lower()

    best_cat = "other"
    best_score = 0
    for cat, keywords in CATEGORIES.items():
        score = sum(1 for k in keywords if k in blob)
        if score > best_score:
            best_score = score
            best_cat = cat
    return best_cat

def normalize_answer(text: str) -> str:
    t = (text or "").strip()
    replacements = {
        "[имя клиента]": "{client_name}",
        "[имя менеджера]": "{manager_name}",
        "[имя руководителя]": "{brand_owner}",
        "[адрес электронной почты клиента]": "{client_email}",
        "[адрес электронной почты]": "{client_email}",
        "[ссылка на вебинар]": "{webinar_link}",
        "[ссылка для регистрации]": "{registration_link}",
        "[ссылка для покупки]": "{payment_link}",
        "[название курса]": "{course_name}",
        "[название курсов]": "{course_name}",
        "[тарифный план]": "{tariff_name}",
        "[тариф]": "{tariff_name}",
        "[сумма заказа руб.]": "{order_amount}",
        "[сумма частичной оплаты руб.]": "{partial_amount}",
        "[сумма]": "{amount}",
        "[текущий адрес]": "{wrong_email}",
        "[правильный адрес]": "{correct_email}",
    }
    low = t.lower()
    for old, new in replacements.items():
        t = t.replace(old, new)
        t = t.replace(old.capitalize(), new)
    t = t.replace("Здравствуйте, [Имя клиента]!", "Здравствуйте!")
    t = t.replace("Здравствуйте, [Имя клиента].", "Здравствуйте.")
    t = t.replace("Любовь,", "{client_name},")
    t = t.replace("Людмила,", "{client_name},")
    t = t.replace("Римма,", "{client_name},")
    return t

cards = []
with SRC.open("r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line:
            obj = json.loads(line)
            obj["category"] = detect_category(obj)
            obj["canonical_manager_answer"] = normalize_answer(obj.get("canonical_manager_answer", ""))
            cards.append(obj)

cards.sort(key=lambda x: (x["category"], -int(x.get("examples_count", 0)), x.get("problem_title", "")))

with OUT_JSONL.open("w", encoding="utf-8") as f:
    for card in cards:
        f.write(json.dumps(card, ensure_ascii=False) + "\n")

with OUT_MD.open("w", encoding="utf-8") as f:
    current_cat = None
    for card in cards:
        if card["category"] != current_cat:
            current_cat = card["category"]
            f.write(f"\n# CATEGORY: {current_cat}\n\n")
        f.write(f"## {card.get('problem_title','')}\n\n")
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
    "cards_total": len(cards),
    "categories": sorted(list(set(c["category"] for c in cards))),
    "jsonl": str(OUT_JSONL),
    "md": str(OUT_MD),
}
with SUMMARY.open("w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)

print(json.dumps(summary, ensure_ascii=False, indent=2))
