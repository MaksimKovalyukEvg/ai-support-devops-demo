import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

HOST = "127.0.0.1"
PORT = 8787

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.4-mini").strip()
APP_TOKEN = os.getenv("APP_TOKEN", "change-me-demo-token").strip()

SYSTEM_PROMPT = """Ты помощник менеджера поддержки онлайн-школы по творчеству и рукоделию.
Твоя задача — писать содержательные, уместные и доброжелательные черновики ответов клиенту.
Отвечай по-русски.
Не используй markdown.
Не пиши, что ты ИИ.
Не добавляй служебные комментарии.
Верни только текст ответа клиенту.
"""

def build_openai_input(messages):
    parts = []
    for msg in messages:
        role = msg.get("role", "user")
        content = (msg.get("content") or "").strip()
        if not content:
            continue

        if role == "assistant":
            parts.append({
                "role": "assistant",
                "content": [{"type": "output_text", "text": content}]
            })
        elif role in ("user", "system", "developer"):
            parts.append({
                "role": role,
                "content": [{"type": "input_text", "text": content}]
            })
        else:
            parts.append({
                "role": "user",
                "content": [{"type": "input_text", "text": content}]
            })
    return parts

def extract_output_text(parsed):
    text = parsed.get("output_text")
    if text:
        return text.strip()

    text_parts = []
    for item in parsed.get("output", []):
        for content in item.get("content", []):
            if content.get("type") == "output_text" and content.get("text"):
                text_parts.append(content["text"])

    return "\n".join(text_parts).strip()

class Handler(BaseHTTPRequestHandler):
    server_version = "GCAIDemoBackend/0.2"

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-App-Token")

    def _send_json(self, status_code, payload):
        self.send_response(status_code)
        self._cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(json.dumps(payload, ensure_ascii=False).encode("utf-8"))

    def _auth_ok(self):
        token = self.headers.get("X-App-Token", "").strip()
        return bool(APP_TOKEN) and token == APP_TOKEN

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            self._send_json(200, {
                "ok": True,
                "model": OPENAI_MODEL,
                "has_openai_key": bool(OPENAI_API_KEY),
                "has_app_token": bool(APP_TOKEN)
            })
            return

        self._send_json(404, {"error": "Not found"})

    def do_POST(self):
        if self.path != "/chat":
            self._send_json(404, {"error": "Not found"})
            return

        if not self._auth_ok():
            self._send_json(401, {"error": "Unauthorized"})
            return

        if not OPENAI_API_KEY:
            self._send_json(500, {"error": "OPENAI_API_KEY not set"})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            data = json.loads(raw.decode("utf-8"))

            user_messages = data.get("messages", [])
            system_prompt = (data.get("system_prompt") or SYSTEM_PROMPT).strip()
            model = (data.get("model") or OPENAI_MODEL).strip()

            response_payload = {
                "model": model,
                "input": [
                    {
                        "role": "system",
                        "content": [{"type": "input_text", "text": system_prompt}]
                    },
                    *build_openai_input(user_messages)
                ]
            }

            # ВАЖНО: ensure_ascii=True убирает проблему latin-1 на кириллице
            request_body = json.dumps(response_payload, ensure_ascii=True).encode("utf-8")

            req = Request(
                "https://api.openai.com/v1/responses",
                data=request_body,
                headers={
                    "Content-Type": "application/json; charset=utf-8",
                    "Authorization": f"Bearer {OPENAI_API_KEY}"
                },
                method="POST"
            )

            with urlopen(req, timeout=120) as resp:
                body = resp.read().decode("utf-8")
                parsed = json.loads(body)

            text = extract_output_text(parsed)

            self._send_json(200, {
                "ok": True,
                "text": text or ""
            })

        except HTTPError as e:
            body = e.read().decode("utf-8", errors="ignore")
            try:
                details = json.loads(body)
            except Exception:
                details = body

            self._send_json(e.code, {
                "error": "OpenAI HTTPError",
                "details": details
            })

        except URLError as e:
            self._send_json(502, {
                "error": "Upstream connection error",
                "details": str(e)
            })

        except Exception as e:
            self._send_json(500, {
                "error": "Internal error",
                "details": str(e)
            })

def main():
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"Backend started on http://{HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping backend...")
    finally:
        server.server_close()

if __name__ == "__main__":
    main()