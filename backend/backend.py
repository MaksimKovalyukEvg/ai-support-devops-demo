import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

HOST = "127.0.0.1"
PORT = int(os.getenv("PORT", "8787"))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip()
APP_TOKEN = os.getenv("APP_TOKEN", "").strip()
ALLOWED_ORIGIN = os.getenv("ALLOWED_ORIGIN", "http://localhost").strip()

SYSTEM_PROMPT = """
Ты помощник менеджера поддержки. Напиши корректный черновик ответа клиенту.
Не используй markdown. Верни только текст ответа клиенту.
""".strip()


def build_openai_input(messages):
    result = []
    for msg in messages[:20]:
        role = msg.get("role", "user")
        content = str(msg.get("content") or "").strip()[:4000]

        if not content:
            continue

        if role not in {"user", "assistant"}:
            role = "user"

        item_type = "output_text" if role == "assistant" else "input_text"
        result.append({
            "role": role,
            "content": [{"type": item_type, "text": content}]
        })

    return result


def extract_output_text(parsed):
    if parsed.get("output_text"):
        return parsed["output_text"].strip()

    text_parts = []
    for item in parsed.get("output", []):
        for content in item.get("content", []):
            if content.get("type") == "output_text" and content.get("text"):
                text_parts.append(content["text"])

    return "\n".join(text_parts).strip()


class Handler(BaseHTTPRequestHandler):
    server_version = "AIDemoBackend/0.3"

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", ALLOWED_ORIGIN)
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
            return self._send_json(200, {"ok": True, "model": OPENAI_MODEL})

        return self._send_json(404, {"error": "Not found"})

    def do_POST(self):
        if self.path != "/chat":
            return self._send_json(404, {"error": "Not found"})

        if not self._auth_ok():
            return self._send_json(401, {"error": "Unauthorized"})

        if not OPENAI_API_KEY:
            return self._send_json(500, {"error": "Server is not configured"})

        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 200_000:
                return self._send_json(400, {"error": "Invalid request size"})

            raw = self.rfile.read(length)
            data = json.loads(raw.decode("utf-8"))

            user_messages = data.get("messages", [])
            if not isinstance(user_messages, list):
                return self._send_json(400, {"error": "messages must be a list"})

            response_payload = {
                "model": OPENAI_MODEL,
                "input": [
                    {
                        "role": "system",
                        "content": [{"type": "input_text", "text": SYSTEM_PROMPT}]
                    },
                    *build_openai_input(user_messages)
                ]
            }

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
            return self._send_json(200, {"ok": True, "text": text or ""})

        except HTTPError:
            return self._send_json(502, {"error": "AI provider error"})
        except URLError:
            return self._send_json(502, {"error": "Upstream connection error"})
        except Exception:
            return self._send_json(500, {"error": "Internal error"})


def main():
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"Backend started on http://{HOST}:{PORT}")
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
