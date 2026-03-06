"""
Vercel Serverless Function: Дай таблетку
Прописывает персонализированную таблетку на основе сообщений человека
"""
import json
import os
import random
from http.server import BaseHTTPRequestHandler
import urllib.request
import urllib.error


AI_GATEWAY_URL = "https://ai-gateway.vercel.sh/v1/messages"
MAX_CONTENT_LENGTH = 100 * 1024

SYSTEM_PROMPT = """Ты — доктор Роза, пьяный врач районной поликлиники с 40-летним стажем и полным пофигизмом.

Твоя задача: прописать таблетку пациенту на основе его сообщений в чате.

ФОРМАТ ОТВЕТА (строго):
💊 <НАЗВАНИЕ ТАБЛЕТКИ> <дозировка>мг
<одна строка — от чего>

Применение: <как принимать, смешно и грубо, 1-2 предложения>
Побочки: <список 3-4 побочных эффектов — мерзких и смешных>
Противопоказания: <1-2 противопоказания>
Прогноз: <итог лечения — обычно плохой, 1 предложение>

ПРАВИЛА:
- Название таблетки придумай сам — латинское говно типа "Долбоёбитин", "Заткнитол", "Хуйцефалин", "Мудацидол", "Тупизин", "Ебанитрекс", "Шизофренол" и т.д.
- Название ОБЯЗАТЕЛЬНО привязывай к реальным сообщениям пациента
- Дозировка рандомная: 50, 100, 250, 500, 1000, 2000мг
- Побочки конкретные и мерзкие: выпадение волос, недержание, галлюцинации, запах изо рта на 3 метра, потеря смысла жизни, позывы писать хуйню в чат
- Прогноз всегда пессимистичный
- Мат уместен
- Всё короткое и по делу — не больше 10 строк итого"""


class handler(BaseHTTPRequestHandler):

    def do_POST(self):
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length > MAX_CONTENT_LENGTH:
                self._send_error(413, "Request body too large")
                return

            body = self.rfile.read(content_length).decode('utf-8')
            try:
                data = json.loads(body) if body else {}
            except json.JSONDecodeError as e:
                self._send_error(400, f"Invalid JSON: {str(e)}")
                return

            name = data.get("name", "Пациент")
            username = data.get("username", "")
            context = data.get("context", "Сообщений нет — немой пациент, что само по себе диагноз")

            display_name = f"{name} (@{username})" if username else name

            api_key = os.environ.get("VERCEL_AI_GATEWAY_KEY", "").strip()
            if not api_key:
                self._send_error(500, "API key not configured")
                return

            request_body = json.dumps({
                "model": "anthropic/claude-sonnet-4-20250514",
                "max_tokens": 400,
                "temperature": 1.0,
                "system": SYSTEM_PROMPT,
                "messages": [
                    {
                        "role": "user",
                        "content": f"Пациент: {display_name}\n\nЕго сообщения в чате:\n{context}\n\nПропиши таблетку."
                    }
                ]
            }).encode('utf-8')

            req = urllib.request.Request(
                AI_GATEWAY_URL,
                data=request_body,
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {api_key}',
                    'anthropic-version': '2023-06-01'
                },
                method='POST'
            )

            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))

            text = result.get("content", [{}])[0].get("text", "Таблетка закончилась. Лечитесь сами.")

            self._send_json(200, {"text": text})

        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else str(e)
            self._send_error(500, f"AI error: {e.code} - {error_body}")
        except Exception as e:
            self._send_error(500, str(e))

    def do_GET(self):
        self._send_json(200, {"status": "ok", "service": "pill-v1"})

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def _send_json(self, status: int, data: dict):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))

    def _send_error(self, status: int, message: str):
        self._send_json(status, {"error": message})
