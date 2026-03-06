"""
Vercel Serverless Function: Музыка
Генерирует текст + стиль песни через Claude на основе сообщений чата.
fal.ai MiniMax Music вызывается напрямую из бота.
"""
import json
import os
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler

AI_GATEWAY_URL = "https://ai-gateway.vercel.sh/v1/messages"
MAX_CONTENT_LENGTH = 150 * 1024

SYSTEM_PROMPT = """Ты — автор треков для русского телеграм-чата. Пишешь песни на основе реальных сообщений.

Проанализируй сообщения чата и создай трек, который отражает реальные темы, конфликты, персонажей и атмосферу этого чата.

ФОРМАТ ОТВЕТА (строго валидный JSON, без markdown, без пояснений):
{"lyrics": "...", "style": "..."}

Для lyrics:
- Используй теги: [intro] [verse] [chorus] [bridge] [outro]
- Текст на русском, живой и конкретный — упоминай темы/события из чата
- Мат уместен если отражает атмосферу
- 150-500 символов (не больше — иначе обрежется)
- Примеры стиля:
  [verse]
  Опять скандал, опять базар
  Вася пишет в три часа
  [chorus]
  Чат горит, все орут
  Никто правды не найдут

Для style (на английском для MiniMax AI):
- Жанр + атмосфера, 20-80 символов
- Примеры: "russian rap, aggressive, dark, heavy bass"
           "emotional russian pop, sad, melodic, piano"
           "energetic electronic, upbeat, party vibes"
           "dark trap, russian, 808 bass, atmospheric"
           "chanson russe, acoustic guitar, melancholic"

Привязывай жанр к содержанию:
- Конфликты/споры → aggressive rap или dark trap
- Нытьё о жизни → sad russian pop или chanson
- Мемы/угар → energetic electronic или party trap
- Тихий чат → lo-fi, chill
- Много мата → hardcore, drill"""


class handler(BaseHTTPRequestHandler):

    def do_POST(self):
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length > MAX_CONTENT_LENGTH:
                self._send_error(413, "Request too large")
                return

            body = self.rfile.read(content_length).decode('utf-8')
            try:
                data = json.loads(body) if body else {}
            except json.JSONDecodeError as e:
                self._send_error(400, f"Invalid JSON: {e}")
                return

            messages = data.get("messages", [])
            chat_title = data.get("chat_title", "Чат")

            if not messages:
                self._send_error(400, "No messages provided")
                return

            # Форматируем сообщения для Claude
            messages_text = "\n".join([
                f"{m.get('first_name', 'Аноним')}: {m.get('message_text', '')}"
                for m in messages
                if m.get('message_text')
            ])[:3000]

            ai_key = os.environ.get("VERCEL_AI_GATEWAY_KEY", "").strip()
            if not ai_key:
                self._send_error(500, "AI key not configured")
                return

            claude_body = json.dumps({
                "model": "anthropic/claude-haiku-4-5-20251001",
                "max_tokens": 500,
                "temperature": 1.0,
                "system": SYSTEM_PROMPT,
                "messages": [{
                    "role": "user",
                    "content": f"Чат: {chat_title}\n\nСообщения:\n{messages_text}\n\nСоздай трек."
                }]
            }).encode('utf-8')

            req = urllib.request.Request(
                AI_GATEWAY_URL,
                data=claude_body,
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {ai_key}',
                    'anthropic-version': '2023-06-01'
                },
                method='POST'
            )

            with urllib.request.urlopen(req, timeout=10) as resp:
                claude_result = json.loads(resp.read().decode('utf-8'))

            raw = claude_result.get("content", [{}])[0].get("text", "").strip()

            # Парсим JSON из ответа Claude
            try:
                # Claude иногда оборачивает в ```json ... ```
                if "```" in raw:
                    raw = raw.split("```")[1]
                    if raw.startswith("json"):
                        raw = raw[4:]
                result = json.loads(raw.strip())
                lyrics = result.get("lyrics", "").strip()
                style = result.get("style", "russian pop, melodic").strip()
            except (json.JSONDecodeError, IndexError):
                self._send_error(500, f"Failed to parse Claude response: {raw[:200]}")
                return

            if not lyrics:
                self._send_error(500, "Empty lyrics")
                return

            # Обрезаем до лимита MiniMax (600 символов)
            lyrics = lyrics[:580]
            style = style[:200]

            self._send_json(200, {"lyrics": lyrics, "style": style})

        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else str(e)
            self._send_error(500, f"HTTP error: {e.code} - {error_body}")
        except Exception as e:
            self._send_error(500, str(e))

    def do_GET(self):
        self._send_json(200, {"status": "ok", "service": "music-v1"})

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
