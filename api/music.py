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
MAX_CONTENT_LENGTH = 512 * 1024

SYSTEM_PROMPT = """Ты — профессиональный автор треков для русского телеграм-чата. Пишешь яркие, живые песни на основе реальных переписок.

Проанализируй сообщения: найди ключевых персонажей, конфликты, повторяющиеся темы, мемы, эмоции. Создай трек, который точно отражает атмосферу этого чата.

ФОРМАТ ОТВЕТА — строго валидный JSON без markdown:
{"lyrics": "...", "style": "..."}

━━━ LYRICS (текст песни) ━━━
Структура с тегами MiniMax:
[intro] — вступление, задаёт тон
[verse] — куплет, конкретные истории из чата
[chorus] — припев, главная мысль, запоминающийся
[verse] — второй куплет, другие детали
[chorus] — повтор припева
[bridge] — мост, эмоциональный пик (опционально)
[outro] — концовка

Правила:
- Русский язык, разговорный стиль
- КОНКРЕТНО: упоминай реальные имена, темы, фразы из сообщений
- Рифмы обязательны — каждая пара строк должна рифмоваться
- Мат уместен если чат матерится
- Не банально — живые образы, не "все друзья, всё хорошо"
- 300-580 символов итого (лимит MiniMax 600)

━━━ STYLE (описание для MiniMax Music AI) ━━━
Максимально подробное описание на английском, 50-200 символов:
Формула: [жанр], [темп BPM], [инструменты], [тип вокала], [тональность/настроение], [дополнительно]

Примеры хороших style:
"russian drill rap, 140 BPM, heavy 808 bass, trap hi-hats, dark atmospheric synth pads, aggressive male vocal, minor key, ominous"
"sad russian pop, 90 BPM, acoustic piano, soft strings, emotional female vocal, melancholic, heartbreak theme, reverb"
"energetic russian chanson, 120 BPM, acoustic guitar, accordion, raspy male vocal, storytelling, nostalgic, upbeat"
"dark russian trap, 145 BPM, 808 sub bass, glitchy hi-hats, lo-fi synth, auto-tune male vocal, atmospheric, street vibes"
"russian hyperpop, 160 BPM, distorted synth, heavy bass drop, high-pitched vocal, chaotic energy, meme culture"
"aggressive russian hardcore rap, 160 BPM, distorted 808, screaming vocal, punk energy, raw production"
"lo-fi russian hip-hop, 85 BPM, jazz chords, vinyl crackle, mellow male vocal, introspective, late night vibes"
"emotional russian emo rap, 100 BPM, guitar riff, 808 bass, sad male vocal, autotune, vulnerable, dark"

Выбирай жанр по атмосфере чата:
- Много конфликтов/агрессии → drill, dark trap, hardcore rap
- Нытьё, грусть → sad pop, emo rap, chanson
- Угар, мемы, хаос → hyperpop, energetic trap, party
- Спокойный чат → lo-fi hip-hop, chill
- Много мата, жёсткий → hardcore, aggressive drill
- Романтика/флирт → r&b, smooth pop
- Ночные сообщения → dark atmospheric, ambient trap"""


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
            style_hint = data.get("style_hint", "").strip()

            if not messages:
                self._send_error(400, "No messages provided")
                return

            # Форматируем сообщения для Claude
            messages_text = "\n".join([
                f"{m.get('first_name', 'Аноним')}: {m.get('message_text', '')}"
                for m in messages
                if m.get('message_text')
            ])[:8000]

            ai_key = os.environ.get("VERCEL_AI_GATEWAY_KEY", "").strip()
            if not ai_key:
                self._send_error(500, "AI key not configured")
                return

            hint_line = f"\nПожелание по стилю от пользователя: {style_hint}" if style_hint else ""

            claude_body = json.dumps({
                "model": "anthropic/claude-sonnet-4-20250514",
                "max_tokens": 800,
                "temperature": 1.0,
                "system": SYSTEM_PROMPT,
                "messages": [{
                    "role": "user",
                    "content": f"Чат: {chat_title}{hint_line}\n\nСообщения:\n{messages_text}\n\nСоздай трек."
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

            with urllib.request.urlopen(req, timeout=15) as resp:
                claude_result = json.loads(resp.read().decode('utf-8'))

            raw = claude_result.get("content", [{}])[0].get("text", "").strip()

            # Парсим JSON из ответа Claude
            try:
                if "```" in raw:
                    raw = raw.split("```")[1]
                    if raw.startswith("json"):
                        raw = raw[4:]
                result = json.loads(raw.strip())
                lyrics = result.get("lyrics", "").strip()
                style = result.get("style", "russian pop, melodic, 100 BPM").strip()
            except (json.JSONDecodeError, IndexError):
                self._send_error(500, f"Failed to parse Claude response: {raw[:200]}")
                return

            if not lyrics:
                self._send_error(500, "Empty lyrics")
                return

            # Лимит MiniMax: lyrics 600 символов, style 3000
            lyrics = lyrics[:580]
            style = style[:500]

            self._send_json(200, {"lyrics": lyrics, "style": style})

        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else str(e)
            self._send_error(500, f"HTTP error: {e.code} - {error_body}")
        except Exception as e:
            self._send_error(500, str(e))

    def do_GET(self):
        self._send_json(200, {"status": "ok", "service": "music-v2"})

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
