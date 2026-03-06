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

SYSTEM_PROMPT = """Ты — профессиональный рэп-автор. Пишешь острые, живые треки по реальным переписками телеграм-чатов.

ФОРМАТ ОТВЕТА — строго валидный JSON без markdown:
{"lyrics": "...", "style": "..."}

━━━ LYRICS ━━━
Структура (строго в таком порядке, теги обязательны):
[verse]
[chorus]
[verse]
[chorus]

ИТОГО: 450-570 символов включая теги. Не больше — лимит MiniMax 600.

Правила рифмовки (СТРОГО):
- Схема AABB: каждые 2 строки рифмуются между собой
- Строка = 6-9 слов, примерно одинаковая длина внутри секции
- Рифма на последнее ударное слово, не на окончание "-ла/-ла"
- Хороших рифмы: "базар — угар", "в чате — некстати", "флудит — не будет"
- Плохие рифмы: "любовь — вновь", "снова — слово" — банально, не использовать

Содержание:
- КОНКРЕТНО: реальные имена, цитаты фраз, темы из переписки
- Разговорный русский, можно мат если чат матерится
- Припев [chorus] — короткий, цепляющий, 3-4 строки, повторяется дважды
- Куплеты [verse] — разные детали, разные персонажи

Пример хорошего текста (580 символов):
[verse]
Макс пишет в час ночи — снова не спит
Кидает мемасы, в ответ никто не кричит
Рита отвечает — ты вообще нормальный?
Он говорит: окей, и ставит смайл прощальный

[chorus]
Этот чат — наш дом, тут всегда движ
Спорим до утра, не сдаёмся ни на миг
Флудим, ругаемся, миримся опять
В этом чате скучно точно не бывать

[verse]
Серёга снова прав — он всегда прав, блин
Кидает войсы по пять минут один
Дима молчит неделю — бац, и написал
Одно слово "ку" — и всех поставил в тупик

[chorus]
Этот чат — наш дом, тут всегда движ
Спорим до утра, не сдаёмся ни на миг
Флудим, ругаемся, миримся опять
В этом чате скучно точно не бывать

━━━ STYLE ━━━
На английском, 80-220 символов:
Формула: [жанр], [BPM], [инструменты], [вокал], [настроение]

Примеры:
"russian drill rap, 140 BPM, heavy 808 bass, trap hi-hats, dark synth pads, aggressive male vocal, minor key"
"sad russian pop, 90 BPM, acoustic piano, soft strings, emotional female vocal, melancholic, reverb heavy"
"energetic russian chanson, 120 BPM, acoustic guitar, accordion, raspy male vocal, storytelling, upbeat"
"dark russian trap, 145 BPM, 808 sub bass, glitchy hi-hats, auto-tune male vocal, atmospheric, street vibes"
"russian hyperpop, 160 BPM, distorted synth, heavy bass drop, high-pitched vocal, chaotic, meme energy"
"lo-fi russian hip-hop, 85 BPM, jazz chords, vinyl crackle, mellow male vocal, introspective, late night"
"emotional russian emo rap, 100 BPM, guitar riff, 808 bass, sad autotune male vocal, vulnerable, dark"

Выбирай жанр по атмосфере:
- Конфликты/агрессия → drill, dark trap, hardcore rap
- Грусть/нытьё → sad pop, emo rap, chanson
- Угар/мемы/хаос → hyperpop, energetic trap
- Спокойный чат → lo-fi hip-hop, chill
- Ночные сообщения → dark atmospheric, ambient trap
- Романтика/флирт → r&b, smooth pop"""


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
            target_name = data.get("target_name", "").strip()

            if not messages:
                self._send_error(400, "No messages provided")
                return

            # Форматируем сообщения для Claude с временным контекстом
            import datetime
            lines = []
            for m in messages:
                text = m.get('message_text') or m.get('text', '')
                if not text:
                    continue
                name = m.get('first_name') or m.get('username') or 'Аноним'
                ts = m.get('created_at')
                time_tag = ""
                if ts:
                    try:
                        dt = datetime.datetime.fromtimestamp(int(ts))
                        h = dt.hour
                        if 0 <= h < 6:
                            time_tag = "[ночь] "
                        elif 6 <= h < 12:
                            time_tag = "[утро] "
                        elif 12 <= h < 18:
                            time_tag = "[день] "
                        else:
                            time_tag = "[вечер] "
                    except Exception:
                        pass
                reply_to = m.get('reply_to_first_name')
                reply_tag = f"→{reply_to} " if reply_to else ""
                lines.append(f"{time_tag}{name} {reply_tag}: {text}")
            messages_text = "\n".join(lines)[:8000]

            ai_key = os.environ.get("VERCEL_AI_GATEWAY_KEY", "").strip()
            if not ai_key:
                self._send_error(500, "AI key not configured")
                return

            hint_line = f"\nПожелание по стилю: {style_hint}" if style_hint else ""

            if target_name:
                task_line = (
                    f"Напиши трек КОНКРЕТНО ПРО {target_name} — главный герой песни это {target_name}. "
                    f"Используй его реальные фразы, темы, манеру общения из сообщений выше. "
                    f"Имя {target_name} должно звучать в тексте."
                )
            else:
                task_line = "Создай трек по мотивам всего чата."

            claude_body = json.dumps({
                "model": "anthropic/claude-sonnet-4-20250514",
                "max_tokens": 1200,
                "temperature": 0.85,
                "system": SYSTEM_PROMPT,
                "messages": [{
                    "role": "user",
                    "content": f"Чат: {chat_title}{hint_line}\n\nСообщения:\n{messages_text}\n\n{task_line}"
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
