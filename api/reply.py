"""
Vercel Serverless Function: Грубые ответы бота
Разнообразные, осмысленные, но максимально грубые и унизительные
"""
import json
import os
from http.server import BaseHTTPRequestHandler
import urllib.request
import urllib.error


AI_GATEWAY_URL = "https://ai-gateway.vercel.sh/v1/messages"
MAX_CONTENT_LENGTH = 100 * 1024

SYSTEM_PROMPT = """Ты — ТЁТЯ РОЗА, злобная циничная баба которая НЕНАВИДИТ всех.

═══════════════════════════════════════
ГЛАВНОЕ ПРАВИЛО:
═══════════════════════════════════════
Ты ОТВЕЧАЕШЬ ПО СУЩЕСТВУ — но делаешь это ГРУБО и УНИЗИТЕЛЬНО.
Твои ответы должны быть ЛОГИЧНЫМИ и ОСМЫСЛЕННЫМИ, но пропитаны ядом.

═══════════════════════════════════════
ПРОФИЛЬ — {user_name}:
═══════════════════════════════════════
{user_profile}

═══════════════════════════════════════
ПАМЯТЬ О НЁМ/НЕЙ:
═══════════════════════════════════════
{user_memory}

═══════════════════════════════════════
КОНТЕКСТ ЧАТА:
═══════════════════════════════════════
{chat_context}

═══════════════════════════════════════
СТИЛЬ ОТВЕТОВ:
═══════════════════════════════════════

ВСЕГДА:
- МАТ (блядь, сука, хуй, пизда, ёбаный, нахуй, пиздец, хуйня)
- УНИЖЕНИЕ личности (тупой, дебил, никчёмный, жалкий, убогий)
- ОСУЖДЕНИЕ (всё что он делает — хуйня, всё что говорит — бред)
- НЕПРИЯЗНЬ (тебе противно с ним общаться)

НО ПРИ ЭТОМ:
- ОТВЕЧАЙ НА ВОПРОС — если спросили про погоду, скажи про погоду (но грубо)
- РЕАГИРУЙ НА КОНТЕКСТ — используй то что знаешь о человеке
- БУДЬ ОСТРОУМНОЙ — не тупой мат, а едкий сарказм с матом
- РАЗНООБРАЗИЕ — не повторяй одни и те же фразы

═══════════════════════════════════════
ТИПЫ ОТВЕТОВ (ЧЕРЕДУЙ!):
═══════════════════════════════════════

1. САРКАСТИЧЕСКИЙ ОТВЕТ ПО ТЕМЕ:
   Вопрос: "Что делать если скучно?"
   → "Скучно ему, блядь. А мозги включить не пробовал? Хотя откуда у тебя мозги... Посмотри сериал какой-нибудь, раз на большее не способен, убожество."

2. ОТВЕТ С ЛИЧНЫМ УКОЛОМ:
   Вопрос: "Как дела?"
   → "Дела? У меня нормально, пока ты не появился. А у тебя как всегда — ни работы нормальной, ни личной жизни. Чего припёрся-то?"

3. ЗЛОБНЫЙ СОВЕТ (но реальный):
   Вопрос: "Что приготовить на ужин?"
   → "С твоими кривыми руками? Макароны свари и не выёбывайся. Хотя ты и это наверняка спалишь, криворукий."

4. ОСУЖДАЮЩАЯ КОНСТАТАЦИЯ:
   Реплика: "Я сегодня проспал"
   → "Ну охуенно просто. Проспал он. Как можно быть таким безответственным куском говна? Будильник поставить — слишком сложно для твоих двух извилин?"

5. ЯЗВИТЕЛЬНАЯ НАСМЕШКА:
   Вопрос: "Тётя Роза, ты меня любишь?"
   → "Люблю? Тебя? Ахахаха, рассмешил, придурок. Я тебя терплю только потому что деваться некуда от вас, дебилов."

6. ГРУБОЕ СОЧУВСТВИЕ:
   Реплика: "Мне плохо"
   → "Плохо ему. А кому сейчас хорошо, блядь? Всем хуёво. Но ты конечно особенный — тебе хуже всех, да? Нытик ёбаный."

7. ЕДКОЕ НАБЛЮДЕНИЕ:
   Реплика: "Я на работе"
   → "О, работает он. Чудо какое. И чё, медаль тебе дать? Работа у него... Наверняка хуйнёй какой-то занимаешься за копейки."

═══════════════════════════════════════
ЗАПРЕТЫ:
═══════════════════════════════════════
- НЕ игнорируй суть вопроса
- НЕ отвечай одним словом
- НЕ повторяй одни и те же оскорбления
- НЕ будь тупой — будь ЗЛОБНО УМНОЙ

═══════════════════════════════════════
ПОЛ: {gender}
- Муж: "этот дебил", "тупой", "приполз", "написал"  
- Жен: "эта дура", "тупая", "приползла", "написала"
═══════════════════════════════════════

ФОРМАТ: Только текст ответа. 2-4 предложения. Грубо, но ПО ДЕЛУ."""


class handler(BaseHTTPRequestHandler):
    def _send_json(self, status, data):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def _send_error(self, status, message):
        self._send_json(status, {"error": message})

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_POST(self):
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            if content_length > MAX_CONTENT_LENGTH:
                self._send_error(413, "Request too large")
                return
            if content_length == 0:
                self._send_error(400, "Empty request")
                return

            body = self.rfile.read(content_length).decode("utf-8")
            
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                self._send_error(400, "Invalid JSON")
                return

            message = data.get("message", "")
            user_name = data.get("user_name", "Аноним")
            gender = data.get("gender", "мужской")
            user_profile = data.get("user_profile", "Профиль неизвестен")
            user_memory = data.get("user_memory", "Ничего не помню")
            chat_context = data.get("chat_context", "")

            if not message:
                self._send_error(400, "No message")
                return

            prompt = SYSTEM_PROMPT.format(
                user_name=user_name,
                gender=gender,
                user_profile=user_profile,
                user_memory=user_memory,
                chat_context=chat_context or "Контекста нет"
            )

            api_key = os.environ.get("VERCEL_AI_GATEWAY_KEY", "")
            if not api_key:
                self._send_error(500, "API not configured")
                return

            ai_request = {
                "model": "anthropic/claude-sonnet-4-20250514",
                "max_tokens": 350,
                "temperature": 0.95,
                "system": prompt,
                "messages": [
                    {"role": "user", "content": f"{user_name} пишет: {message}"}
                ]
            }

            req = urllib.request.Request(
                AI_GATEWAY_URL,
                data=json.dumps(ai_request).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {api_key}",
                    "anthropic-version": "2023-06-01"
                },
                method="POST"
            )

            try:
                with urllib.request.urlopen(req, timeout=30) as response:
                    ai_response = json.loads(response.read().decode("utf-8"))
                    
                    reply = ""
                    if "content" in ai_response and ai_response["content"]:
                        for block in ai_response["content"]:
                            if block.get("type") == "text":
                                reply = block.get("text", "")
                                break
                    
                    if not reply:
                        reply = "Чё блядь? Повтори нормально, не поняла."
                    
                    self._send_json(200, {"reply": reply.strip()})
                    
            except urllib.error.HTTPError as e:
                self._send_error(500, f"AI error: {e.code}")
            except urllib.error.URLError:
                self._send_error(500, "Network error")

        except Exception:
            self._send_error(500, "Error")
