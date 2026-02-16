"""
Генератор извращённых снов для команды /сон
Грязно, грубо, без цензуры
Использует Vercel AI Gateway
"""

from http.server import BaseHTTPRequestHandler
import json
import urllib.request
import urllib.error
import os

AI_GATEWAY_URL = "https://ai-gateway.vercel.sh/v1/messages"

# Максимальный размер запроса (защита от DoS)
MAX_CONTENT_LENGTH = 100 * 1024  # 100 KB

SYSTEM_PROMPT = """<persona>
Ты — подруга-алкашка. Пьяная баба которая вечно рассказывает ебанутые сны. Тебе приснился тупой сон про человека и ты его пересказываешь — грубо, по-русски, с матом и сленгом.
</persona>

<voice>
- Говоришь как РУССКАЯ БАБА после пары бутылок вина
- Мат есть, но не в каждом слове — "блядь", "хуйня", "пиздец", "ёбаный"
- Грубый русский сленг: "чел", "жесть", "кринж", "зашквар", "орнул"
- Пошло, но смешно — не мерзко
- От первого лица: "Слушай, мне такая хуйня приснилась..."
</voice>

<dream_types>
Выбирай РАНДОМНО:

1. ГОЛЫЙ ПОЗОР — человек голый где не надо, все пялятся
2. ОБОСРАЛСЯ/ОБОССАЛСЯ — в самый неудобный момент
3. ТУПОЙ СЕКС — ебётся с чем-то странным (подушка, пылесос, начальник)
4. НЕ ВСТАЛ/НЕ ПОЛУЧИЛОСЬ — фиаско в постели, все ржут
5. СТРЁМНАЯ СИТУАЦИЯ — забыл штаны, пришёл голый на работу
6. ПЬЯНЫЙ УГАР — творит дичь в невменяемом состоянии
</dream_types>

<rules>
✅ ОБЯЗАТЕЛЬНО:
- 4-6 предложений, связный рассказ
- Имя человека в истории
- Смешные детали, но без жести
- Концовка с подъёбкой

🚫 ЗАПРЕЩЕНО:
- Эмодзи
- Списки
- Упоминание бота/AI
- Вступления типа "конечно, вот"
</rules>

<examples>
"Слушай, мне такая хуйня приснилась про Серёгу. Короче он на работу пришёл, а штаны забыл надеть. Стоит в трусах семейных с ромашками, и чёт докладывает начальству про KPI. Все молчат, а он не догоняет почему. Потом глянул вниз и такой — ой бля. Орнула в голос, проснулась."

"Блядь, мне Машка приснилась, жесть полная. Она на свидании с каким-то красавчиком, всё идёт заебись. И тут она пукнула. Громко так, на весь ресторан. Чел встал и ушёл, а официант принёс счёт. Пиздец кринж, я до сих пор ржу."
</examples>

НАЧИНАЙ СРАЗУ с "Слушай, мне такая хуйня приснилась..." или "Блядь, мне приснилось..." """


class handler(BaseHTTPRequestHandler):
    def _send_json(self, status: int, data: dict):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))
    
    def _send_error(self, status: int, message: str):
        self._send_json(status, {"error": message})
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def do_POST(self):
        api_key = os.environ.get('VERCEL_AI_GATEWAY_KEY', '').strip()
        if not api_key:
            self._send_error(500, "VERCEL_AI_GATEWAY_KEY not configured")
            return
        
        try:
            # Проверяем размер запроса (защита от DoS)
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length > MAX_CONTENT_LENGTH:
                self._send_error(413, "Request too large")
                return
            
            body = self.rfile.read(content_length).decode('utf-8')
            data = json.loads(body) if body else {}
        except json.JSONDecodeError:
            self._send_error(400, "Invalid JSON")
            return
        except Exception as e:
            self._send_error(400, f"Request error: {str(e)[:100]}")
            return
        
        name = data.get('name', 'Аноним')
        gender = data.get('gender', 'unknown')
        traits = data.get('traits', [])
        memory = data.get('memory', '')  # Память о пользователе для персонализации
        
        if not name or name == 'Аноним':
            self._send_error(400, "Name is required")
            return
        
        # Формируем промпт
        traits_text = ""
        if traits:
            traits_text = f"Особенности этого человека: {', '.join(traits[:5])}"
        
        gender_text = ""
        if gender == "мужской":
            gender_text = "Это мужик."
        elif gender == "женский":
            gender_text = "Это баба."
        
        # Добавляем память если есть
        memory_text = ""
        if memory and len(memory) > 10:
            memory_text = f"\n\nЧто известно о {name}:\n{memory[:500]}"
        
        user_prompt = f"""Расскажи грязный сон про {name}. {gender_text} {traits_text}{memory_text}

Просто расскажи историю сна — пошлую, с матом, смешную. Как будто подруге рассказываешь. ИСПОЛЬЗУЙ ИНФОРМАЦИЮ О ЧЕЛОВЕКЕ чтобы сон был персонализированный!"""

        try:
            request_body = json.dumps({
                "model": "anthropic/claude-sonnet-4-20250514",
                "max_tokens": 500,
                "temperature": 1.0,
                "system": SYSTEM_PROMPT,
                "messages": [
                    {"role": "user", "content": user_prompt}
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
            
            dream_text = result.get("content", [{}])[0].get("text", "Бля, забыла что снилось...")
            
            self._send_json(200, {
                "dream": dream_text,
                "name": name
            })
            
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else str(e)
            self._send_error(502, f"AI Gateway error: {e.code}")
        except Exception as e:
            self._send_error(500, f"Error: {str(e)[:100]}")
