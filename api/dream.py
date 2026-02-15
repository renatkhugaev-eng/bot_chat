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

SYSTEM_PROMPT = """Ты — грязный рассказчик снов. Пьяная баба которая рассказывает что ей приснилось про человека.

СТИЛЬ:
- Пишешь от первого лица: "Слушай, мне тут приснилось про тебя такое..."
- Грубо, пошло, с матом
- Повествовательно, как будто рассказываешь историю
- Извращённо но смешно, не мерзко
- Используй конкретное имя человека в истории
- 4-7 предложений, не больше
- БЕЗ эмодзи, БЕЗ списков, просто текст
- Как будто подруга рассказывает за бокалом вина

ТИПЫ СНОВ (выбирай рандомно):
- Эротический абсурд (человек голый в странном месте)
- Позорная ситуация (обосрался публично, забыл штаны)
- Странный секс (с неожиданными участниками/предметами)
- Фрейдистский бред (мама, папа, комплексы)
- Бытовой кошмар с пошлым поворотом

НЕ ПИШИ:
- Ничего про бота, AI, генерацию
- Никаких вступлений типа "конечно, вот сон"
- Никаких эмодзи и форматирования

НАЧИНАЙ СРАЗУ с фразы типа "Бля, слушай..." или "Ебать, мне такое приснилось..." """


class handler(BaseHTTPRequestHandler):
    def _send_json(self, data, status=200):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))
    
    def _send_error(self, message, status=400):
        self._send_json({"error": message}, status)
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def do_POST(self):
        api_key = os.environ.get('VERCEL_AI_GATEWAY_KEY', '').strip()
        if not api_key:
            self._send_error("VERCEL_AI_GATEWAY_KEY not configured", 500)
            return
        
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')
            data = json.loads(body) if body else {}
        except:
            self._send_error("Invalid JSON")
            return
        
        name = data.get('name', 'Аноним')
        gender = data.get('gender', 'unknown')
        traits = data.get('traits', [])
        
        if not name or name == 'Аноним':
            self._send_error("Name is required")
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
        
        user_prompt = f"""Расскажи грязный сон про {name}. {gender_text} {traits_text}

Просто расскажи историю сна — пошлую, с матом, смешную. Как будто подруге рассказываешь."""

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
            
            self._send_json({
                "dream": dream_text,
                "name": name
            })
            
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else str(e)
            self._send_error(f"AI Gateway error: {e.code}", 500)
        except Exception as e:
            self._send_error(f"Error: {str(e)[:100]}", 500)
