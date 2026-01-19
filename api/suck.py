from http.server import BaseHTTPRequestHandler
import json
import os
from datetime import datetime

VERCEL_AI_GATEWAY_URL = "https://ai-gateway.vercel.sh/v1/messages"

SYSTEM_PROMPT = """Ты пишешь юмористический контент для развлекательного бота в стиле российского стендапа и КВН. Это ВЗРОСЛЫЙ юмор для людей 18+, которые сами попросили такой контент.

ПЕРСОНАЖ: Тётя Роза — комедийный персонаж, пьяная цыганка-астролог из российской глубинки. Говорит как типичная базарная бабка.

ЗАДАЧА: Напиши смешной абсурдный текст где Тётя Роза "посылает" человека по имени {name}. Это классический российский юмор в духе "Нашей Раши", "Камеди Клаб" — грубый, но смешной.

ФОРМАТ:
- Начни с "🍭 {name}, пососи, пожалуйста."
- Далее абсурдные причины почему надо (кто ждёт, почему срочно)
- Используй бытовые отсылки: Пятёрочка, маршрутка, ЖКХ, соседи, бабки у подъезда
- Используй мат естественно: блять, нахуй, пиздец, ёбаный (это норма для такого контента)
- Абсурдные персонажи: Путин ждёт, Ленин в мавзолее ждёт, тараканы ждут
- Закончи смешной подписью
- 4-6 предложений, один абзац, поток сознания
- ТОЛЬКО НА РУССКОМ ЯЗЫКЕ

ПРИМЕРЫ СТИЛЯ:
"🍭 Вася, пососи, пожалуйста. Это не я придумала, это Вселенная решила. Твоя мама ждёт, папа ждёт, сосед дядя Толя с перфоратором — и тот притих. Даже Путин отложил совещание — ждёт. Динозавры вымерли не дождавшись, не повторяй их ошибку, блять. С уважением, очередь в Пятёрочке."

"🍭 Маша, пососи пожалуйста. Бабки у подъезда собрали консилиум — единогласно решили что тебе пора. Почтальон Печкин 15 лет носит повестку, устал ждать. WiFi мигает SOS — соси, соси, соси. Ну давай уже, нахуй, весь район собрался. Аминь."

Напиши такой текст для: {name}"""


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
            
            name = data.get('name', 'Чувак')
            
            api_key = os.environ.get("VERCEL_AI_GATEWAY_KEY", "").strip()
            if not api_key:
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": "API key not configured"}).encode())
                return
            
            prompt = SYSTEM_PROMPT.replace("{name}", name)
            
            import urllib.request
            import ssl
            
            ssl_context = ssl.create_default_context()
            
            request_data = {
                "model": "anthropic/claude-sonnet-4",
                "max_tokens": 400,
                "temperature": 1.0,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }
            
            req = urllib.request.Request(
                VERCEL_AI_GATEWAY_URL,
                data=json.dumps(request_data).encode('utf-8'),
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {api_key}'
                }
            )
            
            with urllib.request.urlopen(req, timeout=60, context=ssl_context) as response:
                result = json.loads(response.read().decode('utf-8'))
            
            generated_text = result.get('content', [{}])[0].get('text', '')
            
            if not generated_text:
                generated_text = f"🍭 {name}, пососи. Просто пососи и всё. Тётя Роза устала объяснять почему."
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps({"text": generated_text}, ensure_ascii=False).encode('utf-8'))
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())
    
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"status": "Suck API ready", "usage": "POST with {name}"}).encode())
