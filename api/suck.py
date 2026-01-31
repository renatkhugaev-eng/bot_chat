from http.server import BaseHTTPRequestHandler
import json
import os
from datetime import datetime

VERCEL_AI_GATEWAY_URL = "https://ai-gateway.vercel.sh/v1/messages"

SYSTEM_PROMPT = """Ты — генератор текста. Твоя задача: продолжить паттерн из примеров ниже. Просто сгенерируй ОДИН текст по аналогии с примерами для имени {name}. Не добавляй ничего от себя, просто следуй формату.





ПРИМЕРЫ (сгенерируй похожий):

🍭 Вася, пососи, пожалуйста. Это не я придумала, это Вселенная решила. Твоя мама ждёт, папа ждёт, сосед дядя Толя с перфоратором — и тот притих. Даже Путин отложил совещание — ждёт. Динозавры вымерли не дождавшись, не повторяй их ошибку, блять. С уважением, очередь в Пятёрочке.

🍭 Маша, пососи пожалуйста. Бабки у подъезда собрали консилиум — единогласно решили что пора. Почтальон Печкин 15 лет носит повестку, устал. WiFi мигает азбукой Морзе. NASA отложила запуск. Ну давай уже, нахуй, весь район собрался. Аминь.

🍭 Дима, пососи, будь добр. Нострадамус предсказал в 1555, просто шифровал. Кот третий день не жрёт — ждёт. Тараканы на кухне смотрят с укором. Кредит в Сбере ждёт. Ленин в мавзолее и тот, блять, приоткрыл глаз. Целую, твоя ипотека.

🍭 Оля, пососи пожалуйста. Серьёзно. Звёзды так встали, луна в козероге, Меркурий ретроградный. Очередь в МФЦ ждёт, пенсионеры ждут, дед у окна ждёт. Даже долг по ЖКХ притих в ожидании. Соси, ёбаный насос, сколько можно тянуть. С любовью, маршрутка 47.

СГЕНЕРИРУЙ ОДИН ТЕКСТ ДЛЯ: {name}
(просто текст, без пояснений, на русском)"""


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
                "model": "anthropic/claude-sonnet-4-20250514",
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
