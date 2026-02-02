"""
Vercel Serverless Function для транскрипции голосовых сообщений
Поддерживает: Groq Whisper (бесплатно!) или OpenAI Whisper
"""
import json
import os
import base64
from http.server import BaseHTTPRequestHandler
import urllib.request
import urllib.error


# Groq быстрее и бесплатный!
GROQ_API_URL = "https://api.groq.com/openai/v1/audio/transcriptions"
OPENAI_API_URL = "https://api.openai.com/v1/audio/transcriptions"


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
            
            audio_base64 = data.get("audio_base64", "")
            file_format = data.get("format", "ogg")  # Telegram voice = ogg
            
            if not audio_base64:
                self._send_error(400, "No audio_base64 provided")
                return
            
            # Приоритет: Groq (бесплатный) -> OpenAI
            groq_key = os.environ.get("GROQ_API_KEY", "").strip()
            openai_key = os.environ.get("OPENAI_API_KEY", "").strip()
            
            if groq_key:
                api_url = GROQ_API_URL
                api_key = groq_key
                model = "whisper-large-v3"  # Лучшая модель Groq
            elif openai_key:
                api_url = OPENAI_API_URL
                api_key = openai_key
                model = "whisper-1"
            else:
                self._send_error(500, "No API key configured (GROQ_API_KEY or OPENAI_API_KEY)")
                return
            
            # Декодируем аудио из base64
            audio_bytes = base64.b64decode(audio_base64)
            
            # Создаём multipart/form-data запрос
            boundary = "----WebKitFormBoundary7MA4YWxkTrZu0gW"
            
            body = (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="file"; filename="audio.{file_format}"\r\n'
                f"Content-Type: audio/{file_format}\r\n\r\n"
            ).encode('utf-8')
            
            body += audio_bytes
            
            body += (
                f"\r\n--{boundary}\r\n"
                f'Content-Disposition: form-data; name="model"\r\n\r\n'
                f"{model}\r\n"
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="language"\r\n\r\n'
                f"ru\r\n"
                f"--{boundary}--\r\n"
            ).encode('utf-8')
            
            req = urllib.request.Request(
                api_url,
                data=body,
                headers={
                    'Content-Type': f'multipart/form-data; boundary={boundary}',
                    'Authorization': f'Bearer {api_key}',
                },
                method='POST'
            )
            
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))
            
            text = result.get("text", "")
            
            self._send_json(200, {
                "text": text,
                "success": True
            })
            
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else str(e)
            self._send_error(500, f"Whisper API error: {e.code} - {error_body}")
            
        except Exception as e:
            self._send_error(500, str(e))
    
    def do_GET(self):
        self._send_json(200, {"status": "ok", "service": "whisper-transcribe"})
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def _send_json(self, status: int, data: dict):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))
    
    def _send_error(self, status: int, message: str):
        self._send_json(status, {"error": message, "success": False})
