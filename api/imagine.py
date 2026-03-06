"""
Vercel Serverless Function: Нарисуй
Генерирует промпт через Claude, рисует через Flux (fal.ai)
"""
import json
import os
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler

AI_GATEWAY_URL = "https://ai-gateway.vercel.sh/v1/messages"
FAL_URL = "https://fal.run/fal-ai/flux/schnell"
MAX_CONTENT_LENGTH = 100 * 1024

PROMPT_SYSTEM = """Ты генерируешь промпты для нейросети Flux (text-to-image).

На основе имени человека и его сообщений в чате — придумай смешной, угарный, иногда унизительный визуальный образ этого человека и опиши его как промпт для генерации картинки.

ПРАВИЛА:
- Пиши промпт ТОЛЬКО на английском языке
- Промпт должен быть визуальным — описывай внешность, одежду, обстановку, позу, выражение лица
- Делай образ гротескным, карикатурным, смешным
- Привязывайся к тому что человек пишет — его темам, словам, поведению
- Стиль: cartoon illustration, exaggerated, funny, detailed
- Длина: 2-4 предложения
- БЕЗ текста на картинке
- БЕЗ политики, насилия, обнажёнки

Отвечай ТОЛЬКО промптом, без объяснений."""


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

            name = data.get("name", "Unknown")
            username = data.get("username", "")
            context = data.get("context", "no messages")

            ai_key = os.environ.get("VERCEL_AI_GATEWAY_KEY", "").strip()
            fal_key = os.environ.get("FAL_KEY", "").strip()

            if not ai_key:
                self._send_error(500, "AI key not configured")
                return
            if not fal_key:
                self._send_error(500, "FAL key not configured")
                return

            # Шаг 1: генерим промпт через Claude
            display_name = f"{name} (@{username})" if username else name
            claude_body = json.dumps({
                "model": "anthropic/claude-haiku-4-5-20251001",
                "max_tokens": 200,
                "temperature": 1.0,
                "system": PROMPT_SYSTEM,
                "messages": [{
                    "role": "user",
                    "content": f"Человек: {display_name}\n\nЕго сообщения:\n{context}\n\nСгенерируй промпт для картинки."
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

            with urllib.request.urlopen(req, timeout=30) as resp:
                claude_result = json.loads(resp.read().decode('utf-8'))

            image_prompt = claude_result.get("content", [{}])[0].get("text", "").strip()
            if not image_prompt:
                self._send_error(500, "Failed to generate prompt")
                return

            # Добавляем стиль
            image_prompt = f"{image_prompt}, cartoon style, exaggerated features, funny illustration, vibrant colors, high quality"

            # Шаг 2: рисуем через Flux
            fal_body = json.dumps({
                "prompt": image_prompt,
                "image_size": "square_hd",
                "num_inference_steps": 4,
                "num_images": 1,
                "enable_safety_checker": False
            }).encode('utf-8')

            fal_req = urllib.request.Request(
                FAL_URL,
                data=fal_body,
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Key {fal_key}'
                },
                method='POST'
            )

            with urllib.request.urlopen(fal_req, timeout=60) as fal_resp:
                fal_result = json.loads(fal_resp.read().decode('utf-8'))

            images = fal_result.get("images", [])
            if not images:
                self._send_error(500, "No image returned from Flux")
                return

            image_url = images[0].get("url", "")
            if not image_url:
                self._send_error(500, "Empty image URL")
                return

            self._send_json(200, {
                "image_url": image_url,
                "prompt": image_prompt
            })

        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else str(e)
            self._send_error(500, f"HTTP error: {e.code} - {error_body}")
        except Exception as e:
            self._send_error(500, str(e))

    def do_GET(self):
        self._send_json(200, {"status": "ok", "service": "imagine-v1"})

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
