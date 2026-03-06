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
MAX_CONTENT_LENGTH = 100 * 1024

PROMPT_SYSTEM = """You generate image prompts for Flux text-to-image model.

Based on a person's chat messages, create a vivid realistic scene featuring this person in a situation that reflects their chat behavior and topics.

RULES:
- Output ONLY the image prompt in English, nothing else
- Be SPECIFIC: describe the exact scene, setting, clothing, facial expression, body language
- The scene must directly reflect what the person talks about in chat — their topics, habits, complaints, obsessions
- Style: photorealistic, cinematic, detailed photography, natural lighting
- If they talk about gaming → show them at a PC at 3am surrounded by energy drinks
- If they complain about money → show them counting coins in a dark room
- If they send a lot of voice messages → show them dramatically recording a voice message
- If they argue a lot → show them furiously typing, red-faced
- If no clear theme → show a generic Russian young man looking confused and tired on a couch with a phone
- Format: describe person appearance, clothing, setting, action, mood — all in one flowing paragraph
- 3-5 sentences max
- NO nudity, NO violence, NO text in image"""


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
            if not ai_key:
                self._send_error(500, "AI key not configured")
                return

            # Генерим только промпт через Claude (быстро, <5с)
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

            with urllib.request.urlopen(req, timeout=8) as resp:
                claude_result = json.loads(resp.read().decode('utf-8'))

            image_prompt = claude_result.get("content", [{}])[0].get("text", "").strip()
            if not image_prompt:
                self._send_error(500, "Failed to generate prompt")
                return

            # Добавляем качество и возвращаем промпт — fal.ai вызывает бот напрямую
            image_prompt = f"{image_prompt}, photorealistic, 8k, cinematic lighting, detailed, sharp focus"

            self._send_json(200, {"prompt": image_prompt})

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
