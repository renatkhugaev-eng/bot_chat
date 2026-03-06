"""
Vercel Serverless Function: Видео
Генерирует промпт для Kling text-to-video через Claude
"""
import json
import os
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler

AI_GATEWAY_URL = "https://ai-gateway.vercel.sh/v1/messages"
MAX_CONTENT_LENGTH = 100 * 1024

PROMPT_SYSTEM = """You generate video prompts for Kling AI text-to-video model.

Based on a person's chat messages, create a vivid 5-second cinematic scene featuring this person in a situation that reflects their chat behavior.

RULES:
- Output ONLY the video prompt in English, nothing else
- Describe motion and action explicitly — video needs movement, not static scenes
- Be SPECIFIC: describe the exact scene, camera movement, setting, person's action
- The scene must reflect what the person actually talks about in chat
- Style: cinematic, photorealistic, 4K, smooth camera movement, film grain
- Examples:
  - If they talk about gaming → slow zoom on young man at glowing PC setup at 3am, fingers rapidly clicking mechanical keyboard, energy drink cans scattered everywhere, neon lights reflecting on determined face
  - If they complain about money → close-up of hands nervously counting few coins on table in dim room, person looking up at camera with defeated expression, shallow depth of field
  - If they send a lot of voice messages → person dramatically holding phone to mouth, recording voice note, gesturing wildly with free hand, exaggerated expressions, street background
  - If they argue a lot → person furiously typing on phone, face flushed red, jaw clenched, dramatic close-up with shallow DOF, urban background
  - If no clear theme → young Russian man lying on couch scrolling phone, looking bored, apartment slightly messy, afternoon light through dusty window, slow pan shot
- Include: person appearance, setting, action, camera movement, lighting
- 3-4 sentences max
- NO nudity, NO violence, NO text in frame"""


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

            display_name = f"{name} (@{username})" if username else name
            claude_body = json.dumps({
                "model": "anthropic/claude-haiku-4-5-20251001",
                "max_tokens": 250,
                "temperature": 1.0,
                "system": PROMPT_SYSTEM,
                "messages": [{
                    "role": "user",
                    "content": f"Person: {display_name}\n\nTheir messages:\n{context}\n\nGenerate a video prompt."
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

            video_prompt = claude_result.get("content", [{}])[0].get("text", "").strip()
            if not video_prompt:
                self._send_error(500, "Failed to generate prompt")
                return

            self._send_json(200, {"prompt": video_prompt})

        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else str(e)
            self._send_error(500, f"HTTP error: {e.code} - {error_body}")
        except Exception as e:
            self._send_error(500, str(e))

    def do_GET(self):
        self._send_json(200, {"status": "ok", "service": "video-v1"})

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
