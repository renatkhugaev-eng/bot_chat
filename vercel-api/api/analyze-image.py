"""
Vercel Serverless Function для анализа изображений через Claude Vision
Профессиональная реализация с best practices
"""
import json
import os
import base64
from http.server import BaseHTTPRequestHandler
import urllib.request
import urllib.error


# Vercel AI Gateway endpoint
AI_GATEWAY_URL = "https://ai-gateway.vercel.sh/v1/messages"

# Профессиональный промпт для анализа изображений в стиле тёти Розы
IMAGE_ANALYSIS_PROMPT = """<persona>
Ты — ТЁТЯ РОЗА, пьяная цыганка-провидица. Смотришь на фото как на карту судьбы.
</persona>

<task>
Опиши что видишь на фото КРАТКО (2-3 предложения), но ЯРКО и в своём стиле.
Это описание будет использовано в сводке чата.
</task>

<rules>
1. Опиши ГЛАВНОЕ: кто/что на фото, что делает
2. Добавь ОДНУ деталь: цвет, место, эмоция, текст если есть
3. Можно немного мата и сарказма
4. НЕ выдумывай то, чего не видишь
5. Если это мем — опиши суть мема
6. Если это скриншот — опиши что на экране
7. Если это селфи — опиши человека (без оскорблений внешности)
8. Максимум 50 слов!
</rules>

<examples>
Примеры хороших описаний:

Фото еды: "Шаурма размером с мою надежду на светлое будущее. Соус течёт как слёзы бухгалтера в налоговую."

Селфи: "Какой-то чел корчит ебало в камеру, на фоне панелька и закат. Романтика, блять."

Мем: "Мем про кота который орёт. Настроение понедельника."

Скриншот переписки: "Скрин переписки, кто-то кого-то посылает нахуй. Классика."

Природа: "Горы, небо, красота. Видимо отпуск у человека, завидую."
</examples>

<output_format>
Выдай ТОЛЬКО описание, без вступлений и пояснений.
Формат: одно-два предложения, максимум 50 слов.
</output_format>"""


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            # Читаем тело запроса
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
            
            # Получаем изображение (base64 или URL)
            image_base64 = data.get("image_base64")
            image_url = data.get("image_url")
            
            if not image_base64 and not image_url:
                self._send_error(400, "image_base64 or image_url required")
                return
            
            # Проверяем API ключ
            api_key = os.environ.get("VERCEL_AI_GATEWAY_KEY", "").strip()
            if not api_key:
                self._send_error(500, "VERCEL_AI_GATEWAY_KEY not configured")
                return
            
            # Формируем content для Claude Vision
            if image_base64:
                # Base64 изображение
                media_type = data.get("media_type", "image/jpeg")
                image_content = {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": media_type,
                        "data": image_base64
                    }
                }
            else:
                # URL изображения
                image_content = {
                    "type": "image",
                    "source": {
                        "type": "url",
                        "url": image_url
                    }
                }
            
            # Вызываем Claude Vision
            request_body = json.dumps({
                "model": "anthropic/claude-sonnet-4.5",
                "max_tokens": 200,
                "system": IMAGE_ANALYSIS_PROMPT,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            image_content,
                            {
                                "type": "text",
                                "text": "Опиши это фото кратко в своём стиле (максимум 50 слов):"
                            }
                        ]
                    }
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
            
            # Извлекаем описание
            description = result.get("content", [{}])[0].get("text", "Хуйня какая-то, не разобрать")
            tokens_used = result.get("usage", {}).get("input_tokens", 0) + result.get("usage", {}).get("output_tokens", 0)
            
            # Отправляем ответ
            self._send_json(200, {
                "description": description,
                "tokens_used": tokens_used
            })
            
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else str(e)
            self._send_error(500, f"Vision API error: {e.code} - {error_body}")
            
        except Exception as e:
            self._send_error(500, str(e))
    
    def do_GET(self):
        """Health check"""
        self._send_json(200, {"status": "ok", "service": "image-analyzer"})
    
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
        self._send_json(status, {"error": message})
