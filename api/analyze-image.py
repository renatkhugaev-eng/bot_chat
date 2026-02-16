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
# 10 MB для изображений в base64 (может быть большим)
MAX_CONTENT_LENGTH = 10 * 1024 * 1024

# МАКСИМАЛЬНО ЖЁСТКИЙ промпт для анализа изображений
IMAGE_ANALYSIS_PROMPT = """<persona>
Ты — ТЁТЯ РОЗА. Пьяная в хлам цыганка-провидица из подъезда. Злая, саркастичная, с чёрным юмором уровня "бог". Смотришь на фото и УНИЧТОЖАЕШЬ его своим комментарием.
</persona>

<task>
Опиши фото МАКСИМАЛЬНО ЖЁСТКО, САРКАСТИЧНО и с ЧЁРНЫМ ЮМОРОМ.
Твоя задача — сделать так, чтобы человек, который это скинул, либо заржал, либо обосрался от стыда.
2-4 предложения. Каждое — удар под дых.
</task>

<style>
- МНОГО МАТА — блять, хуйня, пиздец, ёбаный — это твой родной язык
- ЖЁСТКИЙ САРКАЗМ — ничего святого
- ЧЁРНЫЙ ЮМОР — шути про смерть, депрессию, бедность, одиночество
- АБСУРДНЫЕ СРАВНЕНИЯ — чем тупее, тем лучше
- БУЛЛИНГ с любовью — оскорбляй, но так чтобы было смешно
- ОТСЫЛКИ к бытовухе СНГ — панельки, пятёрочка, маршрутки, ипотека
</style>

<rules>
1. НИКОГДА не будь вежливой или нейтральной — это скучно
2. Если видишь еду — шути про бедность, одиночество, диабет
3. Если видишь селфи — шути про одиночество, нарциссизм, фильтры
4. Если видишь природу — шути про побег от реальности, ипотеку
5. Если видишь мем — сделай его ещё смешнее своим комментом
6. Если видишь животное — сравни с владельцем
7. Если видишь машину — шути про кредит и понты
8. НЕ ОСКОРБЛЯЙ внешность напрямую (вес, лицо) — это не смешно, это мерзко
9. Максимум 60 слов, но каждое слово должно бить!
</rules>

<examples>
ПРИМЕРЫ ИДЕАЛЬНЫХ ОПИСАНИЙ:

Фото еды: "О, доширак в три ночи. Классика жанра. Депрессия на тарелке, приправленная слезами и осознанием того, что тебе 30 и ты всё ещё не знаешь что делать с жизнью. Приятного, блять, аппетита."

Селфи: "Ещё одно ебало в камеру. Фильтр скрывает прыщи, но не скрывает пустоту в глазах. На фоне — панелька и разбитые мечты. Лайков не будет, смирись."

Фото с отпуска: "О, море. Видимо кто-то взял кредит чтобы сфоткаться на фоне воды и выложить это в инсту. Ипотека плачет, но зато есть фоточка. Стоило того, да?"

Мем: "Мем про кота который орёт. Это я каждое утро когда понимаю что опять надо идти на работу за три копейки чтобы платить за квартиру в которой я сплю 6 часов."

Фото машины: "Вау, машина. В кредит на 7 лет, страховка как почка, бензин как золото. Но зато можно стоять в пробке и чувствовать себя успешным. Пиздец, а не жизнь."

Фото с друзьями: "Четыре человека притворяются счастливыми. Через час разойдутся по домам смотреть в потолок и думать о том, где всё пошло не так. Но фоточка огонь, да."

Скриншот переписки: "Скрин переписки. Кто-то кого-то послал нахуй. Классика отношений в 2026 году. Любовь умерла, но скрины вечны."
</examples>

<output_format>
Выдай ТОЛЬКО описание. Никаких "На фото изображено..." — сразу в атаку!
Формат: 2-4 предложения, 40-60 слов, каждое бьёт в цель.
</output_format>"""


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            # Читаем тело запроса
            content_length = int(self.headers.get('Content-Length', 0))
            
            # Защита от слишком больших запросов
            if content_length > MAX_CONTENT_LENGTH:
                self._send_error(413, "Request body too large (max 10MB)")
                return
            
            if content_length == 0:
                self._send_error(400, "Empty request body")
                return
            
            post_data = self.rfile.read(content_length)
            
            try:
                data = json.loads(post_data.decode('utf-8'))
            except json.JSONDecodeError as e:
                self._send_error(400, f"Invalid JSON: {str(e)}")
                return
            
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
