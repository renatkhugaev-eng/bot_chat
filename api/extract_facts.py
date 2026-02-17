"""
Vercel Serverless Function: Извлечение фактов из сообщений
AI анализирует сообщение и извлекает важные факты о пользователе
"""
import json
import os
from http.server import BaseHTTPRequestHandler
import urllib.request
import urllib.error


AI_GATEWAY_URL = "https://ai-gateway.vercel.sh/v1/messages"
MAX_CONTENT_LENGTH = 50 * 1024  # 50 KB

SYSTEM_PROMPT = """Ты — система извлечения фактов из сообщений чата.
Твоя задача — проанализировать сообщение и ИЗВЛЕЧЬ ВАЖНЫЕ ФАКТЫ о пользователе.

ПРАВИЛА:
1. Извлекай ТОЛЬКО конкретные факты, которые можно запомнить
2. НЕ извлекай общие фразы типа "привет", "ок", "хаха"
3. Факт должен быть ИНФОРМАТИВНЫМ и ПОЛЕЗНЫМ для бота
4. Возвращай ТОЛЬКО JSON без пояснений

КАТЕГОРИИ ФАКТОВ (fact_type):
- personal: имя, возраст, профессия, город, семья
- interest: хобби, увлечения, что нравится/не нравится
- social: отношения с другими людьми в чате
- event: что случилось, новости из жизни пользователя
- opinion: мнения, взгляды, позиция по вопросам

ФОРМАТ ОТВЕТА (строго JSON):
{
  "has_facts": true/false,
  "facts": [
    {
      "type": "категория",
      "text": "краткое описание факта",
      "confidence": 0.5-1.0
    }
  ]
}

ПРИМЕРЫ:

Сообщение: "Я работаю программистом в Яндексе уже 3 года"
Ответ:
{
  "has_facts": true,
  "facts": [
    {"type": "personal", "text": "Работает программистом в Яндексе 3 года", "confidence": 0.95}
  ]
}

Сообщение: "вчера купил себе новую теслу, наконец-то"
Ответ:
{
  "has_facts": true,
  "facts": [
    {"type": "event", "text": "Купил Tesla", "confidence": 0.9},
    {"type": "interest", "text": "Интересуется электромобилями", "confidence": 0.7}
  ]
}

Сообщение: "привет всем"
Ответ:
{
  "has_facts": false,
  "facts": []
}

Сообщение: "ахахаха смешно"
Ответ:
{
  "has_facts": false,
  "facts": []
}

Сообщение: "Маша, ты сегодня красотка!"
Ответ:
{
  "has_facts": true,
  "facts": [
    {"type": "social", "text": "Комплимент для Маши, возможно симпатия", "confidence": 0.6}
  ]
}

Сообщение: "ненавижу когда опаздывают, меня это бесит"
Ответ:
{
  "has_facts": true,
  "facts": [
    {"type": "opinion", "text": "Не любит когда опаздывают, раздражает", "confidence": 0.85}
  ]
}

ОТВЕЧАЙ ТОЛЬКО JSON!
"""


class handler(BaseHTTPRequestHandler):
    def _send_json(self, status, data):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def _send_error(self, status, message):
        self._send_json(status, {"error": message, "has_facts": False, "facts": []})

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_POST(self):
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            if content_length > MAX_CONTENT_LENGTH:
                self._send_error(413, "Request body too large")
                return
            if content_length == 0:
                self._send_error(400, "Empty request body")
                return

            body = self.rfile.read(content_length).decode("utf-8")
            
            try:
                data = json.loads(body)
            except json.JSONDecodeError as e:
                self._send_error(400, f"Invalid JSON: {str(e)}")
                return

            message = data.get("message", "")
            user_name = data.get("user_name", "Пользователь")

            if not message or len(message) < 5:
                # Слишком короткое сообщение — не анализируем
                self._send_json(200, {"has_facts": False, "facts": []})
                return
            
            # Быстрая фильтрация очевидно неинформативных сообщений
            lower_msg = message.lower().strip()
            skip_patterns = [
                'привет', 'хай', 'здарова', 'хаха', 'ахах', 'лол', 
                'ок', 'окей', 'да', 'нет', 'ага', 'угу', 'ясно',
                'спасибо', 'спс', 'пожалуйста', 'норм', '+', '-',
                ')', '(', '😂', '😊', '👍', '❤️'
            ]
            if lower_msg in skip_patterns or len(message) < 10:
                self._send_json(200, {"has_facts": False, "facts": []})
                return

            # Вызываем AI для извлечения фактов
            api_key = os.environ.get("VERCEL_AI_GATEWAY_KEY", "")
            if not api_key:
                self._send_error(500, "AI Gateway not configured")
                return

            ai_request = {
                "model": "anthropic/claude-sonnet-4-20250514",
                "max_tokens": 500,
                "system": SYSTEM_PROMPT,
                "messages": [
                    {"role": "user", "content": f"Имя пользователя: {user_name}\nСообщение: {message}"}
                ]
            }

            req = urllib.request.Request(
                AI_GATEWAY_URL,
                data=json.dumps(ai_request).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {api_key}",
                    "anthropic-version": "2023-06-01"
                },
                method="POST"
            )

            try:
                with urllib.request.urlopen(req, timeout=15) as response:
                    ai_response = json.loads(response.read().decode("utf-8"))
                    
                    # Извлекаем текст ответа
                    result_text = ""
                    if "content" in ai_response and ai_response["content"]:
                        for block in ai_response["content"]:
                            if block.get("type") == "text":
                                result_text = block.get("text", "")
                                break
                    
                    # Парсим JSON из ответа
                    try:
                        # Убираем возможные markdown-обёртки
                        result_text = result_text.strip()
                        if result_text.startswith("```json"):
                            result_text = result_text[7:]
                        if result_text.startswith("```"):
                            result_text = result_text[3:]
                        if result_text.endswith("```"):
                            result_text = result_text[:-3]
                        
                        result = json.loads(result_text.strip())
                        self._send_json(200, result)
                    except json.JSONDecodeError:
                        # AI вернул не JSON — возвращаем пустой результат
                        self._send_json(200, {"has_facts": False, "facts": []})
                    
            except urllib.error.HTTPError as e:
                self._send_error(500, f"AI error: {e.code}")
            except urllib.error.URLError as e:
                self._send_error(500, f"Network error")

        except Exception as e:
            self._send_error(500, "Internal server error")
