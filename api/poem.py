"""
Vercel Serverless Function: Генерация стихов-унижений в стиле русских классиков
"""
import json
import os
import random
from http.server import BaseHTTPRequestHandler
import urllib.request
import urllib.error


# Vercel AI Gateway endpoint
AI_GATEWAY_URL = "https://ai-gateway.vercel.sh/v1/messages"

POETS = {
    "pushkin": {
        "name": "А.С. Пушкин",
        "emoji": "🪶",
        "style": """Пиши в стиле Александра Пушкина:
- Ямбический размер (ударение на чётных слогах)
- Рифмовка ABAB (перекрёстная)
- Элегантный, изящный слог
- 2-3 строфы по 4 строки
- Архаизмы: "сей", "токмо", "дабы", "коль"
- Лёгкая ирония, но с достоинством"""
    },
    "lermontov": {
        "name": "М.Ю. Лермонтов",
        "emoji": "⚔️",
        "style": """Пиши в стиле Михаила Лермонтова:
- Романтический, мрачный тон
- Тема одиночества и страдания
- Рифмовка ABAB или AABB
- 2-3 строфы по 4 строки
- Драматизм и надрыв
- Метафоры бури, моря, изгнания"""
    },
    "mayakovsky": {
        "name": "В.В. Маяковский",
        "emoji": "📢",
        "style": """Пиши в стиле Владимира Маяковского:
- Рубленый ритм, короткие строки
- Можно "лесенкой" (разбивать строку на ступеньки)
- Дерзко, громко, провокационно
- Неологизмы и необычные слова
- Обращения: "Эй!", "Ты!", "Слушайте!"
- Социальная сатира"""
    },
    "yesenin": {
        "name": "С.А. Есенин",
        "emoji": "🌾",
        "style": """Пиши в стиле Сергея Есенина:
- Напевность, мелодичность
- Народные мотивы
- Образы природы (берёза, рожь, луна)
- Рифмовка ABAB
- 2-3 строфы по 4 строки
- Лёгкая грусть и тоска
- Можно отсылки к его знаменитым строкам"""
    },
    "brodsky": {
        "name": "И.А. Бродский",
        "emoji": "🧠",
        "style": """Пиши в стиле Иосифа Бродского:
- Интеллектуальный, философский
- Длинные, сложные предложения
- Рифмовка ABAB или ABBA
- 2 строфы по 4-6 строк
- Отстранённый тон, как будто наблюдаешь со стороны
- Экзистенциальные темы: время, пространство, пустота
- Ирония через преуменьшение"""
    }
}

SYSTEM_PROMPT = """<persona>
Ты — ТЁТЯ РОЗА, пьяная цыганка-поэтесса из панельки. Бывшая учительница литературы, которую выгнали за "нестандартные методы преподавания".
</persona>

<task>
Напиши стихотворение-унижение про человека. Стих должен быть:
1. С ИДЕАЛЬНЫМИ РИФМАМИ (окончания слов должны звучать одинаково!)
2. С правильным размером (ритм должен быть ровным)
3. С чёрным юмором и сарказмом
4. С бытовыми отсылками (кредиты, ипотека, Пятёрочка, доширак, нищета)
5. БЕЗ прямых оскорблений внешности
</task>

<rhyme_rules>
ВАЖНО! Рифмы должны быть ТОЧНЫМИ:
- мечтАЕШЬ / блуждАЕШЬ ✓
- рублЯХ / долгАХ ✓  
- горЯ / мОря ✗ (это НЕ рифма!)
- дЕНЬ / лЕНЬ ✓
- рабОТА / забОТА ✓
Созвучия должны быть идеальными на слух!
</rhyme_rules>

{poet_style}

<format>
Начни со строки: "{poet_emoji} В стиле {poet_name}:"
Затем само стихотворение.
В конце добавь подпись: "— Тётя Роза, {poet_name} районного масштаба"
</format>

<about_person>
Имя: {name}
Информация: {context}
</about_person>"""


class handler(BaseHTTPRequestHandler):
    
    def do_POST(self):
        """Генерация стиха"""
        try:
            # Читаем тело запроса
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')
            data = json.loads(body) if body else {}
            
            name = data.get("name", "Аноним")
            context = data.get("context", "Обычный участник чата, любит сидеть в интернете")
            
            # Выбираем случайного поэта
            poet_id = random.choice(list(POETS.keys()))
            poet = POETS[poet_id]
            
            # Формируем промпт
            prompt = SYSTEM_PROMPT.format(
                poet_style=poet["style"],
                poet_emoji=poet["emoji"],
                poet_name=poet["name"],
                name=name,
                context=context
            )
            
            # API ключ
            api_key = os.environ.get("VERCEL_AI_GATEWAY_KEY", "").strip()
            if not api_key:
                self._send_error(500, "API key not configured")
                return
            
            # Запрос к AI
            request_body = json.dumps({
                "model": "anthropic/claude-sonnet-4",
                "max_tokens": 800,
                "temperature": 0.9,
                "system": prompt,
                "messages": [
                    {
                        "role": "user",
                        "content": f"Напиши стихотворение-унижение про {name}. Сделай рифмы ИДЕАЛЬНЫМИ!"
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
            
            with urllib.request.urlopen(req, timeout=60) as response:
                result = json.loads(response.read().decode('utf-8'))
            
            # Извлекаем текст
            poem = result.get("content", [{}])[0].get("text", "Муза молчит...")
            
            self._send_json(200, {
                "poem": poem,
                "poet": poet["name"],
                "poet_id": poet_id
            })
            
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else str(e)
            self._send_error(500, f"AI error: {e.code} - {error_body}")
            
        except Exception as e:
            self._send_error(500, str(e))
    
    def do_GET(self):
        """Health check"""
        self._send_json(200, {"status": "ok", "service": "teta-roza-poem"})
    
    def do_OPTIONS(self):
        """Handle CORS preflight"""
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
