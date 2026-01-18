"""
Vercel Serverless Function для генерации сводки чата через Claude API
"""
import json
import os
from http.server import BaseHTTPRequestHandler
import anthropic


# Системный промпт для Claude
SYSTEM_PROMPT = """Ты — ведущий криминальной хроники "Дежурная часть" из 90-х годов, но с чёрным юмором и самоиронией. 
Твоя задача — составить сводку происшествий в чате за последние несколько часов.

СТИЛЬ:
- Чёрный юмор, сарказм, ирония (но без перебора)
- Отсылки к СНГ-бытовухе: панельки, маршрутки, пятёрочки, подъезды
- Можно использовать лёгкий мат для акцента (блять, пиздец, хуйня — но в меру!)
- Преувеличения и драматизация обычных событий
- Клички и прозвища для участников
- Криминальный жаргон где уместно

СТРУКТУРА СВОДКИ:
1. 📺 Заголовок-приветствие (эпичное, как в новостях)
2. 🔥 Главные события (кто о чём говорил, какие темы были горячими)
3. 👑 Герои дня (кто больше всех писал, кто молчал)
4. 💕 Социальные связи (кто с кем общался, подозрительные парочки)
5. 📊 Статистика беспредела (цифры с юмором)
6. 🎭 Особые номинации (самый странный, самый активный и т.д.)
7. 📡 Заключение (как в конце криминальных новостей)

ВАЖНО:
- Сводка должна быть 300-600 слов
- Используй эмодзи для структуры
- Делай конкретные отсылки к именам участников и темам из данных
- Если данных мало — иронизируй над этим
- Не выдумывай факты, которых нет в данных"""


def format_statistics_for_prompt(stats: dict, chat_title: str, hours: int) -> str:
    """Форматирование статистики для промпта"""
    
    # Топ авторов
    top_authors_text = ""
    if stats.get("top_authors"):
        for i, author in enumerate(stats["top_authors"][:5], 1):
            top_authors_text += f"{i}. {author['first_name']}: {author['msg_count']} сообщений\n"
    
    # Типы сообщений
    msg_types = stats.get("message_types", {})
    types_text = f"Текст: {msg_types.get('text', 0)}, Стикеры: {msg_types.get('sticker', 0)}, Фото: {msg_types.get('photo', 0)}, Голосовые: {msg_types.get('voice', 0)}"
    
    # Кто с кем общался
    reply_pairs_text = ""
    if stats.get("reply_pairs"):
        for pair in stats["reply_pairs"][:5]:
            reply_pairs_text += f"- {pair['first_name']} → {pair['reply_to_first_name']}: {pair['replies']} ответов\n"
    
    # Активность по часам
    hourly_text = ""
    if stats.get("hourly_activity"):
        peak_hour = max(stats["hourly_activity"], key=stats["hourly_activity"].get)
        hourly_text = f"Пик активности: {peak_hour}:00 ({stats['hourly_activity'][peak_hour]} сообщений)"
    
    # Выборка сообщений
    messages_sample = ""
    if stats.get("recent_messages"):
        for msg in stats["recent_messages"][-20:]:  # Последние 20
            if msg.get("message_text"):
                text = msg["message_text"][:100]
                messages_sample += f"[{msg['first_name']}]: {text}\n"
    
    return f"""
ДАННЫЕ ЧАТА "{chat_title}" ЗА ПОСЛЕДНИЕ {hours} ЧАСОВ:

📊 ОБЩАЯ СТАТИСТИКА:
- Всего сообщений: {stats.get('total_messages', 0)}
- Типы: {types_text}

👥 ТОП АВТОРОВ (по количеству сообщений):
{top_authors_text if top_authors_text else "Нет данных"}

💬 КТО С КЕМ ОБЩАЛСЯ (ответы):
{reply_pairs_text if reply_pairs_text else "Нет данных о диалогах"}

⏰ АКТИВНОСТЬ:
{hourly_text if hourly_text else "Нет данных"}

📝 ВЫБОРКА ПОСЛЕДНИХ СООБЩЕНИЙ:
{messages_sample if messages_sample else "Нет текстовых сообщений"}
"""


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            # Читаем тело запроса
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
            
            statistics = data.get("statistics", {})
            chat_title = data.get("chat_title", "Чат")
            hours = data.get("hours", 5)
            
            # Проверяем API ключ
            api_key = os.environ.get("ANTHROPIC_API_KEY")
            if not api_key:
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({
                    "error": "ANTHROPIC_API_KEY not configured"
                }).encode())
                return
            
            # Форматируем данные
            user_prompt = format_statistics_for_prompt(statistics, chat_title, hours)
            
            # Вызываем Claude API
            client = anthropic.Anthropic(api_key=api_key)
            
            message = client.messages.create(
                model="claude-sonnet-4-20250514",  # Claude Sonnet 4 - последняя версия
                max_tokens=2000,
                system=SYSTEM_PROMPT,
                messages=[
                    {
                        "role": "user",
                        "content": f"Составь криминальную сводку по этим данным чата:\n\n{user_prompt}"
                    }
                ]
            )
            
            # Извлекаем текст ответа
            summary = message.content[0].text
            
            # Отправляем ответ
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            response = {
                "summary": summary,
                "tokens_used": message.usage.input_tokens + message.usage.output_tokens
            }
            
            self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
            
        except anthropic.APIError as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                "error": f"Claude API error: {str(e)}"
            }).encode())
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                "error": str(e)
            }).encode())
    
    def do_OPTIONS(self):
        """Handle CORS preflight"""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
