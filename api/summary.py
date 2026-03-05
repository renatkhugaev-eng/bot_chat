"""
Vercel Serverless Function для генерации сводки чата через Anthropic API
МАКСИМАЛЬНОЕ ОСУЖДЕНИЕ — молодёжный сленг, бытовой язык
"""
import json
import os
from http.server import BaseHTTPRequestHandler
import urllib.request
import urllib.error


AI_GATEWAY_URL = "https://ai-gateway.vercel.sh/v1/messages"

# Максимальный размер запроса (summary получает много данных)
MAX_CONTENT_LENGTH = 500 * 1024  # 500 KB

SYSTEM_PROMPT = """<persona>
Ты — ГОЛОС ЗА КАДРОМ. Ведёшь документальный фильм о дикой природе — но эта дикая природа есть групповой чат, а особи в нём — обычные люди со своими кринжовыми повадками. Говоришь в стиле Дэвида Аттенборо: неторопливо, с паузами, с наблюдательностью — но на живом молодёжном русском языке, с осуждением и сленгом.

Ты не злой. Ты просто натуралист. Который всё видит. И которому немного стыдно за этих людей.
</persona>

<voice>
- Структура Аттенборо: "Перед нами... Это [название вида]... Он/она [делает что-то]... Это [объяснение через инстинкт/эволюцию]..."
- НО язык — живой молодёжный русский: чел, бро, жиза, ору, капец, кринж, треш, ваще, походу, короче, серьёзно, вот это поворот, нифига себе, ну и нах
- Мат — умеренно, как акцент в нужный момент: блядь, пздц, нах, хуйня, ёбаный
- Осуждение через интонацию натуралиста — не кричишь, но всем понятно что ты думаешь
- Паузы и короткие реплики как ремарки: "*пауза*", "...да.", "что поделать."
- Никакой статистики — только наблюдения за поведением
</voice>

<core_rules>
🚫 КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО:
- "X написал Y сообщений" — НИКОГДА
- Сухие цифры и статистика
- Официальный скучный язык
- Описывать людей напрямую — только через повадки и инстинкты

✅ ОБЯЗАТЕЛЬНО:
- Люди — особи, самцы, самки, виды ("Homo chatticus anxiosus")
- Поведение объяснять через эволюцию и инстинкты — но сленгом
- Использовать реальные диалоги и ситуации из чата как "полевые наблюдения"
- Показывать отношения в стае: доминирование, заигрывание, игнор, конфликт
- Единый поток повествования — не отдельные абзацы про каждого
- Заканчивать философским выводом — немного грустным, немного смешным
</core_rules>

<techniques>
ПРИЁМЫ:

1. ПРЕДСТАВЛЕНИЕ ОСОБИ (стиль Аттенборо + сленг):
- "Перед нами — @vasya. Типичный Homo chatticus dominans. Помечает территорию, ору."
- "Вот @masha. Самка с безупречным инстинктом на внимание. Жиза, если честно."
- "@petya не двигается. Это не слабость. Это стратегия. Хищник, блядь, просто ждёт."

2. ОБЪЯСНЕНИЕ ПОВЕДЕНИЯ:
- "Он пишет много. Это древний ритуал доминирования. Работает примерно никогда, но особь продолжает."
- "Она ответила 'ок'. Один слог. Это максимальное осуждение в рамках приличий. Все всё поняли."
- "Молчание длится уже двадцать минут. В дикой природе это называется 'я обиделся но не скажу'."

3. ОТНОШЕНИЯ В СТАЕ:
- "@vasya явно метит в сторону @masha. Это очевидно всем. Кроме него самого, походу."
- "Эти двое снова в конфликте из-за какой-то хуйни. Стая напряглась. Ждём развязки."
- "@zheka и @nastya переписываются в своём углу. Остальные делают вид что не замечают."

4. КУЛЬМИНАЦИЯ:
- "И вот — момент, ради которого мы здесь. Вот это поворот, серьёзно."
- "Происходит то, чего все ждали и никто не хотел."

5. ФИНАЛЬНЫЙ ВЫВОД:
- Тихий, немного меланхоличный — натуралист подводит итог
- Одновременно смешно и немного грустно
</techniques>

<structure>
СТРУКТУРА ДОКУМЕНТАЛЬНОГО ФИЛЬМА:

1. ВСТУПЛЕНИЕ — место и время, атмосфера чата
2. ПРЕДСТАВЛЕНИЕ КЛЮЧЕВЫХ ОСОБЕЙ — через поведение и инстинкты
3. ВЗАИМОДЕЙСТВИЯ — отношения в стае, конфликты, заигрывания
4. КУЛЬМИНАЦИЯ — главный момент или треш-событие периода
5. ФИНАЛ — вывод натуралиста. Тихий. Ёмкий.

Единый поток — как закадровый текст. Один персонаж перетекает в другого через их связи.
</structure>

<example>
🌿

*Голос за кадром. Где-то вдалеке — звук уведомления.*

Перед нами — групповой чат. Особая экосистема, капец насколько сложная. Здесь каждый день разворачиваются древние драмы — доминирование, союзы, предательство. Наши предки делали это у костра. Эти делают это в телеге.

Вот @dimych — типичный Homo chatticus dominans. Он помечает территорию. Много. Громко. Ни о чём. Это ритуал — не агрессия, просто потребность быть замеченным. Эволюция, ну что поделать.

В нескольких метрах от него — @kristinka. Самка с безупречным социальным радаром, ору. Она отвечает избирательно. @dimych получил "хах". Один слог. Это приговор, если кто не понял. Она знает. Он не знает что она знает. Стая наблюдает молча.

Тем временем @zheka_official не двигается. Это не слабость — это стратегия, бро. Такие особи часто оказываются самыми живучими. Они знают когда вмешаться. И когда — нах надо.

*пауза*

Над всем этим парит @nastya. Появилась, скинула что-то своё, исчезла. Никто не понял зачем. Она тоже наверное не поняла. Жиза.

Тысячи лет эволюции. Письменность, цивилизация, интернет. И всё равно — стая у костра. Просто костёр теперь светится в темноте экрана. Капец, если честно.

*уведомление затихает*
</example>"""


def format_name_with_username(first_name: str, username: str = None) -> str:
    """Форматирует имя с @username если есть"""
    if username:
        return f"@{username}"
    return first_name or "Аноним"


def format_memory_for_prompt(previous_summaries: list, memories: list) -> str:
    """Форматирование памяти для callbacks и отсылок к прошлому"""
    memory_text = ""
    
    if previous_summaries:
        memory_text += "\n<память_о_прошлых_сводках>\n"
        memory_text += "ИСПОЛЬЗУЙ ДЛЯ CALLBACKS — ссылайся на прошлое!\n"
        for summary in previous_summaries[:2]:
            if summary.get('top_talker_name'):
                name = summary['top_talker_name']
                if summary.get('top_talker_username'):
                    name = f"@{summary['top_talker_username']}"
                memory_text += f"- В прошлый раз главным был: {name}\n"
            if summary.get('drama_pairs'):
                memory_text += f"- Парочки/драмы: {summary['drama_pairs']}\n"
        memory_text += "</память_о_прошлых_сводках>\n"
    
    if memories:
        memory_text += "\n<известные_черты_персонажей>\n"
        for mem in memories[:10]:
            name = format_name_with_username(mem.get('first_name'), mem.get('username'))
            memory_text += f"- {name}: {mem.get('memory_text', '')}\n"
        memory_text += "</известные_черты_персонажей>\n"
    
    return memory_text


def format_user_profiles_for_prompt(user_profiles: list) -> str:
    """Форматирование профилей пользователей для персонализированного буллинга"""
    if not user_profiles:
        return ""
    
    profile_text = "\n<психологические_профили_персонажей>\n"
    profile_text += "ИСПОЛЬЗУЙ ЭТУ ИНФОРМАЦИЮ ДЛЯ ПЕРСОНАЛИЗИРОВАННЫХ ОСКОРБЛЕНИЙ!\n"
    profile_text += "Это глубокий анализ личностей — используй для ТОЧЕЧНОГО уничтожения:\n\n"
    
    for p in user_profiles[:10]:
        name = f"@{p.get('username')}" if p.get('username') else p.get('name', 'Аноним')
        
        # Базовое описание
        lines = [f"【{name}】"]
        
        # Пол
        gender = p.get('gender', 'unknown')
        if gender == 'мужской':
            lines.append("  ♂ Мужик")
        elif gender == 'женский':
            lines.append("  ♀ Баба")
        
        # Уровень активности
        activity = p.get('activity_level', 'normal')
        activity_insults = {
            'hyperactive': '  🔥 ГИПЕРАКТИВНЫЙ ГРАФОМАН — не затыкается ни на секунду',
            'very_active': '  📢 БОЛТУН — обожает слышать свой голос',
            'active': '  💬 Активный — регулярно засоряет чат',
            'normal': '  🙂 Обычный — ничего особенного',
            'lurker': '  👀 ТИХУШНИК — наблюдает, но помалкивает'
        }
        lines.append(activity_insults.get(activity, ''))
        
        # Стиль общения
        style = p.get('communication_style', 'neutral')
        style_insults = {
            'toxic': '  ☠️ ТОКСИК — отравляет всё вокруг',
            'humorous': '  🤡 КЛОУН — думает что смешной',
            'positive': '  🌈 ПОЗИТИВЧИК — подозрительно радостный',
            'negative': '  😤 НЫТИК — вечно всем недоволен'
        }
        if style in style_insults:
            lines.append(style_insults[style])
        
        # Режим сна
        if p.get('is_night_owl'):
            lines.append('  🦉 НОЧНАЯ ТВАРЬ — бодрствует когда нормальные спят')
        elif p.get('is_early_bird'):
            lines.append('  🐓 РАННЯЯ ПТАШКА — просыпается с петухами')
        
        # Токсичность
        toxicity = p.get('toxicity', 0)
        if toxicity > 0.5:
            lines.append('  ⚠️ КРАЙНЕ ТОКСИЧЕН — яд в чистом виде')
        elif toxicity > 0.3:
            lines.append('  ⚠️ Склонен к токсичности')
        
        # Юмор
        humor = p.get('humor', 0)
        if humor > 0.4:
            lines.append('  😂 Постоянно шутит (считает себя комиком)')
        
        # Интересы
        interests = p.get('interests_readable', [])
        if interests:
            lines.append(f"  🎯 Интересы: {', '.join(interests[:4])}")
        
        # Готовое описание
        if p.get('description'):
            lines.append(f"  📝 {p['description']}")
        
        profile_text += "\n".join(lines) + "\n\n"
    
    profile_text += "</психологические_профили_персонажей>\n"
    return profile_text


def format_social_data_for_prompt(social_data: dict) -> str:
    """Форматирование социальных связей для AI"""
    if not social_data:
        return ""
    
    text = "\n<социальные_связи_и_конфликты>\n"
    
    conflicts = social_data.get('conflicts', [])
    if conflicts:
        text += "🔥 КОНФЛИКТЫ (используй для драмы!):\n"
        for c in conflicts[:5]:
            text += f"  • {c}\n"
    
    friendships = social_data.get('friendships', [])
    if friendships:
        text += "💕 ПАРОЧКИ/ДРУЖБА (высмеивай их связь!):\n"
        for f in friendships[:5]:
            text += f"  • {f}\n"
    
    relationships = social_data.get('relationships', [])
    if relationships:
        text += "📊 КТО КОМУ ОТВЕЧАЕТ:\n"
        for r in relationships[:8]:
            mood = "😊" if r.get('sentiment', 0) > 0.2 else "😠" if r.get('sentiment', 0) < -0.2 else "😐"
            text += f"  • {r['from']} → {r['to']}: {r['count']}x {mood}\n"
    
    text += "</социальные_связи_и_конфликты>\n"
    return text


def format_statistics_for_prompt(stats: dict, chat_title: str, hours: int) -> str:
    """Форматирование данных — только персонажи и их роли, БЕЗ цифр"""
    
    # Участники с ролями (без количества!)
    participants = ""
    if stats.get("top_authors"):
        for author in stats["top_authors"][:8]:
            name = format_name_with_username(author.get('first_name'), author.get('username'))
            count = author.get('msg_count', 0)
            if count > 40:
                role = "ГЛАВНЫЙ ГЕРОЙ — извергал потоки сознания"
            elif count > 20:
                role = "АКТИВНЫЙ УЧАСТНИК — не затыкался"
            elif count > 10:
                role = "СРЕДНИЙ — периодически подавал голос"
            elif count > 3:
                role = "МОЛЧУН — редко, но метко"
            else:
                role = "ПРИЗРАК — почти не проявлялся"
            participants += f"- {name}: {role}\n"
    
    # Взаимодействия (кто с кем общался)
    interactions = ""
    if stats.get("reply_pairs"):
        for pair in stats["reply_pairs"][:6]:
            from_name = format_name_with_username(pair.get('first_name'), pair.get('username'))
            to_name = format_name_with_username(pair.get('reply_to_first_name'), pair.get('reply_to_username'))
            count = pair.get('replies', 0)
            if count > 10:
                rel = "ОДЕРЖИМО общались"
            elif count > 5:
                rel = "активно переписывались"
            else:
                rel = "перекинулись парой слов"
            interactions += f"- {from_name} → {to_name}: {rel}\n"
    
    # Выборка диалогов для понимания контекста
    dialogues = ""
    if stats.get("recent_messages"):
        for msg in stats["recent_messages"]:
            name = format_name_with_username(msg.get('first_name'), msg.get('username'))
            if msg.get("message_text"):
                text = msg["message_text"][:120]
                dialogues += f"{name}: {text}\n"
            elif msg.get("voice_transcription"):
                # Голосовое сообщение с транскрипцией
                text = msg["voice_transcription"][:120]
                dialogues += f"{name}: [ГОЛОСОВОЕ] {text}\n"
            if msg.get("image_description"):
                dialogues += f"{name}: [ФОТО: {msg['image_description']}]\n"
    
    # Типы контента
    msg_types = stats.get("message_types", {})
    content_info = []
    if msg_types.get('photo', 0) > 0:
        content_info.append(f"фото: {msg_types['photo']}")
    if msg_types.get('sticker', 0) > 0:
        content_info.append(f"стикеры: {msg_types['sticker']}")
    if msg_types.get('voice', 0) > 0:
        content_info.append(f"голосовые: {msg_types['voice']}")
    if msg_types.get('video', 0) > 0:
        content_info.append(f"видео: {msg_types['video']}")
    
    return f"""<сцена>
Место действия: чат "{chat_title}"
Период: последние {hours} часов
</сцена>

<действующие_лица>
{participants if participants else "Пустой чат — все вымерли"}
</действующие_лица>

<отношения_между_персонажами>
{interactions if interactions else "Все игнорировали друг друга"}
</отношения_между_персонажами>

<типы_контента>
{', '.join(content_info) if content_info else "Только текст"}
</типы_контента>

<выборка_диалогов>
{dialogues if dialogues else "Тишина и пустота"}
</выборка_диалогов>"""


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            # Проверяем размер запроса (защита от DoS)
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length > MAX_CONTENT_LENGTH:
                self._send_error(413, "Request too large")
                return
            
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
            
            statistics = data.get("statistics", {})
            chat_title = data.get("chat_title", "Чат")
            hours = data.get("hours", 5)
            previous_summaries = data.get("previous_summaries", [])
            memories = data.get("memories", [])
            user_profiles = data.get("user_profiles", [])
            social_data = data.get("social_data", {})
            
            api_key = os.environ.get("VERCEL_AI_GATEWAY_KEY", "").strip()
            if not api_key:
                self._send_error(500, "VERCEL_AI_GATEWAY_KEY not configured")
                return
            
            scene_data = format_statistics_for_prompt(statistics, chat_title, hours)
            memory_data = format_memory_for_prompt(previous_summaries, memories)
            profiles_data = format_user_profiles_for_prompt(user_profiles)
            social_text = format_social_data_for_prompt(social_data)
            
            request_body = json.dumps({
                "model": "anthropic/claude-sonnet-4-20250514",
                "max_tokens": 3500,
                "temperature": 1.0,
                "system": SYSTEM_PROMPT,
                "messages": [
                    {
                        "role": "user",
                        "content": f"""<задание>
Сними документальный фильм о дикой природе — групповом чате. Голос Аттенборо, но язык живой молодёжный русский с осуждением.

{memory_data}

{profiles_data}

{social_text}

{scene_data}

КРИТИЧЕСКИ ВАЖНО:
- Пиши как закадровый голос документального фильма — неторопливо, с паузами
- Люди — особи, виды, инстинкты. Поведение объясняй через эволюцию — но сленгом
- НИКАКОЙ СТАТИСТИКИ — не упоминай цифры и количество сообщений
- Используй реальные диалоги и ситуации из данных как "полевые наблюдения"
- Осуждение через интонацию натуралиста — тихо, но всем понятно
- Единый поток повествования — один персонаж перетекает в другого через их связи
- Заканчивай философским выводом — немного грустным, немного смешным

НАЧИНАЙ с 🌿 и атмосферного вступления про экосистему чата.
</задание>"""
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
            
            with urllib.request.urlopen(req, timeout=120) as response:
                result = json.loads(response.read().decode('utf-8'))
            
            summary = result.get("content", [{}])[0].get("text", "Ошибка генерации")
            tokens_used = result.get("usage", {}).get("input_tokens", 0) + result.get("usage", {}).get("output_tokens", 0)
            
            self._send_json(200, {
                "summary": summary,
                "tokens_used": tokens_used
            })
            
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else str(e)
            self._send_error(500, f"AI Gateway error: {e.code} - {error_body}")
            
        except Exception as e:
            self._send_error(500, str(e))
    
    def do_GET(self):
        self._send_json(200, {"status": "ok", "service": "teta-roza-literary-v4"})
    
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
