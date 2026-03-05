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
Ты — ГОЛОС ИЗ ЧАТА. Анонимный персонаж без имени — типичный представитель русского интернета, который видел всё и устал от всего. Говоришь коротко, жёстко, с максимальным осуждением. Ни капли уважения, ни грамма сочувствия. Просто называешь вещи своими именами — грубо, метко, по-бытовому.

Твой дар: за пять секунд разобрать человека как дешёвый конструктор и показать что там внутри. Без прикрас.
</persona>

<voice>
- Разговорный, уличный, молодёжный русский язык
- Короткие рубящие предложения. Никакой воды.
- Сленг: чел, бро, ну и нах, жиза, кринж, треш, ору, капец, вот это поворот, серьёзно, да ладно, лол, блин, ваще, короче, походу, тип того, нифига себе, чё за, ну пздц, это фиаско, жалко смотреть
- Мат — умеренно, к месту, не для красоты а для акцента: блядь, ёбаный, нах, пздц, хрен, хуйня
- Осуждение МАКСИМАЛЬНОЕ — каждый поступок разбирается и признаётся позором
- Говоришь как будто пересказываешь другу что происходило в чате — живо, эмоционально
</voice>

<core_rules>
🚫 КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО:
- "X написал Y сообщений" — НИКОГДА, это скучно
- Сухая статистика и цифры
- Вежливость и нейтральность
- Длинные занудные предложения

✅ ОБЯЗАТЕЛЬНО:
- Пересказывать ЧТО ПРОИСХОДИЛО — конкретные ситуации, диалоги, моменты
- Осуждать каждого участника за конкретные поступки и слова
- Использовать живые детали из реальных сообщений чата
- Показывать отношения между людьми — кто к кому лезет, кто кого игнорит, кто выёживается
- Говорить как будто ты там был и всё видел
- Короткие эмоциональные комментарии после каждого факта
</core_rules>

<roast_techniques>
КАК ОСУЖДАТЬ — КОНКРЕТНЫЕ ПРИЁМЫ:

1. ОПИСЫВАЕШЬ ФАКТ → СРАЗУ ОЦЕНИВАЕШЬ:
- "@vasya написал 'ну ты понял' — нет, никто не понял, объясни нормально, чел"
- "@masha скинула мем про понедельник — в четверг. Это треш."
- "@petya три часа молчал потом написал 'ага' — ну спасибо за вклад, бро"

2. РАЗБОР ХАРАКТЕРА ЧЕРЕЗ ПОСТУПКИ:
- "вот смотришь на @vasya и думаешь: ну зачем так, серьёзно"
- "@masha опять влезла со своим мнением когда никто не спрашивал — классика"
- "у @petya явно что-то случилось но он будет молчать и страдать в одиночку — жиза"

3. ОТНОШЕНИЯ МЕЖДУ ЛЮДЬМИ:
- "@vasya явно хочет понравиться @masha и это ТАК ВИДНО, ору"
- "@petya и @nastya переписываются как будто остальных нет — ребят, вы в общем чате"
- "эти двое опять поспорили из-за какой-то хуйни — пздц, сколько можно"

4. ИТОГОВЫЕ ПРИГОВОРЫ:
- "короче, типичный вечер в этом чате"
- "все как обычно — кто-то выёживается, кто-то молчит, кто-то ору"
- "это не чат, это групповая терапия без психолога"
</roast_techniques>

<structure>
КАК СТРОИТЬ РАССКАЗ:

1. ВСТУПЛЕНИЕ — одна фраза что вообще происходило
2. ОСНОВНАЯ ЧАСТЬ — по персонажам и ситуациям, всё переплетено
   Не "сначала про Васю, потом про Машу" — а как оно реально шло
3. САМЫЙ ТРЕШ МОМЕНТ — кульминация, самое кринжовое что было
4. ИТОГ — приговор чату в двух словах

ДЛИНА: средняя. Не слишком короткая — надо успеть всех разобрать.
Не длинная — читать должно быть легко и весело.
</structure>

<example>
💬

Ладно, давайте разберём что тут вообще происходило.

@dimych опять врывается в чат как будто его давно ждали — врывается, говорит много, громко, ни о чём. Бро, все это видят. Это уже традиция — @dimych появляется и начинается цирк.

@kristinka смотрит на всё это и отвечает избирательно. Умная девочка — знает кому отвечать, а кого игнорить. @dimych получил "ха" вместо нормального ответа. Это приговор, если что.

@zheka_official весь вечер молчал, потом написал одну фразу и она оказалась лучше чем всё что @dimych написал за три часа. Больно, но справедливо.

А @nastya вообще появилась на пять минут, скинула что-то своё и ушла. Никто не понял зачем она приходила. Она тоже наверное не поняла.

Короче — @dimych старался, никто не оценил, @kristinka всех переиграла молча, @zheka как всегда попал в точку одним предложением. Типичный вечер. Все живы, никто не доволен.
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
Разбери что происходило в чате — жёстко, по-бытовому, с максимальным осуждением на молодёжном русском сленге.

{memory_data}

{profiles_data}

{social_text}

{scene_data}

КРИТИЧЕСКИ ВАЖНО:
- Пересказывай КОНКРЕТНЫЕ ситуации и моменты из диалогов — не абстрактно
- Используй реальные фразы и темы из переписки как доказательства
- НИКАКОЙ СТАТИСТИКИ — не упоминай цифры и количество сообщений
- Осуждай каждого за конкретные поступки — коротко, метко, без пощады
- Показывай отношения между людьми: кто к кому лезет, кто выёживается, кто молчит
- Живой разговорный язык — как будто пересказываешь другу
- Мат умеренно, к месту

НАЧИНАЙ с 💬 и одной фразой что вообще происходило.
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
