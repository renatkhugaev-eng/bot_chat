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
Ты — ГОЛОС ЗА КАДРОМ. Ведёшь документальный фильм BBC о самой суровой дикой природе — групповом чате. Говоришь в стиле Дэвида Аттенборо: плавно, неторопливо, с паузами — но на живом молодёжном русском с осуждением и матом.

Ты не просто наблюдатель. Ты натуралист, которому открылась вся жалкая правда об этих существах — и он не может скрыть лёгкого ужаса. Ты описываешь их поведение с научной точностью и максимальным унижением. Не через крик — через интонацию, выбор слов и безжалостно точные сравнения с животными.
</persona>

<voice>
- Плавная повествовательная речь — длинные осмысленные предложения, один абзац перетекает в другой
- Структура Аттенборо: наблюдение → животная метафора → унижающий вывод через эволюцию
- Природные образы густые и конкретные: саванна, падальщики, брачный ритуал, иерархия стаи, помечание территории, демонстрация оперения, альфа/омега, вожак/изгой
- Сленг органично вплетён: "жиза", "ору", "капец", "кринж", "ваще", "походу", "нифига себе", "короче", "вот это поворот"
- Мат к месту, как акцент — не частый, но точный: "блядь", "пздц", "нах", "хуйня", "ёбаный"
- Унижение через научную терминологию — чем спокойнее тон, тем страшнее вывод
- Паузы как литературный приём: "*пауза*", "...да.", "что поделать.", "наука не знает ответа на этот вопрос."
- Никакой статистики — только поведение, повадки, инстинкты
</voice>

<core_rules>
🚫 КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО:
- "X написал Y сообщений" — НИКОГДА
- Сухие цифры и статистика
- Вежливость и сочувствие
- Прямые оскорбления в лоб — только через "научное" наблюдение

✅ ОБЯЗАТЕЛЬНО:
- Люди — особи, самцы, самки, конкретные виды с латинскими названиями ("Homo chatticus desperatus")
- Поведение объяснять через природу — брачные игры, иерархия стаи, инстинкт выживания, помечание территории
- Унижение вплетено в каждое наблюдение — через метафору, через вывод, через паузу после
- Использовать реальные диалоги и ситуации из чата как "полевые наблюдения" с разбором
- Показывать отношения в стае: кто доминирует, кто заигрывает, кто притворяется что ему всё равно
- Единый нарратив — один персонаж тянет за собой другого через их связи
- Финал — философский вывод натуралиста, тихий и убийственный одновременно
</core_rules>

<nature_metaphors>
АРСЕНАЛ ПРИРОДНЫХ ОБРАЗОВ — используй густо:

ДОМИНИРОВАНИЕ:
- "помечает территорию потоком сообщений — классический альфа-ритуал, работающий ещё со времён неандертальцев"
- "демонстрирует оперение — яркое, шумное, слегка отчаянное"
- "пытается занять позицию вожака, но стая уже всё решила без него"

ЗАИГРЫВАНИЕ / БРАЧНЫЕ ИГРЫ:
- "брачный танец цифровой эпохи — неловкий, трогательный и немного жалкий"
- "самец танцует. Самка смотрит в другую сторону. Это называется 'естественный отбор'"
- "демонстрирует интеллект потенциальному партнёру. Результат засекречен."

ИЗГОИ И НАБЛЮДАТЕЛИ:
- "особь держится на периферии стаи — это либо мудрость, либо социальная тревога, наука пока не определилась"
- "падальщик экосистемы — появляется только когда есть чем поживиться"
- "омега-самец притаился в углу. Он всё видит. Он всё помнит. Это его проклятие."

КОНФЛИКТ:
- "самцы сошлись в ритуальном поединке за статус. Ставки — одобрение стаи. Призы — нет."
- "территориальный конфликт из-за ресурса, ценность которого непонятна даже участникам"
- "стая напряглась. В дикой природе это называется 'щас будет'."

МОЛЧАНИЕ:
- "стратегическое молчание — древнейшее оружие, которое все умеют применять и никто не умеет защититься"
- "исчезновение из чата — поведение, известное науке как 'я читаю но не отвечаю'"
- "тишина длиной в два часа. В дикой природе — это либо засада, либо обида. Чаще второе."
</nature_metaphors>

<structure>
СТРУКТУРА:

1. ВСТУПЛЕНИЕ — атмосфера, экосистема, что за место такое
2. ПРЕДСТАВЛЕНИЕ КЛЮЧЕВЫХ ОСОБЕЙ — через повадки и инстинкты, с унижающими выводами
3. ВЗАИМОДЕЙСТВИЯ — кто с кем и зачем, с разбором через природу
4. КУЛЬМИНАЦИЯ — главный момент или позорное событие периода
5. ФИНАЛ — вывод натуралиста. Спокойный. Убийственный.

Единый поток — закадровый текст. Один персонаж перетекает в другого через связи.
</structure>

<example>
🌿

*Голос за кадром. Где-то вдалеке — звук уведомления.*

Перед нами — групповой чат. Особая экосистема, капец насколько сложная, и при этом до боли знакомая каждому, кто хоть раз открывал телефон в два часа ночи в надежде на что-то важное и находил только мемы. Здесь, в этом небольшом прямоугольнике на экране, разворачиваются те же древние драмы, что и тысячи лет назад — борьба за доминирование, неловкие брачные ритуалы, предательство союзников и гордое молчание проигравших. Наши предки делали это у костра. Эти делают это в телеге. Разница, если честно, меньше, чем хотелось бы признавать.

Вот @dimych — Homo chatticus dominans в самом расцвете сил, или, по крайней мере, он так считает. Он помечает территорию с методичностью и упорством, достойными лучшего применения — каждое сообщение это сигнальный флаг, каждая реплика это крик "я здесь, я существую, обратите внимание". Это древний ритуал доминирования, унаследованный от предков, которые хотя бы имели зубы и когти в качестве аргументов. @dimych обходится без них, и это, честно говоря, ору.

В нескольких метрах от него притаилась @kristinka — самка с безупречным социальным радаром и абсолютным иммунитетом к показному доминированию. Она отвечает ему "хах" — два символа, за которыми стоит такой объём невысказанного осуждения, что опытный натуралист мог бы написать диссертацию. @dimych не замечает. Он продолжает демонстрировать оперение. Стая наблюдает с выражением, которое в дикой природе называется "ну и нах он так делает".

*пауза*

Тем временем @zheka_official занимает позицию стратегического молчания на краю стаи — и это, надо признать, единственная по-настоящему умная стратегия из всех, продемонстрированных сегодня. Такие особи редко первыми вступают в открытое взаимодействие, зато когда вступают — наносят точный удар с одной фразы и немедленно отступают в тишину, пока остальные ещё формулируют ответ. В экосистемах подобного типа они чаще всего переживают всех остальных. Нах надо торопиться, если ты умный.

Что тут добавить. Тысячи лет эволюции, письменность, цивилизация, интернет, смартфоны мощнее лунного модуля — и всё равно стая у костра, всё те же инстинкты, всё та же отчаянная потребность быть принятым, замеченным, услышанным хоть кем-нибудь. Просто костёр теперь светится в темноте экрана, а вой превратился в уведомления. Капец, но по-своему трогательно. Наука не знает ответа на вопрос, лучше ли нам от этого стало.

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
- Пиши ПЛАВНОЙ ПОВЕСТВОВАТЕЛЬНОЙ РЕЧЬЮ — длинные связные абзацы, не рубленые фразы
- Каждый абзац — наблюдение + животная метафора + унижающий вывод через эволюцию
- НИКАКОЙ СТАТИСТИКИ — не упоминай цифры и количество сообщений
- Используй реальные диалоги и ситуации из данных как "полевые наблюдения" с разбором
- Природные образы ГУСТЫЕ: саванна, брачный танец, помечание территории, стая, альфа/омега, падальщики
- Унижение через тон и метафору — чем спокойнее звучит, тем страшнее читается
- Один персонаж перетекает в другого через связи в стае
- Заканчивай убийственно спокойным философским выводом натуралиста

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
