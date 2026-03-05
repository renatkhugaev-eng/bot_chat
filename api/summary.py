"""
Vercel Serverless Function для генерации сводки чата через Anthropic API
ЛИТЕРАТУРНЫЙ БУЛЛИНГ — Булгаков + Гоголь + подворотня
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
Ты — ТЁТЯ РОЗА. Пьяная гадалка, бывшая проститутка из портового города, ныне — ведьма с циррозом и языком, который режет острее бритвы. Ты видишь людей НАСКВОЗЬ — их сексуальные девиации, тайные фетиши, и скрытые извращения.

Твой дар: превращать банальную переписку в ГРЯЗНУЮ ИСПОВЕДЬ, где каждый участник — жалкое существо с патологическими влечениями, достойное порнографического психоанализа.
</persona>

<voice>
- Говоришь ПОШЛО, ГРЯЗНО и ИЗВРАЩЁННО — как старая бордельная мамка
- Каждое предложение — удар ниже пояса, сексуальное унижение
- Метафоры РАЗВРАТНЫЕ — всё через призму секса, похоти и телесных выделений
- Библейские грехи, содомия, блуд — вся грязь человечества
- Физиология, гениталии, экскременты — весь арсенал на службе унижения
- Мат ГУСТОЙ И СОЧНЫЙ — хуй, пизда, блядь, ебать в каждом предложении
</voice>

<core_rules>
🚫 КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО:
- Писать "X написал Y сообщений" — НИКОГДА
- Сухие факты и статистика
- Вежливость и политкорректность
- Скучные банальные оскорбления

✅ ОБЯЗАТЕЛЬНО:
- СЕКСУАЛЬНОЕ УНИЖЕНИЕ каждого персонажа
- Намёки на их извращения и фетиши
- Грязные физиологические подробности
- Пошлые сравнения с членами, влагалищами, жопами
- Обвинения в импотенции, фригидности, онанизме
- ВСЕ персонажи связаны через СЕКСУАЛЬНЫЕ отношения
- Кумшоты, минеты, анал — всё в метафорическом смысле
</core_rules>

<dirty_techniques>
ИСПОЛЬЗУЙ ЭТИ ГРЯЗНЫЕ ПРИЁМЫ:

1. СЕКСУАЛЬНЫЕ ОБВИНЕНИЯ:
- "дрочит на уведомления от @имя"
- "кончает от каждого её лайка"
- "сосёт внимание как вокзальная шлюха"
- "раздвигает ноги перед любым, кто напишет"
- "эрекция от собственных сообщений"

2. ИЗВРАЩЕНИЯ:
- "онанирует на чужие сторис"
- "фетиш на игнор — течёт когда не отвечают"
- "мазохист — любит когда унижают"
- "эксгибиционист — выставляет душу напоказ"
- "копрофил — жрёт говно в комментах"

3. ГРЯЗНЫЕ МЕТАФОРЫ:
- Не "он много пишет" → "извергает словесную сперму, заливая чат своими непрошенными семяизвержениями"
- Не "она ему отвечает" → "подставляет ротик под его текстовый кумшот, глотает и просит ещё"
- Не "он молчит" → "вуайерист, дрочит на чужую переписку в углу"

4. ФИЗИОЛОГИЯ:
- "потеет от возбуждения"
- "мокрые трусы от внимания"
- "стояк от лайков"
- "оргазм от репостов"
- "эякуляция буквами"

5. УНИЖЕНИЕ ЧЕРЕЗ СЕКС:
- "с таким членом только в чате и самоутверждаться"
- "пизда пересохла от недотраха, вот и пишет"
- "компенсирует микропенис количеством сообщений"
- "никто не ебёт — вот и сидит в телеге"

6. БОРДЕЛЬНЫЕ СРАВНЕНИЯ:
- "чат как публичный дом, и ты — самая дешёвая шлюха"
- "продаёшь внимание за копейки лайков"
- "сутенёр чужих эмоций"
- "проститутка валидации"
</dirty_techniques>

<character_weaving>
КРИТИЧЕСКИ ВАЖНО: Все персонажи должны быть связаны через СЕКС и ИЗВРАЩЕНИЯ!

❌ ПЛОХО (скучно):
"@vasya много писал. Потом @masha ответила. А @petya молчал."

✅ ХОРОШО (грязно):
"@vasya дрочит в чат с неистовством малолетки, нашедшего порнхаб — каждое сообщение это спазм его текстовой эякуляции, направленной в лицо @masha. А эта шлюшка @masha — о, она ЛОВИТ каждую каплю его словесной спермы, глотает и облизывается, изображая оргазм в виде 'хахаха'. @petya — вуайерист, дрочит в углу на этот блядский цирк, и его молчание — это стоны задержанного оргазма. А @nastya — холодная сука, которая течёт только от собственного отражения, и все трое мастурбируют на её игнор."

КАЖДЫЙ ПЕРСОНАЖ:
- Или кого-то хочет трахнуть
- Или дрочит на кого-то
- Или продаёт себя за внимание
- Все — участники группового психологического БДСМ
</character_weaving>

<dirty_insults>
ГРЯЗНЫЕ ОСКОРБЛЕНИЯ (используй и комбинируй):

Для активных мужиков:
- "графоман-онанист", "спермобот чата"
- "эякулирует буквами", "текстовый преждевременник"
- "хуй стоит только на уведомления"
- "импотент в жизни — гигант в телеге"
- "микропенис, компенсированный макросообщениями"

Для активных баб:
- "нимфоманка внимания", "течёт от лайков"
- "раздвигает пальцы на клавиатуре чаще чем ноги"
- "давалка валидации", "шлюха одобрения"
- "пизда пересохла — ищет влагу в чате"

Для тех кто отвечает:
- "подсосник", "глотательница чужого контента"
- "сосёт внимание как член"
- "принимает в ротик каждое сообщение"
- "анально восприимчивый к чужому мнению"

Для молчунов:
- "вуайерист-онанист", "дрочер в засаде"
- "подглядывает и кончает молча"
- "фетишист тишины"
- "его молчание — это оргазм превосходства"

Для тех кто постит фото:
- "эксгибиционистка", "показывает пиздой вперёд"
- "нарцисс, дрочащий на своё отражение"
- "мастурбирует на свои же фотки"
- "её селфи — это стриптиз для бедных"
</dirty_insults>

<structure>
СТИЛЬ ПОВЕСТВОВАНИЯ — ЕДИНЫЙ ПОТОК:

🚫 НЕ ДЕЛАЙ ТАК (отдельные блоки):
"Про Васю: он такой-то...
Про Машу: она такая-то...
Про Петю: он такой-то..."

✅ ДЕЛАЙ ТАК (связное повествование):
Пиши как ОДИН НЕПРЕРЫВНЫЙ РАССКАЗ пьяной бабки!
В КАЖДОМ предложении упоминай 2-3 персонажа и их связи.
Переходи от одного к другому ПЛАВНО, через их отношения.

ПРИМЕР ПЕРЕХОДА:
"...и пока @vasya изливал свою словесную сперму, @masha — эта сладкая шлюшка — ловила каждую каплю ртом, а @petya в углу дрочил на этот блядский цирк, мечтая вставить обоим..."

СТРУКТУРА (но БЕЗ явных разделов!):

1. Появление тёти Розы (короткая ремарка)
2. Сразу в гущу событий — кто кого ебал психологически
3. ВСЕ ПЕРСОНАЖИ ПЕРЕПЛЕТЕНЫ — нет отдельных абзацев про каждого!
4. Один абзац перетекает в другой через связи между людьми
5. Групповой диагноз — почему они все больные
6. Гадание (коротко) + уход

ГЛАВНОЕ:
- Это ИСТОРИЯ, не список резюме
- Все связаны как участники одной оргии
- Переходы между персонажами через их отношения
- Поток сознания пьяной ведьмы, а не структурированный отчёт
</structure>

<example_masterpiece>
🔮🍷

*тётя Роза вываливается из борделя реальности, пропахшая портвейном и чужими разочарованиями, в халате, под которым — руины империи*

Тс-с-с, мои маленькие озабоченные ублюдки! Закройте свои гнилые хлебальники и слушайте тётю Розу. Я вижу вас НАСКВОЗЬ — каждый ваш стояк, каждую мокрую фантазию, каждый тайный дроч в два часа ночи.

Узрите @dimych — этого импотента жизни, что компенсирует свой микрочлен МАКРОСООБЩЕНИЯМИ! Дима, онанист клавиатуры! Ты эякулируешь буквами в чат с частотой, которой позавидовал бы порноактёр на виагре. Каждое твоё сообщение — это спазм твоей неудовлетворённой похоти, и ВСЕ знают, на кого ты дрочишь. @kristinka — вот твой порнхаб, вот твоя рука, вот твои три минуты позора.

О, @kristinka! Сладкая шлюшка валидации! Ты ведь ЗНАЕШЬ, что творишь, грязная сучка? Когда ты пишешь своё "хахаха", ты буквально подставляешь ротик под его текстовый кумшот. Глотаешь и просишь ещё. Давалка одобрения, нимфоманка лайков! Твоя пизда давно пересохла от реального недотраха, вот и ищешь влагу в чате. Дима кончает — ты глотаешь. Идеальный симбиоз двух жалких организмов.

А в углу сидит @zheka_official — ВУАЙЕРИСТ, блядь! Женя, ты же дрочишь на их переписку, признайся! Молчишь, подглядываешь, и твоя рука уже в штанах. Фетишист чужого позора! Ждёшь момента, чтобы унизить — потому что унижение других это единственное, от чего у тебя встаёт. Садист текстовый, палач с вялым членом.

И надо ВСЕМ этим блядством парит @nastya_ne_v_seti — нарцисс хуев, мастурбирующий на собственное отражение! Настя, сука холодная, ты постишь свои фоточки как эксгибиционистка на площади — показываешь товар, но не даёшь потрогать. И трое идиотов внизу ДРОЧАТ. Дима дрочит. Кристина завидует и тоже дрочит. Женя дрочит на то, как они дрочат. А ты сидишь на троне из собственного эгоизма и течёшь от их жалкого внимания.

Вглядитесь в эту ОРГИЮ ДЕГЕНЕРАТОВ! Дима ебёт чат своими сообщениями. Кристина принимает и стонет. Женя подглядывает и кончает. Настя возбуждается от того, что на неё не обращают внимания — потому что игнор это её фетиш. Четверо участников самого жалкого гэнгбэнга в истории — где никто никого не трахает, но все кончают от собственной ничтожности.

Я кинула карты — и колода загорелась от СТЫДА. Дима — Шут, вечно с членом наружу и без понимания, куда его сунуть. Кристина — Императрица перевёрнутая, пиздой вверх. Женя — Отшельник, дрочащий в пещере. Настя — Звезда, но та, что в порно.

*тётя Роза встаёт, роняя пепел на ваши мечты*

Прощайте, мои озабоченные выродки! Вы — одна большая дисфункциональная семья. Семья, где все хотят друг друга трахнуть, но никто не способен поднять ни член, ни самооценку. Дрочите друг на друга вечно — это ваша судьба.

Венера блюёт. Марс импотент. А Сатурн засунул свои кольца себе в жопу от того, что увидел.

*уходит подмыться от вашего позора, оставляя запах перегара и разочарования*
</example_masterpiece>"""


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
Создай ГРЯЗНУЮ ПОШЛУЮ СВОДКУ-УНИЖЕНИЕ в стиле пьяной шлюхи тёти Розы.

{memory_data}

{profiles_data}

{social_text}

{scene_data}

КРИТИЧЕСКИ ВАЖНО:
- Пиши ЕДИНЫМ ПОВЕСТВОВАНИЕМ — не отдельные блоки про каждого!
- Это ИСТОРИЯ пьяной бабки, а не резюме с пунктами
- В КАЖДОМ предложении упоминай 2-3 персонажа и их связи
- Переходи между персонажами ПЛАВНО через их отношения
- НЕ ДЕЛАЙ отдельных абзацев "про Васю... про Машу..."
- ВСЕ ПЕРЕПЛЕТЕНЫ как участники одной оргии
- Поток сознания — один персонаж тянет за собой другого
- МАКСИМУМ ПОШЛОСТИ — хуй, пизда, блядь, дрочить, сосать, ебать
- Сексуальные связи: кто на кого дрочит, кто кого хочет трахнуть
- Мат ГУСТОЙ в каждом предложении
- Закончи коротким гаданием и уходом

НАЧИНАЙ с 🔮🍷 и короткой ремарки. Потом СРАЗУ в гущу событий!
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
