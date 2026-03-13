import asyncio
import functools
import logging
import random
import re
import time
from typing import Optional, List, Dict, Any

from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ChatMemberUpdated, BufferedInputFile, TelegramObject,
    BotCommand, BotCommandScopeAllGroupChats, BotCommandScopeAllPrivateChats,
    BotCommandScopeChat
)
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import BOT_TOKEN, CLASSES, CRIMES, RANDOM_EVENTS, WELCOME_MESSAGES, JAIL_PHRASES
import aiohttp
import json
import os
from dotenv import load_dotenv
from contextlib import asynccontextmanager

load_dotenv()

# ==================== ГЛОБАЛЬНАЯ HTTP СЕССИЯ ====================
# Переиспользуем одну сессию для всех API запросов — +30% скорость

_http_session: Optional[aiohttp.ClientSession] = None
_http_session_lock = asyncio.Lock()


async def get_http_session() -> aiohttp.ClientSession:
    """Получить глобальную HTTP сессию (создаёт если нет, потокобезопасно)"""
    global _http_session
    if _http_session is None or _http_session.closed:
        async with _http_session_lock:
            # Double-check после получения блокировки
            if _http_session is None or _http_session.closed:
                timeout = aiohttp.ClientTimeout(total=60, connect=10)
                _http_session = aiohttp.ClientSession(
                    timeout=timeout,
                    headers={"User-Agent": "TetaRozaBot/1.0"}
                )
    return _http_session


async def close_http_session():
    """Закрыть HTTP сессию при выключении"""
    global _http_session
    if _http_session and not _http_session.closed:
        await _http_session.close()
        _http_session = None

# Выбор базы данных: PostgreSQL (продакшн) или SQLite (локально)
USE_POSTGRES = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL")

if USE_POSTGRES:
    from database_postgres import (
        init_db, get_player, create_player, set_player_class, update_player_stats,
        get_top_players, is_in_jail, put_in_jail, get_all_active_players,
        add_to_treasury, get_treasury, log_event, add_achievement,
        save_chat_message, get_chat_statistics, get_player_achievements, close_db,
        save_summary, get_previous_summaries, save_memory, get_memories,
        get_user_messages, full_cleanup, get_database_stats,
        get_all_chats_stats, get_chat_details, get_top_users_global, search_user,
        health_check, save_chat_info,
        save_media, get_random_media, get_media_stats, increment_media_usage,
        migrate_media_from_messages,
        get_user_profile, get_user_gender, analyze_and_update_user_gender,
        update_user_gender_incrementally, update_user_profile_comprehensive,
        get_user_full_profile, get_user_activity_report, get_chat_social_graph,
        get_user_profile_for_ai, get_enriched_chat_data_for_ai, get_chat_social_data_for_ai,
        find_user_in_chat, get_all_chat_profiles, get_user_memories,
        get_active_chats_for_auto_summary,
        get_random_messages_for_music, get_random_user_messages_for_music
    )
else:
    from database import (
        init_db, get_player, create_player, set_player_class, update_player_stats,
        get_top_players, is_in_jail, put_in_jail, get_all_active_players,
        add_to_treasury, get_treasury, log_event, add_achievement,
        save_chat_message, get_chat_statistics, get_player_achievements,
        save_summary, get_previous_summaries, save_memory, get_memories,
        get_user_messages, get_user_memories, find_user_in_chat,
        get_all_chat_profiles, get_active_chats_for_auto_summary
    )
    close_db = None
    # Заглушки для SQLite
    async def full_cleanup(): return {}
    async def get_database_stats(): return {}
    async def get_all_chats_stats(): return []
    async def get_chat_details(chat_id): return {}
    async def get_top_users_global(limit=20): return []
    async def search_user(query): return []
    async def health_check(): return False
    async def save_chat_info(chat_id, title=None, username=None, chat_type=None): pass
    async def save_media(chat_id, user_id, file_id, file_type, file_unique_id=None, description=None, caption=None): return False
    async def get_random_media(chat_id, file_type=None): return None
    async def get_media_stats(chat_id): return {'total': 0}
    async def increment_media_usage(media_id): pass
    async def migrate_media_from_messages(): return {'migrated': 0, 'skipped': 0, 'errors': 0}
    # Заглушки для профилирования пользователей (только PostgreSQL)
    async def get_user_profile(user_id, chat_id=None): return None
    async def get_user_gender(user_id, chat_id=None): return 'unknown'
    async def analyze_and_update_user_gender(user_id, chat_id, first_name="", username=""): return {'gender': 'unknown', 'confidence': 0.0, 'female_score': 0, 'male_score': 0, 'messages_analyzed': 0}
    async def update_user_gender_incrementally(user_id, chat_id, new_message, first_name="", username=""): return {'gender': 'unknown', 'confidence': 0.0, 'female_score': 0, 'male_score': 0, 'messages_analyzed': 0}
    async def update_user_profile_comprehensive(user_id, chat_id, message_text, timestamp, first_name="", username="", reply_to_user_id=None, message_type="text", sticker_emoji=None): pass
    async def get_user_full_profile(user_id, chat_id): return None
    async def get_user_activity_report(user_id, chat_id): return {'error': 'PostgreSQL required'}
    async def get_chat_social_graph(chat_id): return []
    async def get_user_profile_for_ai(user_id, chat_id, first_name="", username=""): return {'user_id': user_id, 'name': first_name or username or 'Аноним', 'gender': 'unknown', 'description': '', 'traits': [], 'interests': [], 'social': {}}
    async def get_enriched_chat_data_for_ai(chat_id, hours=5): return {'profiles': [], 'profiles_text': '', 'social': {}, 'social_text': ''}
    async def get_chat_social_data_for_ai(chat_id): return {'relationships': [], 'conflicts': [], 'friendships': [], 'description': ''}
    async def get_all_chat_profiles(chat_id, limit=50): return []
    async def get_user_memories(chat_id, user_id, limit=10): return []
    async def get_active_chats_for_auto_summary(min_messages=50, hours=12): return []
    async def find_user_in_chat(chat_id, search_term): return None
from game_utils import (
    format_player_card, format_top_players, get_rank, get_next_rank,
    calculate_crime_success, calculate_crime_reward, get_random_crime_message,
    calculate_pvp_success, calculate_pvp_steal_amount, get_random_attack_message,
    get_experience_for_action, check_achievements, get_random_phrase, ACHIEVEMENTS
)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
scheduler = AsyncIOScheduler()

# Хранение активных событий и кулдаунов
active_events = {}  # chat_id -> event_data
cooldowns = {}  # (user_id, chat_id, action) -> timestamp

# ==================== ЗАЩИТА ОТ КОМАНД В РЕПЛАЙ НА БОТА ====================

FUCK_OFF_REPLIES = [
    "Ты чё, ёбнутый? Нахуя ты меня тегаешь командой?",
    "Отъебись от меня со своими командами",
    "Блядина, кого ты тегаешь? Иди нахуй",
    "Пошёл нахуй со своими командами, не трогай меня",
    "Ебать ты дурак. Команды мне кидает. Отвали",
    "Чё те надо, уёбок? Зачем меня тегать командой?",
    "Иди нахуй, я тебе не собачка на команды реагировать",
    "Ты кому команды шлёшь, блядь? Отъебись",
    "Нахуй иди со своими командами, долбоёб",
    "Ебанько, ты зачем меня тегнул командой? Отвали нахуй",
    "Сука, ещё раз тегнешь — заблокирую нахуй",
    "Команды он мне шлёт, пиздец. Иди на хуй",
    "Ты чё творишь, мразь? Не тегай меня командами",
    "Блять, ну и дебил. Отъебись со своими командами",
    "Пиздуй отсюда с командами, не трогай тётю Розу",
    "Ебать ты наглый. Команды мне. В пизду иди",
    "Чё за хуйня? Зачем меня тегать командой, уёбище?",
    "Нет блять, ты серьёзно? Команды мне кидаешь? Нахуй",
    "Слышь, ты, ёбаный рот. Не тегай меня командами",
    "Ахуеть ты дерзкий. Пошёл нахуй с командами",
    "Мне похуй на твои команды, отъебись",
    "Ебанутый? Зачем команду на меня? Иди нахуй",
    "Тётя Роза не отвечает на команды в реплай. Отъебись.",
    "Команды свои себе в жопу засунь, уёбок",
    "Нахуя ты меня тегнул командой, дебил?",
]

# Кэш информации о боте (ID и username)
_cached_bot_id: Optional[int] = None
_cached_bot_username: Optional[str] = None
_bot_info_lock = asyncio.Lock()


async def _ensure_bot_info_cached():
    """Загрузить и кэшировать информацию о боте (потокобезопасно)"""
    global _cached_bot_id, _cached_bot_username
    if _cached_bot_id is None or _cached_bot_username is None:
        async with _bot_info_lock:
            if _cached_bot_id is None or _cached_bot_username is None:
                bot_info = await bot.get_me()
                _cached_bot_id = bot_info.id
                _cached_bot_username = bot_info.username


async def get_bot_id() -> int:
    """Получить ID бота (кэшируется)"""
    await _ensure_bot_info_cached()
    return _cached_bot_id


async def get_bot_username() -> str:
    """Получить username бота (кэшируется)"""
    await _ensure_bot_info_cached()
    return _cached_bot_username


async def is_reply_to_bot(message: Message) -> bool:
    """Проверить, является ли сообщение реплаем на бота"""
    if not message.reply_to_message or not message.reply_to_message.from_user:
        return False
    bot_id = await get_bot_id()
    return message.reply_to_message.from_user.id == bot_id


async def check_command_reply_to_bot(message: Message) -> bool:
    """
    Проверяет, является ли сообщение командой в реплай на бота.
    Если да — отправляет нахуй и возвращает True (команду выполнять не надо).
    """
    # Проверяем что это команда
    if not message.text or not message.text.startswith("/"):
        return False
    
    # Проверяем что это реплай
    if not message.reply_to_message or not message.reply_to_message.from_user:
        return False
    
    # Проверяем что реплай на бота
    bot_id = await get_bot_id()
    if message.reply_to_message.from_user.id != bot_id:
        return False
    
    # Это реплай на бота с командой — посылаем нахуй
    response = random.choice(FUCK_OFF_REPLIES)
    await message.reply(response)
    logger.info(f"Blocked command reply to bot from user {message.from_user.id}: {message.text[:30]}")
    return True


def check_cooldown(user_id: int, chat_id: int, action: str, cooldown_seconds: int) -> tuple[bool, int]:
    """Проверить кулдаун. Возвращает (можно_ли, оставшееся_время)"""
    key = (user_id, chat_id, action)
    current_time = time.time()
    
    if key in cooldowns:
        remaining = cooldowns[key] - current_time
        if remaining > 0:
            return False, int(remaining)
    
    cooldowns[key] = current_time + cooldown_seconds
    
    # Очистка старых записей (раз в 100 записей или каждые 500 записей)
    if len(cooldowns) > 500 or (len(cooldowns) > 100 and len(cooldowns) % 50 == 0):
        cleanup_cooldowns()
    
    return True, 0


def cleanup_cooldowns():
    """Удалить истёкшие кулдауны"""
    current_time = time.time()
    expired_keys = [k for k, v in cooldowns.items() if v < current_time]
    for key in expired_keys:
        del cooldowns[key]


def cleanup_api_calls():
    """Удалить устаревшие записи API вызовов"""
    current_time = time.time()
    for key in list(api_calls.keys()):
        if key in api_calls:
            # Удаляем записи старше 5 минут
            api_calls[key] = [t for t in api_calls[key] if current_time - t < 300]
            # Если список пустой — удаляем ключ
            if not api_calls[key]:
                del api_calls[key]


# ==================== УТИЛИТЫ ====================

def split_long_message(text: str, max_length: int = 4000) -> list[str]:
    """
    Безопасно разбивает длинный текст на части.
    Разрезает по границам абзацев или предложений, не посередине слов/HTML-тегов.
    """
    if len(text) <= max_length:
        return [text]
    
    parts = []
    current = ""
    
    # Сначала пробуем разбить по абзацам
    paragraphs = text.split('\n\n')
    
    for para in paragraphs:
        if len(current) + len(para) + 2 <= max_length:
            current = current + "\n\n" + para if current else para
        else:
            # Если абзац слишком длинный, разбиваем по предложениям
            if len(para) > max_length:
                if current:
                    parts.append(current.strip())
                    current = ""
                
                # Разбиваем абзац по предложениям
                sentences = para.replace('. ', '.|').replace('! ', '!|').replace('? ', '?|').split('|')
                for sent in sentences:
                    if len(current) + len(sent) + 1 <= max_length:
                        current = current + " " + sent if current else sent
                    else:
                        if current:
                            parts.append(current.strip())
                        # Если предложение слишком длинное, режем по словам
                        if len(sent) > max_length:
                            words = sent.split(' ')
                            current = ""
                            for word in words:
                                if len(current) + len(word) + 1 <= max_length:
                                    current = current + " " + word if current else word
                                else:
                                    if current:
                                        parts.append(current.strip())
                                    current = word
                        else:
                            current = sent
            else:
                if current:
                    parts.append(current.strip())
                current = para
    
    if current:
        parts.append(current.strip())
    
    return parts if parts else [text[:max_length]]


# ==================== СБОР КОНТЕКСТА (DRY) ====================

async def gather_user_context(chat_id: int, user_id: int, limit: int = 1000) -> tuple[str, int]:
    """
    Собирает контекст сообщений пользователя для AI-команд.
    Загружает до 1000 сообщений, выбирает ~40 самых информативных.
    Возвращает (context_string, messages_count)
    """
    context_parts = []
    messages_found = 0
    
    if not USE_POSTGRES:
        return "Сообщений нет — база данных недоступна", 0
    
    try:
        user_messages = await get_user_messages(chat_id, user_id, limit=limit)
        if user_messages:
            texts = [
                msg['message_text'] 
                for msg in user_messages 
                if msg.get('message_text') and len(msg.get('message_text', '')) > 3
            ]
            messages_found = len(texts)
            
            if texts:
                # Берём разнообразный контекст из всей истории:
                # - 20 самых длинных (информативных)
                # - 15 последних (актуальных)
                # - 5 случайных из середины (для разнообразия)
                interesting = sorted(texts, key=len, reverse=True)[:20]
                recent = texts[:15]
                
                # Случайные из середины (если есть)
                middle_texts = texts[15:-20] if len(texts) > 35 else []
                import random
                random_middle = random.sample(middle_texts, min(5, len(middle_texts))) if middle_texts else []
                
                all_texts = list(dict.fromkeys(interesting + recent + random_middle))[:40]
                
                for i, text in enumerate(all_texts, 1):
                    truncated = text[:200] + "..." if len(text) > 200 else text
                    context_parts.append(f'{i}. "{truncated}"')
    except Exception as e:
        logger.warning(f"Could not fetch user messages: {e}")
    
    if context_parts:
        return "\n".join(context_parts), messages_found
    else:
        return "Сообщений нет — молчит как партизан", 0


async def gather_user_memory(chat_id: int, user_id: int, user_name: str = "") -> str:
    """
    Собирает МНОГОУРОВНЕВУЮ память о пользователе для AI-команд:
    
    Уровень 1: Профиль (пол, стиль, активность)
    Уровень 2: Умные факты (извлечённые AI)
    Уровень 3: Воспоминания (старая система)
    Уровень 4: Социальные связи
    Уровень 5: События чата (опционально)
    
    Возвращает форматированную строку для контекста AI.
    """
    if not USE_POSTGRES:
        return ""
    
    memory_parts = []
    
    try:
        # 1. Профиль пользователя (базовый уровень)
        profile = await get_user_profile_for_ai(user_id, chat_id, user_name, "")
        if profile:
            gender = profile.get('gender', 'неизвестно')
            style = profile.get('communication_style', '')
            activity = profile.get('activity_level', '')
            traits = profile.get('traits', [])
            interests = profile.get('interests', [])
            
            profile_lines = []
            if gender != 'unknown':
                profile_lines.append(f"Пол: {gender}")
            if style:
                profile_lines.append(f"Стиль общения: {style}")
            if activity:
                profile_lines.append(f"Активность: {activity}")
            if traits:
                profile_lines.append(f"Черты: {', '.join(traits[:5])}")
            if interests:
                profile_lines.append(f"Интересы: {', '.join(interests[:5])}")
            
            # Социальные связи
            social = profile.get('social', {})
            if social.get('friends'):
                profile_lines.append(f"Друзья: {', '.join(social['friends'][:3])}")
            if social.get('enemies'):
                profile_lines.append(f"Конфликты с: {', '.join(social['enemies'][:3])}")
            
            if profile_lines:
                memory_parts.append("=== ПРОФИЛЬ ===")
                memory_parts.extend(profile_lines)
        
        # 2. УМНЫЕ ФАКТЫ (новая система — извлечённые AI)
        try:
            from database_postgres import get_user_facts
            facts = await get_user_facts(chat_id, user_id, limit=15, min_confidence=0.6)
            if facts:
                memory_parts.append("\n=== ИЗВЕСТНЫЕ ФАКТЫ ===")
                for f in facts:
                    fact_type = f.get('fact_type', '')
                    fact_text = f.get('fact_text', '')
                    confidence = f.get('confidence', 0)
                    times = f.get('times_confirmed', 1)
                    # Более уверенные факты помечаем
                    marker = "✓" if confidence > 0.8 or times > 2 else "•"
                    if fact_text:
                        memory_parts.append(f"{marker} [{fact_type}] {fact_text}")
        except ImportError:
            pass
        except Exception as e:
            logger.debug(f"Could not get user facts: {e}")
        
        # 3. Старые воспоминания (legacy система)
        memories = await get_user_memories(chat_id, user_id, limit=10)
        if memories:
            memory_parts.append("\n=== ВОСПОМИНАНИЯ ===")
            for m in memories:
                memory_type = m.get('memory_type', '')
                memory_text = m.get('memory_text', '')
                if memory_text:
                    memory_parts.append(f"• [{memory_type}] {memory_text[:150]}")
        
        # 4. Последние сводки чата (контекст)
        try:
            from database_postgres import get_recent_summaries
            summaries = await get_recent_summaries(chat_id, limit=2, hours=48)
            if summaries:
                memory_parts.append("\n=== ЧТО БЫЛО В ЧАТЕ ===")
                for s in summaries:
                    text = s.get('summary_text', '')[:200]
                    if text:
                        memory_parts.append(f"• {text}")
        except ImportError:
            pass
        except Exception as e:
            logger.debug(f"Could not get summaries: {e}")
        
    except Exception as e:
        logger.warning(f"Could not gather user memory: {e}")
    
    return "\n".join(memory_parts) if memory_parts else ""


# ==================== RATE LIMITER ДЛЯ API ====================

# Глобальный счётчик API вызовов (защита от спама)
api_calls = {}  # (chat_id, api_type) -> [timestamps]
API_LIMITS = {
    "poem": (5, 60),      # 5 вызовов в минуту на чат
    "diagnosis": (5, 60),
    "burn": (5, 60),
    "drink": (5, 60),
    "suck": (10, 60),
    "summary": (2, 300),  # 2 сводки за 5 минут
    "vision": (10, 60),
    "ventilate": (10, 60),  # 10 проветриваний в минуту
    "dream": (5, 60),     # 5 снов в минуту на чат
}


def check_api_rate_limit(chat_id: int, api_type: str) -> tuple[bool, int]:
    """
    Проверить rate limit для API.
    Возвращает (можно_ли, секунд_до_сброса)
    """
    if api_type not in API_LIMITS:
        return True, 0
    
    max_calls, window_seconds = API_LIMITS[api_type]
    key = (chat_id, api_type)
    current_time = time.time()
    
    # Очищаем старые записи
    if key in api_calls:
        api_calls[key] = [t for t in api_calls[key] if current_time - t < window_seconds]
    else:
        api_calls[key] = []
    
    # Проверяем лимит
    if len(api_calls[key]) >= max_calls:
        oldest = min(api_calls[key])
        wait_time = int(window_seconds - (current_time - oldest))
        return False, max(1, wait_time)
    
    # Добавляем текущий вызов
    api_calls[key].append(current_time)
    return True, 0


# ==================== МЕТРИКИ ====================

class BotMetrics:
    """Простые метрики для мониторинга"""
    def __init__(self):
        self.commands_count = {}  # command -> count
        self.api_calls_count = {}  # api_type -> count
        self.errors_count = 0
        self.start_time = time.time()
    
    def track_command(self, command: str):
        self.commands_count[command] = self.commands_count.get(command, 0) + 1
    
    def track_api_call(self, api_type: str):
        self.api_calls_count[api_type] = self.api_calls_count.get(api_type, 0) + 1
    
    def track_error(self):
        self.errors_count += 1
    
    def get_stats(self) -> dict:
        uptime = int(time.time() - self.start_time)
        return {
            "uptime_seconds": uptime,
            "uptime_human": f"{uptime // 3600}ч {(uptime % 3600) // 60}м",
            "total_commands": sum(self.commands_count.values()),
            "top_commands": sorted(self.commands_count.items(), key=lambda x: -x[1])[:5],
            "total_api_calls": sum(self.api_calls_count.values()),
            "api_calls": self.api_calls_count,
            "errors": self.errors_count
        }

metrics = BotMetrics()


# ==================== КОМАНДЫ ====================

@router.message(CommandStart())
async def cmd_start(message: Message):
    """Начало игры — РАЗЪЁБ приветствие"""
    if message.chat.type == "private":
        welcome_private = """
🦯 *ХРОМАЯ ШЛЮХА ТЁТЯ РОЗА*

Здарова. Я Тётя Роза — пьяная цыганка-астролог из соседнего подъезда.

Добавь меня в *групповой чат* и я буду:
• Следить за каждым сообщением 👁
• Писать сводки с матом и унижениями 📺
• Сжигать друзей на костре правды 🔥
• Бухать и сливать секреты 🍻
• Ставить диагнозы из подвала 🏥
• Посылать сосать (философски) 🍭

Обидчивым — нахуй в другой бот.

/help — команды

_Бот разработан каналом_ [Чернила и Кровь](https://t.me/dark_bookshelf)
"""
        await message.answer(welcome_private, parse_mode=ParseMode.MARKDOWN)
        return
    
    welcome_group = f"""
🦯 *ХРОМАЯ ШЛЮХА ТЁТЯ РОЗА*

{message.from_user.first_name}, ты попал.

Тётя Роза — пьяная цыганка-астролог из соседнего подъезда — теперь живёт в этом чате. Она видит каждое твоё сообщение. Каждую фотку. Каждый стикер. Она запоминает. Она ждёт.

Когда придёт время — она расскажет всё. С матом. С унижениями. С правдой, которую ты не хотел слышать.

Обратной дороги нет. Добро пожаловать в ад.

/help — узнать на что способна Тётя Роза

━━━━━━━━━━━━━━━━━━━━━━━━
_Бот разработан каналом_ [Чернила и Кровь](https://t.me/dark_bookshelf)
"""
    
    await message.answer(
        welcome_group,
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True
    )


@router.callback_query(F.data.startswith("class_"))
async def choose_class(callback: CallbackQuery):
    """Выбор класса персонажа"""
    class_id = callback.data.replace("class_", "")
    
    if class_id not in CLASSES:
        await callback.answer("❌ Такого класса не существует!", show_alert=True)
        return
    
    user_id = callback.from_user.id
    chat_id = callback.message.chat.id
    
    player = await get_player(user_id, chat_id)
    if player and player['player_class']:
        await callback.answer("😏 Ты уже выбрал класс, братиш!", show_alert=True)
        return
    
    class_data = CLASSES[class_id]
    await set_player_class(user_id, chat_id, class_id, class_data)
    
    welcome = random.choice(WELCOME_MESSAGES).format(name=callback.from_user.first_name or "Братан")
    
    result_text = (
        f"🎉 *ПОЗДРАВЛЯЕМ!*\n\n"
        f"{welcome}\n\n"
        f"Твой класс: {class_data['emoji']} *{class_data['name']}*\n"
        f"_{class_data['starter_phrase']}_\n\n"
        f"💰 Стартовый капитал: 100 лавэ\n"
        f"🎯 Теперь ты можешь:\n"
        f"• /crime — пойти на дело\n"
        f"• /attack @username — наехать на лоха\n"
        f"• /profile — глянуть досье\n"
        f"• /top — топ авторитетов\n\n"
        f"Да начнётся беспредел! 😈"
    )
    
    try:
        await callback.message.edit_text(result_text, parse_mode=ParseMode.MARKDOWN)
    except TelegramBadRequest:
        # Сообщение удалено или слишком старое — отправляем новое
        await callback.message.answer(result_text, parse_mode=ParseMode.MARKDOWN)
    await callback.answer()


@router.message(Command("profile", "me", "stats"))
async def cmd_profile(message: Message):
    """Показать профиль игрока"""
    if message.chat.type == "private":
        await message.answer("❌ Эта команда работает только в групповых чатах!")
        return
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Если упомянут другой пользователь
    if message.reply_to_message and message.reply_to_message.from_user:
        user_id = message.reply_to_message.from_user.id
    
    player = await get_player(user_id, chat_id)
    
    if not player or not player['player_class']:
        if user_id == message.from_user.id:
            await message.answer(
                "❌ Ты ещё не в деле!\n"
                "Напиши /start чтобы начать криминальную карьеру!"
            )
        else:
            await message.answer("❌ Этот человек ещё не в криминале!")
        return
    
    card = format_player_card(player)
    await message.answer(f"```\n{card}\n```", parse_mode=ParseMode.MARKDOWN)


@router.message(Command("top", "leaderboard", "rating"))
async def cmd_top(message: Message):
    """Показать топ игроков"""
    if message.chat.type == "private":
        await message.answer("❌ Эта команда работает только в групповых чатах!")
        return
    
    chat_id = message.chat.id
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⭐ По опыту", callback_data="top_experience"),
            InlineKeyboardButton(text="💰 По лавэ", callback_data="top_money")
        ],
        [
            InlineKeyboardButton(text="🎯 По делам", callback_data="top_crimes_success"),
            InlineKeyboardButton(text="⚔️ По PvP", callback_data="top_pvp_wins")
        ]
    ])
    
    players = await get_top_players(chat_id, limit=10, sort_by="experience")
    text = format_top_players(players, "experience")
    
    await message.answer(text, reply_markup=keyboard)


@router.callback_query(F.data.startswith("top_"))
async def show_top(callback: CallbackQuery):
    """Показать разные топы"""
    sort_by = callback.data.replace("top_", "")
    chat_id = callback.message.chat.id
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⭐ По опыту", callback_data="top_experience"),
            InlineKeyboardButton(text="💰 По лавэ", callback_data="top_money")
        ],
        [
            InlineKeyboardButton(text="🎯 По делам", callback_data="top_crimes_success"),
            InlineKeyboardButton(text="⚔️ По PvP", callback_data="top_pvp_wins")
        ]
    ])
    
    players = await get_top_players(chat_id, limit=10, sort_by=sort_by)
    text = format_top_players(players, sort_by)
    
    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
    except TelegramBadRequest:
        await callback.message.answer(text, reply_markup=keyboard)
    await callback.answer()


@router.message(Command("crime", "delo", "work"))
async def cmd_crime(message: Message):
    """Пойти на дело"""
    if message.chat.type == "private":
        await message.answer("❌ Криминал — дело групповое!")
        return
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    player = await get_player(user_id, chat_id)
    if not player or not player['player_class']:
        await message.answer("❌ Сначала вступи в гильдию! /start")
        return
    
    # Проверка тюрьмы
    in_jail, remaining = await is_in_jail(user_id, chat_id)
    if in_jail:
        phrase = random.choice(JAIL_PHRASES).format(time=remaining)
        await message.answer(phrase)
        return
    
    # Показываем доступные дела
    rank = get_rank(player['experience'])
    player_level = rank['level']
    
    available_crimes = [c for c in CRIMES if c['min_level'] <= player_level]
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"{crime['name']} (ур.{crime['min_level']}+)",
            callback_data=f"crime_{i}"
        )]
        for i, crime in enumerate(CRIMES)
        if crime['min_level'] <= player_level
    ])
    
    crimes_text = "\n".join([
        f"{crime['name']}\n"
        f"  💰 {crime['min_reward']}-{crime['max_reward']} лавэ | "
        f"🎯 {crime['success_rate']}% | ⏰ КД {crime['cooldown']}с"
        for crime in available_crimes
    ])
    
    await message.answer(
        f"🔫 *ВЫБЕРИ ДЕЛО:*\n\n{crimes_text}",
        reply_markup=keyboard,
        parse_mode=ParseMode.MARKDOWN
    )


@router.callback_query(F.data.startswith("crime_"))
async def do_crime(callback: CallbackQuery):
    """Выполнить преступление"""
    try:
        crime_index = int(callback.data.replace("crime_", ""))
    except ValueError:
        await callback.answer("❌ Некорректные данные!", show_alert=True)
        return
    
    if crime_index < 0 or crime_index >= len(CRIMES):
        await callback.answer("❌ Такого дела не существует!", show_alert=True)
        return
    
    crime = CRIMES[crime_index]
    user_id = callback.from_user.id
    chat_id = callback.message.chat.id
    
    player = await get_player(user_id, chat_id)
    if not player:
        await callback.answer("❌ Ты не в игре!", show_alert=True)
        return
    
    # Проверка уровня
    rank = get_rank(player['experience'])
    if rank['level'] < crime['min_level']:
        await callback.answer(f"❌ Нужен уровень {crime['min_level']}!", show_alert=True)
        return
    
    # Проверка тюрьмы
    in_jail, remaining = await is_in_jail(user_id, chat_id)
    if in_jail:
        await callback.answer(f"⛓️ Ты в тюрьме ещё {remaining} сек!", show_alert=True)
        return
    
    # Проверка кулдауна
    can_do, cooldown_remaining = check_cooldown(user_id, chat_id, f"crime_{crime_index}", crime['cooldown'])
    if not can_do:
        await callback.answer(f"⏰ Подожди ещё {cooldown_remaining} сек!", show_alert=True)
        return
    
    # Выполняем преступление
    success = calculate_crime_success(player, crime)
    
    if success:
        reward = calculate_crime_reward(crime, player)
        exp_gain = get_experience_for_action("crime_medium", True)
        
        # Обновляем статистику
        await update_player_stats(
            user_id, chat_id,
            money=f"+{reward}",
            experience=f"+{exp_gain}",
            crimes_success=f"+1",
            total_stolen=f"+{reward}"
        )
        
        # 10% идёт в общак
        treasury_cut = int(reward * 0.1)
        await add_to_treasury(chat_id, treasury_cut)
        
        crime_msg = get_random_crime_message(crime, True, reward=reward)
        
        result_text = (
            f"✅ *ДЕЛО ВЫГОРЕЛО!*\n\n"
            f"{crime_msg}\n\n"
            f"💰 +{reward} лавэ\n"
            f"⭐ +{exp_gain} опыта\n"
            f"🏦 {treasury_cut} ушло в общак"
        )
        
        # Проверяем достижения
        updated_player = await get_player(user_id, chat_id)
        achievements = check_achievements(updated_player)
        for ach_id, ach_data in achievements:
            if await add_achievement(user_id, ach_id):
                result_text += f"\n\n🏆 *НОВОЕ ДОСТИЖЕНИЕ!*\n{ach_data['name']}"
        
        # Проверяем повышение ранга
        old_rank = get_rank(player['experience'])
        new_rank = get_rank(updated_player['experience'])
        if new_rank['level'] > old_rank['level']:
            result_text += f"\n\n🎉 *ПОВЫШЕНИЕ!*\nТеперь ты {new_rank['name']}!"
    
    else:
        # Провал — садимся в тюрьму
        jail_time = crime['jail_time']
        exp_gain = get_experience_for_action("crime_medium", False)
        
        await put_in_jail(user_id, chat_id, jail_time)
        await update_player_stats(
            user_id, chat_id,
            crimes_fail=f"+1",
            experience=f"+{exp_gain}"
        )
        
        crime_msg = get_random_crime_message(crime, False, jail=jail_time)
        
        result_text = (
            f"❌ *ПРОВАЛ!*\n\n"
            f"{crime_msg}\n\n"
            f"⛓️ Сел на {jail_time} сек\n"
            f"⭐ +{exp_gain} опыта (за попытку)"
        )
    
    try:
        await callback.message.edit_text(result_text, parse_mode=ParseMode.MARKDOWN)
    except TelegramBadRequest:
        await callback.message.answer(result_text, parse_mode=ParseMode.MARKDOWN)
    await callback.answer()


@router.message(Command("attack", "naezd", "rob"))
async def cmd_attack(message: Message):
    """Наехать на другого игрока"""
    if message.chat.type == "private":
        await message.answer("❌ Наезды — дело групповое!")
        return
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    player = await get_player(user_id, chat_id)
    if not player or not player['player_class']:
        await message.answer("❌ Сначала вступи в гильдию! /start")
        return
    
    # Проверка тюрьмы
    in_jail, remaining = await is_in_jail(user_id, chat_id)
    if in_jail:
        phrase = random.choice(JAIL_PHRASES).format(time=remaining)
        await message.answer(phrase)
        return
    
    # Проверка кулдауна
    can_do, cooldown_remaining = check_cooldown(user_id, chat_id, "attack", 60)
    if not can_do:
        await message.answer(f"⏰ Братиш, не гони! Подожди {cooldown_remaining} сек")
        return
    
    # Определяем жертву
    victim_user = None
    
    if message.reply_to_message and message.reply_to_message.from_user:
        victim_user = message.reply_to_message.from_user
    elif message.entities:
        for entity in message.entities:
            if entity.type == "mention":
                # Тут нужно получить пользователя по username - сложно без кеша
                pass
    
    if not victim_user:
        await message.answer(
            "❌ На кого наезжать-то?\n"
            "Ответь на сообщение жертвы или упомяни её!"
        )
        # Сбрасываем кулдаун, так как действие не выполнено
        cooldowns.pop((user_id, chat_id, "attack"), None)
        return
    
    if victim_user.id == user_id:
        await message.answer("🤡 Сам на себя наезжать? Ты чё, дурак?")
        cooldowns.pop((user_id, chat_id, "attack"), None)
        return
    
    if victim_user.is_bot:
        await message.answer("🤖 На ботов не наезжают, это западло!")
        cooldowns.pop((user_id, chat_id, "attack"), None)
        return
    
    victim = await get_player(victim_user.id, chat_id)
    if not victim or not victim['player_class']:
        await message.answer("❌ Этот лох не в криминале! Нечего брать.")
        cooldowns.pop((user_id, chat_id, "attack"), None)
        return
    
    # Проверяем, есть ли что брать
    if victim['money'] < 10:
        msg = get_random_attack_message(
            False, False,
            attacker=message.from_user.first_name,
            victim=victim_user.first_name
        )
        await message.answer(msg)
        return
    
    # Выполняем наезд
    success = calculate_pvp_success(player, victim)
    attacker_name = message.from_user.first_name
    victim_name = victim_user.first_name
    
    if success:
        steal_amount = calculate_pvp_steal_amount(victim)
        exp_gain = get_experience_for_action("pvp_win", True)
        
        # Обновляем атакующего
        await update_player_stats(
            user_id, chat_id,
            money=f"+{steal_amount}",
            experience=f"+{exp_gain}",
            pvp_wins=f"+1",
            total_stolen=f"+{steal_amount}"
        )
        
        # Обновляем жертву
        await update_player_stats(
            victim_user.id, chat_id,
            money=f"-{steal_amount}",
            pvp_losses=f"+1",
            total_lost=f"+{steal_amount}"
        )
        
        msg = get_random_attack_message(
            True, True,
            attacker=attacker_name,
            victim=victim_name,
            amount=steal_amount
        )
        
        result_text = f"{msg}\n\n⭐ +{exp_gain} опыта"
        
        # Проверяем достижения
        updated_player = await get_player(user_id, chat_id)
        achievements = check_achievements(updated_player)
        for ach_id, ach_data in achievements:
            if await add_achievement(user_id, ach_id):
                result_text += f"\n\n🏆 *ДОСТИЖЕНИЕ!* {ach_data['name']}"
    
    else:
        exp_gain = get_experience_for_action("pvp_lose", False)
        
        await update_player_stats(
            user_id, chat_id,
            pvp_losses=f"+1",
            experience=f"+{exp_gain}"
        )
        
        await update_player_stats(
            victim_user.id, chat_id,
            pvp_wins=f"+1",
            experience=f"+{get_experience_for_action('pvp_win', True)}"
        )
        
        msg = get_random_attack_message(
            False, True,
            attacker=attacker_name,
            victim=victim_name
        )
        
        result_text = msg
    
    await message.answer(result_text, parse_mode=ParseMode.MARKDOWN)




@router.message(Command("treasury", "obshak", "bank"))
async def cmd_treasury(message: Message):
    """Показать общак чата"""
    if message.chat.type == "private":
        return
    
    chat_id = message.chat.id
    treasury = await get_treasury(chat_id)
    
    await message.answer(
        f"🏦 *ВОРОВСКОЙ ОБЩАК*\n\n"
        f"💰 В кассе: {treasury:,} лавэ\n\n"
        f"_10% со всех дел идёт в общак.\n"
        f"Иногда пахан раздаёт бабки..._",
        parse_mode=ParseMode.MARKDOWN
    )


@router.message(Command("help", "commands", "info"))
async def cmd_help(message: Message):
    """Справка по командам"""
    help_text = """
🦯 *ХРОМАЯ ШЛЮХА ТЁТЯ РОЗА*

━━━━━━━━━━━━━━━━━━━━━━━━

/svodka — Сводка чата за 5ч 📺
/describe — Опишет фото (пожалеешь)
/poem — Стих-унижение 📜
/диагноз — Диагноз из подвала 🏥
/сжечь — Сжечь на костре 🔥
/бухнуть — Бухнуть и слить секреты 🍻
/пососи — Философское напутствие 🍭
/проветрить — Открыть форточку в чате 🪟
/мем — Рандомный мем из коллекции 🎭
/мемы — Статистика мемов 📊
/pic — Найти картинку 🖼

━━━━━━━━━━━━━━━━━━━━━━━━

_Бот запоминает все мемы и иногда выдаёт их сам!_

/profile — Твоё досье 📋
/социал — Социальный граф чата 🕸️
"""
    await message.answer(help_text, parse_mode=ParseMode.MARKDOWN)


@router.message(Command("досье", "профиль", "dossier"))
async def cmd_ai_profile(message: Message):
    """Показать AI-профиль пользователя с полным анализом"""
    if not USE_POSTGRES:
        await message.answer("⚠️ Профили доступны только в полной версии")
        return
    
    # Определяем чей профиль показать
    target_user = message.from_user
    target_name = target_user.first_name or target_user.username or "Аноним"
    
    # Если реплай - показываем профиль того, кому реплай
    if message.reply_to_message and message.reply_to_message.from_user:
        target_user = message.reply_to_message.from_user
        target_name = target_user.first_name or target_user.username or "Аноним"
    
    # Показываем что работаем (может занять время при пересборке)
    processing = await message.answer(f"🔍 Собираю досье на *{target_name}*...", parse_mode=ParseMode.MARKDOWN)
    
    try:
        report = await get_user_activity_report(target_user.id, message.chat.id)
        
        if report.get('error'):
            await processing.edit_text(f"🔍 Досье на *{target_name}* пока не собрано. Пусть побольше болтает!", parse_mode=ParseMode.MARKDOWN)
            return
        
        # Формируем красивый вывод
        gender_icon = "👨" if report['gender'] == 'мужской' else "👩" if report['gender'] == 'женский' else "🤷"
        activity_icons = {
            'hyperactive': '🔥🔥🔥',
            'very_active': '🔥🔥',
            'active': '🔥',
            'normal': '🙂',
            'lurker': '👀'
        }
        style_icons = {
            'toxic': '☠️ Токсичный',
            'humorous': '😂 Юморист',
            'positive': '😊 Позитивный',
            'negative': '😔 Негативный',
            'neutral': '😐 Нейтральный'
        }
        
        activity_icon = activity_icons.get(report['activity_level'], '🙂')
        style_text = style_icons.get(report['communication_style'], '😐 Нейтральный')
        
        # Временные паттерны
        time_pattern = ""
        if report['behavior']['is_night_owl']:
            time_pattern = "🦉 Сова (ночная тварь)"
        elif report['behavior']['is_early_bird']:
            time_pattern = "🐓 Жаворонок (ранняя пташка)"
        else:
            time_pattern = "🌞 Обычный режим"
        
        peak = report['behavior']['peak_hour']
        if peak is not None:
            time_pattern += f" | Пик: {peak}:00"
        
        # Интересы
        interests_text = ", ".join(report['interests'][:5]) if report['interests'] else "Не определены"
        
        # Sentiment bar
        sent = report['sentiment']
        total_sent = sent['positive'] + sent['negative'] + sent['neutral']
        if total_sent > 0:
            pos_pct = sent['positive'] / total_sent * 100
            neg_pct = sent['negative'] / total_sent * 100
            sentiment_bar = f"🟢 {pos_pct:.0f}% | 🔴 {neg_pct:.0f}%"
        else:
            sentiment_bar = "Нет данных"
        
        text = f"""
📋 *ДОСЬЕ: {target_name}*
{gender_icon} Пол: {report['gender']} ({report['gender_confidence']})
━━━━━━━━━━━━━━━━━━━━━━━━

📊 *АКТИВНОСТЬ*
{activity_icon} Уровень: {report['activity_level']}
💬 Сообщений: {report['total_messages']}
📏 Средняя длина: {report['behavior']['avg_message_length']} символов

🎭 *ХАРАКТЕР*
{style_text}
{sentiment_bar}
😀 Эмодзи: {report['behavior']['emoji_rate']}
☠️ Токсичность: {report['behavior']['toxicity']}
😂 Юмор: {report['behavior']['humor']}

⏰ *РЕЖИМ*
{time_pattern}

🎯 *ИНТЕРЕСЫ*
{interests_text}
"""
        
        # Если профиль был пересобран — добавляем инфо
        if report.get('_rebuilt'):
            text += f"\n✨ _Профиль пересобран из истории сообщений_"
        # Если профиль устарел — добавляем подсказку
        elif report.get('_note'):
            text += f"\n⚠️ _{report['_note']}_"
        
        await processing.edit_text(text, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        logger.error(f"Profile error: {e}")
        try:
            await processing.edit_text("❌ Ошибка получения профиля")
        except:
            await message.answer("❌ Ошибка получения профиля")


@router.message(Command("память", "memory", "факты", "facts"))
async def cmd_smart_memory(message: Message):
    """Показать что бот помнит о пользователе (умная память)"""
    if not USE_POSTGRES:
        await message.answer("⚠️ Умная память доступна только в полной версии")
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.first_name or "Аноним"
    
    # Определяем цель — себя или кого-то по реплаю
    target_id = user_id
    target_name = user_name
    
    if message.reply_to_message and message.reply_to_message.from_user:
        target_id = message.reply_to_message.from_user.id
        target_name = message.reply_to_message.from_user.first_name or "Аноним"
    
    processing = await message.answer("🧠 Собираю умную память...")
    
    try:
        from database_postgres import get_user_facts, get_user_memories, get_recent_summaries
        
        report_parts = [f"🧠 **Умная память о {target_name}:**\n"]
        
        # 1. Факты (извлечённые AI)
        facts = await get_user_facts(chat_id, target_id, limit=20, min_confidence=0.5)
        if facts:
            report_parts.append("**📌 Известные факты:**")
            by_type = {}
            for f in facts:
                ft = f.get('fact_type', 'other')
                if ft not in by_type:
                    by_type[ft] = []
                by_type[ft].append(f)
            
            type_icons = {
                'personal': '👤', 'interest': '⭐', 'social': '👥',
                'event': '📅', 'opinion': '💭'
            }
            
            for ft, items in by_type.items():
                icon = type_icons.get(ft, '•')
                report_parts.append(f"\n{icon} _{ft}_:")
                for item in items[:5]:
                    conf = item.get('confidence', 0)
                    times = item.get('times_confirmed', 1)
                    marker = "✓" if conf > 0.8 or times > 2 else "○"
                    report_parts.append(f"  {marker} {item.get('fact_text', '')}")
        else:
            report_parts.append("_Фактов пока нет. Бот извлекает факты из информативных сообщений автоматически._")
        
        # 2. Воспоминания (legacy)
        memories = await get_user_memories(chat_id, target_id, limit=10)
        if memories:
            report_parts.append("\n**💾 Воспоминания:**")
            for m in memories[:5]:
                mtype = m.get('memory_type', '')
                mtext = m.get('memory_text', '')[:100]
                if mtext:
                    report_parts.append(f"• [{mtype}] {mtext}")
        
        # 3. Статистика
        report_parts.append(f"\n**📊 Итого:**")
        report_parts.append(f"• Фактов: {len(facts)}")
        report_parts.append(f"• Воспоминаний: {len(memories)}")
        
        report = "\n".join(report_parts)
        
        try:
            await processing.edit_text(report[:4000], parse_mode=ParseMode.MARKDOWN)
        except:
            await processing.edit_text(report[:4000].replace("**", "").replace("_", ""))
    
    except Exception as e:
        logger.error(f"Memory command error: {e}")
        try:
            await processing.edit_text("❌ Ошибка получения памяти")
        except:
            await message.answer("❌ Ошибка получения памяти")


@router.message(Command("обучи", "learnme", "запомни", "учись"))
async def cmd_learn_user(message: Message):
    """Принудительное обучение бота на последних сообщениях пользователя"""
    if not USE_POSTGRES:
        await message.answer("⚠️ Обучение доступно только в полной версии")
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.first_name or "Аноним"
    
    # Rate limit — не чаще раза в 5 минут
    if not check_cooldown(user_id, chat_id, "learnme", 300):
        await message.reply("⏳ Подожди 5 минут между обучениями!")
        return
    
    processing = await message.answer("🧠 Анализирую твои сообщения...")
    
    try:
        from database_postgres import get_user_messages, save_user_fact
        
        # Получаем последние 500 сообщений пользователя для глубокого обучения
        messages = await get_user_messages(chat_id, user_id, limit=500)
        
        if len(messages) < 5:
            await processing.edit_text("📝 Недостаточно сообщений. Напиши побольше, а потом запусти обучение!")
            return
        
        # Фильтруем только информативные сообщения (>20 символов)
        informative_messages = [
            m for m in messages 
            if m.get('message_text') and len(m.get('message_text', '')) > 20
        ]
        
        if len(informative_messages) < 3:
            await processing.edit_text("📝 Твои сообщения слишком короткие для анализа. Пиши более развёрнуто!")
            return
        
        # Отбираем до 30 самых длинных/информативных для глубокого обучения
        informative_messages.sort(key=lambda x: len(x.get('message_text', '')), reverse=True)
        to_analyze = informative_messages[:30]
        
        # Проверяем API
        extract_url = EXTRACT_FACTS_API_URL or get_api_url("extract_facts")
        if not extract_url or "your-vercel" in extract_url:
            await processing.edit_text("❌ API для извлечения фактов не настроен")
            return
        
        await processing.edit_text(f"🧠 Анализирую {len(to_analyze)} сообщений...")
        
        facts_saved = 0
        session = await get_http_session()
        
        for msg in to_analyze:
            text = msg.get('message_text', '')
            
            try:
                async with session.post(extract_url, json={
                    "message": text[:1000],
                    "user_name": user_name
                }, timeout=aiohttp.ClientTimeout(total=15)) as response:
                    if response.status == 200:
                        result = await response.json()
                        
                        if result.get("has_facts") and result.get("facts"):
                            for fact in result["facts"][:3]:
                                fact_type = fact.get("type", "personal")
                                fact_text = fact.get("text", "")
                                confidence = fact.get("confidence", 0.7)
                                
                                if fact_text and len(fact_text) > 3:
                                    success = await save_user_fact(
                                        chat_id=chat_id,
                                        user_id=user_id,
                                        fact_type=fact_type,
                                        fact_text=fact_text,
                                        confidence=confidence
                                    )
                                    if success:
                                        facts_saved += 1
                
                # Небольшая пауза между запросами
                await asyncio.sleep(0.5)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.debug(f"Learn extraction error: {e}")
                continue
        
        if facts_saved > 0:
            await processing.edit_text(
                f"✅ **Обучение завершено!**\n\n"
                f"🧠 Проанализировано: {len(to_analyze)} сообщений\n"
                f"📌 Новых фактов: {facts_saved}\n\n"
                f"Используй /память чтобы увидеть что я запомнил!",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await processing.edit_text(
                "🤔 Не нашёл новых фактов в твоих сообщениях.\n"
                "Попробуй рассказать что-то о себе: работа, хобби, семья, мнения!"
            )
    
    except Exception as e:
        logger.error(f"Learn command error: {e}")
        try:
            await processing.edit_text("❌ Ошибка обучения")
        except:
            await message.answer("❌ Ошибка обучения")


@router.message(Command("глубокое", "deeplearn", "fulllearn", "полное"))
async def cmd_deep_learn(message: Message):
    """
    ГЛУБОКОЕ обучение — анализирует ВСЕ сообщения пользователя в чате.
    Может занять несколько минут, но бот запомнит максимум информации.
    """
    if not USE_POSTGRES:
        await message.answer("⚠️ Глубокое обучение доступно только в полной версии")
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.first_name or "Аноним"
    
    # Rate limit — не чаще раза в 30 минут (это тяжёлая операция)
    if not check_cooldown(user_id, chat_id, "deeplearn", 1800):
        await message.reply("⏳ Глубокое обучение можно запускать раз в 30 минут!")
        return
    
    processing = await message.answer("🧠 Запускаю ГЛУБОКОЕ обучение...\nЭто может занять несколько минут.")
    
    try:
        from database_postgres import get_user_messages, save_user_fact
        
        # Получаем ВСЕ сообщения пользователя (до 5000)
        messages = await get_user_messages(chat_id, user_id, limit=5000)
        total_messages = len(messages)
        
        if total_messages < 10:
            await processing.edit_text("📝 Недостаточно сообщений для глубокого обучения. Нужно минимум 10.")
            return
        
        await processing.edit_text(f"🧠 Найдено {total_messages} сообщений. Анализирую...")
        
        # Фильтруем информативные сообщения (>30 символов для глубокого анализа)
        informative_messages = [
            m for m in messages 
            if m.get('message_text') and len(m.get('message_text', '')) > 30
        ]
        
        if len(informative_messages) < 5:
            await processing.edit_text("📝 Слишком мало информативных сообщений для глубокого обучения.")
            return
        
        # Сортируем по длине и берём до 100 самых информативных
        informative_messages.sort(key=lambda x: len(x.get('message_text', '')), reverse=True)
        to_analyze = informative_messages[:100]  # До 100 сообщений для глубокого анализа
        
        # Проверяем API
        extract_url = EXTRACT_FACTS_API_URL or get_api_url("extract_facts")
        if not extract_url or "your-vercel" in extract_url:
            await processing.edit_text("❌ API для извлечения фактов не настроен")
            return
        
        await processing.edit_text(f"🧠 Глубокий анализ {len(to_analyze)} сообщений из {total_messages}...")
        
        facts_saved = 0
        analyzed = 0
        session = await get_http_session()
        
        for i, msg in enumerate(to_analyze):
            text = msg.get('message_text', '')
            
            try:
                async with session.post(extract_url, json={
                    "message": text[:1000],
                    "user_name": user_name
                }, timeout=aiohttp.ClientTimeout(total=15)) as response:
                    if response.status == 200:
                        result = await response.json()
                        analyzed += 1
                        
                        if result.get("has_facts") and result.get("facts"):
                            for fact in result["facts"][:5]:  # До 5 фактов на сообщение
                                fact_type = fact.get("type", "personal")
                                fact_text = fact.get("text", "")
                                confidence = fact.get("confidence", 0.7)
                                
                                if fact_text and len(fact_text) > 3:
                                    success = await save_user_fact(
                                        chat_id=chat_id,
                                        user_id=user_id,
                                        fact_type=fact_type,
                                        fact_text=fact_text,
                                        confidence=confidence
                                    )
                                    if success:
                                        facts_saved += 1
                
                # Обновляем прогресс каждые 10 сообщений
                if (i + 1) % 10 == 0:
                    try:
                        await processing.edit_text(
                            f"🧠 Глубокое обучение: {i+1}/{len(to_analyze)} сообщений...\n"
                            f"Найдено фактов: {facts_saved}"
                        )
                    except:
                        pass
                
                # Пауза между запросами (чтобы не перегружать API)
                await asyncio.sleep(0.3)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.debug(f"Deep learn extraction error: {e}")
                continue
        
        if facts_saved > 0:
            await processing.edit_text(
                f"✅ **Глубокое обучение завершено!**\n\n"
                f"📊 Всего сообщений: {total_messages}\n"
                f"🔍 Проанализировано: {analyzed}\n"
                f"📌 Новых фактов: {facts_saved}\n\n"
                f"Теперь бот знает о тебе гораздо больше!\n"
                f"Используй /память чтобы увидеть всё.",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await processing.edit_text(
                f"🤔 Проанализировал {analyzed} сообщений, но новых фактов не нашёл.\n"
                f"Возможно, я уже всё знаю о тебе, или попробуй рассказать что-то новое!"
            )
    
    except Exception as e:
        logger.error(f"Deep learn error: {e}")
        try:
            await processing.edit_text("❌ Ошибка глубокого обучения")
        except:
            await message.answer("❌ Ошибка глубокого обучения")


@router.message(Command("психоанализ", "psycho", "анализ", "разбор"))
async def cmd_psychoanalysis(message: Message):
    """Глубокий AI-психоанализ личности на основе всех данных"""
    if not USE_POSTGRES:
        await message.answer("⚠️ Психоанализ доступен только в полной версии")
        return
    
    if message.chat.type == "private":
        await message.answer("🧠 Психоанализ работает только в групповых чатах!")
        return
    
    # Определяем цель анализа
    target_user = message.from_user
    
    if message.reply_to_message and message.reply_to_message.from_user:
        target_user = message.reply_to_message.from_user
    
    target_id = target_user.id
    target_name = target_user.first_name or target_user.username or "Аноним"
    target_username = target_user.username
    
    # Кулдаун 2 минуты на пользователя
    cooldown_key = f"psycho_{target_id}"
    can_do, remaining = check_cooldown(message.from_user.id, message.chat.id, cooldown_key, 120)
    if not can_do:
        await message.answer(f"⏰ Психоанализ {target_name} можно делать раз в 2 минуты. Подожди {remaining} сек")
        return
    
    processing = await message.answer(f"🧠 Анализирую личность {target_name}...")
    
    try:
        # Получаем последние сообщения для анализа (до 1000 для полной картины)
        # ВАЖНО: сначала получаем реальные сообщения, чтобы проверить их количество
        messages = await get_user_messages(message.chat.id, target_id, limit=1000)
        real_message_count = len(messages)
        
        if real_message_count < 10:
            await processing.edit_text(
                f"🔍 Недостаточно данных для психоанализа {target_name}.\n"
                f"Нужно минимум 10 сообщений, а найдено только {real_message_count}.\n\n"
                f"💡 Пусть {target_name} побольше пишет в чате!"
            )
            return
        
        # Получаем профиль (может быть None если профилирование ещё не накопило данные)
        profile = await get_user_profile_for_ai(target_id, message.chat.id, target_name, target_username or "")
        full_profile = await get_user_full_profile(target_id, message.chat.id)
        # Берём 30 самых информативных для контекста
        msg_texts = [m.get('message_text', '') for m in messages if m.get('message_text')]
        interesting_msgs = sorted(msg_texts, key=len, reverse=True)[:20]
        recent_msgs = msg_texts[:15]
        selected_msgs = list(dict.fromkeys(interesting_msgs + recent_msgs))[:30]
        messages_text = "\n".join([f"- {t[:100]}" for t in selected_msgs])
        
        # Создаём кликабельное упоминание
        user_mention = make_user_mention(target_id, target_name, target_username)
        
        # Формируем данные для анализа
        gender = profile.get('gender', 'неизвестно')
        gender_icon = "👨" if gender == 'мужской' else "👩" if gender == 'женский' else "🤷"
        
        activity = profile.get('activity_level', 'normal')
        activity_desc = {
            'hyperactive': 'ГИПЕРАКТИВНЫЙ (графоман, не затыкается)',
            'very_active': 'Очень активный (любит поболтать)',
            'active': 'Активный (регулярно участвует)',
            'normal': 'Обычный (средняя активность)',
            'lurker': 'Молчун (редко пишет, больше читает)'
        }.get(activity, 'Обычный')
        
        style = profile.get('communication_style', 'neutral')
        style_desc = {
            'toxic': '☠️ ТОКСИЧНЫЙ (агрессивен, конфликтен)',
            'humorous': '😂 Юморист (часто шутит)',
            'positive': '😊 Позитивный (доброжелателен)',
            'negative': '😔 Негативный (склонен ныть)',
            'neutral': '😐 Нейтральный'
        }.get(style, 'Нейтральный')
        
        toxicity = profile.get('toxicity', 0)
        toxicity_level = "🟢 Низкая" if toxicity < 0.2 else "🟡 Средняя" if toxicity < 0.4 else "🔴 ВЫСОКАЯ"
        
        humor = profile.get('humor', 0)
        humor_level = "Не особо" if humor < 0.2 else "Иногда шутит" if humor < 0.4 else "Постоянно шутит"
        
        # Интересы
        interests = profile.get('interests_readable', []) or profile.get('interests', [])
        interests_text = ", ".join(interests[:5]) if interests else "Не определены"
        
        # Режим
        time_mode = "🦉 Ночная сова" if profile.get('is_night_owl') else "🐓 Жаворонок" if profile.get('is_early_bird') else "🌞 Обычный"
        peak_hour = profile.get('peak_hour')
        peak_text = f"Пик активности: {peak_hour}:00" if peak_hour is not None else ""
        
        # Социальные связи
        social = profile.get('social', {})
        talks_to = social.get('frequently_talks_to', [])
        
        # Формируем отчёт
        report = f"""
🧠 *ПСИХОАНАЛИЗ: {user_mention}*
{'━' * 25}

{gender_icon} *БАЗОВЫЕ ДАННЫЕ*
• Пол: {gender}
• Сообщений проанализировано: {real_message_count}
• В чате с: {full_profile.get('first_seen_at', 'неизвестно') if full_profile else 'недавно'}

📊 *АКТИВНОСТЬ*
• Уровень: {activity_desc}
• Режим: {time_mode}
{f'• {peak_text}' if peak_text else ''}

🎭 *ХАРАКТЕР*
• Стиль: {style_desc}
• Токсичность: {toxicity_level} ({toxicity:.0%})
• Чувство юмора: {humor_level}
• Эмодзи: {full_profile.get('emoji_usage_rate', 0) if full_profile else 0:.1f}%

🎯 *ИНТЕРЕСЫ*
• {interests_text}

{'━' * 25}

🔮 *ДИАГНОЗ ТЁТИ РОЗЫ:*
"""
        
        # Генерируем диагноз на основе данных
        diagnoses = []
        
        if toxicity > 0.4:
            diagnoses.append(f"— {target_name} — токсичная тварь. Каждое второе сообщение — яд. Держитесь от него подальше, или надевайте противогаз.")
        elif toxicity > 0.2:
            diagnoses.append(f"— {target_name} иногда срывается на токсичность. Наверное, проблемы дома. Или на работе. Или везде.")
        
        if activity == 'hyperactive':
            diagnoses.append(f"— Графоман уровня «бог». {target_name} пишет столько, будто ему платят за каждую букву. Спойлер: не платят.")
        elif activity == 'lurker':
            diagnoses.append(f"— Тихушник. {target_name} всё видит, всё читает, но молчит как партизан. Подозрительно.")
        
        if style == 'negative':
            diagnoses.append(f"— Вечный нытик. {target_name} жалуется на жизнь чаще, чем дышит. Кто-то дайте ему обнимашки. Или антидепрессанты.")
        elif style == 'humorous':
            diagnoses.append(f"— Считает себя стендап-комиком. {target_name} шутит постоянно. Иногда даже смешно.")
        
        if profile.get('is_night_owl'):
            diagnoses.append(f"— Ночной зомби. {target_name} активен когда нормальные люди спят. Возможно, вампир.")
        
        if 'crypto' in interests:
            diagnoses.append(f"— Криптобро. {target_name} верит в биткоин больше, чем в себя. Ещё не поздно спасти.")
        if 'gaming' in interests:
            diagnoses.append(f"— Геймер. {target_name} просиживает жизнь в играх. Но хотя бы не на улице.")
        if 'politics' in interests:
            diagnoses.append(f"— Политолог диванный. {target_name} знает как управлять страной. Жаль, не знает как управлять своей жизнью.")
        
        if not diagnoses:
            diagnoses.append(f"— {target_name} — обычный человек. Ничего интересного. Скукота.")
        
        report += "\n".join(diagnoses[:4])  # Максимум 4 диагноза
        
        await processing.edit_text(report, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"Psychoanalysis error: {e}")
        await processing.edit_text(f"❌ Ошибка анализа: {e}")


@router.message(Command("социал", "social", "граф"))
async def cmd_social_graph(message: Message):
    """Показать социальный граф чата - кто с кем общается"""
    if not USE_POSTGRES:
        await message.answer("⚠️ Социальный граф доступен только в полной версии")
        return
    
    if message.chat.type == "private":
        await message.answer("👥 Команда работает только в групповых чатах")
        return
    
    try:
        chat_id = message.chat.id
        graph = await get_chat_social_graph(chat_id)
        
        if not graph:
            await message.answer("🕸️ Социальный граф пуст. Чатьтесь больше!")
            return
        
        # Собираем уникальных пользователей
        user_ids = set()
        for edge in graph[:20]:  # Топ-20 связей
            user_ids.add(edge['user_id'])
            user_ids.add(edge['target_user_id'])
        
        # Получаем имена
        user_names = {}
        for uid in user_ids:
            try:
                member = await message.bot.get_chat_member(chat_id, uid)
                user_names[uid] = member.user.first_name or member.user.username or str(uid)
            except Exception:
                user_names[uid] = f"User_{uid}"
        
        text = "🕸️ *СОЦИАЛЬНЫЙ ГРАФ ЧАТА*\n\n"
        text += "_Кто кому чаще отвечает:_\n\n"
        
        for i, edge in enumerate(graph[:15], 1):
            from_name = user_names.get(edge['user_id'], '?')
            to_name = user_names.get(edge['target_user_id'], '?')
            count = edge['total_interactions']
            sentiment = edge['avg_sentiment'] or 0
            
            # Эмодзи по настроению
            if sentiment > 0.2:
                mood = "💚"
            elif sentiment < -0.2:
                mood = "💔"
            else:
                mood = "💬"
            
            text += f"{i}. {from_name} → {to_name}: {count}x {mood}\n"
        
        text += "\n_💚 позитив | 💔 негатив | 💬 нейтрал_"
        
        await message.answer(text, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        logger.error(f"Social graph error: {e}")
        await message.answer("❌ Ошибка построения графа")


@router.message(Command("achievements", "ach"))
async def cmd_achievements(message: Message):
    """Показать достижения"""
    if message.chat.type == "private":
        return
    
    user_id = message.from_user.id
    earned = await get_player_achievements(user_id)
    
    text = "🏆 *ТВОИ ДОСТИЖЕНИЯ*\n\n"
    
    for ach_id, ach_data in ACHIEVEMENTS.items():
        if ach_id in earned:
            text += f"✅ {ach_data['name']}\n_{ach_data['description']}_\n\n"
        else:
            text += f"🔒 ???\n_{ach_data['description']}_\n\n"
    
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)


# ==================== СЛУЧАЙНЫЕ СОБЫТИЯ ====================

async def trigger_random_event(chat_id: int):
    """Запустить случайное событие в чате"""
    event = random.choice(RANDOM_EVENTS)
    
    if event['type'] == 'jackpot':
        # Инкассатор
        amount = random.randint(500, 2000)
        active_events[chat_id] = {
            'type': 'jackpot',
            'amount': amount,
            'grabbed': [],
            'max_grabbers': 3,
            'expires': time.time() + 30
        }
        
        await bot.send_message(
            chat_id,
            f"🚨 *{event['name']}*\n\n"
            f"{event['description']}\n"
            f"💰 В машине {amount} лавэ!\n\n"
            f"{event['action']}",
            parse_mode=ParseMode.MARKDOWN
        )
    
    elif event['type'] == 'raid':
        # Облава
        active_events[chat_id] = {
            'type': 'raid',
            'hidden': [],
            'expires': time.time() + 30
        }
        
        await bot.send_message(
            chat_id,
            f"🚨 *{event['name']}*\n\n"
            f"{event['description']}\n\n"
            f"{event['action']}",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Через 30 секунд проверяем кто не спрятался
        await asyncio.sleep(30)
        await finish_raid_event(chat_id)
    
    elif event['type'] == 'lottery':
        # Общак раздаёт
        treasury = await get_treasury(chat_id)
        if treasury < 100:
            return
        
        amount = min(treasury // 2, random.randint(200, 1000))
        active_events[chat_id] = {
            'type': 'lottery',
            'amount': amount,
            'taken': [],
            'max_takers': 5,
            'expires': time.time() + 20
        }
        
        await bot.send_message(
            chat_id,
            f"🎉 *{event['name']}*\n\n"
            f"{event['description']}\n"
            f"💰 Раздаёт {amount} лавэ!\n\n"
            f"{event['action']}",
            parse_mode=ParseMode.MARKDOWN
        )


@router.message(Command("grab"))
async def cmd_grab(message: Message):
    """Хапнуть деньги при событии 'инкассатор'"""
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    if chat_id not in active_events:
        return
    
    event = active_events[chat_id]
    if event['type'] != 'jackpot' or time.time() > event['expires']:
        return
    
    if user_id in event['grabbed']:
        await message.answer("😤 Ты уже хапнул, жадина!")
        return
    
    if len(event['grabbed']) >= event['max_grabbers']:
        await message.answer("😭 Опоздал! Всё уже разобрали!")
        return
    
    player = await get_player(user_id, chat_id)
    if not player or not player['player_class']:
        return
    
    share = event['amount'] // event['max_grabbers']
    event['grabbed'].append(user_id)
    
    await update_player_stats(user_id, chat_id, money=f"+{share}")
    
    await message.answer(
        f"💰 {message.from_user.first_name} хапнул {share} лавэ! "
        f"({len(event['grabbed'])}/{event['max_grabbers']})"
    )


@router.message(Command("hide"))
async def cmd_hide(message: Message):
    """Спрятаться при облаве"""
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    if chat_id not in active_events:
        return
    
    event = active_events[chat_id]
    if event['type'] != 'raid' or time.time() > event['expires']:
        return
    
    if user_id in event['hidden']:
        await message.answer("🙈 Ты уже спрятался!")
        return
    
    event['hidden'].append(user_id)
    await message.answer(f"🏃 {message.from_user.first_name} спрятался!")


async def finish_raid_event(chat_id: int):
    """Завершить событие облавы"""
    if chat_id not in active_events:
        return
    
    event = active_events.get(chat_id)
    if not event or event['type'] != 'raid':
        return
    
    hidden_users = event['hidden']
    all_players = await get_all_active_players(chat_id)
    
    caught = []
    for player in all_players:
        if player['user_id'] not in hidden_users:
            # Проверяем, был ли игрок активен недавно
            if player['money'] > 50:
                fine = min(player['money'] // 2, 200)
                await update_player_stats(player['user_id'], chat_id, money=f"-{fine}")
                caught.append((player['first_name'], fine))
    
    if caught:
        caught_text = "\n".join([f"• {name}: -{fine} лавэ" for name, fine in caught])
        await bot.send_message(
            chat_id,
            f"🚔 *ОБЛАВА ЗАВЕРШЕНА!*\n\n"
            f"Попались:\n{caught_text}",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await bot.send_message(
            chat_id,
            "🚔 *ОБЛАВА ЗАВЕРШЕНА!*\n\n"
            "Все спрятались! Менты уехали ни с чем 😎"
        )
    
    del active_events[chat_id]


@router.message(Command("take"))
async def cmd_take(message: Message):
    """Взять долю из общака при событии"""
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    if chat_id not in active_events:
        return
    
    event = active_events[chat_id]
    if event['type'] != 'lottery' or time.time() > event['expires']:
        return
    
    if user_id in event['taken']:
        await message.answer("😤 Ты уже взял свою долю!")
        return
    
    if len(event['taken']) >= event['max_takers']:
        await message.answer("😭 Всё уже разобрали!")
        return
    
    player = await get_player(user_id, chat_id)
    if not player or not player['player_class']:
        return
    
    share = event['amount'] // event['max_takers']
    event['taken'].append(user_id)
    
    await update_player_stats(user_id, chat_id, money=f"+{share}")
    
    # Уменьшаем общак
    await add_to_treasury(chat_id, -share)
    
    await message.answer(
        f"💸 {message.from_user.first_name} урвал {share} лавэ из общака! "
        f"({len(event['taken'])}/{event['max_takers']})"
    )


# ==================== СВОДКА ЧАТА ====================

# URL твоего Vercel API (замени на свой после деплоя)
VERCEL_API_URL = os.getenv("VERCEL_API_URL", "https://your-vercel-app.vercel.app/api/summary")
VISION_API_URL = os.getenv("VISION_API_URL", "")
POEM_API_URL = os.getenv("POEM_API_URL", "")


def get_api_url(endpoint: str) -> str:
    """
    Надёжно формирует URL для Vercel API endpoint.
    
    Args:
        endpoint: Название endpoint без слэша (например: "burn", "dream", "reply")
    
    Returns:
        Полный URL или пустая строка если не настроено
    
    Examples:
        get_api_url("burn") -> "https://botchat-six.vercel.app/api/burn"
        get_api_url("dream") -> "https://botchat-six.vercel.app/api/dream"
    """
    if not VERCEL_API_URL or "your-vercel" in VERCEL_API_URL:
        return ""
    
    # Извлекаем базовый URL (до /api/)
    if "/api/" in VERCEL_API_URL:
        base_url = VERCEL_API_URL.rsplit('/api/', 1)[0]
    else:
        # Если нет /api/, пробуем отрезать последний сегмент
        base_url = VERCEL_API_URL.rsplit('/', 1)[0]
    
    return f"{base_url}/api/{endpoint}"


# ==================== ОПИСАНИЕ ФОТО ====================

@router.message(Command("describe", "photo", "wtf"))
async def cmd_describe_photo(message: Message):
    """Описание фото через Claude Vision — ответь на фото или кинь фото с командой"""
    import base64
    import io
    
    photo = None
    
    # Проверяем: это ответ на сообщение с фото?
    if message.reply_to_message and message.reply_to_message.photo:
        photo = message.reply_to_message.photo[-1]
    # Или это фото с командой в caption?
    elif message.photo:
        photo = message.photo[-1]
    
    if not photo:
        await message.answer(
            "📸 *Как использовать:*\n\n"
            "1️⃣ Ответь на фото командой `/describe`\n"
            "2️⃣ Или кинь фото с подписью `/describe`\n\n"
            "Тётя Роза расскажет что видит! 🔮",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    if not VISION_API_URL:
        await message.answer("❌ Vision API не настроен!")
        return
    
    # Проверка кулдауна
    user_id = message.from_user.id
    chat_id = message.chat.id
    if not check_cooldown(user_id, chat_id, "describe", 15):
        await message.answer("⏳ Подожди немного, глаза устали!")
        return
    
    # Rate limit для Vision API
    if not check_api_rate_limit(chat_id, "vision"):
        await message.answer("⏳ Слишком много запросов. Подожди минутку!")
        return
    
    # Показываем что работаем
    processing_msg = await message.answer("🔮 Тётя Роза смотрит в хрустальный шар... ⏳")
    
    try:
        # Скачиваем фото
        file = await bot.get_file(photo.file_id)
        photo_bytes = await bot.download_file(file.file_path)
        
        # Конвертируем в base64
        if isinstance(photo_bytes, io.BytesIO):
            photo_data = photo_bytes.getvalue()
        else:
            photo_data = photo_bytes
        
        image_base64 = base64.b64encode(photo_data).decode('utf-8')
        
        # Отправляем на анализ (используем глобальную сессию)
        session = await get_http_session()
        async with session.post(
            VISION_API_URL,
            json={
                "image_base64": image_base64,
                "media_type": "image/jpeg"
            },
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:
                if response.status == 200:
                    result = await response.json()
                    description = result.get("description", "Хуйня какая-то, не разобрать...")
                    
                    # Красиво оформляем ответ
                    await processing_msg.edit_text(
                        f"🔮 *Тётя Роза видит:*\n\n{description}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    error = await response.text()
                    logger.error(f"Vision API error: {response.status} - {error}")
                    await processing_msg.edit_text("❌ Карты затуманились... Попробуй позже!")
    
    except asyncio.TimeoutError:
        await processing_msg.edit_text("⏰ Слишком долго смотрела в шар, устала. Попробуй ещё раз!")
    except Exception as e:
        logger.error(f"Error in describe command: {e}")
        await processing_msg.edit_text("❌ Что-то пошло не так. Попробуй ещё раз!")


# ==================== СТИХИ-УНИЖЕНИЯ ====================

@router.message(Command("poem", "stih", "стих", "роаст", "roast", "унизь", "ода", "поэма", "verses"))
async def cmd_poem(message: Message):
    """Сгенерировать стих-унижение про человека в стиле русских классиков"""
    if message.chat.type == "private":
        await message.answer("❌ Стихи работают только в групповых чатах!")
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    # Определяем цель
    target_user = None
    target_name = None
    target_user_id = None
    
    if message.reply_to_message and message.reply_to_message.from_user:
        target_user = message.reply_to_message.from_user
        target_name = target_user.first_name
        target_user_id = target_user.id
    else:
        # Пробуем получить имя из текста команды
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            target_name = parts[1].strip().replace("@", "")
        else:
            await message.answer(
                "📜 *Как заказать стих-унижение:*\n\n"
                "1️⃣ Ответь на сообщение: `/poem`\n"
                "2️⃣ Или укажи имя: `/poem Вася`\n\n"
                "🎭 Триггеры: /poem /стих /роаст /унизь /ода\n\n"
                "Тётя Роза напишет ЖЁСТКИЙ стих в стиле классиков! 🪶🔥",
                parse_mode=ParseMode.MARKDOWN
            )
            return
    
    if not target_name:
        target_name = "Аноним"
    
    # Проверяем API URL
    poem_api_url = os.getenv("POEM_API_URL") or get_api_url("poem")
    
    if not poem_api_url or "your-vercel" in poem_api_url:
        await message.answer("❌ API для стихов не настроен!")
        return
    
    # Кулдаун 30 секунд
    can_do, cooldown_remaining = check_cooldown(user_id, chat_id, "poem", 30)
    if not can_do:
        await message.answer(f"⏰ Муза отдыхает! Подожди {cooldown_remaining} сек")
        return
    
    # Rate limit check
    can_call, wait_time = check_api_rate_limit(chat_id, "poem")
    if not can_call:
        await message.answer(f"⏰ Слишком много стихов! Подожди {wait_time} сек")
        return
    
    # Показываем что работаем
    processing_msg = await message.answer(f"🪶 Тётя Роза изучает досье на {target_name} и берёт перо... ✨")
    metrics.track_command("poem")
    
    try:
        # Собираем ПОЛНЫЙ контекст: сообщения + память + профиль
        context_parts = []
        if target_user:
            context_parts.append(f"Ник: @{target_user.username}" if target_user.username else "Ник: нет")
        
        # Память о пользователе (профиль, воспоминания, связи)
        if target_user_id:
            user_memory = await gather_user_memory(chat_id, target_user_id, target_name)
            if user_memory:
                context_parts.append(user_memory)
            
            user_context, messages_found = await gather_user_context(chat_id, target_user_id)
            if messages_found > 0:
                context_parts.append(f"\n=== СООБЩЕНИЯ ({messages_found} шт) ===")
                context_parts.append(user_context)
                context_parts.append("=== ИСПОЛЬЗУЙ ВСЁ ВЫШЕ ДЛЯ УНИЖЕНИЯ! ===")
        else:
            messages_found = 0
        
        context = "\n".join(context_parts) if context_parts else "Обычный участник чата"
        logger.info(f"Poem: {target_name}, {messages_found} msgs, memory: {bool(target_user_id)}")
        
        metrics.track_api_call("poem")
        session = await get_http_session()
        async with session.post(
                poem_api_url,
                json={"name": target_name, "context": context}
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    
                    if "error" in result:
                        await processing_msg.edit_text(f"❌ Ошибка: {result['error']}")
                        return
                    
                    poem = result.get("poem", "Муза молчит...")
                    
                    await processing_msg.edit_text(poem)
                else:
                    error = await response.text()
                    logger.error(f"Poem API error: {response.status} - {error}")
                    await processing_msg.edit_text("❌ Муза сегодня не в духе. Попробуй позже!")
                    
    except asyncio.TimeoutError:
        await processing_msg.edit_text("⏰ Муза задумалась слишком надолго...")
    except Exception as e:
        logger.error(f"Error in poem command: {e}")
        await processing_msg.edit_text("❌ Муза отказала. Попробуй позже!")


# ==================== ДИАГНОЗ ОТ ТЁТИ РОЗЫ ====================

@router.message(Command("diagnosis", "diagnoz", "диагноз", "болезнь", "псих"))
async def cmd_diagnosis(message: Message):
    """Поставить диагноз человеку на основе его сообщений"""
    if message.chat.type == "private":
        await message.answer("❌ Диагнозы ставятся только в групповых чатах!")
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    # Определяем цель
    target_user = None
    target_name = None
    target_username = None
    target_user_id = None
    
    if message.reply_to_message and message.reply_to_message.from_user:
        target_user = message.reply_to_message.from_user
        target_name = target_user.first_name
        target_username = target_user.username
        target_user_id = target_user.id
    else:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            target_name = parts[1].strip().replace("@", "")
        else:
            await message.answer(
                "🏥 *Как получить диагноз:*\n\n"
                "1️⃣ Ответь на сообщение: `/диагноз`\n"
                "2️⃣ Или укажи имя: `/диагноз Вася`\n\n"
                "Тётя Роза поставит диагноз на основе сообщений! 💀",
                parse_mode=ParseMode.MARKDOWN
            )
            return
    
    if not target_name:
        target_name = "Аноним"
    
    # Проверяем API URL
    diagnosis_api_url = get_api_url("diagnosis")
    if not diagnosis_api_url:
        await message.answer("❌ API для диагнозов не настроен! Нужна переменная VERCEL_API_URL")
        return

    # Кулдаун 30 секунд
    can_do, cooldown_remaining = check_cooldown(user_id, chat_id, "diagnosis", 30)
    if not can_do:
        await message.answer(f"⏰ Тётя Роза ещё не протрезвела! Подожди {cooldown_remaining} сек")
        return
    
    # Rate limit
    can_call, wait_time = check_api_rate_limit(chat_id, "diagnosis")
    if not can_call:
        await message.answer(f"⏰ Приём окончен! Подожди {wait_time} сек")
        return
    
    processing_msg = await message.answer(f"🏥 Тётя Роза надевает очки и изучает историю болезни {target_name}... 🔬")
    metrics.track_command("diagnosis")
    
    try:
        # Собираем ПОЛНЫЙ контекст: сообщения + память
        context, messages_found = await gather_user_context(chat_id, target_user_id) if target_user_id else ("Пациент молчалив — это симптом", 0)
        
        # Добавляем память о пользователе
        user_memory = ""
        user_profile = {}
        if USE_POSTGRES and target_user_id:
            try:
                user_memory = await gather_user_memory(chat_id, target_user_id, target_name)
                user_profile = await get_user_profile_for_ai(target_user_id, message.chat.id, target_name, target_username or "")
            except Exception as e:
                logger.debug(f"Could not get memory/profile for diagnosis: {e}")
        
        # Объединяем память и сообщения
        full_context = f"{user_memory}\n\n=== СООБЩЕНИЯ ===\n{context}" if user_memory else context
        logger.info(f"Diagnosis: {target_name}, {messages_found} msgs, memory: {bool(user_memory)}")
        
        metrics.track_api_call("diagnosis")
        session = await get_http_session()
        async with session.post(
                diagnosis_api_url,
                json={
                    "name": target_name, 
                    "username": target_username or "", 
                    "context": full_context,
                    "profile": user_profile
                }
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    
                    if "error" in result:
                        await processing_msg.edit_text(f"❌ Ошибка: {result['error']}")
                        return
                    
                    diagnosis = result.get("diagnosis", "Диагноз: хуй знает")
                    await processing_msg.edit_text(diagnosis)
                else:
                    error = await response.text()
                    logger.error(f"Diagnosis API error: {response.status} - {error}")
                    await processing_msg.edit_text("❌ Тётя Роза уснула. Попробуй позже!")
                    
    except asyncio.TimeoutError:
        await processing_msg.edit_text("⏰ Тётя Роза слишком долго искала очки...")
    except Exception as e:
        logger.error(f"Error in diagnosis command: {e}")
        await processing_msg.edit_text("❌ Диспансер закрыт. Приходи позже!")


# ==================== НАРИСУЙ ====================

@router.message(Command("нарисуй", "imagine", "draw", "нарисовать", "портрет"))
async def cmd_imagine(message: Message):
    """Нарисовать портрет человека через Flux"""
    if message.chat.type == "private":
        await message.answer("Команда работает только в групповых чатах")
        return

    # Определяем цель
    target_user = None
    target_name = None
    target_username = None

    if message.reply_to_message and message.reply_to_message.from_user:
        target_user = message.reply_to_message.from_user
        target_name = target_user.first_name or target_user.username or "Аноним"
        target_username = target_user.username or ""
    else:
        # Без реплая — рисуем самого отправителя
        target_user = message.from_user
        target_name = target_user.first_name or target_user.username or "Аноним"
        target_username = target_user.username or ""

    imagine_url = get_api_url("imagine")
    if not imagine_url:
        await message.answer("❌ API для генерации картинок не настроен (нужна переменная VERCEL_API_URL)")
        return

    can_do, cooldown_remaining = check_cooldown(message.from_user.id, message.chat.id, "imagine", 60)
    if not can_do:
        await message.answer(f"⏳ Подожди ещё {cooldown_remaining:.0f} сек")
        return

    clickable = f'<a href="tg://user?id={target_user.id}">{target_name}</a>'
    processing = await message.answer(f"🎨 Рисую портрет {clickable}...", parse_mode=ParseMode.HTML)

    # Собираем сообщения цели для персонализации
    context = ""
    if USE_POSTGRES:
        try:
            user_msgs = await get_user_messages(message.chat.id, target_user.id, limit=30)
            if user_msgs:
                context = "\n".join([m.get("message_text", "") for m in reversed(user_msgs) if m.get("message_text")])[:2000]
        except Exception:
            pass

    fal_key = os.getenv("FAL_KEY", "")
    if not fal_key:
        await processing.edit_text("❌ FAL_KEY не настроен")
        return

    try:
        metrics.track_api_call("imagine")
        session = await get_http_session()

        # Шаг 1: получаем промпт от Vercel (быстро, <5с)
        async with session.post(
            imagine_url,
            json={"name": target_name, "username": target_username, "context": context},
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status != 200:
                error = await resp.text()
                await processing.edit_text(f"❌ Ошибка генерации промпта: {error[:200]}")
                return
            result = await resp.json()

        image_prompt = result.get("prompt", "")
        if not image_prompt:
            await processing.edit_text("❌ Промпт не сгенерировался")
            return

        await processing.edit_text(f"🖌 Рисую {clickable}...", parse_mode=ParseMode.HTML)

        # Шаг 2: fal.ai Flux напрямую из бота (~10-20с)
        async with session.post(
            "https://fal.run/fal-ai/flux/dev",
            json={
                "prompt": image_prompt,
                "image_size": "square_hd",
                "num_inference_steps": 28,
                "num_images": 1,
                "guidance_scale": 3.5
            },
            headers={"Authorization": f"Key {fal_key}"},
            timeout=aiohttp.ClientTimeout(total=60)
        ) as fal_resp:
            if fal_resp.status != 200:
                error = await fal_resp.text()
                await processing.edit_text(f"❌ Flux вернул ошибку: {error[:200]}")
                return
            fal_result = await fal_resp.json()

        images = fal_result.get("images", [])
        if not images:
            await processing.edit_text("❌ Картинка не вернулась")
            return

        image_url = images[0].get("url", "")
        if not image_url:
            await processing.edit_text("❌ Пустой URL картинки")
            return

        # Скачиваем и отправляем
        async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=30)) as img_resp:
            img_data = await img_resp.read()

        await processing.delete()
        await message.answer_photo(
            BufferedInputFile(img_data, filename="portrait.jpg"),
            caption=f"🎨 Портрет {clickable}",
            parse_mode=ParseMode.HTML
        )

    except asyncio.TimeoutError:
        await processing.edit_text("⏰ Нейросеть думает слишком долго, попробуй позже")
    except Exception as e:
        logger.warning(f"IMAGINE error: {e}")
        try:
            await processing.edit_text(f"❌ Не смог нарисовать: {e}")
        except Exception:
            pass


# ==================== ВИДЕО (TEXT-TO-VIDEO) ====================

@router.message(Command("видео", "video", "клип", "снять"))
async def cmd_video(message: Message, command: CommandObject):
    """Снять видео через Kling AI — /видео описание или реплай на человека"""
    if message.chat.type == "private":
        await message.answer("Команда работает только в групповых чатах")
        return

    fal_key = os.getenv("FAL_KEY", "")
    if not fal_key:
        await message.answer("❌ FAL_KEY не настроен")
        return

    can_do, cooldown_remaining = check_cooldown(message.from_user.id, message.chat.id, "video", 300)
    if not can_do:
        await message.answer(f"⏳ Подожди ещё {cooldown_remaining:.0f} сек (видео — дорогая штука)")
        return

    # Правильное извлечение аргументов через CommandObject (обрабатывает @botname)
    custom_prompt = (command.args or "").strip()

    # Режим: видео-портрет человека (реплай без кастомного текста)
    target_user = None
    target_name = None
    clickable = None
    if message.reply_to_message and message.reply_to_message.from_user and not custom_prompt:
        target_user = message.reply_to_message.from_user
        target_name = target_user.first_name or target_user.username or "Аноним"
        clickable = f'<a href="tg://user?id={target_user.id}">{target_name}</a>'

    if not custom_prompt and not target_user:
        await message.answer(
            "🎬 <b>Как использовать:</b>\n"
            "<code>/видео описание сцены</code> — снять видео по описанию\n"
            "Или ответь на сообщение человека — снимем видео-портрет\n\n"
            "Примеры: <code>/видео закат над морем, волны бьются о скалы</code>",
            parse_mode=ParseMode.HTML
        )
        return

    processing = await message.answer("🎬 Готовлю сцену...")

    video_prompt = ""
    try:
        session = await get_http_session()
        fal_headers = {"Authorization": f"Key {fal_key}", "Content-Type": "application/json"}

        if target_user:
            # Получаем контекст человека и строим промпт локально
            keywords = []
            if USE_POSTGRES:
                try:
                    user_msgs = await get_user_messages(message.chat.id, target_user.id, limit=20)
                    for m in user_msgs:
                        txt = m.get("message_text", "")
                        if txt and len(txt) > 5:
                            keywords.append(txt[:60].strip())
                        if len(keywords) >= 5:
                            break
                except Exception:
                    pass

            import random as _rnd
            styles = [
                "cinematic portrait, dramatic golden hour lighting, shallow depth of field, film grain, 4K",
                "walking through foggy city streets at night, neon reflections, cinematic atmosphere, 4K",
                "epic close-up, emotional expression, stormy sky background, dramatic shadows, cinematic",
                "slow motion urban scene, sunrise, atmospheric haze, documentary style, 4K",
                "sitting in a cafe, warm light, candid moment, bokeh background, cinematic 35mm",
            ]
            style = _rnd.choice(styles)
            kw_str = (", ".join(keywords[:3])[:80] + ", ") if keywords else ""
            video_prompt = f"Cinematic video of a person named {target_name}, {kw_str}{style}"

            await processing.edit_text(f"🎬 Снимаю {clickable}...", parse_mode=ParseMode.HTML)
        else:
            video_prompt = custom_prompt
            await processing.edit_text("🎬 Снимаю...")

        # Kling 2.1 standard через queue
        kling_endpoint = "fal-ai/kling-video/v2.1/standard/text-to-video"
        async with session.post(
            f"https://queue.fal.run/{kling_endpoint}",
            json={"prompt": video_prompt, "duration": "5", "aspect_ratio": "16:9"},
            headers=fal_headers,
            timeout=aiohttp.ClientTimeout(total=30)
        ) as resp:
            if resp.status not in (200, 201):
                await processing.edit_text(f"❌ Kling ошибка: {(await resp.text())[:200]}")
                return
            submit_result = await resp.json()

        request_id = submit_result.get("request_id")
        if not request_id:
            await processing.edit_text("❌ Не получил request_id от Kling")
            return

        # Polling — Kling генерирует ~60-90 секунд
        for attempt in range(36):  # max 3 минуты
            await asyncio.sleep(5)
            async with session.get(
                f"https://queue.fal.run/{kling_endpoint}/requests/{request_id}/status",
                headers=fal_headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                status_data = await resp.json()

            status = status_data.get("status", "")
            if status == "COMPLETED":
                break
            elif status in ("FAILED", "CANCELLED"):
                await processing.edit_text(f"❌ Генерация провалилась: {status}")
                return
            if attempt % 4 == 0:
                eta = max(0, (36 - attempt) * 5)
                try:
                    await processing.edit_text(f"🎬 Рендерим... (~{eta}с)")
                except Exception:
                    pass
        else:
            await processing.edit_text("⏰ Kling думает слишком долго, попробуй позже")
            return

        # Получаем результат
        async with session.get(
            f"https://queue.fal.run/{kling_endpoint}/requests/{request_id}",
            headers=fal_headers,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            video_result = await resp.json()

        video_url = video_result.get("video", {}).get("url", "")
        if not video_url:
            await processing.edit_text("❌ Пустой URL видео")
            return

        async with session.get(video_url, timeout=aiohttp.ClientTimeout(total=60)) as vid_resp:
            vid_data = await vid_resp.read()

        await processing.delete()
        caption = f"🎬 Видео-портрет {clickable}" if target_user else "🎬 Готово"
        await message.answer_video(
            BufferedInputFile(vid_data, filename="video.mp4"),
            caption=caption,
            parse_mode=ParseMode.HTML if target_user else None
        )

    except asyncio.TimeoutError:
        await processing.edit_text("⏰ Нейросеть думает слишком долго, попробуй позже")
    except Exception as e:
        logger.warning(f"VIDEO error: {e}")
        try:
            await processing.edit_text(f"❌ Не смог снять: {e}")
        except Exception:
            pass


# ==================== ОЖИВИ ФОТО (IMAGE-TO-VIDEO) ====================

@router.message(Command("оживи", "animate", "анимация", "анимировать"))
async def cmd_animate(message: Message):
    """Анимировать фотографию через Kling AI image-to-video"""
    if message.chat.type == "private":
        await message.answer("Команда работает только в групповых чатах")
        return

    fal_key = os.getenv("FAL_KEY", "")
    if not fal_key:
        await message.answer("❌ FAL_KEY не настроен")
        return

    # Ищем фото: в реплае или в самом сообщении
    photo = None
    if message.reply_to_message and message.reply_to_message.photo:
        photo = message.reply_to_message.photo[-1]
    elif message.photo:
        photo = message.photo[-1]

    if not photo:
        await message.answer(
            "📸 Ответь на фото командой /оживи — и я его анимирую!\n"
            "Или отправь фото с подписью /оживи"
        )
        return

    can_do, cooldown_remaining = check_cooldown(message.from_user.id, message.chat.id, "animate", 300)
    if not can_do:
        await message.answer(f"⏳ Подожди ещё {cooldown_remaining:.0f} сек")
        return

    processing = await message.answer("✨ Оживляю фото...")

    try:
        file = await bot.get_file(photo.file_id)
        image_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"

        session = await get_http_session()
        fal_headers = {"Authorization": f"Key {fal_key}", "Content-Type": "application/json"}
        kling_endpoint = "fal-ai/kling-video/v2.1/standard/image-to-video"

        # Промпт для анимации — берём из подписи если есть
        extra_caption = ""
        if message.reply_to_message and message.reply_to_message.caption:
            extra_caption = message.reply_to_message.caption[:200]
        elif message.caption:
            parts = message.caption.split(None, 1)
            extra_caption = parts[1] if len(parts) > 1 else ""

        prompt = extra_caption.strip() or "smooth natural motion, realistic movement, cinematic quality"

        async with session.post(
            f"https://queue.fal.run/{kling_endpoint}",
            json={"prompt": prompt, "image_url": image_url, "duration": "5"},
            headers=fal_headers,
            timeout=aiohttp.ClientTimeout(total=30)
        ) as resp:
            if resp.status not in (200, 201):
                await processing.edit_text(f"❌ Kling ошибка: {(await resp.text())[:200]}")
                return
            submit_result = await resp.json()

        request_id = submit_result.get("request_id")
        if not request_id:
            await processing.edit_text("❌ Не получил request_id")
            return

        for attempt in range(36):
            await asyncio.sleep(5)
            async with session.get(
                f"https://queue.fal.run/{kling_endpoint}/requests/{request_id}/status",
                headers=fal_headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                status_data = await resp.json()

            status = status_data.get("status", "")
            if status == "COMPLETED":
                break
            elif status in ("FAILED", "CANCELLED"):
                await processing.edit_text(f"❌ Анимация провалилась: {status}")
                return
            if attempt % 4 == 0:
                eta = max(0, (36 - attempt) * 5)
                try:
                    await processing.edit_text(f"✨ Оживляю... (~{eta}с)")
                except Exception:
                    pass
        else:
            await processing.edit_text("⏰ Слишком долго, попробуй позже")
            return

        async with session.get(
            f"https://queue.fal.run/{kling_endpoint}/requests/{request_id}",
            headers=fal_headers,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            video_result = await resp.json()

        video_url = video_result.get("video", {}).get("url", "")
        if not video_url:
            await processing.edit_text("❌ Пустой URL видео")
            return

        async with session.get(video_url, timeout=aiohttp.ClientTimeout(total=60)) as vid_resp:
            vid_data = await vid_resp.read()

        await processing.delete()
        await message.answer_video(
            BufferedInputFile(vid_data, filename="animation.mp4"),
            caption="✨ Ожило!"
        )

    except asyncio.TimeoutError:
        await processing.edit_text("⏰ Слишком долго, попробуй позже")
    except Exception as e:
        logger.warning(f"ANIMATE error: {e}")
        try:
            await processing.edit_text(f"❌ Не смог оживить: {e}")
        except Exception:
            pass


# ==================== УЛУЧШИ ФОТО (UPSCALE 4x) ====================

@router.message(Command("улучши", "upscale", "enhance", "улучшить", "апскейл"))
async def cmd_enhance(message: Message):
    """Улучшить качество фото в 4x через AuraSR"""
    fal_key = os.getenv("FAL_KEY", "")
    if not fal_key:
        await message.answer("❌ FAL_KEY не настроен")
        return

    # Ищем фото
    photo = None
    if message.reply_to_message and message.reply_to_message.photo:
        photo = message.reply_to_message.photo[-1]
    elif message.photo:
        photo = message.photo[-1]

    if not photo:
        await message.answer(
            "📸 Ответь на фото командой /улучши — увеличу качество в 4 раза!\n"
            "Или отправь фото с подписью /улучши"
        )
        return

    can_do, cooldown_remaining = check_cooldown(message.from_user.id, message.chat.id, "enhance", 60)
    if not can_do:
        await message.answer(f"⏳ Подожди ещё {cooldown_remaining:.0f} сек")
        return

    processing = await message.answer("🔍 Улучшаю качество 4x...")

    try:
        file = await bot.get_file(photo.file_id)
        image_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"

        session = await get_http_session()
        fal_headers = {"Authorization": f"Key {fal_key}", "Content-Type": "application/json"}

        async with session.post(
            "https://fal.run/fal-ai/aura-sr",
            json={"image_url": image_url, "upscaling_factor": 4, "overlapping_tiles": True, "checkpoint": "v2"},
            headers=fal_headers,
            timeout=aiohttp.ClientTimeout(total=90)
        ) as resp:
            if resp.status != 200:
                await processing.edit_text(f"❌ AuraSR ошибка: {(await resp.text())[:200]}")
                return
            result = await resp.json()

        enhanced_url = result.get("image", {}).get("url", "")
        if not enhanced_url:
            await processing.edit_text("❌ Не получил улучшенное изображение")
            return

        async with session.get(enhanced_url, timeout=aiohttp.ClientTimeout(total=30)) as img_resp:
            img_data = await img_resp.read()

        await processing.delete()
        await message.answer_photo(
            BufferedInputFile(img_data, filename="enhanced.jpg"),
            caption="🔍 Готово! Качество улучшено в 4 раза"
        )

    except asyncio.TimeoutError:
        await processing.edit_text("⏰ AuraSR думает слишком долго, попробуй позже")
    except Exception as e:
        logger.warning(f"ENHANCE error: {e}")
        try:
            await processing.edit_text(f"❌ Не смог улучшить: {e}")
        except Exception:
            pass


# ==================== МУЗЫКА (TEXT-TO-MUSIC) ====================

@router.message(Command("музыка", "music", "трек", "track", "песня"))
async def cmd_music(message: Message, command: CommandObject):
    """Генерировать трек на основе сообщений чата через MiniMax Music"""
    if message.chat.type == "private":
        await message.answer("Команда работает только в групповых чатах")
        return

    apiframe_key = os.getenv("APIFRAME_KEY", "")
    if not apiframe_key:
        await message.answer("❌ APIFRAME_KEY не настроен")
        return

    ai_gateway_key = os.getenv("VERCEL_AI_GATEWAY_KEY", "")
    if not ai_gateway_key:
        await message.answer("❌ VERCEL_AI_GATEWAY_KEY не настроен")
        return

    can_do, cooldown_remaining = check_cooldown(message.from_user.id, message.chat.id, "music", 120)
    if not can_do:
        await message.answer(f"⏳ Подожди ещё {cooldown_remaining:.0f} сек")
        return

    # Опциональный стиль: /музыка рэп  или  /музыка грустный
    style_hint = (command.args or "").strip()

    # Режим реплая — трек про конкретного человека
    target_user = None
    target_name = None
    if message.reply_to_message and message.reply_to_message.from_user:
        target_user = message.reply_to_message.from_user
        target_name = target_user.first_name or target_user.username or "Аноним"

    if target_user:
        clickable = f'<a href="tg://user?id={target_user.id}">{target_name}</a>'
        hint_text = f" в стиле <b>{style_hint}</b>" if style_hint else ""
        processing = await message.answer(
            f"🎵 Изучаю {clickable} и пишу трек{hint_text}...",
            parse_mode=ParseMode.HTML
        )
    else:
        hint_text = f" в стиле <b>{style_hint}</b>" if style_hint else ""
        processing = await message.answer(
            f"🎵 Слушаю ваш чат и пишу трек{hint_text}...",
            parse_mode=ParseMode.HTML
        )

    try:
        session = await get_http_session()
        text_msgs = []

        if target_user:
            # Режим персонального трека — случайные сообщения из всей истории человека
            if USE_POSTGRES:
                try:
                    user_msgs = await get_random_user_messages_for_music(message.chat.id, target_user.id, limit=500)
                    text_msgs = [
                        {"first_name": target_name, "message_text": m["message_text"], "created_at": m.get("created_at")}
                        for m in user_msgs
                        if m.get("message_text")
                    ]
                except Exception:
                    pass

            if len(text_msgs) < 3:
                await processing.edit_text(
                    f"📭 У {target_name} слишком мало сохранённых сообщений — не о чём петь!"
                )
                cooldowns.pop((message.from_user.id, message.chat.id, "music"), None)
                return
        else:
            # Режим чата — случайные сообщения из всей истории чата
            all_msgs = await get_random_messages_for_music(message.chat.id, limit=1000)

            if len(all_msgs) < 5:
                await processing.edit_text(
                    "📭 Слишком мало сообщений в базе — не о чём петь!\n"
                    "Напишите хоть что-нибудь сначала."
                )
                cooldowns.pop((message.from_user.id, message.chat.id, "music"), None)
                return

            # Равномерная выборка: не более 15 сообщений на автора,
            # чтобы даже самые активные не доминировали
            author_counts: dict = {}
            balanced = []
            for m in all_msgs:
                author = m.get("first_name") or m.get("username") or "?"
                if author_counts.get(author, 0) < 15:
                    balanced.append(m)
                    author_counts[author] = author_counts.get(author, 0) + 1
            text_msgs = balanced[:500]

        # Шаг 1: форматируем сообщения для Claude
        import datetime as _dt
        lines = []
        for m in text_msgs:
            text = m.get("message_text") or m.get("text", "")
            if not text:
                continue
            name = m.get("first_name") or m.get("username") or "Аноним"
            ts = m.get("created_at")
            time_tag = ""
            if ts:
                try:
                    h = _dt.datetime.fromtimestamp(int(ts)).hour
                    time_tag = "[ночь] " if h < 6 else "[утро] " if h < 12 else "[день] " if h < 18 else "[вечер] "
                except Exception:
                    pass
            reply_to = m.get("reply_to_first_name")
            reply_tag = f"→{reply_to} " if reply_to else ""
            lines.append(f"{time_tag}{name} {reply_tag}: {text}")
        messages_text = "\n".join(lines)[:20000]

        hint_line = f"\nПожелание по стилю: {style_hint}" if style_hint else ""
        if target_name:
            task_line = (
                f"Напиши трек КОНКРЕТНО ПРО {target_name} — главный герой песни. "
                f"Используй его реальные фразы и манеру общения. Имя {target_name} должно звучать в тексте."
            )
        else:
            task_line = "Создай трек по мотивам всего чата."

        music_system_prompt = """Ты — профессиональный рэп-автор. Пишешь острые, живые треки по реальным перепискам телеграм-чатов.

ФОРМАТ ОТВЕТА — строго валидный JSON без markdown:
{"lyrics": "...", "tags": "...", "title": "..."}

━━━ LYRICS ━━━
Структура (теги обязательны): [verse] [chorus] [verse] [chorus] [bridge] [outro]
Объём: 1200-2000 символов.

Правила рифмовки (СТРОГО):
- Схема AABB: каждые 2 строки рифмуются
- Строка = 6-9 слов, одинаковая длина внутри секции
- Хорошие рифмы: "базар — угар", "в чате — некстати"
- Плохие рифмы: "любовь — вновь", "снова — слово" — не использовать
- Каждый куплет — новые рифмы

Содержание:
- КОНКРЕТНО: реальные имена, цитаты фраз из переписки
- Разговорный русский, можно мат если чат матерится
- [chorus] — 4 строки, цепляющий, одинаковый оба раза
- [verse] — 5-6 строк, разные персонажи
- [bridge] — 3 строки, эмоциональный пик
- [outro] — 2 строки

━━━ TAGS ━━━
Английский, 50-120 символов: жанр, BPM, инструменты, вокал.
Примеры: "russian drill rap, 140 BPM, 808 bass, trap hi-hats, aggressive male vocal"
"sad russian pop, 90 BPM, piano, strings, emotional female vocal, melancholic"

Выбирай по атмосфере: конфликты→drill/trap, грусть→sad pop/emo rap, угар→hyperpop, спокойно→lo-fi, ночь→dark ambient trap

━━━ TITLE ━━━
Название трека на русском, 2-5 слов."""

        # Шаг 1: генерим текст напрямую через AI Gateway (без Vercel)
        async with session.post(
            "https://ai-gateway.vercel.sh/v1/messages",
            json={
                "model": "anthropic/claude-sonnet-4-20250514",
                "max_tokens": 2500,
                "temperature": 0.85,
                "system": music_system_prompt,
                "messages": [{"role": "user", "content": f"Чат: {message.chat.title or 'Чат'}{hint_line}\n\nСообщения:\n{messages_text}\n\n{task_line}"}]
            },
            headers={
                "Authorization": f"Bearer {ai_gateway_key}",
                "Content-Type": "application/json",
                "anthropic-version": "2023-06-01"
            },
            timeout=aiohttp.ClientTimeout(total=60)
        ) as resp:
            if resp.status != 200:
                await processing.edit_text(f"❌ Ошибка генерации текста: {(await resp.text())[:200]}")
                return
            claude_result = await resp.json()

        raw = claude_result.get("content", [{}])[0].get("text", "").strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        try:
            music_data = json.loads(raw.strip())
        except Exception:
            await processing.edit_text(f"❌ Claude вернул не JSON: {raw[:200]}")
            return

        lyrics = music_data.get("lyrics", "")
        tags = music_data.get("tags", "russian pop, melodic")
        song_title = music_data.get("title", "Трек чата")

        if not lyrics:
            await processing.edit_text("❌ Не смог сочинить текст")
            return

        await processing.edit_text(
            f"🎼 Текст готов, записываю трек...\n\n<i>{song_title} • {tags}</i>",
            parse_mode=ParseMode.HTML
        )

        # Шаг 2: генерим музыку через Suno на apiframe.ai
        suno_headers = {
            "Authorization": f"Bearer {apiframe_key}",
            "Content-Type": "application/json"
        }

        async with session.post(
            "https://api.apiframe.pro/suno-imagine",
            json={
                "lyrics": lyrics,
                "tags": tags,
                "title": song_title,
                "model": "chirp-v4"
            },
            headers=suno_headers,
            timeout=aiohttp.ClientTimeout(total=30)
        ) as suno_resp:
            if suno_resp.status != 200:
                await processing.edit_text(f"❌ Suno ошибка: {(await suno_resp.text())[:200]}")
                return
            suno_result = await suno_resp.json()

        task_id = suno_result.get("task_id", "")
        if not task_id:
            await processing.edit_text("❌ Suno не вернул task_id")
            return

        # Шаг 3: полинг статуса (до 5 минут, каждые 8 сек)
        audio_url = ""
        for attempt in range(38):
            await asyncio.sleep(8)
            async with session.post(
                "https://api.apiframe.pro/fetch",
                json={"task_id": task_id},
                headers=suno_headers,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as fetch_resp:
                if fetch_resp.status != 200:
                    logger.warning(f"SUNO fetch HTTP {fetch_resp.status}: {await fetch_resp.text()}")
                    continue
                fetch_data = await fetch_resp.json()

            status = fetch_data.get("status", "")
            logger.info(f"SUNO poll #{attempt+1} status={status!r} full={fetch_data}")

            if status in ("finished", "completed", "success", "FINISHED", "COMPLETED", "SUCCESS", "done", "DONE"):
                # Пробуем разные варианты структуры ответа
                songs = fetch_data.get("songs") or fetch_data.get("output") or []
                if isinstance(songs, list) and songs:
                    audio_url = (songs[0].get("audio_url") or songs[0].get("url") or "")
                elif isinstance(fetch_data.get("audio_url"), str):
                    audio_url = fetch_data["audio_url"]
                logger.info(f"SUNO finished, audio_url={audio_url!r}")
                break
            elif status in ("failed", "error", "FAILED", "ERROR", "cancelled", "CANCELLED"):
                logger.warning(f"SUNO task failed: {fetch_data}")
                await processing.edit_text(f"❌ Suno не смог создать трек: {fetch_data.get('error', 'unknown error')}")
                return

        if not audio_url:
            await processing.edit_text("⏰ Suno думает слишком долго, попробуй позже")
            return

        async with session.get(audio_url, timeout=aiohttp.ClientTimeout(total=60)) as audio_resp:
            audio_data = await audio_resp.read()

        await processing.delete()

        if target_user:
            caption = f"🎵 Трек про {clickable}"
            performer = target_name
        else:
            caption = f"🎵 {song_title}"
            performer = "AI x Suno"

        await message.answer_audio(
            BufferedInputFile(audio_data, filename="track.mp3"),
            caption=caption,
            title=song_title,
            performer=performer,
            parse_mode=ParseMode.HTML if target_user else None
        )

    except asyncio.TimeoutError:
        await processing.edit_text("⏰ Suno думает слишком долго, попробуй позже")
    except Exception as e:
        logger.warning(f"MUSIC error: {e}")
        try:
            await processing.edit_text(f"❌ Не смог записать трек: {e}")
        except Exception:
            pass


# ==================== ПОДКАСТ ====================

@router.message(Command("подкаст", "podcast"))
async def cmd_podcast(message: Message, command: CommandObject):
    google_key = os.getenv("GOOGLE_API_KEY", "")
    if not google_key:
        await message.answer("❌ GOOGLE_API_KEY не настроен")
        return

    ai_gateway_key = os.getenv("VERCEL_AI_GATEWAY_KEY", "")
    if not ai_gateway_key:
        await message.answer("❌ VERCEL_AI_GATEWAY_KEY не настроен")
        return

    can_do, cooldown_remaining = check_cooldown(message.from_user.id, message.chat.id, "podcast", 300)
    if not can_do:
        await message.answer(f"⏳ Подожди ещё {cooldown_remaining:.0f} сек")
        return

    processing = await message.answer("🎙️ Изучаю чат и готовлю подкаст...")

    try:
        session = await get_http_session()

        # Берём случайные сообщения из всей истории
        all_msgs = await get_random_messages_for_music(message.chat.id, limit=800)
        if len(all_msgs) < 5:
            await processing.edit_text("📭 Слишком мало сообщений — не о чём говорить!")
            cooldowns.pop((message.from_user.id, message.chat.id, "podcast"), None)
            return

        # Балансировка по авторам
        author_counts: dict = {}
        balanced = []
        for m in all_msgs:
            author = m.get("first_name") or m.get("username") or "?"
            if author_counts.get(author, 0) < 12:
                balanced.append(m)
                author_counts[author] = author_counts.get(author, 0) + 1

        import datetime as _dt2
        lines = []
        for m in balanced[:400]:
            text = m.get("message_text", "")
            if not text:
                continue
            name = m.get("first_name") or m.get("username") or "Аноним"
            ts = m.get("created_at")
            time_tag = ""
            if ts:
                try:
                    h = _dt2.datetime.fromtimestamp(int(ts)).hour
                    time_tag = "[ночь] " if h < 6 else "[утро] " if h < 12 else "[день] " if h < 18 else "[вечер] "
                except Exception:
                    pass
            lines.append(f"{time_tag}{name}: {text}")
        messages_text = "\n".join(lines)[:15000]

        chat_title = message.chat.title or "чат"

        podcast_system = """Ты — сценарист самого токсичного подкаста рунета. Без цензуры, без фильтров, максимальные эмоции.

Ведущие:
- Макс — абсолютно токсичный, злобный, матерится через слово, постоянно в шоке и ярости от тупости людей, жёстко унижает и троллит участников чата, использует оскорбительные сравнения и сарказм на грани
- Катя — невыносимая истеричка, орёт от удивления и возмущения, не может поверить в происходящее, переживает каждую мелочь как личную трагедию или конец света, матерится, истерически ржёт над чужими факапами

ФОРМАТ — строго чередующийся диалог:
Макс: ...
Катя: ...
Макс: ...

Правила (СТРОГО):
- 30-40 реплик, каждая 1-3 предложения, темп быстрый и рваный
- Мат обязателен — это основа стиля, не исключение
- Цитируют конкретные фразы участников дословно и тут же жёстко их разносят
- Называют людей по именам, стебутся над их словами и поступками
- Эмоции передаются словами и знаками: "ТЫ СЕРЬЁЗНО?!", "это же полный пиздец!", "я не могу, блять!", "КАК ЭТО ВООБЩЕ ВОЗМОЖНО"
- Восклицательные знаки, капслок, многоточия — всё в ход
- Начало: орут приветствие и сразу набрасываются на тему
- Конец: финальный токсичный приговор всему чату и всем его участникам
- Только русский язык
- Никаких ремарок типа (смеётся) — эмоции только через текст"""

        async with session.post(
            "https://ai-gateway.vercel.sh/v1/messages",
            json={
                "model": "anthropic/claude-sonnet-4-20250514",
                "max_tokens": 2000,
                "temperature": 0.9,
                "system": podcast_system,
                "messages": [{"role": "user", "content": f"Чат «{chat_title}». Сообщения:\n{messages_text}\n\nНапиши подкаст-эпизод про этот чат."}]
            },
            headers={
                "Authorization": f"Bearer {ai_gateway_key}",
                "Content-Type": "application/json",
                "anthropic-version": "2023-06-01"
            },
            timeout=aiohttp.ClientTimeout(total=60)
        ) as resp:
            if resp.status != 200:
                await processing.edit_text(f"❌ Ошибка генерации скрипта: {(await resp.text())[:200]}")
                return
            claude_result = await resp.json()

        script = claude_result.get("content", [{}])[0].get("text", "").strip()
        if not script:
            await processing.edit_text("❌ Не смог написать скрипт")
            return

        await processing.edit_text("🔊 Скрипт готов, озвучиваю...")

        # Gemini TTS — multi-speaker
        import asyncio as _asyncio
        from google import genai as _genai
        from google.genai import types as _gtypes
        import wave as _wave
        import io as _io

        def _generate_audio(api_key: str, script_text: str) -> bytes:
            client = _genai.Client(api_key=api_key)
            prompt = f"TTS the following podcast conversation between Макс and Катя:\n{script_text}"
            response = client.models.generate_content(
                model="gemini-2.5-flash-preview-tts",
                contents=prompt,
                config=_gtypes.GenerateContentConfig(
                    response_modalities=["AUDIO"],
                    speech_config=_gtypes.SpeechConfig(
                        multi_speaker_voice_config=_gtypes.MultiSpeakerVoiceConfig(
                            speaker_voice_configs=[
                                _gtypes.SpeakerVoiceConfig(
                                    speaker="Макс",
                                    voice_config=_gtypes.VoiceConfig(
                                        prebuilt_voice_config=_gtypes.PrebuiltVoiceConfig(
                                            voice_name="Fenrir"
                                        )
                                    )
                                ),
                                _gtypes.SpeakerVoiceConfig(
                                    speaker="Катя",
                                    voice_config=_gtypes.VoiceConfig(
                                        prebuilt_voice_config=_gtypes.PrebuiltVoiceConfig(
                                            voice_name="Aoede"
                                        )
                                    )
                                ),
                            ]
                        )
                    )
                )
            )
            raw = response.candidates[0].content.parts[0].inline_data.data

            # inline_data.data может быть base64-строкой или bytes — декодируем
            import base64 as _b64
            if isinstance(raw, str):
                pcm_data = _b64.b64decode(raw)
            elif isinstance(raw, bytes):
                # Проверяем: вдруг это всё же base64 в bytes
                try:
                    pcm_data = _b64.b64decode(raw)
                except Exception:
                    pcm_data = raw
            else:
                pcm_data = bytes(raw)

            # PCM → WAV в памяти (24kHz, 16-bit, mono)
            buf = _io.BytesIO()
            with _wave.open(buf, "wb") as wf:
                wf.setnchannels(1)
                wf.setsampwidth(2)
                wf.setframerate(24000)
                wf.writeframes(pcm_data)
            return buf.getvalue()

        loop = _asyncio.get_event_loop()
        wav_bytes = await loop.run_in_executor(None, _generate_audio, google_key, script)

        await processing.delete()

        await message.answer_audio(
            BufferedInputFile(wav_bytes, filename="podcast.wav"),
            caption=f"🎙️ Подкаст про «{chat_title}»\n<i>Ведущие: Макс и Катя</i>",
            title=f"Подкаст — {chat_title}",
            performer="AI Podcast",
            parse_mode=ParseMode.HTML
        )

    except Exception as e:
        logger.warning(f"PODCAST error: {e}")
        try:
            await processing.edit_text(f"❌ Ошибка подкаста: {e}")
        except Exception:
            pass


# ==================== СЖЕЧЬ ЧЕЛОВЕКА ====================

@router.message(Command("burn", "сжечь", "кремация", "костёр", "поджечь"))
async def cmd_burn(message: Message):
    """Сжечь человека на костре правды"""
    if message.chat.type == "private":
        await message.answer("❌ Сожжения проводятся только публично!")
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    target_user = None
    target_name = None
    target_username = None
    target_user_id = None
    
    if message.reply_to_message and message.reply_to_message.from_user:
        target_user = message.reply_to_message.from_user
        target_name = target_user.first_name
        target_username = target_user.username
        target_user_id = target_user.id
    else:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            target_name = parts[1].strip().replace("@", "")
        else:
            await message.answer(
                "🔥 *Как сжечь человека:*\n\n"
                "1️⃣ Ответь на сообщение: `/сжечь`\n"
                "2️⃣ Или укажи имя: `/сжечь Вася`\n\n"
                "Тётя Роза разожжёт костёр правды! 🪵",
                parse_mode=ParseMode.MARKDOWN
            )
            return
    
    if not target_name:
        target_name = "Хуй с горы"
    
    burn_api_url = get_api_url("burn")
    
    can_do, cooldown_remaining = check_cooldown(user_id, chat_id, "burn", 30)
    if not can_do:
        await message.answer(f"⏰ Костёр ещё не остыл! Подожди {cooldown_remaining} сек")
        return
    
    # Rate limit
    can_call, wait_time = check_api_rate_limit(chat_id, "burn")
    if not can_call:
        await message.answer(f"⏰ Костёр перегрелся! Подожди {wait_time} сек")
        return
    
    processing_msg = await message.answer(f"🔥 Тётя Роза собирает хворост и поджигает {target_name}... 🪵")
    metrics.track_command("burn")
    
    try:
        # Собираем ПОЛНЫЙ контекст: память + сообщения
        user_memory = ""
        if target_user_id:
            user_memory = await gather_user_memory(chat_id, target_user_id, target_name)
        
        context, messages_found = await gather_user_context(chat_id, target_user_id) if target_user_id else ("Горел молча, как и жил", 0)
        full_context = f"{user_memory}\n\n=== СООБЩЕНИЯ ===\n{context}" if user_memory else context
        logger.info(f"Burn: {target_name}, {messages_found} msgs, memory: {bool(user_memory)}")
        
        metrics.track_api_call("burn")
        session = await get_http_session()
        async with session.post(
                burn_api_url,
                json={"name": target_name, "username": target_username or "", "context": full_context}
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    
                    if "error" in result:
                        await processing_msg.edit_text(f"❌ Ошибка: {result['error']}")
                        return
                    
                    burn_text = result.get("result", "Не загорелся — слишком сырой")
                    await processing_msg.edit_text(burn_text)
                else:
                    error = await response.text()
                    logger.error(f"Burn API error: {response.status} - {error}")
                    await processing_msg.edit_text("❌ Костёр потух. Попробуй позже!")
                    
    except asyncio.TimeoutError:
        await processing_msg.edit_text("⏰ Долго горит... слишком много пиздежа было")
    except Exception as e:
        logger.error(f"Error in burn command: {e}")
        await processing_msg.edit_text("❌ Дрова кончились. Попробуй позже!")


# ==================== БУХНУТЬ С ЧЕЛОВЕКОМ ====================

@router.message(Command("drink", "бухнуть", "выпить", "бухло", "накатить"))
async def cmd_drink(message: Message):
    """Бухнуть с человеком и слить его секреты"""
    if message.chat.type == "private":
        await message.answer("❌ Бухать только в компании!")
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    target_user = None
    target_name = None
    target_username = None
    target_user_id = None
    
    if message.reply_to_message and message.reply_to_message.from_user:
        target_user = message.reply_to_message.from_user
        target_name = target_user.first_name
        target_username = target_user.username
        target_user_id = target_user.id
    else:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            target_name = parts[1].strip().replace("@", "")
        else:
            await message.answer(
                "🍻 *Как бухнуть с человеком:*\n\n"
                "1️⃣ Ответь на сообщение: `/бухнуть`\n"
                "2️⃣ Или укажи имя: `/бухнуть Вася`\n\n"
                "Тётя Роза напоит и сольёт все секреты! 🍺",
                parse_mode=ParseMode.MARKDOWN
            )
            return
    
    if not target_name:
        target_name = "этот хрен"
    
    drink_api_url = get_api_url("drink")
    
    can_do, cooldown_remaining = check_cooldown(user_id, chat_id, "drink", 30)
    if not can_do:
        await message.answer(f"⏰ Тётя Роза ещё не протрезвела! Подожди {cooldown_remaining} сек")
        return
    
    # Rate limit
    can_call, wait_time = check_api_rate_limit(chat_id, "drink")
    if not can_call:
        await message.answer(f"⏰ Тётя Роза ещё не протрезвела! Подожди {wait_time} сек")
        return
    
    processing_msg = await message.answer(f"🍻 Тётя Роза открывает бутылку и зовёт {target_name} бухать... 🥃")
    metrics.track_command("drink")
    
    try:
        # Собираем ПОЛНЫЙ контекст: память + сообщения
        user_memory = ""
        if target_user_id:
            user_memory = await gather_user_memory(chat_id, target_user_id, target_name)
        
        context, messages_found = await gather_user_context(chat_id, target_user_id) if target_user_id else ("Молчал как партизан", 0)
        full_context = f"{user_memory}\n\n=== СООБЩЕНИЯ ===\n{context}" if user_memory else context
        logger.info(f"Drink: {target_name}, {messages_found} msgs, memory: {bool(user_memory)}")
        
        metrics.track_api_call("drink")
        session = await get_http_session()
        async with session.post(
                drink_api_url,
                json={"name": target_name, "username": target_username or "", "context": full_context}
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    
                    if "error" in result:
                        await processing_msg.edit_text(f"❌ Ошибка: {result['error']}")
                        return
                    
                    drink_text = result.get("result", "Отказался бухать — ссыкло")
                    await processing_msg.edit_text(drink_text)
                else:
                    error = await response.text()
                    logger.error(f"Drink API error: {response.status} - {error}")
                    await processing_msg.edit_text("❌ Тётя Роза уже в отключке. Попробуй позже!")
                    
    except asyncio.TimeoutError:
        await processing_msg.edit_text("⏰ Слишком долго бухали... оба вырубились")
    except Exception as e:
        logger.error(f"Error in drink command: {e}")
        await processing_msg.edit_text("❌ Бар закрыт. Приходи позже!")


# ==================== ПОСОСИ ====================

SUCK_API_URL = os.getenv("SUCK_API_URL", "")

@router.message(Command("suck", "пососи", "соси", "сосни"))
async def cmd_suck(message: Message):
    """Послать человека сосать — AI генерация"""
    if message.chat.type == "private":
        await message.answer("❌ Сосать только публично!")
        return
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Проверка кулдауна
    if not check_cooldown(user_id, chat_id, "suck", 10):
        await message.answer("⏳ Рот занят. Подожди!")
        return
    
    target_name = None
    target_id = None
    target_username = None
    target_profile = {}
    
    # Приоритет 1: реплай на сообщение
    if message.reply_to_message and message.reply_to_message.from_user:
        target = message.reply_to_message.from_user
        target_name = target.first_name
        target_id = target.id
        target_username = target.username
        logger.info(f"SUCK: Reply to user - name={target_name}, id={target_id}, username={target_username}")
    else:
        # Приоритет 2: упоминание через @username или text_mention в команде
        if message.entities:
            for entity in message.entities:
                if entity.type == "mention":
                    # @username в тексте команды
                    mentioned = message.text[entity.offset:entity.offset + entity.length]
                    target_username = mentioned.lstrip("@")
                    target_name = target_username
                    # Ищем в реестре пользователей чата
                    if USE_POSTGRES:
                        try:
                            found = await find_user_in_chat(message.chat.id, target_username)
                            if found:
                                target_id = found['user_id']
                                target_name = found['first_name'] or target_username
                                target_username = found['username']
                        except Exception as e:
                            logger.debug(f"Could not find user by username: {e}")
                    break
                elif entity.type == "text_mention" and entity.user:
                    # Упоминание через ID (text_mention)
                    target_id = entity.user.id
                    target_name = entity.user.first_name or entity.user.username or "Кто-то"
                    target_username = entity.user.username
                    break
        
        # Приоритет 3: просто текст после команды
        if not target_name:
            parts = message.text.split(maxsplit=1)
            if len(parts) > 1:
                raw_name = parts[1].strip().replace("@", "")
                target_name = raw_name
                # Ищем пользователя по имени в реестре чата
                if USE_POSTGRES and raw_name:
                    try:
                        found = await find_user_in_chat(message.chat.id, raw_name)
                        if found:
                            target_id = found['user_id']
                            target_name = found['first_name'] or raw_name
                            target_username = found['username']
                    except Exception as e:
                        logger.debug(f"Could not find user by name: {e}")
            else:
                await message.answer("🍭 Кому сосать? Ответь на сообщение или укажи имя!")
                return
    
    if not target_name:
        target_name = "Эй ты"
    
    # Получаем профиль для персонализации (per-chat!)
    if USE_POSTGRES and target_id:
        try:
            target_profile = await get_user_profile_for_ai(target_id, message.chat.id, target_name, target_username or "")
        except Exception as e:
            logger.debug(f"Could not get profile for suck: {e}")
    
    # Создаём кликабельное упоминание если есть ID
    if target_id:
        display_name = make_user_mention(target_id, target_name, target_username)
        logger.info(f"SUCK: Created mention - display_name={display_name}")
    else:
        display_name = target_name  # Просто текст без ссылки
        logger.info(f"SUCK: No ID, using plain name - {target_name}")
    
    if not SUCK_API_URL:
        # Fallback если API не настроен
        await message.answer(f"🍭 {display_name}, пососи, пожалуйста. Вселенная ждёт. Соси, блять.", parse_mode=ParseMode.HTML)
        return
    
    processing_msg = await message.answer("🍭 Готовлю послание...")
    metrics.track_command("suck")
    
    try:
        # Собираем память для персонализации
        user_memory = ""
        if target_id:
            user_memory = await gather_user_memory(chat_id, target_id, target_name)
        
        metrics.track_api_call("suck")
        session = await get_http_session()
        async with session.post(SUCK_API_URL, json={
            "name": target_name,
            "profile": target_profile,
            "memory": user_memory
        }) as response:
                if response.status == 200:
                    result = await response.json()
                    text = result.get("text", f"🍭 {target_name}, соси. Тётя Роза так сказала.")
                    
                    # Заменяем имя на кликабельное упоминание в ответе
                    if target_id:
                        # Заменяем все вхождения имени (регистронезависимо)
                        # Экранируем спецсимволы в имени для regex
                        escaped_name = re.escape(target_name)
                        # Заменяем с сохранением регистра
                        text = re.sub(escaped_name, display_name, text, flags=re.IGNORECASE)
                    
                    await processing_msg.edit_text(text, parse_mode=ParseMode.HTML)
                else:
                    error_text = await response.text()
                    logger.error(f"Suck API error: {response.status} - {error_text}")
                    await processing_msg.edit_text(f"🍭 {display_name}, пососи. API сломался, но посыл остался.", parse_mode=ParseMode.HTML)
    
    except asyncio.TimeoutError:
        await processing_msg.edit_text(f"🍭 {display_name}, пососи. Тётя Роза задумалась, но посыл ясен.", parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Error in suck command: {e}")
        await processing_msg.edit_text(f"🍭 {display_name}, соси. Ошибка, но соси.", parse_mode=ParseMode.HTML)


# ==================== ПРОВЕТРИТЬ ЧАТ ====================

VENTILATE_API_URL = os.getenv("VENTILATE_API_URL", "")


def make_user_mention(user_id: int, name: str, username: str = None) -> str:
    """Создаёт кликабельное упоминание пользователя (HTML формат)"""
    # Экранируем HTML символы в имени
    safe_name = name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f'<a href="tg://user?id={user_id}">{safe_name}</a>'


def decline_russian_name(name: str, gender: str = "мужской") -> dict:
    """
    Склонение русских имён по падежам.
    Возвращает словарь с формами: nom, gen, dat, acc, ins, pre
    """
    name = name.strip()
    if not name:
        return {case: name for case in ['nom', 'gen', 'dat', 'acc', 'ins', 'pre']}
    
    # Определяем тип окончания
    name_lower = name.lower()
    
    # Неизменяемые имена (иностранные)
    unchangeable = ['алекс', 'макс', 'крис', 'ким', 'ли', 'джон', 'том', 'бен', 'сэм', 'дэн']
    if name_lower in unchangeable or len(name) <= 2:
        return {case: name for case in ['nom', 'gen', 'dat', 'acc', 'ins', 'pre']}
    
    base = name[:-1] if len(name) > 1 else name
    last = name[-1].lower()
    last2 = name[-2:].lower() if len(name) >= 2 else ""
    
    result = {'nom': name}
    
    # Женские имена на -а (Маша, Аня, Лена)
    if last == 'а' and gender == "женский":
        result['gen'] = base + 'ы' if last2 not in ['ка', 'га', 'ха', 'ша', 'ча', 'ща', 'жа'] else base + 'и'
        result['dat'] = base + 'е'
        result['acc'] = base + 'у'
        result['ins'] = base + 'ой'
        result['pre'] = base + 'е'
        
    # Женские имена на -я (Юля, Настя, Мария)
    elif last == 'я' and gender == "женский":
        if last2 == 'ия':  # Мария, София
            base2 = name[:-2]
            result['gen'] = base2 + 'ии'
            result['dat'] = base2 + 'ии'
            result['acc'] = base2 + 'ию'
            result['ins'] = base2 + 'ией'
            result['pre'] = base2 + 'ии'
        else:  # Юля, Настя
            result['gen'] = base + 'и'
            result['dat'] = base + 'е'
            result['acc'] = base + 'ю'
            result['ins'] = base + 'ей'
            result['pre'] = base + 'е'
            
    # Мужские имена на -а/-я (Никита, Илья, Саша)
    elif last in ['а', 'я'] and gender == "мужской":
        if last == 'а':
            result['gen'] = base + 'ы' if last2 not in ['ка', 'га', 'ха', 'ша', 'ча'] else base + 'и'
            result['dat'] = base + 'е'
            result['acc'] = base + 'у'
            result['ins'] = base + 'ой'
            result['pre'] = base + 'е'
        else:  # -я (Илья)
            result['gen'] = base + 'и'
            result['dat'] = base + 'е'
            result['acc'] = base + 'ю'
            result['ins'] = base + 'ёй'
            result['pre'] = base + 'е'
            
    # Мужские имена на -й (Сергей, Алексей, Андрей, Дмитрий)
    elif last == 'й':
        if last2 == 'ий':  # Дмитрий, Василий
            base2 = name[:-2]
            result['gen'] = base2 + 'ия'
            result['dat'] = base2 + 'ию'
            result['acc'] = base2 + 'ия'
            result['ins'] = base2 + 'ием'
            result['pre'] = base2 + 'ии'
        else:  # Сергей, Алексей
            result['gen'] = base + 'я'
            result['dat'] = base + 'ю'
            result['acc'] = base + 'я'
            result['ins'] = base + 'ем'
            result['pre'] = base + 'е'
            
    # Мужские имена на -ь (Игорь)
    elif last == 'ь' and gender == "мужской":
        result['gen'] = base + 'я'
        result['dat'] = base + 'ю'
        result['acc'] = base + 'я'
        result['ins'] = base + 'ем'
        result['pre'] = base + 'е'
        
    # Женские имена на -ь (Любовь)
    elif last == 'ь' and gender == "женский":
        result['gen'] = base + 'и'
        result['dat'] = base + 'и'
        result['acc'] = name  # Любовь
        result['ins'] = base + 'ью'
        result['pre'] = base + 'и'
        
    # Мужские имена на согласную (Иван, Пётр, Олег, Максим)
    elif last not in 'аеёиоуыэюя':
        result['gen'] = name + 'а'
        result['dat'] = name + 'у'
        result['acc'] = name + 'а'
        result['ins'] = name + 'ом'
        result['pre'] = name + 'е'
        
    # Для остальных — без изменений
    else:
        result = {case: name for case in ['nom', 'gen', 'dat', 'acc', 'ins', 'pre']}
    
    return result


@router.message(Command("ventilate", "проветрить", "форточка", "свежесть"))
async def cmd_ventilate(message: Message):
    """Проветрить чат — абсурдное событие с рандомным участником"""
    if message.chat.type == "private":
        await message.answer("❌ Проветривать можно только групповые чаты!")
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    # Кулдаун 30 секунд
    can_do, cooldown_remaining = check_cooldown(user_id, chat_id, "ventilate", 30)
    if not can_do:
        await message.answer(f"⏰ Форточка ещё не закрылась! Подожди {cooldown_remaining} сек")
        return
    
    # Rate limit
    can_call, wait_time = check_api_rate_limit(chat_id, "ventilate")
    if not can_call:
        await message.answer(f"⏰ Слишком часто проветриваете! Подожди {wait_time} сек")
        return
    
    # Определяем жертву: либо реплай, либо рандом из активных
    victim_name = None
    victim_username = None
    victim_id = None
    victim_messages = []
    
    if message.reply_to_message and message.reply_to_message.from_user:
        # Если ответ на сообщение — жертва тот, кому отвечают
        victim = message.reply_to_message.from_user
        victim_name = victim.first_name
        victim_username = victim.username
        victim_id = victim.id
    else:
        # Иначе берём случайного из последних активных
        try:
            if USE_POSTGRES:
                stats = await get_chat_statistics(chat_id, hours=24)
                if stats.get('top_authors'):
                    # Берём рандомного из топ-10 активных
                    active_users = stats['top_authors'][:10]
                    if active_users:
                        victim_data = random.choice(active_users)
                        victim_name = victim_data.get('first_name', 'Кто-то')
                        victim_username = victim_data.get('username', '')
                        victim_id = victim_data.get('user_id')
        except Exception as e:
            logger.warning(f"Could not get active users for ventilate: {e}")
    
    # Если не нашли жертву — берём того, кто вызвал команду
    if not victim_name:
        victim_name = message.from_user.first_name
        victim_username = message.from_user.username
        victim_id = message.from_user.id
    
    # Получаем последние сообщения жертвы для определения пола
    victim_profile = {}
    try:
        if USE_POSTGRES and victim_id:
            # Берём до 1000 сообщений для точного определения пола по глаголам
            messages = await get_user_messages(chat_id, victim_id, limit=1000)
            victim_messages = [m.get('message_text', '') for m in messages if m.get('message_text')]
            
            # Получаем полный профиль жертвы для персонализации (per-chat!)
            victim_profile = await get_user_profile_for_ai(victim_id, chat_id, victim_name, victim_username or "")
    except Exception as e:
        logger.warning(f"Could not get victim data: {e}")
    
    # Определяем пол: сначала из профиля, потом по имени
    if victim_profile and victim_profile.get('gender') and victim_profile.get('gender') != 'unknown':
        gender = victim_profile['gender']
    else:
        # Fallback по имени
        is_female = False
        name_lower = victim_name.lower() if victim_name else ""
        female_endings = ['а', 'я', 'ия', 'ья']
        # Расширенный список мужских имён на -а/-я
        male_with_a = [
            'никита', 'илья', 'кузьма', 'фома', 'лука', 'саша', 'женя',
            'вова', 'дима', 'миша', 'коля', 'толя', 'витя', 'петя', 'ваня',
            'лёша', 'лёня', 'гоша', 'гриша', 'паша', 'сеня', 'стёпа', 'тёма',
            'данила', 'кирилла', 'савва', 'наума'
        ]
        if name_lower not in male_with_a:
            for ending in female_endings:
                if name_lower.endswith(ending):
                    is_female = True
                    break
        gender = "женский" if is_female else "мужской"
    
    # Склоняем имя
    declined = decline_russian_name(victim_name, gender)
    
    # Создаём кликабельные упоминания для всех падежей
    def mention_with_case(case_form: str) -> str:
        safe_form = case_form.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        return f'<a href="tg://user?id={victim_id}">{safe_form}</a>'
    
    mentions = {
        'nom': mention_with_case(declined['nom']),
        'gen': mention_with_case(declined['gen']),
        'dat': mention_with_case(declined['dat']),
        'acc': mention_with_case(declined['acc']),
        'ins': mention_with_case(declined['ins']),
        'pre': mention_with_case(declined['pre']),
    }
    
    # Проверяем API
    ventilate_url = VENTILATE_API_URL or get_api_url("ventilate")
    
    processing_msg = await message.answer("🪟 Открываю форточку...")
    metrics.track_command("ventilate")
    
    try:
        metrics.track_api_call("ventilate")
        session = await get_http_session()
        async with session.post(
                ventilate_url,
                json={
                    "victim_name": victim_name,
                    "victim_username": victim_username or "",
                    "victim_id": victim_id,
                    "victim_messages": victim_messages,
                    "initial_gender": gender,  # Передаём начальное определение пола
                    "victim_profile": victim_profile  # Полный профиль для персонализации
                }
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    text = result.get("text", "🪟 Форточка не открылась. Заклинило.")
                    
                    # API возвращает пол — ИСПОЛЬЗУЕМ ЕГО (он точнее, т.к. анализирует сообщения)
                    api_gender = result.get("gender", gender)
                    
                    # Пересклоняем имя с правильным полом
                    declined = decline_russian_name(victim_name, api_gender)
                    mentions = {
                        'nom': mention_with_case(declined['nom']),
                        'gen': mention_with_case(declined['gen']),
                        'dat': mention_with_case(declined['dat']),
                        'acc': mention_with_case(declined['acc']),
                        'ins': mention_with_case(declined['ins']),
                        'pre': mention_with_case(declined['pre']),
                    }
                    
                    # 1. Заменяем плейсхолдеры на кликабельные склонённые упоминания
                    text = text.replace("{VICTIM_NOM}", mentions['nom'])
                    text = text.replace("{VICTIM_GEN}", mentions['gen'])
                    text = text.replace("{VICTIM_DAT}", mentions['dat'])
                    text = text.replace("{VICTIM_ACC}", mentions['acc'])
                    text = text.replace("{VICTIM_INS}", mentions['ins'])
                    text = text.replace("{VICTIM_PRE}", mentions['pre'])
                    text = text.replace("{VICTIM}", mentions['nom'])
                    
                    # 2. Заменяем @username на кликабельную ссылку
                    if victim_username:
                        text = text.replace(f"@{victim_username}", mentions['nom'])
                    
                    # 3. Заменяем все формы имени на кликабельные (если AI написал напрямую)
                    # Собираем все уникальные формы имени
                    unique_forms = list(set(declined.values()))
                    # Сортируем по длине (сначала длинные, чтобы "Александра" заменилась раньше "Александр")
                    unique_forms.sort(key=len, reverse=True)
                    
                    for case_form in unique_forms:
                        if case_form and len(case_form) > 1:
                            # Находим какой падеж это
                            case_key = next((k for k, v in declined.items() if v == case_form), 'nom')
                            mention = mentions[case_key]
                            
                            # Пропускаем если форма уже в тексте как часть ссылки
                            if f'>{case_form}<' in text:
                                continue
                            
                            # Заменяем только если не внутри HTML тега
                            # Паттерн: имя окружено не-буквами и не > или <
                            pattern = r'(?<![а-яА-Яa-zA-Z>])' + re.escape(case_form) + r'(?![а-яА-Яa-zA-Z<])'
                            text = re.sub(pattern, mention, text, count=5)
                    
                    await processing_msg.edit_text(text, parse_mode=ParseMode.HTML)
                else:
                    error_text = await response.text()
                    logger.error(f"Ventilate API error: {response.status} - {error_text}")
                    # Fallback с кликабельным упоминанием и склонением
                    # Используем gender (не api_gender), т.к. api_gender определён только при успехе
                    fallback_events = [
                        f"🪟 Тётя Роза открыла форточку в чате.\n\nЗалетел голубь. Насрал на {mentions['acc']}. Улетел.\n\nПроветрено.",
                        f"🪟 Тётя Роза открыла форточку в чате.\n\nСквозняком сдуло {mentions['acc']} куда-то в угол чата. {mentions['nom']} там теперь сидит.\n\nСвежо.",
                        f"🪟 Тётя Роза открыла форточку в чате.\n\nВорвался холод. {mentions['nom']} {'замёрзла' if gender == 'женский' else 'замёрз'} нахуй.\n\nЗакрываю."
                    ]
                    await processing_msg.edit_text(random.choice(fallback_events), parse_mode=ParseMode.HTML)
    
    except asyncio.TimeoutError:
        await processing_msg.edit_text("🪟 Форточка заклинила. Попробуй позже.")
    except Exception as e:
        logger.error(f"Error in ventilate command: {e}")
        metrics.track_error()
        await processing_msg.edit_text("🪟 Форточка заклинила. Попробуй позже!")


# ==================== УМНЫЕ ОТВЕТЫ НА УПОМИНАНИЯ ====================

REPLY_API_URL = os.getenv("REPLY_API_URL", "")


async def generate_smart_reply(message: Message) -> str:
    """
    Генерирует умный AI-ответ с МНОГОУРОВНЕВЫМ контекстом:
    
    Уровень 1: Профиль пользователя (пол, стиль)
    Уровень 2: Умная память (факты, воспоминания)
    Уровень 3: Контекст чата (последние сообщения)
    Уровень 4: Сводки и события (что было раньше)
    
    Использует build_smart_context для максимальной контекстуальности.
    """
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.first_name or "Аноним"
    text = message.text or message.caption or ""
    
    # Собираем контекст
    user_profile = "Профиль не известен"
    user_memory = ""
    chat_context = ""
    gender = "мужской"
    
    if USE_POSTGRES:
        try:
            # Профиль пользователя
            profile = await get_user_profile_for_ai(user_id, chat_id, user_name, message.from_user.username or "")
            if profile:
                gender = profile.get('gender', 'мужской')
                traits = profile.get('traits', [])
                interests = profile.get('interests', [])
                style = profile.get('communication_style', '')
                user_profile = f"Пол: {gender}\nСтиль: {style}\nЧерты: {', '.join(traits[:5])}\nИнтересы: {', '.join(interests[:5])}"
            
            # МНОГОУРОВНЕВАЯ ПАМЯТЬ — собираем всё
            user_memory = await gather_user_memory(chat_id, user_id, user_name)
            
            # УМНЫЙ КОНТЕКСТ — факты, сводки, события + сообщения
            try:
                from database_postgres import build_smart_context
                smart_ctx = await build_smart_context(
                    chat_id=chat_id,
                    user_id=user_id,
                    max_messages=150,  # Последние 150 сообщений для полного контекста
                    include_facts=True,
                    include_summaries=True,
                    include_events=True
                )
                
                # Используем готовый форматированный контекст
                chat_context = smart_ctx.get('formatted_context', '')[:4000]
                
            except ImportError:
                # Fallback на старую систему
                from database_postgres import get_chat_statistics
                stats = await get_chat_statistics(chat_id, hours=1)
                if stats and stats.get('recent_messages'):
                    recent = stats['recent_messages'][:20]
                    chat_lines = []
                    for msg in recent:
                        sender = msg.get('first_name', 'Аноним')
                        text_msg = msg.get('message_text', '')[:100]
                        if text_msg:
                            chat_lines.append(f"{sender}: {text_msg}")
                    chat_context = "\n".join(chat_lines)
                    
        except Exception as e:
            logger.debug(f"Could not gather context for smart reply: {e}")
    
    # Fallback на локальный ответ если API не настроен
    reply_url = REPLY_API_URL or get_api_url("reply")
    if not reply_url or "your-vercel" in reply_url:
        return get_contextual_reply(text)
    
    try:
        session = await get_http_session()
        async with session.post(reply_url, json={
            "message": text,
            "user_name": user_name,
            "gender": gender,
            "user_profile": user_profile,
            "user_memory": user_memory[:2000] if user_memory else "",
            "chat_context": chat_context[:1500] if chat_context else ""
        }, timeout=aiohttp.ClientTimeout(total=15)) as response:
            if response.status == 200:
                result = await response.json()
                reply = result.get("reply", "")
                if reply:
                    logger.info(f"SMART REPLY generated for {user_name}: {reply[:50]}...")
                    return reply
    except asyncio.TimeoutError:
        logger.warning("Smart reply timeout, falling back to local")
    except Exception as e:
        logger.warning(f"Smart reply error: {e}, falling back to local")
    
    # Fallback на локальный ответ
    return get_contextual_reply(text)


# ==================== АВТОЗАПОМИНАНИЕ ФАКТОВ ====================

EXTRACT_FACTS_API_URL = os.getenv("EXTRACT_FACTS_API_URL", "")

# Кэш для rate limiting извлечения фактов (chat_id -> last_extraction_time)
fact_extraction_cache = {}
FACT_EXTRACTION_INTERVAL = 30  # Секунд между извлечениями фактов для одного чата


def extract_facts_locally(text: str, user_name: str) -> List[Dict[str, Any]]:
    """
    Локальное извлечение базовых фактов без AI.
    Используется как fallback если API недоступен.
    """
    facts = []
    text_lower = text.lower()
    
    # Паттерны для работы/профессии
    work_patterns = [
        (r'работаю\s+(\w+)', 'work', 'работает {0}'),
        (r'я\s+(\w+ист|программист|дизайнер|менеджер|врач|учитель|инженер)', 'work', 'по профессии {0}'),
        (r'в\s+(компании|офисе|банке|магазине)\s+(\w+)', 'work', 'работает в {1}'),
    ]
    
    # Паттерны для семьи
    family_patterns = [
        (r'(женился|вышла замуж)', 'family', 'состоит в браке'),
        (r'(развёлся|развелась|развод)', 'family', 'в разводе'),
        (r'(ребёнок|дети|сын|дочь)', 'family', 'есть дети'),
        (r'(жена|муж|супруг)', 'family', 'в браке'),
    ]
    
    # Паттерны для хобби/интересов
    hobby_patterns = [
        (r'люблю\s+(\w+)', 'hobby', 'любит {0}'),
        (r'играю в\s+(\w+)', 'hobby', 'играет в {0}'),
        (r'смотрю\s+(\w+)', 'hobby', 'смотрит {0}'),
        (r'слушаю\s+(\w+)', 'hobby', 'слушает {0}'),
    ]
    
    # Паттерны для места жительства
    location_patterns = [
        (r'живу в\s+(\w+)', 'location', 'живёт в {0}'),
        (r'из\s+(москвы|питера|киева|минска|\w+)', 'location', 'из города {0}'),
        (r'переехал в\s+(\w+)', 'location', 'переехал в {0}'),
    ]
    
    import re
    all_patterns = work_patterns + family_patterns + hobby_patterns + location_patterns
    
    for pattern, fact_type, template in all_patterns:
        match = re.search(pattern, text_lower)
        if match:
            try:
                groups = match.groups()
                if groups:
                    fact_text = template.format(*groups)
                else:
                    fact_text = template
                facts.append({
                    'type': fact_type,
                    'text': f"{user_name} {fact_text}",
                    'confidence': 0.6  # Локальные факты менее уверенные
                })
            except:
                pass
    
    return facts[:3]  # Максимум 3 факта локально


async def extract_and_save_facts(message: Message) -> bool:
    """
    Извлекает факты из сообщения и сохраняет в умную память.
    Использует AI API с fallback на локальное извлечение.
    """
    text = message.text or ""
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.first_name or "Аноним"
    
    # Не анализируем короткие сообщения
    if len(text) < 20:
        return False
    
    # Rate limiting — не чаще раза в 30 секунд на чат
    now = time.time()
    last_extraction = fact_extraction_cache.get(chat_id, 0)
    if now - last_extraction < FACT_EXTRACTION_INTERVAL:
        return False
    
    # Быстрая фильтрация — только потенциально информативные сообщения
    personal_markers = ['я ', 'мой ', 'моя ', 'моё ', 'мне ', 'меня ', 'работаю', 'живу', 
                        'люблю', 'ненавижу', 'купил', 'поехал', 'женился', 'развёлся']
    has_personal = any(marker in text.lower() for marker in personal_markers)
    
    social_markers = ['@', 'он ', 'она ', 'они ', 'вместе', 'встречаемся', 'друг', 'подруга']
    has_social = any(marker in text.lower() for marker in social_markers)
    
    is_long = len(text) > 100
    
    if not (has_personal or has_social or is_long):
        return False
    
    # Обновляем cache
    fact_extraction_cache[chat_id] = now
    
    facts_to_save = []
    api_success = False
    
    # Пробуем API извлечения фактов
    extract_url = EXTRACT_FACTS_API_URL or get_api_url("extract_facts")
    if extract_url and "your-vercel" not in extract_url:
        try:
            session = await get_http_session()
            async with session.post(extract_url, json={
                "message": text[:1000],
                "user_name": user_name
            }, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    result = await response.json()
                    if result.get("has_facts") and result.get("facts"):
                        facts_to_save = result["facts"][:5]
                        api_success = True
                else:
                    logger.warning(f"[FACTS] API returned {response.status}")
        except asyncio.TimeoutError:
            logger.warning("[FACTS] API timeout, using local extraction")
        except Exception as e:
            logger.warning(f"[FACTS] API error: {e}, using local extraction")
    
    # Fallback на локальное извлечение если API не сработал
    if not api_success and has_personal:
        local_facts = extract_facts_locally(text, user_name)
        if local_facts:
            facts_to_save = local_facts
            logger.info(f"[FACTS] Local extraction found {len(local_facts)} facts")
    
    # Сохраняем факты
    if facts_to_save and USE_POSTGRES:
        from database_postgres import save_user_fact
        
        saved_count = 0
        for fact in facts_to_save:
            fact_type = fact.get("type", "personal")
            fact_text = fact.get("text", "")
            confidence = fact.get("confidence", 0.7)
            
            if fact_text and len(fact_text) > 3:
                try:
                    success = await save_user_fact(
                        chat_id=chat_id,
                        user_id=user_id,
                        fact_type=fact_type,
                        fact_text=fact_text,
                        confidence=confidence,
                        source_message_id=message.message_id
                    )
                    if success:
                        saved_count += 1
                except Exception as e:
                    logger.debug(f"[FACTS] Save error: {e}")
        
        if saved_count > 0:
            logger.info(f"[FACTS] Saved {saved_count} facts for {user_name} in chat {chat_id}")
        return saved_count > 0
    
    return False


# ==================== ГОЛОСОВЫЕ СООБЩЕНИЯ (ElevenLabs TTS) ====================

TTS_API_URL = os.getenv("TTS_API_URL", "")

# Голоса ElevenLabs (можно менять)
ELEVENLABS_VOICE_ID = os.getenv("ELEVENLABS_VOICE_ID", "21m00Tcm4TlvDq8ikWAM")


@router.message(Command("скажи", "say", "voice", "голос"))
async def cmd_say(message: Message):
    """Тётя Роза говорит голосом! /скажи <текст>"""
    
    # Проверка кулдауна (TTS дорогой)
    user_id = message.from_user.id
    chat_id = message.chat.id
    if not check_cooldown(user_id, chat_id, "say", 20):
        await message.reply("⏳ Голосовые связки отдыхают. Подожди!")
        return
    
    # Rate limit для TTS API
    if not check_api_rate_limit(chat_id, "tts"):
        await message.reply("⏳ Слишком много голосовых запросов!")
        return
    
    # Извлекаем текст после команды
    command_text = message.text or ""
    
    # Убираем команду из текста
    for cmd in ["/скажи", "/say", "/voice", "/голос"]:
        if command_text.lower().startswith(cmd):
            text = command_text[len(cmd):].strip()
            break
    else:
        text = ""
    
    # Если нет текста, проверяем реплай
    if not text and message.reply_to_message and message.reply_to_message.text:
        text = message.reply_to_message.text[:500]
    
    if not text:
        await message.reply(
            "🎤 <b>Использование:</b>\n"
            "<code>/скажи текст</code> — тётя Роза скажет это голосом\n\n"
            "Или ответь на сообщение командой /скажи",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Лимит текста
    if len(text) > 500:
        text = text[:500]
        await message.reply("⚠️ Текст обрезан до 500 символов")
    
    # Проверяем API URL
    tts_url = TTS_API_URL or get_api_url("tts")
    if not tts_url or "your-vercel" in tts_url:
        await message.reply("❌ TTS API не настроен")
        return
    
    processing_msg = await message.reply("🎤 Записываю голосовое...")
    
    try:
        session = await get_http_session()
        
        async with session.post(
            tts_url,
            json={
                "text": text,
                "voice_id": ELEVENLABS_VOICE_ID
            },
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"TTS API error: {response.status} - {error_text}")
                await processing_msg.edit_text(f"❌ Ошибка генерации: {response.status}")
                return
            
            result = await response.json()
        
        audio_base64 = result.get("audio")
        if not audio_base64:
            await processing_msg.edit_text("❌ Не удалось сгенерировать аудио")
            return
        
        # Декодируем base64 в bytes
        import base64
        audio_bytes = base64.b64decode(audio_base64)
        
        # Создаём файл для отправки
        audio_file = BufferedInputFile(audio_bytes, filename="teta_roza.mp3")
        
        # Удаляем сообщение о процессе
        await processing_msg.delete()
        
        # Отправляем голосовое (без подписи)
        await message.reply_voice(voice=audio_file)
        
        logger.info(f"TTS generated for user {message.from_user.id}: '{text[:30]}...'")
        metrics.track_command("скажи")
        
    except asyncio.TimeoutError:
        await processing_msg.edit_text("⏱️ Тётя Роза задумалась слишком надолго...")
    except Exception as e:
        logger.error(f"TTS error: {e}")
        metrics.track_error()
        await processing_msg.edit_text("❌ Голос сел. Попробуй позже!")


# ==================== ГРЯЗНЫЙ СОН ====================

DREAM_API_URL = os.getenv("DREAM_API_URL", "")

@router.message(Command("сон", "dream", "son"))
async def cmd_dream(message: Message):
    """Генерирует грязный извращённый сон про человека"""
    
    # Проверяем реплай на бота
    if await check_command_reply_to_bot(message):
        return
    
    if message.chat.type == "private":
        await message.reply("Эта команда только для групповых чатов, одиночка ебаный")
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    # Кулдаун 30 секунд
    can_do, cooldown_remaining = check_cooldown(user_id, chat_id, "dream", 30)
    if not can_do:
        await message.reply(f"⏰ Подожди {cooldown_remaining} сек, ещё не проснулась")
        return
    
    # Rate limit
    can_call, wait_time = check_api_rate_limit(chat_id, "dream")
    if not can_call:
        await message.reply(f"⏰ Слишком много снов! Подожди {wait_time} сек")
        return
    
    # Определяем цель
    target_user = None
    target_name = None
    target_id = None
    
    # Если реплай — сон про того на кого реплай
    if message.reply_to_message and message.reply_to_message.from_user:
        target_user = message.reply_to_message.from_user
        target_name = target_user.first_name or target_user.username or "Аноним"
        target_id = target_user.id
    else:
        # Проверяем текст команды на имя/юзернейм
        command_text = message.text or ""
        parts = command_text.split(maxsplit=1)
        if len(parts) > 1:
            search_term = parts[1].strip()
            # Ищем пользователя
            if USE_POSTGRES:
                found = await find_user_in_chat(chat_id, search_term)
                if found:
                    target_name = found.get('first_name') or found.get('username') or search_term
                    target_id = found.get('user_id')
                else:
                    target_name = search_term
            else:
                target_name = search_term
        else:
            # Сон про автора сообщения
            target_user = message.from_user
            target_name = target_user.first_name or target_user.username or "Аноним"
            target_id = target_user.id
    
    if not target_name:
        await message.reply("Про кого сон-то? Напиши /сон Имя или реплайни на сообщение")
        return
    
    # Получаем профиль И память для персонализации
    gender = "unknown"
    traits = []
    user_memory = ""
    
    if USE_POSTGRES and target_id:
        try:
            profile = await get_user_profile_for_ai(target_id, chat_id, target_name, "")
            if profile:
                gender = profile.get('gender', 'unknown')
                traits = profile.get('traits', [])
            
            # Собираем память для более персонализированного сна
            user_memory = await gather_user_memory(chat_id, target_id, target_name)
        except Exception as e:
            logger.debug(f"Could not get profile/memory for dream: {e}")
    
    # Показываем что думаем
    processing_msg = await message.reply("💤 Вспоминаю что приснилось...")
    
    try:
        # Вызываем API
        # Формируем URL для dream API
        dream_url = DREAM_API_URL or get_api_url("dream")
        
        session = await get_http_session()
        async with session.post(
            dream_url,
            json={
                "name": target_name,
                "gender": gender,
                "traits": traits[:10],
                "memory": user_memory  # Передаём память для персонализации
            },
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                await processing_msg.edit_text(f"Бля, забыла что снилось...")
                return
            
            result = await response.json()
        
        dream_text = result.get("dream", "Ничего не помню, память отшибло...")
        
        # Удаляем сообщение о процессе и отправляем сон
        await processing_msg.delete()
        await message.reply(dream_text)
        
        logger.info(f"Dream generated for {target_name}")
        metrics.track_command("сон")
        
    except asyncio.TimeoutError:
        await processing_msg.edit_text("Бля, заснула пока вспоминала...")
    except Exception as e:
        logger.error(f"Dream error: {e}")
        metrics.track_error()
        await processing_msg.edit_text("Хуй там, ничего не помню...")


# ==================== ПОИСК КАРТИНОК (SerpAPI - Google Images) ====================

SERPAPI_KEY = os.getenv("SERPAPI_KEY", "")


async def search_images_serpapi(query: str, num_results: int = 20) -> list:
    """Поиск картинок через SerpAPI (Google Images)"""
    if not SERPAPI_KEY:
        logger.error("SERPAPI_KEY not set!")
        return []
    
    try:
        params = {
            "engine": "google_images",
            "q": query,
            "api_key": SERPAPI_KEY,
            "num": num_results,
            "safe": "off",
            "hl": "ru",
            "gl": "ru",
        }
        
        session = await get_http_session()
        async with session.get(
            "https://serpapi.com/search",
            params=params,
            timeout=aiohttp.ClientTimeout(total=20)
        ) as response:
            if response.status == 200:
                data = await response.json()
                return data.get("images_results", [])
            else:
                error = await response.text()
                logger.error(f"SerpAPI error: {response.status} - {error}")
                return []
    except Exception as e:
        logger.error(f"SerpAPI search error: {e}")
        return []


@router.message(Command("pic", "findpic", "photo_search", "картинка"))
async def cmd_find_pic(message: Message):
    """Найти и отправить картинку по запросу через Google Images"""
    # Получаем текст запроса
    text = message.text or message.caption or ""
    query = text.split(maxsplit=1)
    
    if len(query) < 2:
        await message.answer(
            "🔍 *Как искать картинки:*\n\n"
            "`/pic как какает птичка`\n"
            "`/pic котик в шапке`\n"
            "`/pic грустный кот на работе`\n\n"
            "Ищу через Google Images! 🖼️",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    search_query = query[1].strip()
    
    if len(search_query) < 2:
        await message.answer("❌ Запрос слишком короткий! Напиши хотя бы 2 символа.")
        return
    
    if not SERPAPI_KEY:
        await message.answer("❌ API ключ для поиска не настроен!")
        return
    
    # Кулдаун 5 секунд
    user_id = message.from_user.id
    chat_id = message.chat.id
    can_do, cooldown_remaining = check_cooldown(user_id, chat_id, "pic_search", 5)
    if not can_do:
        await message.answer(f"⏰ Подожди {cooldown_remaining} сек перед следующим поиском!")
        return
    
    # Показываем что ищем
    processing_msg = await message.answer(f"🔍 Ищу в Google: *{search_query}*...", parse_mode=ParseMode.MARKDOWN)
    
    try:
        # Ищем картинки через SerpAPI (больше результатов для выбора)
        results = await search_images_serpapi(search_query, 20)
        
        if not results:
            await processing_msg.edit_text(
                f"😔 Не нашёл картинок по запросу *{search_query}*\n"
                f"Попробуй другой запрос!",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Перемешиваем топ-10 результатов для разнообразия
        top_results = results[:10]
        random.shuffle(top_results)
        
        # Пробуем отправить картинку (перебираем результаты, если первая не загрузится)
        sent = False
        session = await get_http_session()
        download_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        for result in top_results:
            image_url = result.get('original') or result.get('thumbnail')
            if not image_url:
                continue
            
            try:
                # Скачиваем картинку
                async with session.get(
                    image_url, 
                    timeout=aiohttp.ClientTimeout(total=15),
                    headers=download_headers
                ) as response:
                    if response.status != 200:
                        continue
                    
                    content_type = response.headers.get('Content-Type', '')
                    if not content_type.startswith('image/'):
                        continue
                    
                    image_data = await response.read()
                    
                    # Проверяем размер (не больше 10 МБ)
                    if len(image_data) > 10 * 1024 * 1024:
                        continue
                    
                    # Минимальный размер (не меньше 5 КБ - иначе битая)
                    if len(image_data) < 5 * 1024:
                        continue
                    
                    # Определяем расширение
                    if 'png' in content_type:
                        ext = 'png'
                    elif 'gif' in content_type:
                        ext = 'gif'
                    elif 'webp' in content_type:
                        ext = 'webp'
                    else:
                        ext = 'jpg'
                    
                    # Отправляем без подписи
                    photo = BufferedInputFile(image_data, filename=f"image.{ext}")
                    
                    await processing_msg.delete()
                    await message.answer_photo(photo)
                    sent = True
                    break
            
            except Exception as e:
                logger.warning(f"Failed to download image {image_url}: {e}")
                continue
        
        if not sent:
            await processing_msg.edit_text(
                f"😔 Нашёл картинки, но не смог их загрузить.\n"
                f"Попробуй другой запрос!",
                parse_mode=ParseMode.MARKDOWN
            )
    
    except Exception as e:
        logger.error(f"Error in pic search: {e}")
        await processing_msg.edit_text("❌ Поиск сломался. Попробуй позже!")


@router.message(Command("svodka", "summary", "digest"))
async def cmd_svodka(message: Message):
    """Генерация сводки чата через AI с памятью"""
    if message.chat.type == "private":
        await message.answer("❌ Сводка работает только в групповых чатах!")
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    # Кулдаун 5 минут на сводку (чтобы не спамить API)
    can_do, cooldown_remaining = check_cooldown(user_id, chat_id, "svodka", 300)
    if not can_do:
        await message.answer(
            f"⏰ Сводку можно запрашивать раз в 5 минут.\n"
            f"Подожди ещё {cooldown_remaining} сек"
        )
        return
    
    # Отправляем "печатает..."
    await bot.send_chat_action(chat_id, "typing")
    
    # Получаем статистику
    stats = await get_chat_statistics(chat_id, hours=5)
    
    if stats['total_messages'] < 5:
        await message.answer(
            "📭 Слишком мало сообщений за последние 5 часов.\n"
            "Нужно хотя бы 5 сообщений для сводки!"
        )
        cooldowns.pop((user_id, chat_id, "svodka"), None)
        return
    
    # Получаем память (предыдущие сводки и воспоминания)
    previous_summaries = await get_previous_summaries(chat_id, limit=3)
    memories = await get_memories(chat_id, limit=20)
    
    # Получаем обогащённые данные профилей для персонализации (только PostgreSQL)
    user_profiles = []
    social_data = {}
    if USE_POSTGRES:
        try:
            enriched = await get_enriched_chat_data_for_ai(chat_id, hours=5)
            user_profiles = enriched.get('profiles', [])
            social_data = enriched.get('social', {})
        except Exception as e:
            logger.warning(f"Failed to get enriched data: {e}")
    
    # Отправляем запрос к Vercel API с памятью и профилями
    metrics.track_command("svodka")
    try:
        metrics.track_api_call("summary")
        session = await get_http_session()
        async with session.post(
                VERCEL_API_URL,
                json={
                    "statistics": stats,
                    "chat_title": message.chat.title or "Чат",
                    "hours": 5,
                    "previous_summaries": previous_summaries,
                    "memories": memories,
                    "user_profiles": user_profiles,
                    "social_data": social_data
                }
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    summary = result.get("summary", "Ошибка генерации сводки")
                    
                    # Сохраняем сводку в память
                    top_author = stats['top_authors'][0] if stats['top_authors'] else {}
                    drama_pairs_str = ", ".join([
                        f"{p.get('first_name', '?')} и {p.get('reply_to_first_name', '?')}"
                        for p in stats.get('reply_pairs', [])[:3]
                    ]) if stats.get('reply_pairs') else None
                    
                    await save_summary(
                        chat_id=chat_id,
                        summary_text=summary[:2000],  # Ограничиваем размер
                        top_talker_username=top_author.get('username'),
                        top_talker_name=top_author.get('first_name'),
                        top_talker_count=top_author.get('msg_count'),
                        drama_pairs=drama_pairs_str
                    )
                    
                    # Сохраняем воспоминания о топ-участниках
                    for author in stats['top_authors'][:5]:
                        if author.get('msg_count', 0) >= 10:
                            await save_memory(
                                chat_id=chat_id,
                                user_id=author.get('user_id', 0),
                                username=author.get('username'),
                                first_name=author.get('first_name'),
                                memory_type="activity",
                                memory_text=f"написал {author['msg_count']} сообщений за 5 часов",
                                relevance_score=min(author['msg_count'] // 10, 10)
                            )
                    
                    # Сохраняем воспоминания о парочках
                    for pair in stats.get('reply_pairs', [])[:3]:
                        if pair.get('replies', 0) >= 5:
                            await save_memory(
                                chat_id=chat_id,
                                user_id=pair.get('user_id', 0),
                                username=pair.get('username'),
                                first_name=pair.get('first_name'),
                                memory_type="relationship",
                                memory_text=f"активно общался с {pair.get('reply_to_first_name', '?')}",
                                relevance_score=min(pair['replies'], 10)
                            )
                    
                    # Разбиваем на части если слишком длинное (безопасно по границам слов)
                    parts = split_long_message(summary, max_length=4000)
                    for part in parts:
                        await message.answer(part)
                else:
                    error_text = await response.text()
                    logger.error(f"Vercel API error: {response.status} - {error_text}")
                    await message.answer(
                        "❌ Ошибка при генерации сводки.\n"
                        "Попробуй позже или проверь настройки API."
                    )
                    cooldowns.pop((user_id, chat_id, "svodka"), None)
    
    except asyncio.TimeoutError:
        await message.answer("⏰ Таймаут при генерации сводки. Попробуй позже.")
        cooldowns.pop((user_id, chat_id, "svodka"), None)
    except Exception as e:
        logger.error(f"Error generating summary: {e}")
        await message.answer("❌ Не удалось сгенерировать сводку. Попробуй позже!")
        cooldowns.pop((user_id, chat_id, "svodka"), None)


# ==================== СБОР СООБЩЕНИЙ ====================

# ==================== ЭТО КТО? ====================

WHO_IS_THIS_TRIGGERS = [
    "это кто", "кто это", "а это кто", "это кто такой", "это кто такая",
    "кто это такой", "кто это такая", "а кто это", "who is this", "who's this",
    "ху из зис", "кто эта", "кто этот", "это чё за", "это что за",
    "а это чё за", "кто такой", "кто такая", "шо за тип", "шо за типок"
]

WHO_IS_THIS_RESPONSES = [
    # Спермо-тема (20)
    "🔞 {name}? Это ёбаный спермобак. Желудок на 90% состоит из чужой кончи.",
    "🔞 {name}? Профессиональный кончеглот. За день принимает больше спермы, чем банк крови.",
    "🔞 {name} — мусорка для спермы. Сливают в {name_acc} всё — глотает и просит добавки.",
    "🔞 {name}? Ходячий контейнер для кончи. Литрами жрёт — и не толстеет.",
    "🔞 {name}? Спермоприёмник с ногами. Функция одна — глотать.",
    "🔞 {name}? Глотательный аппарат. Ни капли мимо — всё в желудок.",
    "🔞 {name} — биологический унитаз для спермы. Слил в {name_acc} — забыл.",
    "🔞 {name}? Кончежор. Питается исключительно спермой. Протеиновая диета.",
    "🔞 {name}? Сперма-вампир. Высасывает досуха и идёт за следующим.",
    "🔞 {name}? Человек-гандон. Только не выбрасывают, а сливают прямо в рот.",
    "🔞 {name} — живой презерватив. Собирает всё что кончают.",
    "🔞 {name}? Кончехранилище. Там столько спермы, можно детей делать оптом.",
    "🔞 {name}? Ёбаный спермоглот. Завтрак, обед, ужин — сперма.",
    "🔞 {name}? Профессиональный дояр. Только доит не коров, а мужиков.",
    "🔞 {name} — спермобаночка. Храним кончу {name_gen} до востребования.",
    "🔞 {name}? Живая сперма-банка. Принимает вклады 24/7.",
    "🔞 {name}? Это желудок для спермы с телом вокруг.",
    "🔞 {name}? Кончеглотатель со стажем. 10 лет опыта, ни одного рвотного позыва.",
    "🔞 {name} — спермоёмкость промышленная. Литраж неограничен.",
    "🔞 {name}? Ёбаный семяприёмник. Всё в себя, ничего мимо.",
    
    # Шлюхо-тема (20)
    "🔞 {name}? Это шлюха. Просто шлюха. Шлюха и всё.",
    "🔞 {name}? Блядища районного масштаба. {name} ебалась со всем подъездом.",
    "🔞 {name}? Подстилка общего пользования. Пользуйте {name_acc} бесплатно и круглосуточно.",
    "🔞 {name} — профессиональная блядь. Диплом с отличием.",
    "🔞 {name}? Шлюхоёбина. Родилась блядью, живёт блядью, сдохнет блядью.",
    "🔞 {name}? Вокзальная подстилка. В {name_acc} все были.",
    "🔞 {name}? Ебливая тварь. Ноги раздвигаются автоматически.",
    "🔞 {name}? Бесплатная проститутка. Даже денег не берёт — для удовольствия.",
    "🔞 {name} — блядина. Профессия — шлюха. Хобби — шлюха. Мечта — ещё больше шлюхи.",
    "🔞 {name}? Ёбаная давалка. Даёт {name} всем без разбора.",
    "🔞 {name}? Шлюха в кубе. Ебётся в три дырки одновременно.",
    "🔞 {name}? Местная блядь. Знает каждый — пользовался {name_acc} каждый.",
    "🔞 {name} — публичная женщина. Очень публичная. Слишком публичная.",
    "🔞 {name}? Шлюшара. Ебалась столько раз — сбилась со счёта на первой неделе.",
    "🔞 {name}? Выблядок-переросток. Мамка {name_gen} была шлюхой — яблоко от яблони.",
    "🔞 {name}? Блядь со стажем. 10 лет непрерывного стажа.",
    "🔞 {name} — профессиональная шлюха. Любительский разряд давно позади.",
    "🔞 {name}? Ебучая тварюга. Ебёт всё что движется. И не движется тоже.",
    "🔞 {name}? Это портовая шлюха. Моряки плачут от ностальгии по {name_dat}.",
    "🔞 {name}? Блядища уличная. Асфальт протёрла коленями.",
    
    # Ротовая тема (20)
    "🔞 {name}? Это ходячий минет. Рот {name_gen} создан для члена.",
    "🔞 {name}? Сосалка профессиональная. Сертификат висит на стене.",
    "🔞 {name}? Ротовая дырка с телом. Функция — сосать.",
    "🔞 {name} — хуесос-самоучка. Природный талант у {name_gen}.",
    "🔞 {name}? Отсосун со стажем. Рот не закрывается с детства.",
    "🔞 {name}? Живой глорихол. Дырка в стене сортира — вот что такое {name}.",
    "🔞 {name}? Профессиональный минетчик. Языком творит чудеса.",
    "🔞 {name}? Сосательный аппарат. Модель последняя, с глубоким горлом.",
    "🔞 {name}? Человек-минет. Ходит и сосёт — вот и вся жизнь {name_gen}.",
    "🔞 {name} — ротовая шлюха. Рот = пизда, только на лице.",
    "🔞 {name}? Это глоточник. Берёт до яиц, не морщится.",
    "🔞 {name}? Члено-сосательная машина. Безотказная.",
    "🔞 {name}? Губошлёп. Губами работает — головой нет.",
    "🔞 {name}? Минет на ножках. Ходит-бродит, ищет что пососать.",
    "🔞 {name}? Оральный террорист. Ни один хуй не ушёл от {name_gen} обиженным.",
    "🔞 {name} — пылесос для члена. Всасывает с характерным звуком.",
    "🔞 {name}? Ротожоп. Жопа и рот — одно и то же. Оба рабочие.",
    "🔞 {name} — сосун-универсал. Любой размер, любая форма.",
    "🔞 {name}? Язычник. В религиозном смысле — нет. В ротовом — да.",
    "🔞 {name}? Глоточная проститутка. Горло {name_gen} = рабочий инструмент.",
    
    # Анальная тема (20)
    "🔞 {name}? Анальная прорва. Жопа не закрывается.",
    "🔞 Кто это? Жопоблядь. Задницу подставляет автоматически.",
    "🔞 Это? Анальный экстремал. Туда влезет что угодно.",
    "🔞 {name} — жопошник. В жопу берёт чаще чем дышит.",
    "🔞 Кто? Анальная мразь. Там автобан — въезд свободный.",
    "🔞 Это? Жопа без тормозов. Открыта 24/7, выходных нет.",
    "🔞 {name}? Очкошник. Очко = главный вход.",
    "🔞 Кто это? Анальный энтузиаст. Жопой думает — жопой и работает.",
    "🔞 Это жопник со стажем. Туда побывало население небольшого города.",
    "🔞 {name}? Анальщик-профессионал. Там уже эхо слышно.",
    "🔞 Кто? Жопоёб в пассиве. Ебут — и нравится.",
    "🔞 Это? Задница на ножках. Всё остальное — приложение.",
    "🔞 {name} — дупло ходячее. Дупло = очко. Всё понятно.",
    "🔞 Кто это? Жопастый. Жопа — главный актив.",
    "🔞 {name}? Анальная фея. Волшебство в заднице.",
    "🔞 Это очкодав. Очко даёт — сдачи не просит.",
    "🔞 Кто? Жопная дырища. Там пропасть — не дно.",
    "🔞 {name}? Это срака рабочая. Работает — не жалуется.",
    "🔞 Это? Анальный аттракцион. Вход бесплатный, ощущения — бесценны.",
    "🔞 Кто это? Жоподырка. Дырка + жопа = судьба.",
    
    # Пиздолиз-тема (15)
    "🔞 {name}? Пиздолиз с дипломом. Вылизывает всё подряд.",
    "🔞 Кто это? Лизоблюд. Языком работает как собака.",
    "🔞 Это? Подлиза-профессионал. Лижет жопы начальству и не только.",
    "🔞 {name} — очколиз. Вылизывает очки до блеска.",
    "🔞 Кто? Языкастая тварь. Язык длинный — достаёт везде.",
    "🔞 Это лизун. Покемон такой был — вот это он.",
    "🔞 {name}? Пиздоед. Ест пизду на завтрак, обед и ужин.",
    "🔞 Кто это? Жополиз сертифицированный. Жопы блестят после него.",
    "🔞 Это? Языковый раб. Раб своего языка. Лижет всё.",
    "🔞 {name} — мастер куни. Клитор находит с закрытыми глазами.",
    "🔞 Кто? Лизатель. Лижет что дают. Дают — всё.",
    "🔞 {name}? Пиздолизательный аппарат. Автоматика полная.",
    "🔞 Это влагожёр. Жрёт влагу. Ну вы поняли какую.",
    "🔞 Кто это? Кунилингвист. Языковед, блять. В буквальном смысле.",
    "🔞 {name}? Лизунчик. Мимими снаружи — пиздоед внутри.",
    
    # Унижение (15)
    "🔞 {name}? Это мразь. Просто мразь. Больше добавить нечего.",
    "🔞 {name}? Ошибка природы. {name_acc} надо было проглотить — а выносили.",
    "🔞 {name}? Человеческий мусор. Место {name_gen} — на помойке.",
    "🔞 {name} — биоотход. Утилизации подлежит.",
    "🔞 {name}? Тварь ебучая. Тварь — потому что тварь.",
    "🔞 {name}? Параша. В {name_acc} ссут, срут и кончают.",
    "🔞 {name}? Унитаз с ногами. Говно принимает на постоянной основе.",
    "🔞 {name}? Выблядок. Папка {name_acc} не признал — и правильно сделал.",
    "🔞 {name}? Ёбаный урод. Ебали {name_acc} — и стал уродом.",
    "🔞 {name}? Отброс общества. Общество {name_acc} отбросило — не жалеет.",
    "🔞 {name}? Мразота. Мразь + срамота = мразота.",
    "🔞 {name}? Конченый. В обоих смыслах конченый.",
    "🔞 {name} — человек-говно. Состав: 100% говно.",
    "🔞 {name}? Пиздёныш. Из пизды вылез — в пизду и катится.",
    "🔞 {name}? Тупорылое мудило. Тупое, рылое, мудак.",
    
    # Смешанные (10+)
    "🔞 {name}? Это трёхдырочная радость. Все три — рабочие.",
    "🔞 Кто это? Секс-кукла с пульсом. Только дешевле.",
    "🔞 Это? Ебальник ходячий. Ебальник = весь организм.",
    "🔞 {name} — порноактёр без камеры. Снимать не надо — и так ебётся.",
    "🔞 Кто? Это ёбырь-любитель. Любит ёбыр. Сам ёбырь.",
    "🔞 {name}? Хуеглот-универсал. Любой хуй — в глотку.",
    "🔞 Это? Ебанат. Ебал — и стал ебанатом.",
    "🔞 Кто это? Сексуальный террорист. Ебёт мозг и не только.",
    "🔞 {name}? Это блядомразь. Блядь + мразь = идеальная комбинация.",
    "🔞 Кто? Хуеплёт. Плетёт хуйню — сосёт хуи.",
    "🔞 Это? Ёбанько. Ёбаный + Ванько = Ёбанько.",
    "🔞 {name} — пиздобол. Болтает пиздой. В смысле пиздит. В смысле врёт.",
    "🔞 Кто это? Блядун. Мужик, но блядует как баба.",
    "🔞 {name}? Хуила. Хуй + мудила = хуила.",
    "🔞 Это? Сракоёб. Ёбет сраки. Профессионально.",
    
    # ========== АМОРАЛЬНЫЕ И ИЗВРАЩЁННЫЕ (200+) ==========
    
    # Инцест (25)
    "🔞 {name}? Мамкоёб. Буквально ебал свою мамку.",
    "🔞 Кто это? Папкин любимчик. ОЧЕНЬ любимчик. Слишком.",
    "🔞 Это? Продукт инцеста. Дедушка постарался с бабушкой-дочкой.",
    "🔞 {name} — сестроёб. Сестра подтверждает. С ужасом.",
    "🔞 Кто? Семейный ебарь. Вся семья — его гарем.",
    "🔞 {name}? Ебал тётку на даче. Дядька смотрел.",
    "🔞 Это? Кузен-любовник. Кузина до сих пор в терапии.",
    "🔞 Кто это? Бабкоёб. Бабка сопротивлялась — но слабо.",
    "🔞 {name}? Внучок особенный. Дедушка знает почему.",
    "🔞 Это инцестуоз. Всю семью перетрахал — и гордится.",
    "🔞 Кто? Тот, кого папка учил любви. Личным примером.",
    "🔞 {name} — мамкина радость. В неправильном смысле.",
    "🔞 Это? Ебал родню. Всю. По кругу.",
    "🔞 {name}? Семейный позор. Ебёт кровных родственников.",
    "🔞 Кто это? Генеалогический пиздец. На нём ветка закончится.",
    "🔞 Это? Родственный ёбырь. Чужих не признаёт — только своих.",
    "🔞 {name}? Ебал мать — теперь ебёт сестру. Семейные ценности.",
    "🔞 Кто? Инцест-машина. Семейное древо = список ебли.",
    "🔞 {name} — плод запретной любви. Мама + брат мамы.",
    "🔞 Это? Отцоёб. Папу сделал пассивом.",
    "🔞 Кто это? Семьянин. В худшем смысле этого слова.",
    "🔞 {name}? Ебал всех до седьмого колена. Буквально.",
    "🔞 Это родовое проклятие. Ебёт род — и продолжает.",
    "🔞 Кто? Династический ёбырь. Королевская кровь ебёт королевскую.",
    "🔞 {name}? Фамильный позор. Фамилию лучше не называть.",
    
    # Зоофилия (25)
    "🔞 {name}? Собакоёб. Шарик в психушке.",
    "🔞 Кто это? Конелюб. Лошади прячутся при виде.",
    "🔞 Это? Овцеёб. Стадо в ужасе.",
    "🔞 {name} — котоёб. Мурка больше не мурчит.",
    "🔞 Кто? Свиноёб. Хрюшки визжат не от радости.",
    "🔞 {name}? Козоёб. Деревня знает. Деревня молчит.",
    "🔞 Это? Куроёб. Яйца теперь несут с травмой.",
    "🔞 Кто это? Зоофил со стажем. Весь зоопарк обошёл.",
    "🔞 {name}? Скотоложец. Скот в шоке.",
    "🔞 Это животнолюб. В очень неправильном смысле.",
    "🔞 Кто? Ферма его боится. Вся. Целиком.",
    "🔞 {name} — петухоёб. Кукареку звучит жалобно.",
    "🔞 Это? Ослоёб. Осёл теперь атеист.",
    "🔞 {name}? Ебал хомяка. Хомяк не выжил.",
    "🔞 Кто это? Рыбоёб. Аквариум — его бордель.",
    "🔞 Это? Голубеёб. Птички улетели навсегда.",
    "🔞 {name}? Крысоёб. Крысы эмигрировали.",
    "🔞 Кто? Кролиководом прикидывается. На деле — кролоёб.",
    "🔞 {name} — утколюб. Кря-кря стало криком о помощи.",
    "🔞 Это? Индюкоёб. Индюки объявили забастовку.",
    "🔞 Кто это? Лосеёб. В лесу не появляется — лоси караулят.",
    "🔞 {name}? Медведеёб. Медведь теперь веган.",
    "🔞 Это змееёб. Даже змеи охуели.",
    "🔞 Кто? Черепахоёб. Панцирь не спас.",
    "🔞 {name}? Любая тварь — его тварь. В сексуальном смысле.",
    
    # Некрофилия (20)
    "🔞 {name}? Трупоёб. На кладбище по ночам.",
    "🔞 Кто это? Некрофил. Мёртвые — его типаж.",
    "🔞 Это? Могильщик-романтик. Романтика с трупами.",
    "🔞 {name} — мертвечелюб. Холодные — самые горячие.",
    "🔞 Кто? Гробокопатель с целью. Цель — ебля.",
    "🔞 {name}? Труполюб со стажем. Формалин — его одеколон.",
    "🔞 Это? В морге работал. Уволили. За дело.",
    "🔞 Кто это? Кладбищенский ёбырь. Ни один покойник не ушёл девственником.",
    "🔞 {name}? Холодное тело — горячая ебля. Его девиз.",
    "🔞 Это некроромантик. Свечи, вино, труп.",
    "🔞 Кто? Мёртвых ебёт — живых боится.",
    "🔞 {name} — посмертный ёбарь. Ебёт после смерти.",
    "🔞 Это? Прозекторской романтик. Вскрытие = прелюдия.",
    "🔞 {name}? Труповёрт. Вертит трупы. И ебёт.",
    "🔞 Кто это? Формалиновый любовник. Химия + ебля.",
    "🔞 Это? Гробовая любовь. Ебёт в гробу.",
    "🔞 {name}? Склепоёб. Склеп = спальня.",
    "🔞 Кто? Катафалк — его такси в бордель. Бордель = морг.",
    "🔞 {name} — ритуальный ёбырь. Похороны + оргия.",
    "🔞 Это? Посмертная ебля — его хобби.",
    
    # Копро/Моча (25)
    "🔞 {name}? Говноед. Жрёт говно на завтрак.",
    "🔞 Кто это? Копрофил. Срать ему в рот — комплимент.",
    "🔞 Это? Дерьможуй. Жуёт дерьмо — и наслаждается.",
    "🔞 {name} — говножор. Жрёт говно тоннами.",
    "🔞 Кто? Унитаз живой. Ешь и срать ему.",
    "🔞 {name}? Какофил. Какашки — его страсть.",
    "🔞 Это? Фекалоед. Фекалии = деликатес.",
    "🔞 Кто это? Писсуар ходячий. Ссы — не промахнёшься.",
    "🔞 {name}? Золотой дождь — его стихия. Ссут ему — кайфует.",
    "🔞 Это моченюх. Нюхает мочу. Коллекционирует.",
    "🔞 Кто? Ссаный рот. Вечно открыт для струи.",
    "🔞 {name} — подссыкун. Ссать на него — ритуал.",
    "🔞 Это? Говнофетишист. Говно = возбуждение.",
    "🔞 {name}? Дристолюб. Понос — его радость.",
    "🔞 Кто это? Какашкоед. Ест какашки — и добавки просит.",
    "🔞 Это? Мочеглот. Глотает мочу литрами.",
    "🔞 {name}? Срачелюб. Срач — его среда обитания.",
    "🔞 Кто? Подгузникофил. Памперсы — его фетиш.",
    "🔞 {name} — блевотолюб. Рвота = афродизиак.",
    "🔞 Это? Рвотоглот. Глотает блевотину. Свою и чужую.",
    "🔞 Кто это? Сопли — тоже ест. Всё ест. Мерзость.",
    "🔞 {name}? Туалетный раб. Рабство в сортире.",
    "🔞 Это гнилофил. Гниль — его парфюм.",
    "🔞 Кто? Вонючколюб. Чем вонючее — тем возбуждённее.",
    "🔞 {name}? Помоечник. Помойка = ресторан.",
    
    # Психопатия/Маньячество (25)
    "🔞 {name}? Маньяк в зародыше. Пока только смотрит.",
    "🔞 Кто это? Психопат. Режет кошек для разминки.",
    "🔞 Это? Серийник будущий. Пока считает жертв.",
    "🔞 {name} — садист. Боль чужая = кайф свой.",
    "🔞 Кто? Живодёр. Животные — тренировка.",
    "🔞 {name}? Потрошитель начинающий. Куклы уже распотрошены.",
    "🔞 Это? На учёте у психиатра. И у полиции.",
    "🔞 Кто это? Шизоёб. Ебёт голоса в голове.",
    "🔞 {name}? Душитель-любитель. Скоро станет профи.",
    "🔞 Это расчленитель. Пока в мечтах.",
    "🔞 Кто? Кровофетишист. Кровь = возбуждение.",
    "🔞 {name} — ножелюб. Ножи + люди = мечта.",
    "🔞 Это? Подвальный житель. В подвале что-то прячет.",
    "🔞 {name}? Каннибал-теоретик. Пока только читает рецепты.",
    "🔞 Кто это? Людоед в душе. Душа голодная.",
    "🔞 Это? Пыточник. Пытки = хобби.",
    "🔞 {name}? Маньяк-коллекционер. Коллекция растёт.",
    "🔞 Кто? Трупохранитель. Где-то есть тайник.",
    "🔞 {name} — снаффер. Снафф смотрит — и завидует.",
    "🔞 Это? Киллер-мечтатель. Мечтает убивать.",
    "🔞 Кто это? Психоёб. Ебёт психику окружающим.",
    "🔞 {name}? Параноик агрессивный. Все враги — всех убить.",
    "🔞 Это социопат. Людей не считает за людей.",
    "🔞 Кто? Мучитель. Мучить — его призвание.",
    "🔞 {name}? Тёмная душа. Чернее ночи. Страшнее ада.",
    
    # Наркоманы/Алкаши (20)
    "🔞 {name}? Нарик конченый. Вены в труху.",
    "🔞 Кто это? Торчок. Торчит на всём подряд.",
    "🔞 Это? Ширяльщик. Шприц — лучший друг.",
    "🔞 {name} — крэкоед. Мозг давно сгорел.",
    "🔞 Кто? Алкаш подзаборный. Забор — его дом.",
    "🔞 {name}? Синяк. Синий каждый день.",
    "🔞 Это? Спидозник. СПИД от иглы.",
    "🔞 Кто это? Героинщик. Героин победил.",
    "🔞 {name}? Кокс-пылесос. Нос съеден.",
    "🔞 Это наркоша. Наркота — единственный друг.",
    "🔞 Кто? Солевой. Соль сожрала мозг.",
    "🔞 {name} — метадонщик. На программе — но не помогает.",
    "🔞 Это? Клей нюхает. С детства. Мозг как клей.",
    "🔞 {name}? Грибоед. Грибы съели разум.",
    "🔞 Кто это? Кислотник. Реальность — галлюцинация.",
    "🔞 Это? Фенибутчик. На фене. Всегда на фене.",
    "🔞 {name}? Барбитуратчик. Барбитураты = жизнь.",
    "🔞 Кто? Алко-нарко комбайн. Бухло + наркота.",
    "🔞 {name} — дно. Ниже дна. Под дном.",
    "🔞 Это? Обдолбыш. Обдолбан 24/7.",
    
    # Преступники (20)
    "🔞 {name}? Зек бывший. Или будущий.",
    "🔞 Кто это? Сиделец. Сидел — будет сидеть.",
    "🔞 Это? Вор. Ворует всё — включая трусы.",
    "🔞 {name} — рецидивист. Ходки считать устали.",
    "🔞 Кто? Бандюган. Бандитизм в крови.",
    "🔞 {name}? Барыга. Барыжит всем — от наркоты до органов.",
    "🔞 Это? Киданщик. Кидает всех. Всегда.",
    "🔞 Кто это? Мошенник. Развёл бы родную мать.",
    "🔞 {name}? Насильник. Был — или будет.",
    "🔞 Это грабитель. Грабит бабушек. Для удовольствия.",
    "🔞 Кто? Сутенёр. Блядей держит. Сам блядь.",
    "🔞 {name} — закладчик. Закладки — его бизнес.",
    "🔞 Это? Форточник. Лазит в форточки. И не только.",
    "🔞 {name}? Угонщик. Угонял — и будет угонять.",
    "🔞 Кто это? Домушник. Твой дом — его добыча.",
    "🔞 Это? Скупщик краденого. Всё краденое — к нему.",
    "🔞 {name}? Налётчик. Налёты — образ жизни.",
    "🔞 Кто? На зоне был петухом. И гордится.",
    "🔞 {name} — опущенный. Опустили — и правильно.",
    "🔞 Это? Стукач. Стучит на всех. Даже на себя.",
    
    # Извращения разные (20)
    "🔞 {name}? Фут-фетишист. Ноги лижет — платит за это.",
    "🔞 Кто это? Подглядыватель. В женском туалете живёт.",
    "🔞 Это? Эксгибиционист. Хуй показывает всем.",
    "🔞 {name} — вуайерист. Подсматривает за всеми.",
    "🔞 Кто? Трансвестит. Мамкины трусы носит.",
    "🔞 {name}? Фроттерист. В метро трётся о бабушек.",
    "🔞 Это? Клизмофил. Клизмы — его страсть.",
    "🔞 Кто это? Дендрофил. Ебёт деревья. Берёзки страдают.",
    "🔞 {name}? Плюшелюб. Плюшевые игрушки — его любовники.",
    "🔞 Это формикофил. Муравьи ползают — он кончает.",
    "🔞 Кто? Акротомофил. Инвалиды — его фетиш.",
    "🔞 {name} — геронтофил. Старухи — его мечта.",
    "🔞 Это? Пигофил. Жопы обожает. Любые.",
    "🔞 {name}? Мизофил. Грязь возбуждает.",
    "🔞 Кто это? Трихофил. Волосы везде — кайф.",
    "🔞 Это? Лактофил. Молоко сосёт. Из сисек.",
    "🔞 {name}? Инфантофил. Одевается младенцем. Ебётся.",
    "🔞 Кто? Автоэротофил. В машинах кончает. На машины.",
    "🔞 {name} — механофил. Ебёт механизмы. Пылесос пострадал.",
    "🔞 Это? Агальматофил. Статуи ебёт. Музеи закрыты для него.",
    
    # Социальное дно (20)
    "🔞 {name}? Бомж. Вонючий бомж.",
    "🔞 Кто это? Бич. Бичует с 90-х.",
    "🔞 Это? Попрошайка. Попрошайничает и ворует.",
    "🔞 {name} — тунеядец. Тунеядит профессионально.",
    "🔞 Кто? Дармоед. Жрёт дармовщинку.",
    "🔞 {name}? Альфонс. На бабьи деньги живёт.",
    "🔞 Это? Жиголо для бабушек. Бабушки богатые.",
    "🔞 Кто это? Содержанка. Содержат — пользуют.",
    "🔞 {name}? Халявщик. Халява — религия.",
    "🔞 Это паразит. Паразитирует на всех.",
    "🔞 Кто? Нахлебник. Жрёт чужое.",
    "🔞 {name} — иждивенец. Иждивение — образ жизни.",
    "🔞 Это? Обрыган. Рыгает — и этим живёт.",
    "🔞 {name}? Оборванец. В оборванцах с рождения.",
    "🔞 Кто это? Голодранец. Голый и драный.",
    "🔞 Это? Нищеброд. Нищий + ебрóдит.",
    "🔞 {name}? Лох. Лохнутый лох.",
    "🔞 Кто? Терпила. Терпит всё от всех.",
    "🔞 {name} — чмо. Человек Морально Обосранный.",
    "🔞 Это? Чушпан. Чушка + пацан.",
    
    # Физические оскорбления (20)
    "🔞 {name}? Урод. Природа постаралась.",
    "🔞 Кто это? Страхоёбина. Страшный как атомная война.",
    "🔞 Это? Мордоворот. Морда воротит.",
    "🔞 {name} — рожа кирпичом. Кирпич бы постеснялся.",
    "🔞 Кто? Ёбаный стыд. Стыдно за него.",
    "🔞 {name}? Квазимода местный. Только хуже.",
    "🔞 Это? Чучело. Чучело огородное — и то красивее.",
    "🔞 Кто это? Жиробас. Жира больше чем мозга.",
    "🔞 {name}? Дрыщ. Дрыщавый дрыщ.",
    "🔞 Это задрот. Задротил до посинения.",
    "🔞 Кто? Ботан. Ботанил — не помогло.",
    "🔞 {name} — очкарик. Очки не помогают видеть суть.",
    "🔞 Это? Лысый. Лысина от мыслей — их нет.",
    "🔞 {name}? Прыщавый. Прыщи умнее него.",
    "🔞 Кто это? Вонючка. Воняет издалека.",
    "🔞 Это? Потный. Потеет от существования.",
    "🔞 {name}? Слюнявый. Слюни текут.",
    "🔞 Кто? Сопливый. Сопли вечные.",
    "🔞 {name} — кривой. Кривой во всех смыслах.",
    "🔞 Это? Косой. Косит на оба глаза.",
    
    # Умственные оскорбления (25)
    "🔞 {name}? Дебил. Клинический дебил.",
    "🔞 Кто это? Даун. Синдром — весь он.",
    "🔞 Это? Имбецил. Имбецильность зашкаливает.",
    "🔞 {name} — олигофрен. Олигофрения прогрессирует.",
    "🔞 Кто? Кретин. Кретинизм в последней стадии.",
    "🔞 {name}? Дурак. Дурак дураком.",
    "🔞 Это? Тупица. Тупость бесконечна.",
    "🔞 Кто это? Идиот. Идиотизм — его суперсила.",
    "🔞 {name}? Баран. Бараны умнее.",
    "🔞 Это болван. Болванка для мозга — пустая.",
    "🔞 Кто? Остолоп. Столб — и тот сообразительнее.",
    "🔞 {name} — недоумок. Недо- во всём.",
    "🔞 Это? Тормоз. Тормозит вечно.",
    "🔞 {name}? Овощ. Овощная жизнь.",
    "🔞 Кто это? Растение. Фотосинтез — его максимум.",
    "🔞 Это? Пень. Пень пнём.",
    "🔞 {name}? Бревно. С глазами. Но без мозга.",
    "🔞 Кто? Дуб. Дубовый дуб.",
    "🔞 {name} — тупорылый. Тупое рыло.",
    "🔞 Это? Безмозглый. Мозг отсутствует.",
    "🔞 {name}? Скудоумный. Ум скуден.",
    "🔞 Кто это? Слабоумный. Ум слабый. Очень.",
    "🔞 Это? Придурок. Придурковатость врождённая.",
    "🔞 {name}? Долбоёб. Долбит и ебётся. Точнее — долбят его.",
    "🔞 Кто? Мудозвон. Звонит мудями. Постоянно.",
]


def detect_gender_simple(name: str) -> str:
    """Простое определение пола по имени для 'это кто?'"""
    name_lower = name.lower().strip()
    
    # Женские имена
    female_names = [
        'анна', 'аня', 'мария', 'маша', 'екатерина', 'катя', 'ольга', 'оля',
        'наталья', 'наташа', 'елена', 'лена', 'татьяна', 'таня', 'ирина', 'ира',
        'светлана', 'света', 'юлия', 'юля', 'анастасия', 'настя', 'дарья', 'даша',
        'полина', 'алина', 'виктория', 'вика', 'кристина', 'александра', 'саша',
        'софья', 'софия', 'алёна', 'алена', 'ксения', 'ксюша', 'вероника', 'марина',
        'валерия', 'лера', 'диана', 'карина', 'арина', 'милана', 'ева', 'яна',
        'регина', 'ангелина', 'валентина', 'людмила', 'люда', 'надежда', 'надя',
        'галина', 'галя', 'лилия', 'лиля', 'жанна', 'инна', 'эльвира', 'элина'
    ]
    
    if name_lower in female_names:
        return "женский"
    
    # По окончанию имени
    if name_lower.endswith(('а', 'я')) and not name_lower.endswith(('ья', 'ия')):
        if name_lower not in ['никита', 'илья', 'саша', 'дима', 'лёша', 'миша', 'коля', 'вася', 'петя', 'ваня', 'гоша', 'паша', 'лёня', 'толя', 'федя', 'сеня', 'костя', 'стёпа']:
            return "женский"
    
    return "мужской"


async def _save_text_message(message: Message):
    """Вспомогательная функция для сохранения текстового сообщения в БД"""
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    # Сохраняем информацию о чате
    await save_chat_info(
        chat_id=chat_id,
        title=message.chat.title,
        username=message.chat.username,
        chat_type=message.chat.type
    )
    
    # Сохраняем сообщение
    reply_to_user_id = None
    reply_to_first_name = None
    reply_to_username = None
    
    if message.reply_to_message and message.reply_to_message.from_user:
        reply_to_user_id = message.reply_to_message.from_user.id
        reply_to_first_name = message.reply_to_message.from_user.first_name
        reply_to_username = message.reply_to_message.from_user.username
    
    await save_chat_message(
        chat_id=chat_id,
        user_id=user_id,
        username=message.from_user.username or "",
        first_name=message.from_user.first_name or "Аноним",
        message_text=message.text[:500] if message.text else "",
        message_type="text",
        reply_to_user_id=reply_to_user_id,
        reply_to_first_name=reply_to_first_name,
        reply_to_username=reply_to_username
    )
    
    # Обновляем профиль пользователя (v2 - с расширенными данными)
    if USE_POSTGRES and message.text:
        try:
            await update_user_profile_comprehensive(
                user_id=user_id,
                chat_id=chat_id,
                message_text=message.text,
                timestamp=int(time.time()),
                first_name=message.from_user.first_name or "",
                username=message.from_user.username or "",
                reply_to_user_id=reply_to_user_id,
                message_type="text"
            )
        except Exception as e:
            logger.warning(f"Profile update error (text): {e}", exc_info=True)
    
    # Пассивный опыт для игроков
    player = await get_player(user_id, chat_id)
    if player and player.get('player_class'):
        can_get_exp, _ = check_cooldown(user_id, chat_id, "message_exp", 30)
        if can_get_exp:
            exp_gain = random.randint(1, 3)
            money_gain = random.randint(0, 2)
            await update_player_stats(user_id, chat_id, experience=f"+{exp_gain}", money=f"+{money_gain}")


# Счётчики сообщений по чатам для периодических комментариев
_chat_msg_counter: Dict[int, int] = {}
_chat_next_threshold: Dict[int, int] = {}

PERIODIC_COMMENTS = [
    "ну и дебилы блять, я не могу ахах",
    "вы вообще нормальные? риторический вопрос",
    "читаю вас и тупею на глазах",
    "боже, за что мне это",
    "это что сейчас было вообще",
    "я с вами с ума сойду",
    "стоп, что. ЧТО?",
    "нет ну серьёзно, вы это всерьёз?",
    "ахах блять. нет. просто нет",
    "я молчу. просто молчу и страдаю",
    "окей. окееей. понял. всё понял про вас",
    "чат деградирует с каждым сообщением и это факт",
    "я устал. вы меня утомили",
    "ну и компания блять",
    "за что мне такой чат господи",
    "вы специально или это само получается?",
    "читаю и не верю глазам своим",
    "чат 10 из 10, рекомендую всем врагам",
    "я б помолчал но вы сами напросились",
    "это норма для вас? это НОРМА?",
    "ладно я понял, продолжайте позориться",
    "смотрю на вас и думаю — а зачем",
    "уровень iq чата падает, я это чувствую физически",
    "вы когда-нибудь говорите что-то умное? просто интересно",
    "блять, ну вы даёте",
    "ору с вас тихо",
    "нет слов. одни эмоции. плохие",
    "продолжайте-продолжайте, я запомню это",
    "я не злой. просто вы странные",
    "у меня нет слов но есть разочарование",
    "пиздец тихий творится тут",
    "мне кажется или становится хуже? нет, становится",
    "жалею что умею читать",
    "вы лучшее что со мной не случалось",
    "это клиника или чат? не пойму никак",
]


async def maybe_periodic_comment(message: Message):
    """Комментарий каждые 20-30 сообщений в чате"""
    chat_id = message.chat.id

    _chat_msg_counter[chat_id] = _chat_msg_counter.get(chat_id, 0) + 1
    current = _chat_msg_counter[chat_id]

    if chat_id not in _chat_next_threshold:
        _chat_next_threshold[chat_id] = random.randint(20, 30)

    threshold = _chat_next_threshold[chat_id]

    if current >= threshold:
        _chat_msg_counter[chat_id] = 0
        _chat_next_threshold[chat_id] = random.randint(20, 30)

        text = random.choice(PERIODIC_COMMENTS)
        try:
            await message.answer(text)
        except Exception as e:
            logger.warning(f"Periodic comment failed: {e}")


async def maybe_random_comment(message: Message) -> bool:
    """
    Случайный умный комментарий бота на интересное сообщение.
    Срабатывает редко (2-5%) на длинные или эмоциональные сообщения.
    """
    text = message.text or ""
    chat_id = message.chat.id
    
    # Не отвечаем на короткие сообщения
    if len(text) < 50:
        return False
    
    # Проверяем кулдаун (не чаще раза в 5 минут на чат)
    cooldown_key = f"random_comment_{chat_id}"
    can_do, _ = check_cooldown(0, chat_id, cooldown_key, 300)
    if not can_do:
        return False
    
    # Определяем "интересность" сообщения
    interest_score = 0
    text_lower = text.lower()
    
    # Длина добавляет интерес
    if len(text) > 100:
        interest_score += 1
    if len(text) > 200:
        interest_score += 1
    
    # Вопросительные предложения интересны
    if "?" in text:
        interest_score += 1
    
    # Эмоциональные маркеры
    if any(w in text_lower for w in ['охуеть', 'пиздец', 'блять', 'ахаха', 'ору', 'жесть', 'капец', 'нихуя себе']):
        interest_score += 2
    
    # Личные истории интересны
    if any(w in text_lower for w in ['сегодня', 'вчера', 'случилось', 'представьте', 'короче', 'история']):
        interest_score += 1
    
    # Базовый шанс 1%, увеличивается с interest_score
    # interest_score 0 = 1%, 1 = 2%, 2 = 3%, 3 = 4%, 4+ = 5%
    chance = min(0.01 + interest_score * 0.01, 0.05)
    
    if random.random() > chance:
        return False
    
    # Генерируем умный комментарий
    try:
        response = await generate_smart_reply(message)
        if response and len(response) > 5:
            await message.reply(response)
            logger.info(f"RANDOM COMMENT in chat {chat_id}: {response[:50]}...")
            return True
    except Exception as e:
        logger.debug(f"Random comment failed: {e}")
    
    return False


@router.message(F.text, ~F.text.startswith("/"))
async def who_is_this_handler(message: Message):
    """Обработчик 'это кто?' с реплаем или упоминанием + общая обработка текста"""
    if message.chat.type == "private":
        return
    
    # === ОБЩАЯ ОБРАБОТКА ВСЕХ ТЕКСТОВЫХ СООБЩЕНИЙ ===
    # Проверяем на кринж (до проверки триггеров!)
    await check_cringe_and_react(message)
    
    # Проверяем упоминание бота
    await check_bot_mention(message)
    
    # Случайный умный комментарий на интересные сообщения (1-5% шанс)
    await maybe_random_comment(message)

    # Периодический комментарий каждые 20-30 сообщений
    await maybe_periodic_comment(message)
    
    # Сохраняем сообщение в БД (делаем это здесь, т.к. этот хэндлер ловит все текстовые)
    await _save_text_message(message)
    
    # УМНАЯ ПАМЯТЬ: Фоновое извлечение фактов из информативных сообщений
    # Не блокируем — запускаем асинхронно
    if USE_POSTGRES and message.text and len(message.text) > 20:
        asyncio.create_task(extract_and_save_facts(message))
    
    text_lower = message.text.lower().strip()

    # === ДАЙ ТАБЛЕТКУ ===
    if message.reply_to_message and any(trigger in text_lower for trigger in PILL_TRIGGERS):
        target_user = message.reply_to_message.from_user
        if target_user:
            target_name = target_user.first_name or target_user.username or "Пациент"
            target_username = target_user.username or ""
            clickable = f'<a href="tg://user?id={target_user.id}">{target_name}</a>'

            pill_url = get_api_url("pill")
            if not pill_url:
                await message.reply(f"💊 {clickable}, выпей таблетку от долбоёбизма. Доза — максимальная.", parse_mode=ParseMode.HTML)
                return

            processing = await message.reply("🩺 Доктор Роза изучает историю болезни...", parse_mode=ParseMode.HTML)

            # Собираем последние сообщения цели для персонализации
            context = ""
            if USE_POSTGRES:
                try:
                    user_msgs = await get_user_messages(message.chat.id, target_user.id, limit=20)
                    if user_msgs:
                        context = "\n".join([m.get("message_text", "") for m in reversed(user_msgs) if m.get("message_text")])[:1500]
                except Exception:
                    pass

            try:
                session = await get_http_session()
                async with session.post(
                    pill_url,
                    json={"name": target_name, "username": target_username, "context": context},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        text = result.get("text", "Таблетка закончилась.")
                        await processing.edit_text(f"💊 Рецепт для {clickable}:\n\n{text}", parse_mode=ParseMode.HTML)
                    else:
                        await processing.edit_text(f"💊 {clickable}, прими что-нибудь от тупости. Дозировка — на глаз.")
            except Exception as e:
                logger.warning(f"PILL API error: {e}")
                await processing.edit_text(f"💊 {clickable}, доктор пьян. Пей аспирин и не мешай.", parse_mode=ParseMode.HTML)
        return

    # === ПЁРДНУТЬ ===
    if message.reply_to_message and any(trigger in text_lower for trigger in FART_TRIGGERS):
        target_user = message.reply_to_message.from_user
        if target_user:
            target_name = target_user.first_name or target_user.username or "этого"
            clickable = f'<a href="tg://user?id={target_user.id}">{target_name}</a>'
            fart_response = random.choice(FART_RESPONSES)
            fart_response = fart_response.format(
                name=clickable, name_gen=clickable,
                name_dat=clickable, name_acc=clickable,
            )
            await message.reply(fart_response, parse_mode=ParseMode.HTML)
            return

    # === СПЕЦИФИЧНАЯ ЛОГИКА "ЭТО КТО?" ===
    # Проверяем триггер
    is_trigger = any(trigger in text_lower for trigger in WHO_IS_THIS_TRIGGERS)
    if not is_trigger:
        return
    
    # Ищем цель: реплай или упоминание
    target_name = None
    target_id = None
    target_username = None
    
    # Приоритет 1: реплай на сообщение
    if message.reply_to_message and message.reply_to_message.from_user:
        target_user = message.reply_to_message.from_user
        target_name = target_user.first_name or target_user.username or "Этот"
        target_id = target_user.id
        target_username = target_user.username
    
    # Приоритет 2: упоминание в сообщении
    elif message.entities:
        for entity in message.entities:
            if entity.type == "mention":
                # @username
                mentioned = message.text[entity.offset:entity.offset + entity.length]
                target_name = mentioned.lstrip("@")
                target_username = target_name
                break
            elif entity.type == "text_mention" and entity.user:
                # Упоминание без юзернейма
                target_name = entity.user.first_name or "Этот"
                target_id = entity.user.id
                target_username = entity.user.username
                break
    
    if not target_name:
        return  # Нет цели — не отвечаем
    
    # Получаем профиль пользователя для персонализации
    target_profile = {}
    gender = "мужской"  # default
    
    if USE_POSTGRES and target_id:
        try:
            # Получаем полный профиль (per-chat!)
            target_profile = await get_user_profile_for_ai(target_id, message.chat.id, target_name, target_username or "")
            
            # Пол из профиля
            if target_profile.get('gender') and target_profile['gender'] != 'unknown':
                gender = target_profile['gender']
            else:
                # Fallback на анализ
                db_gender = await get_user_gender(target_id, message.chat.id)
                if db_gender and db_gender != 'unknown':
                    gender = db_gender
                else:
                    result = await analyze_and_update_user_gender(
                        target_id, message.chat.id, target_name, target_username or ""
                    )
                    if result['gender'] != 'unknown':
                        gender = result['gender']
                    else:
                        gender = detect_gender_simple(target_name)
        except Exception as e:
            logger.debug(f"Profile/gender detection error: {e}")
            gender = detect_gender_simple(target_name)
    else:
        gender = detect_gender_simple(target_name)
    
    declined = decline_russian_name(target_name, gender)
    
    # Создаём кликабельную ссылку
    if target_id:
        clickable_name = make_user_mention(target_id, target_name, target_username)
        clickable_gen = make_user_mention(target_id, declined['gen'], target_username)
        clickable_acc = make_user_mention(target_id, declined['acc'], target_username)
        clickable_dat = make_user_mention(target_id, declined['dat'], target_username)
    else:
        # Если нет ID — просто имя без ссылки
        clickable_name = target_name
        clickable_gen = declined['gen']
        clickable_acc = declined['acc']
        clickable_dat = declined['dat']
    
    # Формируем персонализированную добавку на основе профиля
    profile_addition = ""
    if target_profile:
        additions = []
        
        # По активности
        activity = target_profile.get('activity_level', '')
        if activity == 'hyperactive':
            additions.append("(Графоман, кстати — весь чат засрал своими высерами.)")
        elif activity == 'lurker':
            additions.append("(Тихушник — сидит молчит, но всё читает. Извращенец.)")
        
        # По стилю
        style = target_profile.get('communication_style', '')
        if style == 'toxic':
            additions.append("(Токсичная тварь, между прочим — отравляет всё вокруг.)")
        elif style == 'humorous':
            additions.append("(Думает что смешной. Спойлер: нет.)")
        
        # По режиму
        if target_profile.get('is_night_owl'):
            additions.append("(Ночная тварь — бодрствует когда нормальные спят.)")
        
        # По интересам
        interests = target_profile.get('interests', [])
        interest_insults = {
            'gaming': "(Задрот-геймер. Просиживает жизнь в играх.)",
            'crypto': "(Криптодебил. Всё ещё верит в биткоин.)",
            'politics': "(Политолох. Вечно ноет про власть.)",
            'memes': "(Мемоед. Жрёт мемы вместо еды.)"
        }
        for interest in interests[:1]:  # Только одно
            if interest in interest_insults:
                additions.append(interest_insults[interest])
                break
        
        if additions:
            profile_addition = "\n\n" + random.choice(additions)
    
    # Выбираем рандомный ответ и подставляем склонения
    response = random.choice(WHO_IS_THIS_RESPONSES)
    response = response.format(
        name=clickable_name,
        name_gen=clickable_gen,
        name_acc=clickable_acc,
        name_dat=clickable_dat
    )
    
    # Добавляем персонализированную добавку с шансом 40%
    if profile_addition and random.random() < 0.4:
        response += profile_addition
    
    await message.reply(response, parse_mode=ParseMode.HTML)


# ==================== ДАЙ ТАБЛЕТКУ ====================

PILL_TRIGGERS = [
    "дай таблетку", "таблетку", "выпей таблетку", "прими таблетку",
    "таблетка", "пропиши таблетку", "назначь таблетку", "дайте таблетку",
]

# ==================== ПЁРДНУТЬ ====================

FART_TRIGGERS = [
    "пёрднуть", "пёрднуть на", "пёрдни на", "пёрдни ему", "пёрдни ей",
    "пернуть", "пёрнуть", "пёрни", "пёрни на", "пёрни ему", "пёрни ей",
    "пукнуть", "пукни", "пукни на", "пукни ему", "пукни ей",
    "пёрд на", "пёрдни", "пукнуть на",
]

FART_RESPONSES = [
    "💨 *влажный булькающий пёрд прямо в ебало {name_dat}*\n\nЗвук — как кто-то наступил на мокрую лягушку. {name} открыл рот чтобы что-то сказать и тут же получил полный рот горячего кишечного газа. Теперь {name} знает каким был вчерашний ужин.",

    "💨 *долгий, скрипучий, с мокрыми переливами — прямо на {name_acc}*\n\nДлился 6 секунд. Первые 3 — {name} ещё надеялся что это скрипит стул. Следующие 3 — тёплая влажная волна уже облепила лицо. Пиздец пришёл тихо и неспешно.",

    "💨 *серия коротких влажных хлопков — очередью в {name_acc}*\n\nПять штук подряд, каждый с брызгами. {name} получил полную обойму говённого тумана в рожу. На пятом хлопке что-то твёрдое вылетело отдельно — откатилось к ноге {name_gen}.",

    "💨 *один короткий но ЧУДОВИЩНО громкий — в упор на {name_acc}*\n\nКАК ПУШЕЧНЫЙ ВЫСТРЕЛ, блять. {name} дёрнулся, чуть не упал. Запах ударил через секунду — концентрированный, тёплый, пахнущий позавчерашней едой и кишечным отчаянием.",

    "💨 *бесшумное, почти нежное — прямо под нос {name_dat}*\n\nТихий убийца. {name} даже не понял сразу — просто глаза начали гореть и слезиться. Потом ноздри уловили тухлятину. Потом мозг отключился от брезгливости. Три стадии за пять секунд.",

    "💨 *мокрое, хлюпающее — на {name_acc}*\n\nВлажное. Очень влажное, сука. Тёплые брызги долетели до щеки {name_gen}. {name} потрогал лицо — рука пришла влажная. Это была не вода. Это была жопная роса.",

    "💨 *нарастающее, как сирена воздушной тревоги — в ебальник {name_dat}*\n\nНачалось как невинный писк, закончилось как сигнал ядерной тревоги. {name} за это время прошёл все стадии горя. На последней просто стоял и нюхал. Смирился с судьбой.",

    "💨 *тихо пустил облако и деловито отошёл — {name} в эпицентре*\n\nЗапах — как будто в прямой кишке сварили тухлые яйца с гнилой капустой и дохлой мышью. {name} попробовал дышать ртом. Почувствовал вкус. Это было хуже.",

    "💨 *неторопливо, с достоинством — облако накрыло {name_acc}*\n\nЗапах расплылся как ядерный гриб. Первое кольцо вошло в нос {name_gen}, второе осело на одежде, третье въелось в волосы навсегда. Духи уже не помогут — это теперь запах {name_gen}.",

    "💨 *прицельно в морду {name_dat}*\n\nПахнет серой, тухлым яйцом, прокисшим молоком и ещё чем-то неопознанным но очень тревожным. {name} попытался описать запах вслух — не смог. Человеческий язык для такой мерзости не предназначен.",

    "💨 *без предупреждения — горячее облако накрыло {name_acc} с головой*\n\nЗапах осел на волосах, коже, одежде и прямо на сетчатке глаза, блять. {name} понял — это не выветрится. Ни душ, ни духи, ни время. Это теперь его запах.",

    "💨 *деловито, в упор — {name} в зоне поражения*\n\nТёплое и густое как суп. {name} почувствовал как горячая вонючая волна прилипает к лицу. Как поцелуй. Только из жопы. Только пахнет разложением.",

    "💨 *натужно, с кряхтением и красным лицом — на {name_acc}*\n\n{name} слышал весь процесс: сначала натуживание, потом скрип, потом хлюп. Выло в три этапа с паузами. {name} каждый раз думал что всё — и каждый раз ошибался. Предательство.",

    "💨 *что-то твёрдое вылетело бонусом — прямо на {name_acc}*\n\nНачиналось как обычный пёрд. В середине что-то пошло не так. Небольшой коричневый кусочек приземлился на ботинок {name_gen}. {name} смотрел на него секунд семь не двигаясь.",

    "💨 *вибрирующее, с дребезжанием жопных складок — в сторону {name_gen}*\n\nЯгодицы тряслись как динамик на полной мощности. {name} почувствовал тёплый воздух на лице раньше чем понял что это такое. Потом понял. Потом пришёл запах. Всё три — лишние.",

    "💨 *резкое как выстрел — {name} получил в упор*\n\nЗадница сработала как реактивный двигатель. {name} ощутил давление тёплого воздуха на лице. Потом запах. Потом заметил влажное пятно на щеке и понял — это был не просто воздух.",

    "💨 *прямо в открытый рот {name_dat} — нечаянно, честно*\n\n{name} как раз что-то говорил. Набрал воздуха на вдохе. Получил полный рот горячего кишечного газа с привкусом. Замолчал. Долго стоял с закрытым ртом и не знал куда это деть теперь.",

    "💨 *мощное как выхлопная труба КамАЗа — в харю {name_dat}*\n\nПо силе — фен на максимуме. По запаху — мусоропровод в августе плюс общественный туалет без вентиляции. {name} получил всё это в лицо одновременно. Глаза слезятся до сих пор.",

    "💨 *долгое, протяжное, как реквием — {name_dat} посвящается*\n\nДлилось столько что {name} успел написать сообщение и убрать телефон. Когда поднял глаза — облако уже нависло над головой. Пахло говном, безысходностью и чьими-то непереваренными проблемами.",

    "💨 *компактное но ядрёное — прямо в нос {name_dat}*\n\nМаленькое. Почти незаметное. Но такое концентрированное что у {name_gen} моментально потекли слёзы, пропал аппетит и желание общаться с людьми. Объём не важен — важна концентрация говна.",

    "💨 *горячее влажное облако — облепило {name_acc} с головой*\n\nТемпература — как из духовки. Влажность — 100%. Состав — лучше не знать. {name} почувствовал как тёплая мерзость оседает на коже, пропитывает одежду, лезет в уши. Душ не поможет.",

    "💨 *пёрднул и спокойно ушёл — {name} остался один с этим*\n\nКлассика жанра, сука. {name} обнаружил облако самостоятельно через две секунды. Оглянулся — никого. Только запах тухлятины и философские вопросы о смысле существования.",

    "💨 *с явными кишечными звуками ДО — прямо на {name_acc}*\n\nСначала было слышно как внутри что-то перекатывается и булькает. Бурление. Клокотание. {name} всё слышал, всё понял, но убежать не успел. Горячий пёрд накрыл лицо как приговор.",

    "💨 *мокрый, с брызгами — {name} в первом ряду*\n\nТёплые капли долетели до щеки {name_gen}. {name} машинально потрогал — влажно. Потом нюхнул руку. Потом долго стоял с лицом человека который хочет развидеть и разнюхать произошедшее.",

    "💨 *затяжное с предательской паузой посередине — не отходя от {name_gen}*\n\nВышла первая половина — {name} решил что всё кончилось, уже почти улыбнулся. И тут вторая. Влажная. Громкая. Горячая. Прямо в лицо. Предательство хуже самого пёрда.",

    "💨 *под одеялом копилось три часа, потом выпустил на {name_acc}*\n\nКопилось. Зрело. Концентрировалось, блять. {name} получил недельный запас кишечных газов в лицо. Одеяло теперь биологически опасный объект — его надо сжигать, а пепел захоранивать.",

    "💨 *с явным удовольствием и стоном — в харю {name_dat}*\n\nВышло смачно и со звуком. {name} стоял и наблюдал как источник кайфует от процесса, слегка постанывает. Это было оскорбительнее самого пёрда. Запах пришёл чуть позже как добивающий удар.",

    "💨 *залп из трёх штук без пауз — {name} получил всё сразу*\n\nПервый — шок. Второй — {name} ещё пытался отойти. Третий — самый мокрый и горячий — настиг в спину. Три слоя запаха наслоились друг на друга. Симфония говна в трёх частях.",

    "💨 *бесшумное и тёплое — осело на {name_acc} невидимым слоем*\n\nНикакого звука. Никаких предупреждений. Просто через секунду {name} понял что что-то не так — слишком тепло вокруг лица, слишком много тухлятины в воздухе. Оглянулся. Поздно.",

    "💨 *прицельно в ухо {name_dat} с расстояния 5 сантиметров*\n\nТёплый газ ввинтился прямо в ушной канал. {name} несколько секунд стоял с непонимающим лицом — внутри уха было тепло и воняло. Мозг отказывался это обрабатывать. Это нормальная реакция.",

    "💨 *громкое как фанфары — {name_dat} торжественный приветственный пёрд*\n\nЗвук разлетелся на весь чат. {name} оказался в центре внимания и тёплого вонючего облака одновременно. Торжественный момент. Незабываемый. Терапевт будет слушать про это годами.",

    "💨 *с кряхтением и красным лицом — {name} всё видел*\n\n{name} видел выражение лица в процессе подготовки — красное, сосредоточенное, с прикушенной губой. Потом услышал звук. Потом почувствовал запах. Все три ощущения были абсолютно лишними.",

    "💨 *неожиданный — {name} стоял слишком близко и зря*\n\nНе планировалось. Вышло само, блять. {name} оказался в радиусе поражения — тёплая волна смешанного воздуха и кишечных газов ударила в лицо. Горячо. Влажно. Пахнет едой трёхдневной давности.",

    "💨 *долгое с нарастающим запахом — {name} стоял и ждал конца*\n\nНе кончалось. 9 секунд непрерывного потока. {name} стоял и с каждой секундой запах становился гуще, теплее, мерзотнее. На девятой секунде что-то перегорело внутри навсегда.",

    "💨 *тихое но убийственное — {name} вдохнул полной грудью*\n\n{name} как раз делал глубокий вдох когда облако достигло ноздрей. Весь объём лёгких заполнился тухлятиной, серой и кишечными испарениями. Выдох был медленным и полным экзистенциального отчаяния.",

    "💨 *снизу в нос {name_dat} — специально присел для точности*\n\nТёплая струя ударила {name_gen} снизу прямо в ноздри с хирургической точностью. {name} даже не успел опустить взгляд — уже получил полную порцию в нюхалку.",

    "💨 *жидкое, с характерным хлюпом — {name} в опасной близости*\n\nНачиналось как нормальный пёрд, закончилось с влажным финалом. Несколько капель тёплой коричневатой жидкости долетело до штанины {name_gen}. {name} смотрел вниз минуту, не двигаясь.",

    "💨 *залпом прямо в лицо {name_dat} — с уважением и смотря в глаза*\n\nСмотрел в глаза пока пёрдел. {name} смотрел в ответ. Это был особый вид близости — тёплый, вонючий, нежелательный. Запах тухлых яиц и прокисшего бульона повис между ними как третий участник диалога.",

    "💨 *серьёзное, деловое, без лишних слов — прямо на {name_acc}*\n\nНикакой театральности. Никаких объяснений. Просто факт — горячий, вонючий, влажный. {name} теперь дышит чужими кишечными газами. Жизнь продолжается. Но как-то по-другому теперь.",
]


# ==================== ДЕТЕКТОР КРИНЖА "ГОСПОДИ ДОПОМОЖИ" ====================

# Паттерны кринжовых сообщений
CRINGE_PATTERNS = [
    # Токсичная позитивность / мотивашки
    r'всё будет хорошо',
    r'все будет хорошо',
    r'верь в себя',
    r'ты справишься',
    r'ты сможешь',
    r'успех неизбежен',
    r'мечты сбываются',
    r'я в тебя верю',
    r'никогда не сдавайся',
    r'думай позитивно',
    r'будь собой',
    r'люби себя',
    r'ты достоин',
    r'ты заслуживаешь',
    r'всё получится',
    r'держись',
    r'не переживай',
    r'всё наладится',
    r'верю в лучшее',
    r'главное верить',
    r'мысли материальны',
    r'визуализируй',
    r'притягивай',
    r'закон притяжения',
    
    # Нытьё и жалобы
    r'никто меня не понимает',
    r'никто не понимает',
    r'почему я такой',
    r'почему я такая',
    r'жизнь — боль',
    r'жизнь боль',
    r'всё плохо',
    r'все плохо',
    r'хочу умереть',
    r'хочу сдохнуть',
    r'ненавижу свою жизнь',
    r'я одинок',
    r'я одинока',
    r'меня никто не любит',
    r'я никому не нужен',
    r'я никому не нужна',
    r'устал от всего',
    r'устала от всего',
    r'нет сил',
    r'выгорел',
    r'выгорела',
    r'депрессия',
    r'тревожность',
    r'панические атаки',
    r'не вижу смысла',
    r'зачем всё это',
    r'бессмысленно',
    
    # Кринжовая романтика
    r'ты моё солнышко',
    r'ты мое солнышко',
    r'люблю тебя до луны',
    r'моя половинка',
    r'мой человек',
    r'скучаю по тебе',
    r'без тебя не могу',
    r'ты мой мир',
    r'ты моя жизнь',
    r'родная душа',
    r'родственная душа',
    r'мы созданы друг для друга',
    r'судьба свела',
    r'любовь всей жизни',
    r'единственный и неповторимый',
    r'единственная и неповторимая',
    r'навсегда вместе',
    r'вечная любовь',
    r'сердце моё',
    r'зайка',
    r'котик',
    r'малыш',
    r'пупсик',
    r'лапочка',
    r'милашка',
    
    # Псевдоумные высказывания
    r'люди такие люди',
    r'в этом мире',
    r'такова жизнь',
    r'время покажет',
    r'всё относительно',
    r'каждому своё',
    r'всё не случайно',
    r'всё к лучшему',
    r'что ни делается',
    r'значит так надо',
    r'вселенная знает',
    r'всему своё время',
    r'всё приходит вовремя',
    r'не суди',
    r'принимай как есть',
    r'отпусти',
    r'отпустить ситуацию',
    r'это урок',
    r'это опыт',
    r'жизнь учит',
    r'делай выводы',
    
    # Хвастовство и самолюбование
    r'я такой умный',
    r'я такая умная',
    r'все завидуют',
    r'я лучше всех',
    r'никто не может как я',
    r'я особенный',
    r'я особенная',
    r'я не такой как все',
    r'я уникальный',
    r'я уникальная',
    r'меня не понимают',
    r'я слишком хорош',
    r'я слишком хороша',
    r'не для всех',
    r'не каждому дано',
    r'высокий уровень',
    r'другой уровень',
    r'я на другом уровне',
    
    # Типичный молодёжный сленг
    r'\bрофл\b',
    r'\bкринж\b',
    r'\bбаза\b',
    r'\bимба\b',
    r'\bдушнила\b',
    r'\bвайб\b',
    r'\bфлекс\b',
    r'\bфлексить\b',
    r'на расслабоне',
    r'\bчилл\b',
    r'\bчиллить\b',
    r'\bкайф\b',
    r'\bкайфую\b',
    r'\bтоп\b',
    r'\bогонь\b',
    r'\bнереально\b',
    r'\bжиза\b',
    r'\bжизненно\b',
    r'\bощущаю\b',
    r'\bчувствую\b вайб',
    r'\bреспект\b',
    r'\bауф\b',
    r'\bэщкере\b',
    r'\bфактс\b',
    r'\bноу кэп\b',
    r'\bон годе\b',
    r'\bчекни\b',
    r'\bчекать\b',
    r'\bвписка\b',
    r'\bтусить\b',
    r'\bтусовка\b',
    
    # Инфоцыганство и бизнес-кринж
    r'пассивный доход',
    r'финансовая свобода',
    r'финансовая независимость',
    r'заработок без вложений',
    r'лёгкие деньги',
    r'легкие деньги',
    r'работа на себя',
    r'свой бизнес',
    r'стартап',
    r'инвестиции',
    r'инвестировать в себя',
    r'масштабировать',
    r'монетизировать',
    r'личный бренд',
    r'прокачать',
    r'прокачка',
    r'воронка продаж',
    r'трафик',
    r'лиды',
    r'конверсия',
    r'кейс',
    r'инсайт',
    r'лайфхак',
    r'продуктивность',
    r'эффективность',
    r'тайм-менеджмент',
    r'делегировать',
    r'нетворкинг',
    r'коллаборация',
    r'синергия',
    r'win-win',
    
    # Эзотерика и духовность
    r'энергия вселенной',
    r'высшие силы',
    r'космос',
    r'карма',
    r'прошлые жизни',
    r'реинкарнация',
    r'чакры',
    r'аура',
    r'энергетика',
    r'вибрации',
    r'частоты',
    r'астрология',
    r'ретроградный меркурий',
    r'луна в',
    r'знак зодиака',
    r'гороскоп',
    r'таро',
    r'руны',
    r'нумерология',
    r'ангельские числа',
    r'11:11',
    r'22:22',
    r'предзнаменование',
    r'интуиция',
    r'шестое чувство',
    r'третий глаз',
    r'просветление',
    r'пробуждение',
    r'духовный путь',
    r'духовный рост',
    
    # Отношения и токсичность
    r'токсичн',
    r'абьюз',
    r'абьюзер',
    r'газлайтинг',
    r'манипуляция',
    r'манипулятор',
    r'нарцисс',
    r'созависимость',
    r'личные границы',
    r'мои границы',
    r'твои границы',
    r'ресурс',
    r'ресурсное состояние',
    r'в ресурсе',
    r'не в ресурсе',
    r'выйти из ресурса',
    r'эмоциональный интеллект',
    r'проработать',
    r'проработка',
    r'терапия',
    r'психотерапия',
    r'мой терапевт',
    r'мой психолог',
    
    # Фитнес и ЗОЖ кринж
    r'пп',
    r'правильное питание',
    r'калории',
    r'дефицит калорий',
    r'профицит',
    r'бжу',
    r'белки жиры углеводы',
    r'интервальное голодание',
    r'детокс',
    r'токсины',
    r'шлаки',
    r'суперфуд',
    r'органик',
    r'натуральный',
    r'без гмо',
    r'глютен',
    r'лактоза',
    r'веган',
    r'зож',
    r'марафон',
    r'челлендж',
    r'трансформация',
    r'до и после',
    r'результат',
    
    # Ванильные статусы и эмодзи-паттерны
    r'♡|♥|💕|💖|💗|💓|💞|💝',
    r'🥺|🥹|😭{2,}|😢{2,}',
    r'\.{4,}',  # Многоточие...........
    r'\){3,}',  # Скобки ))))
    r'хах+',    # хахаха
    r'ахах+',   # ахахах
    r'лол+',    # лол, лоооол
    r'ору+',    # ору, оруну
    
    # Пацанские/бандитские цитатки
    r'жизнь научит',
    r'жизнь покажет',
    r'не верь.*не бойся.*не проси',
    r'волк одиночка',
    r'один против всех',
    r'сам за себя',
    r'доверяй но проверяй',
    r'лучше.*чем',
    r'враги.*завидуют',
    r'делай.*деньги',
    r'молча делай',
    r'пока.*спят.*работаю',
    r'hustle|хасл',
    r'grind|гринд',
    r'на вершине',
    r'путь воина',
    r'путь самурая',
    r'честь и достоинство',
    r'мужик сказал',
    r'слово пацана',
    r'за базар отвечаю',
    r'по понятиям',
    r'авторитет',
    r'уважуха',
    
    # Сигма/альфа мемы
    r'сигма',
    r'sigma',
    r'альфа.?самец',
    r'alpha',
    r'бета.?самец',
    r'омега',
    r'грайндсет',
    r'grindset',
    r'миллионер',
    r'billionaire',
    r'mindset',
    r'майндсет',
    r'патрик бейтман',
    r'patrick bateman',
    r'джокер',
    r'joker',
    r'томас шелби',
    r'peaky blinders',
    r'острые козырьки',
    
    # Женские пабликовые фразы
    r'настоящая женщина',
    r'настоящий мужчина',
    r'королева',
    r'принцесса',
    r'богиня',
    r'стерва',
    r'сильная женщина',
    r'слабый мужчина',
    r'достойн.*мужчин',
    r'достойн.*женщин',
    r'цени.*пока.*рядом',
    r'потеряешь.*поймёшь',
    r'потеряешь.*поймешь',
    r'заслуживаю лучшего',
    r'лучшая версия себя',
    r'женская энергия',
    r'мужская энергия',
    r'инь.*янь',
    r'женственность',
    r'мужественность',
    r'хозяйка.*жизни',
    r'сама себе',
    
    # Фразы про ex/бывших
    r'бывш',
    r'экс',
    r'ex ',
    r'прошлые отношения',
    r'токсичные отношения',
    r'ушёл.*вернулся',
    r'ушел.*вернулся',
    r'простила',
    r'простил',
    r'измен',
    r'предал',
    r'предательство',
    r'разбитое сердце',
    r'сердце разбито',
    r'больше не верю',
    r'не верю в любовь',
    r'все мужики',
    r'все бабы',
    r'одинаковые',
    
    # Подписи к фото/селфи
    r'без фильтров',
    r'естественная красота',
    r'просто я',
    r'настоящ.*я',
    r'принимаю себя',
    r'люблю себя такой',
    r'люблю себя таким',
    r'идеальн.*нет',
    r'несовершенн.*идеальн',
    r'красота.*спасёт',
    r'красота.*спасет',
    r'улыбайся',
    r'живи.*моментом',
    r'здесь и сейчас',
    r'наслаждайся',
    r'ловлю момент',
    r'жизнь прекрасна',
    r'каждый день.*подарок',
    
    # Тиктокерский/рилсовый контент
    r'pov:',
    r'пов:',
    r'сторителлинг',
    r'вайрал',
    r'viral',
    r'хайп',
    r'залететь',
    r'попасть в реки',
    r'рекомендации',
    r'подписывайся',
    r'подписывайтесь',
    r'лайк',
    r'репост',
    r'сохрани',
    r'поделись',
    r'отмечай',
    r'комментируй',
    r'пиши в коменты',
    
    # Астрология расширенная
    r'меркурий',
    r'венера',
    r'марс',
    r'юпитер',
    r'сатурн',
    r'плутон',
    r'нептун',
    r'уран',
    r'натальная карта',
    r'натальн.*карт',
    r'синастрия',
    r'соляр',
    r'транзит',
    r'овен',
    r'телец',
    r'близнецы',
    r'рак',
    r'лев',
    r'дева',
    r'весы',
    r'скорпион',
    r'стрелец',
    r'козерог',
    r'водолей',
    r'рыбы',
    r'знак зодиака',
    r'гороскоп',
    r'совместимость знаков',
    r'лунный календарь',
    r'полнолуние',
    r'новолуние',
    
    # Алкоголь/тусовки кринж
    r'бухать',
    r'бухло',
    r'нажраться',
    r'накидаться',
    r'шоты',
    r'вписка',
    r'тусить',
    r'кальян',
    r'дымим',
    r'похмел',
    r'перепил',
    r'перепила',
    r'не помню вчера',
    r'вчера было',
    r'ночь была',
    r'гулять так гулять',
    r'жизнь одна',
    r'один раз живём',
    r'один раз живем',
    r'yolo',
    
    # Фразы про деньги кринж
    r'деньги.*счастье',
    r'счастье.*деньги',
    r'богатство',
    r'роскош',
    r'luxury',
    r'лакшери',
    r'премиум',
    r'элитн',
    r'vip',
    r'вип',
    r'exclusive',
    r'эксклюзив',
    r'успешный успех',
    r'миллион',
    r'первый миллион',
    r'мотивация.*деньги',
    r'хочу быть богатым',
    r'хочу быть богатой',
    r'финансов.*грамотн',
    r'пассивный заработок',
    
    # Клише и банальности
    r'всё проходит',
    r'все проходит',
    r'и это пройдёт',
    r'и это пройдет',
    r'время лечит',
    r'что посеешь',
    r'как аукнется',
    r'бог.*даст',
    r'на всё воля',
    r'на все воля',
    r'бог терпел',
    r'терпение.*труд',
    r'без труда',
    r'тише едешь',
    r'поспешишь',
    r'семь раз отмерь',
    r'утро вечера',
    r'не всё золото',
    r'не все золото',
    r'в тихом омуте',
    r'своя ноша',
    r'нет худа без добра',
    
    # Фразы из пабликов ВК
    r'типичн',
    r'подслушано',
    r'признавашки',
    r'цитаты.*великих',
    r'мудрость',
    r'умные мысли',
    r'философия жизни',
    r'жизненная мудрость',
    r'правда жизни',
    r'суровая правда',
    r'горькая правда',
    r'истина',
    
    # Нарциссизм и ЧСВ
    r'я.*лучше.*всех',
    r'никто.*меня.*понимает',
    r'слишком.*умн',
    r'слишком.*хорош',
    r'все.*ниже',
    r'выше.*этого',
    r'мне.*не.*ровня',
    r'достоин.*большего',
    r'заслуживаю.*большего',
    r'мой уровень',
    r'не мой уровень',
    r'beneath me',
    r'above',
    
    # Геймерский кринж
    r'нуб',
    r'noob',
    r'изи',
    r'ez',
    r'gg wp',
    r'гг.*вп',
    r'раш',
    r'rush',
    r'затащил',
    r'затащила',
    r'нагиб',
    r'вынес',
    r'вынесла',
    r'апнул',
    r'апнула',
    r'фармить',
    r'гриндить',
    r'донат',
    r'задонатил',
    r'лутбокс',
    r'скин',
    r'скины',
]

# Кринжовые фразы для AI-анализа (частичное совпадение)
CRINGE_KEYWORDS = [
    # Психология
    'токсичн', 'абьюз', 'газлайт', 'триггер', 'травма', 'терапевт',
    'психолог', 'нарцисс', 'созависим', 'границ', 'ресурс',
    'проработ', 'осознанн', 'принят', 'отпуст',
    
    # Саморазвитие
    'саморазвит', 'осознанност', 'медитаци', 'практик', 'ритуал',
    'аффирмаци', 'визуализ', 'манифест', 'интенци',
    
    # Эзотерика
    'вселенн', 'энерги', 'вибраци', 'частот', 'кармич',
    'судьб', 'знак', 'предназначен', 'миссия', 'путь',
    'чакр', 'аур', 'астрал', 'ретроград',
    
    # Крипта и финансы
    'криптовалют', 'биткоин', 'эфир', 'блокчейн', 'нфт', 'nft',
    'метаверс', 'web3', 'defi', 'стейкинг', 'холд', 'памп', 'дамп',
    'пассивн', 'доход', 'свобод', 'инвестиц',
    
    # Бизнес-сленг
    'масштаб', 'монетиз', 'конверс', 'воронк', 'трафик',
    'таргет', 'продвиж', 'smm', 'контент', 'сторис',
    'рилс', 'охват', 'вовлечен', 'аудитор',
    
    # Отношения
    'половинк', 'родств', 'душ', 'связ', 'отношени',
    'чувств', 'эмоци', 'привязан', 'влюблен',
    
    # Сигма/грайнд культура
    'сигма', 'альфа', 'бета', 'омега', 'грайнд', 'хасл',
    'майндсет', 'mindset', 'billionaire', 'миллионер',
    
    # Астрология
    'меркурий', 'венер', 'ретроград', 'натальн', 'транзит',
    'гороскоп', 'зодиак', 'совместимост', 'полнолун',
    
    # ЧСВ и нарциссизм
    'уникальн', 'особенн', 'избранн', 'исключительн',
    'лучш', 'достойн', 'заслужив',
    
    # Тусовки
    'вписк', 'тусовк', 'бухать', 'кальян', 'похмел',
    
    # Контент
    'вайрал', 'хайп', 'рекомендаци', 'подписыва', 'лайк',
    
    # Мемные персонажи
    'бейтман', 'шелби', 'джокер', 'joker',
]

# Шанс срабатывания (60% при обнаружении паттерна)
CRINGE_TRIGGER_CHANCE = 0.60

# Кулдаун на чат (минимум 2 минуты между реакциями)
cringe_cooldowns: Dict[int, float] = {}
CRINGE_COOLDOWN_SECONDS = 120  # 2 минуты


async def check_cringe_and_react(message: Message) -> bool:
    """
    Проверяет сообщение на кринж и реагирует с шансом.
    Шанс персонализирован на основе профиля пользователя.
    Возвращает True если среагировал.
    """
    if not message.text or len(message.text) < 10:
        return False
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    text_lower = message.text.lower()
    
    # Проверяем кулдаун
    last_reaction = cringe_cooldowns.get(chat_id, 0)
    time_since_last = time.time() - last_reaction
    if time_since_last < CRINGE_COOLDOWN_SECONDS:
        # Логируем только если текст похож на кринж (чтобы не спамить)
        if len(message.text) > 15:
            logger.info(f"CRINGE cooldown active: {CRINGE_COOLDOWN_SECONDS - time_since_last:.0f}s remaining for chat {chat_id}")
        return False
    
    # Получаем профиль пользователя для персонализации шанса (per-chat!)
    user_profile = {}
    if USE_POSTGRES:
        try:
            user_profile = await get_user_profile_for_ai(
                user_id,
                message.chat.id,  # per-chat!
                message.from_user.first_name or "", 
                message.from_user.username or ""
            )
        except Exception as e:
            logger.debug(f"Could not get profile for cringe check: {e}")
    
    # Проверяем паттерны
    is_cringe = False
    cringe_reason = ""
    
    # Проверка по regex паттернам
    for pattern in CRINGE_PATTERNS:
        if re.search(pattern, text_lower):
            is_cringe = True
            cringe_reason = "pattern"
            break
    
    # Проверка по ключевым словам
    if not is_cringe:
        for keyword in CRINGE_KEYWORDS:
            if keyword in text_lower:
                is_cringe = True
                cringe_reason = "keyword"
                break
    
    # Проверка на избыток эмодзи (больше 5)
    if not is_cringe:
        emoji_count = len(re.findall(r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF\U00002702-\U000027B0]', message.text))
        if emoji_count > 5:
            is_cringe = True
            cringe_reason = "emoji_spam"
    
    # Проверка на КАПС (больше 50% заглавных)
    if not is_cringe and len(message.text) > 20:
        upper_count = sum(1 for c in message.text if c.isupper())
        if upper_count / len(message.text) > 0.5:
            is_cringe = True
            cringe_reason = "caps"
    
    if not is_cringe:
        return False
    
    logger.info(f"CRINGE detected ({cringe_reason}): '{message.text[:50]}...'")
    
    # ПЕРСОНАЛИЗИРОВАННЫЙ шанс срабатывания на основе профиля
    trigger_chance = CRINGE_TRIGGER_CHANCE  # База 60%
    
    # Увеличиваем шанс для определённых типов личности
    if user_profile:
        # Токсичные пользователи — больше кринжа (+15%)
        if user_profile.get('toxicity', 0) > 0.3:
            trigger_chance += 0.15
        
        # Негативный стиль общения — больше нытья (+10%)
        if user_profile.get('communication_style') == 'negative':
            trigger_chance += 0.10
        
        # Гиперактивные — чаще пишут кринж (+10%)
        if user_profile.get('activity_level') in ['hyperactive', 'very_active']:
            trigger_chance += 0.10
        
        # Определённые интересы увеличивают шанс
        interests = user_profile.get('interests', [])
        cringe_interests = ['crypto', 'politics', 'relationships', 'fitness']
        if any(i in interests for i in cringe_interests):
            trigger_chance += 0.10
        
        # Ночные совы — более эмоциональны ночью (+5%)
        if user_profile.get('is_night_owl') and 0 <= time.localtime().tm_hour < 6:
            trigger_chance += 0.05
    
    # Ограничиваем максимум 85%
    trigger_chance = min(trigger_chance, 0.85)
    
    # Проверяем шанс
    roll = random.random()
    if roll > trigger_chance:
        logger.info(f"CRINGE chance failed: rolled {roll:.2f} > {trigger_chance:.2f}, not reacting")
        return False
    
    logger.info(f"CRINGE chance success: rolled {roll:.2f} <= {trigger_chance:.2f}, REACTING!")
    
    # Создаём кликабельное упоминание
    user_mention = make_user_mention(
        message.from_user.id,
        message.from_user.first_name or "Братан",
        message.from_user.username
    )
    
    # Варианты реакций в зависимости от типа кринжа
    reactions = {
        "pattern": [
            f"🙏 {user_mention}, господи допоможи...",
            f"🙏 Господи допоможи, {user_mention} опять начинает...",
            f"✝️ {user_mention}, господи допоможи тебе и всем нам",
            f"🙏 {user_mention}... господи допоможи, шо це було?",
            f"⛪ Господи допоможи {user_mention}, ибо не ведает что творит",
            f"🙏 {user_mention}, господи допоможи... я не могу это развидеть",
            f"✝️ Господи допоможи, {user_mention} снова в своём репертуаре",
            f"🙏 {user_mention}... господи допоможи нам всем это пережить",
            f"⛪ {user_mention}, господи допоможи... за что нам это?",
            f"🙏 Господи допоможи {user_mention} и тем кто это прочитал",
            f"✝️ {user_mention}, господи допоможи... мои глаза",
            f"🙏 Господи допоможи, {user_mention} опять за своё",
            f"⛪ {user_mention}... господи допоможи, это же кринж",
            f"🙏 {user_mention}, господи допоможи... зачем ты это написал?",
            f"✝️ Господи допоможи всем кто это видит, особенно {user_mention}",
        ],
        "keyword": [
            f"🙏 {user_mention}, господи допоможи... токсичность детектед",
            f"🙏 Господи допоможи, {user_mention} в режиме саморазвития",
            f"✝️ {user_mention}, господи допоможи тебе с этими словами",
            f"🙏 {user_mention}, господи допоможи... инфоцыган детектед",
            f"✝️ Господи допоможи, {user_mention} пошёл по пути просветления",
            f"🙏 {user_mention}... господи допоможи, откуда ты это взял?",
            f"⛪ Господи допоможи {user_mention}, эзотерика не поможет",
            f"🙏 {user_mention}, господи допоможи... опять про энергии?",
            f"✝️ Господи допоможи, {user_mention} открыл курс по успеху",
        ],
        "emoji_spam": [
            f"🙏 {user_mention}, господи допоможи... сколько эмодзи",
            f"🙏 Господи допоможи, {user_mention} открыл клавиатуру эмодзи",
            f"✝️ Эмодзи-апокалипсис от {user_mention}... господи допоможи",
            f"🙏 {user_mention}, господи допоможи... это же эмодзи-понос",
            f"⛪ Господи допоможи, {user_mention} не знает меры в эмодзи",
            f"🙏 {user_mention}... господи допоможи, хватит смайликов",
        ],
        "caps": [
            f"🙏 {user_mention}, ГОСПОДИ ДОПОМОЖИ, НЕ КРИЧИ",
            f"🙏 Господи допоможи, {user_mention} сломал капслок",
            f"✝️ {user_mention}, господи допоможи... caps lock — не круиз-контроль для крутости",
            f"🙏 {user_mention}, ГОСПОДИ ДОПОМОЖИ, МЫ ТЕБЯ СЛЫШИМ",
            f"⛪ Господи допоможи, {user_mention} забыл выключить капс",
            f"🙏 {user_mention}... господи допоможи, не ори",
        ],
    }
    
    response = random.choice(reactions.get(cringe_reason, reactions["pattern"]))
    
    # Добавляем персонализированную добавку на основе профиля (30% шанс)
    if user_profile and random.random() < 0.30:
        profile_additions = []
        
        # По токсичности
        if user_profile.get('toxicity', 0) > 0.4:
            profile_additions.append(f"\n\n(Кстати, {user_mention} — рецидивист. Токсичность зашкаливает.)")
        
        # По стилю общения
        style = user_profile.get('communication_style', '')
        if style == 'negative':
            profile_additions.append(f"\n\n(Вечно ноет этот {user_mention}. Уже привыкли.)")
        elif style == 'toxic':
            profile_additions.append(f"\n\n({user_mention} опять токсит. Классика.)")
        
        # По интересам
        interests = user_profile.get('interests', [])
        if 'crypto' in interests:
            profile_additions.append(f"\n\n(Ещё и криптан. Господи допоможи вдвойне.)")
        elif 'politics' in interests:
            profile_additions.append(f"\n\n(Политолог, блин. Ну конечно.)")
        elif 'fitness' in interests:
            profile_additions.append(f"\n\n(Качок-ЗОЖник. Ожидаемо.)")
        
        # По активности
        if user_profile.get('activity_level') == 'hyperactive':
            profile_additions.append(f"\n\n({user_mention} как всегда — графоманит без остановки.)")
        
        if profile_additions:
            response += random.choice(profile_additions)
    
    # Обновляем кулдаун
    cringe_cooldowns[chat_id] = time.time()
    
    # Отправляем реакцию
    try:
        await message.reply(response, parse_mode=ParseMode.HTML)
        logger.info(f"CRINGE detected ({cringe_reason}) for user {user_id}, chance was {trigger_chance:.0%}: {message.text[:50]}...")
        return True
    except Exception as e:
        logger.error(f"Failed to send cringe reaction: {e}")
        return False


# ==================== ОТВЕТ НА РЕПЛАЙ/ТЕГ БОТА ====================

# Шанс для СЛУЧАЙНЫХ комментариев (не прямые обращения)
BOT_RANDOM_COMMENT_CHANCE = 0.40
# Кулдаун на ответы (5 сек на чат для быстрых диалогов)
bot_reply_cooldowns: Dict[int, float] = {}
BOT_REPLY_COOLDOWN = 5

# Ответы на разные типы контента
BOT_REPLIES_TEXT = [
    "Чё надо?",
    "Иди нахуй",
    "Отъебись",
    "Чё те, блять?",
    "Я занята",
    "Пошёл нахуй",
    "Ну и?",
    "Это всё?",
    "Мне похуй",
    "Заебал",
    "Чё пристал?",
    "Не до тебя",
    "Хуйня какая-то",
    "И чё?",
    "Серьёзно?",
    "Тебе заняться нечем?",
    "Опять ты",
    "Бля, ну хватит",
    "Достал уже",
    "Нахуя ты это прислал?",
]

BOT_REPLIES_STICKER = [
    "Стикер ебучий. И чё?",
    "Охуенный стикер. Нет.",
    "Стикером отвечаешь? Красава, блять",
    "Это типа смешно? Стикер прислал",
    "Стикер. Вау. Иди нахуй",
    "О, стикер. Мне похуй",
    "Классный стикер. Отъебись теперь",
    "Стикерами общаешься? Слов не хватает?",
]

BOT_REPLIES_GIF = [
    "Гифка. Охуенно. И чё?",
    "О, гифка. Мне заебись как интересно",
    "Гифку прислал. Молодец. Иди нахуй",
    "Это типа я должна впечатлиться гифкой?",
    "Гифка. Вау. Похуй.",
    "Классная гифка. Отвали.",
]

BOT_REPLIES_PHOTO = [
    "Фото. И чё ты хотел этим сказать?",
    "Фоточка. Охуенно. Дальше что?",
    "Картинку прислал. Молодец. Иди нахуй",
    "Это что за хуйня на фото?",
    "Фото. Интересно. Нет. Похуй.",
    "Красивая фотка. Шутка. Мне похуй.",
]

BOT_REPLIES_VOICE = [
    "Голосовое? Серьёзно? Не буду слушать эту хуйню",
    "Войс прислал. Мне лень слушать",
    "Голосовое. Напиши текстом, я не твоя мать",
    "Войсы шлёшь? Тебе лет сколько?",
    "Голосовое... Хуй я это буду слушать",
]

BOT_REPLIES_VIDEO = [
    "Видос. Не буду смотреть",
    "Видео прислал. Охуеть какой контент",
    "Это что за видос? Похуй, не интересно",
    "Видео. Вау. Мне заебись как интересно. Нет.",
]

BOT_REPLIES_VIDEO_NOTE = [
    "Кружочек. Охуенно. Не буду смотреть",
    "О, кружочек. Ебало своё снял? Молодец",
    "Видеокружок. Инновационно. Мне похуй",
]

# Контекстные ответы на текст
def get_contextual_reply(text: str) -> str:
    """Генерирует контекстный ответ на основе текста - ГЛУБОКИЙ АНАЛИЗ"""
    text_lower = text.lower()
    
    # Приветствия
    if any(w in text_lower for w in ['привет', 'здравствуй', 'хай', 'хей', 'здорова', 'приветик', 'добрый день', 'доброе утро', 'добрый вечер']):
        return random.choice([
            "Привет-привет. Чё надо?",
            "Здорова. И чё?",
            "Привет. Отъебись.",
            "Здарова. Дальше что?",
            "О, явился. Чего хотел?",
        ])
    
    # Вопросы о делах
    if any(w in text_lower for w in ['как дела', 'как ты', 'чё делаешь', 'что делаешь', 'как жизнь', 'как сама', 'чем занимаешься']):
        return random.choice([
            "Норм. А тебе какое дело?",
            "Заебись. Чё надо?",
            "Нормально. Не твоё дело.",
            "Дела? Ебашу. Отвали.",
            "Жизнь? Всё заебись. Чего хотел?",
        ])
    
    # Благодарность
    if any(w in text_lower for w in ['спасибо', 'благодарю', 'спс', 'thx', 'thanks']):
        return random.choice([
            "Пожалуйста. А теперь отъебись.",
            "Не за что. Иди нахуй.",
            "Ага-ага. Дальше что?",
            "Рада стараться. Шутка. Мне похуй.",
        ])
    
    # Похвала
    if any(w in text_lower for w in ['молодец', 'красава', 'круто', 'класс', 'супер', 'охуенно', 'заебись', 'умница', 'красотка']):
        return random.choice([
            "Спасибо. А теперь иди нахуй.",
            "Знаю. Дальше?",
            "Ага. И чё?",
            "Круто. Мне похуй. Но круто.",
            "Льстишь? Не поможет.",
        ])
    
    # Оскорбления
    if any(w in text_lower for w in ['дура', 'тупая', 'идиот', 'мудак', 'сука', 'бля', 'хуй', 'пизд', 'ебан', 'долбо', 'дебил', 'придурок']):
        return random.choice([
            "Сам такой, блять",
            "На себя посмотри",
            "Ой, кто бы говорил",
            "Иди нахуй сам",
            "Сам ты хуй",
            "Зеркало дома есть?",
            "С твоим интеллектом лучше молчи",
        ])
    
    # Смех
    if any(w in text_lower for w in ['хаха', 'ахах', 'лол', 'ржу', 'смешно', 'угар', 'ору', 'кек', 'лмао', 'ржака']):
        return random.choice([
            "Ржёшь? Над собой посмейся",
            "Смешно ему. А мне нет.",
            "Угар, да. Иди нахуй.",
            "Ору с тебя. Но не от смеха.",
            "Поржал и хватит. Отвали.",
        ])
    
    # Просьбы о помощи
    if any(w in text_lower for w in ['помоги', 'помощь', 'подскажи', 'скажи', 'расскажи', 'объясни', 'научи']):
        return random.choice([
            "Я тебе не гугл. Сам разбирайся.",
            "Помочь? Ага, щас. Разбегусь.",
            "Подсказать? Иди нахуй - вот тебе подсказка.",
            "Я похожа на справочное бюро?",
            "Гугл в помощь. Или нахуй.",
        ])
    
    # Извинения
    if any(w in text_lower for w in ['извини', 'прости', 'сорри', 'sorry', 'пардон', 'виноват']):
        return random.choice([
            "Поздно извиняться. Иди нахуй.",
            "Прощаю. Шутка. Не прощаю.",
            "Извинения не приняты. Отвали.",
            "Ну извинился и извинился. Дальше что?",
        ])
    
    # Комплименты боту
    if any(w in text_lower for w in ['красивая', 'милая', 'хорошая', 'классная', 'нравишься', 'люблю тебя', 'обожаю']):
        return random.choice([
            "Знаю, что красивая. И чё?",
            "Комплименты? Не подлизывайся.",
            "Милая? Я грубая тётка. Отъебись.",
            "Любовь? Я замужем. За нахуем.",
        ])
    
    # Жалобы
    if any(w in text_lower for w in ['устал', 'заебал', 'надоел', 'бесит', 'достал', 'задолбал', 'плохо', 'грустно', 'печально']):
        return random.choice([
            "Устал? Мне похуй. Отдохни и отъебись.",
            "Бесит? Меня тоже всё бесит. Особенно ты.",
            "Плохо? Бывает. Мне всё равно.",
            "Жалуешься? Не тому жалуешься.",
        ])
    
    # Еда/голод
    if any(w in text_lower for w in ['есть хочу', 'жрать', 'голодный', 'кушать', 'еда', 'пожрать', 'поесть']):
        return random.choice([
            "Голодный? Иди поешь и не еби мозги.",
            "Жрать хочешь? Я тебе не столовая.",
            "Еда? Мне похуй что ты ешь.",
            "Покушай и отвали.",
        ])
    
    # Сон/усталость
    if any(w in text_lower for w in ['спать хочу', 'сонный', 'засыпаю', 'устал', 'спатки', 'баиньки']):
        return random.choice([
            "Спать? Иди спи и не еби мозги.",
            "Сонный? Не моя проблема.",
            "Засыпаешь? Проснись и отъебись.",
            "Спатеньки? Тебе сколько лет, блять?",
        ])
    
    # Алкоголь/вечеринки
    if any(w in text_lower for w in ['выпить', 'пиво', 'водка', 'бухать', 'пьяный', 'напился', 'вечеринка', 'тусить']):
        return random.choice([
            "Бухаешь? Ну бухай. Мне похуй.",
            "Пьяный? Протрезвей и отвали.",
            "Вечеринка? Без меня. Отъебись.",
            "Выпить? Пей и не писи мне.",
        ])
    
    # Работа
    if any(w in text_lower for w in ['работа', 'работаю', 'на работе', 'начальник', 'коллеги', 'офис']):
        return random.choice([
            "Работаешь? Работай молча.",
            "На работе? Тогда работай, а не мне пиши.",
            "Начальник бесит? Мне тоже всё бесит. Отвали.",
            "Офис? Мне похуй на твой офис.",
        ])
    
    # Погода
    if any(w in text_lower for w in ['погода', 'дождь', 'солнце', 'холодно', 'жарко', 'снег']):
        return random.choice([
            "Погода? Мне похуй какая погода.",
            "Холодно? Оденься и отвали.",
            "Дождь? Зонт возьми. И иди нахуй.",
            "Жарко? Не моя проблема.",
        ])
    
    # Вопрос с вопросительным знаком
    if '?' in text:
        # Попробуем понять суть вопроса
        if any(w in text_lower for w in ['почему', 'зачем', 'отчего']):
            return random.choice([
                "Почему? Потому что иди нахуй.",
                "Зачем? Затем. Отвали.",
                "Потому что гладиолус. Отъебись.",
            ])
        if any(w in text_lower for w in ['где', 'куда', 'откуда']):
            return random.choice([
                "Где? Там. Отвали.",
                "Куда? Нахуй. Вот куда.",
                "Откуда мне знать? Сам ищи.",
            ])
        if any(w in text_lower for w in ['кто', 'кого', 'кому']):
            return random.choice([
                "Кто? Да хуй знает кто.",
                "Я почём знаю кто это?",
                "Спроси у кого-нибудь другого.",
            ])
        if any(w in text_lower for w in ['когда', 'во сколько']):
            return random.choice([
                "Когда? Никогда. Отвали.",
                "Тогда, когда отъебёшься.",
                "Я тебе не календарь.",
            ])
        if any(w in text_lower for w in ['сколько', 'много', 'мало']):
            return random.choice([
                "Сколько? Дохуя. Всё, отвали.",
                "Много. Или мало. Мне похуй.",
                "Считай сам, я не калькулятор.",
            ])
        # Общий вопрос
        return random.choice([
            "Хуй знает",
            "Не знаю. И знать не хочу.",
            "А мне откуда знать?",
            "Спроси у кого-нибудь другого",
            "Это ты у меня спрашиваешь? Серьёзно?",
            "Гугл знает. Я - нет.",
        ])
    
    # По умолчанию
    return random.choice(BOT_REPLIES_TEXT)


async def transcribe_voice_message(message: Message) -> str:
    """Транскрибирует голосовое сообщение через Whisper API"""
    transcribe_api_url = os.getenv("TRANSCRIBE_API_URL")
    if not transcribe_api_url:
        return ""
    
    try:
        voice = message.voice or message.video_note
        if not voice:
            return ""
        
        # Скачиваем голосовое
        file = await bot.get_file(voice.file_id)
        voice_bytes = await bot.download_file(file.file_path)
        
        import base64
        audio_base64 = base64.b64encode(voice_bytes.read()).decode('utf-8')
        
        # Определяем формат
        file_format = "ogg" if message.voice else "mp4"
        
        # Отправляем на транскрипцию
        session = await get_http_session()
        async with session.post(
            transcribe_api_url,
            json={
                "audio_base64": audio_base64,
                "format": file_format
            },
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:
            if response.status == 200:
                result = await response.json()
                text = result.get("text", "")
                if text:
                    logger.info(f"Voice transcribed: {text[:50]}...")
                    return text
    except Exception as e:
        logger.warning(f"Failed to transcribe voice: {e}")
    
    return ""


async def analyze_voice_for_reply(message: Message) -> str:
    """Анализирует голосовое сообщение и генерирует грубый ответ"""
    # Пробуем транскрибировать
    transcription = await transcribe_voice_message(message)
    
    if transcription:
        # Если удалось транскрибировать - отвечаем по существу
        voice_type = "голосовом" if message.voice else "кружочке"
        
        # Генерируем ответ на основе текста
        response = await generate_rude_response_to_content(voice_type, transcription)
        if response:
            return response
        
        # Фоллбэк - используем контекстный анализ текста
        return get_contextual_reply(transcription)
    
    # Если не удалось транскрибировать - стандартный ответ
    if message.voice:
        return random.choice(BOT_REPLIES_VOICE)
    else:
        return random.choice(BOT_REPLIES_VIDEO_NOTE)


async def analyze_photo_for_reply(message: Message) -> str:
    """Анализирует фото через Vision API и генерирует грубый ответ"""
    vision_api_url = os.getenv("VISION_API_URL")
    if not vision_api_url:
        return random.choice(BOT_REPLIES_PHOTO)
    
    try:
        photo = message.photo[-1]
        file = await bot.get_file(photo.file_id)
        photo_bytes = await bot.download_file(file.file_path)
        
        import base64
        image_base64 = base64.b64encode(photo_bytes.read()).decode('utf-8')
        
        # Получаем описание фото
        session = await get_http_session()
        async with session.post(
            vision_api_url,
            json={
                "image_base64": image_base64,
                "media_type": "image/jpeg",
                "custom_prompt": "Опиши что на фото ОЧЕНЬ кратко, в 5-10 слов. Без формальностей."
            },
            timeout=aiohttp.ClientTimeout(total=10)
        ) as response:
            if response.status == 200:
                result = await response.json()
                description = result.get("description", "")
                if description:
                    # Генерируем грубый ответ на основе описания
                    return await generate_rude_response_to_content("фото", description)
    except Exception as e:
        logger.warning(f"Failed to analyze photo for reply: {e}")
    
    return random.choice(BOT_REPLIES_PHOTO)


async def analyze_sticker_for_reply(sticker) -> str:
    """Анализирует стикер по эмодзи и генерирует ответ"""
    emoji = sticker.emoji if sticker.emoji else ""
    
    # Карта эмодзи -> контекстный ответ
    emoji_responses = {
        # Смех
        ('😂', '🤣', '😆', '😹', '🙃'): [
            "Смешно ему. А мне нет, блять.",
            "Ржёшь? Над собой поржи лучше.",
            "Стикер с ржачкой. Охуенно смешно. Нет.",
            "Угорел? Ну угорай дальше, мне похуй.",
        ],
        # Грусть
        ('😢', '😭', '😿', '🥺', '😞'): [
            "Плачешь? Поплачь ещё, мне не жалко.",
            "Грустный стикер. Мне похуй на твои чувства.",
            "Слёзки? Утрись и иди нахуй.",
        ],
        # Злость
        ('😡', '🤬', '😤', '💢'): [
            "О, злится. Ну злись-злись, мне похуй.",
            "Агрессивный какой. Успокойся, дебил.",
            "Злой стикер? На себя злись.",
        ],
        # Любовь
        ('❤️', '🥰', '😍', '💕', '😘', '💋'): [
            "Любовь? Иди нахуй со своей любовью.",
            "Сердечки мне шлёшь? Я замужем, отъебись.",
            "Романтик, блять. Иди романтику ищи в другом месте.",
        ],
        # Удивление
        ('😱', '😨', '🤯', '😳'): [
            "Удивился? Ну охуеть теперь.",
            "Шок? А мне похуй.",
            "В ахуе? Бывает. Иди нахуй.",
        ],
        # Пальцы
        ('👍', '👎', '🖕', '✌️', '🤙'): [
            "Палец показываешь? Засунь его себе знаешь куда.",
            "Жест. Охуенно. Дальше что?",
            "Показал. Молодец. Иди нахуй.",
        ],
        # Еда
        ('🍕', '🍔', '🍟', '🍦', '🍩', '🍪'): [
            "Жрать хочешь? Мне похуй.",
            "Еду показываешь? Жри и отвали.",
            "Вкусняшки? Подавись.",
        ],
        # Животные
        ('🐱', '🐶', '🐰', '🦊', '🐻'): [
            "Котик? Мне похуй на котиков.",
            "Зверушка. Мило. Отъебись теперь.",
            "Животное прислал. И чё?",
        ],
    }
    
    for emojis, responses in emoji_responses.items():
        if emoji in emojis:
            return random.choice(responses)
    
    # Если эмодзи не распознан
    return random.choice(BOT_REPLIES_STICKER)


async def generate_rude_response_to_content(content_type: str, description: str) -> str:
    """Генерирует грубый ответ на основе описания контента через API"""
    api_url = os.getenv("SUCK_API_URL") or os.getenv("VERCEL_API_URL")
    if not api_url:
        return f"О, {content_type}. И чё? Мне похуй."
    
    try:
        # Используем API для генерации грубого ответа
        session = await get_http_session()
        # Формируем запрос на генерацию грубого ответа
        prompt = f"""Ты тётя Роза - грубая тётка из чата. Тебе прислали {content_type}. 
Описание: {description}

Напиши ОЧЕНЬ короткий (1 предложение, максимум 15 слов) грубый, хамский ответ по существу того, что на {content_type}.
Используй мат. Покажи что тебе не интересно, но прокомментируй по существу.
НЕ используй кавычки. Пиши как в чате."""

        async with session.post(
            api_url.replace("/suck", "/ventilate") if "/suck" in api_url else api_url,
            json={"victim_name": "контент", "custom_prompt": prompt},
            timeout=aiohttp.ClientTimeout(total=8)
        ) as response:
            if response.status == 200:
                result = await response.json()
                text = result.get("text", "")
                if text and len(text) < 200:
                    return text
    except Exception as e:
        logger.warning(f"Failed to generate rude response: {e}")
    
    # Фоллбэк - простой ответ на основе описания
    rude_templates = [
        f"О, {description}. И чё? Мне похуй.",
        f"{description.capitalize()}? Охуенно. Дальше что?",
        f"Вижу {description}. Заебись. Иди нахуй.",
        f"{description.capitalize()} прислал. Молодец, блять.",
        f"Это {description}? Нахуя мне это?",
    ]
    return random.choice(rude_templates)


async def handle_bot_mention_or_reply(message: Message, use_chance: bool = False) -> bool:
    """
    Обрабатывает реплай на бота или упоминание бота.
    АНАЛИЗИРУЕТ контент и отвечает ПО СУЩЕСТВУ.
    
    Args:
        message: Сообщение от пользователя
        use_chance: Если True - применяет случайный шанс ответа (для случайных комментариев)
                   Если False - отвечает всегда (для прямых обращений)
    
    Returns:
        True если ответили, False если нет
    """
    chat_id = message.chat.id
    
    # Проверяем кулдаун (короткий, для быстрых диалогов)
    last_reply = bot_reply_cooldowns.get(chat_id, 0)
    if time.time() - last_reply < BOT_REPLY_COOLDOWN:
        logger.debug(f"[BOT_REPLY] Cooldown active for chat {chat_id}")
        return False
    
    # Шанс применяется ТОЛЬКО для случайных комментариев, НЕ для прямых обращений
    if use_chance and random.random() > BOT_RANDOM_COMMENT_CHANCE:
        logger.debug(f"[BOT_REPLY] Random chance failed for chat {chat_id}")
        return False
    
    # Определяем тип контента и АНАЛИЗИРУЕМ его
    response = ""
    
    if message.sticker:
        # Анализируем стикер по эмодзи
        response = await analyze_sticker_for_reply(message.sticker)
        logger.info(f"BOT REPLY: Analyzed sticker emoji: {message.sticker.emoji}")
        
    elif message.photo:
        # Анализируем фото через Vision API
        response = await analyze_photo_for_reply(message)
        logger.info("BOT REPLY: Analyzed photo content")
        
    elif message.animation:
        # Для гифок - пока базовый ответ (можно добавить анализ)
        caption = message.caption or ""
        if caption:
            response = await generate_rude_response_to_content("гифка", caption)
        else:
            response = random.choice(BOT_REPLIES_GIF)
            
    elif message.voice or message.video_note:
        # Транскрибируем и анализируем голосовое/кружочек
        response = await analyze_voice_for_reply(message)
        media_type = "voice" if message.voice else "video_note"
        logger.info(f"BOT REPLY: Analyzed {media_type} content")
        
    elif message.video:
        caption = message.caption or ""
        if caption:
            response = await generate_rude_response_to_content("видео", caption)
        else:
            response = random.choice(BOT_REPLIES_VIDEO)
        
    elif message.text:
        # УМНЫЙ AI-ответ с контекстом (профиль + память + чат)
        response = await generate_smart_reply(message)
        
    else:
        response = random.choice(BOT_REPLIES_TEXT)
    
    # Обновляем кулдаун
    bot_reply_cooldowns[chat_id] = time.time()
    
    # Отправляем ответ
    try:
        await message.reply(response)
        logger.info(f"BOT REPLY: {response[:50]}...")
        return True
    except Exception as e:
        logger.error(f"Failed to send bot reply: {e}")
        return False


@router.message(F.reply_to_message)
async def handle_reply_to_bot(message: Message):
    """Обработчик реплаев на сообщения бота"""
    if message.chat.type == "private":
        return
    
    # Проверяем, что реплай на сообщение бота (используем кэш)
    if await is_reply_to_bot(message):
        await handle_bot_mention_or_reply(message)


async def check_bot_mention(message: Message) -> bool:
    """Проверяет упоминание бота в сообщении и отвечает"""
    if not message.text and not message.caption:
        return False
    
    text = message.text or message.caption or ""
    
    # Получаем username бота (кэшировано)
    bot_username = await get_bot_username()
    
    if not bot_username:
        return False
    
    # Проверяем упоминание @botusername
    if f"@{bot_username.lower()}" in text.lower():
        await handle_bot_mention_or_reply(message)
        return True
    
    # Проверяем также упоминание "тётя роза" или "тетя роза"
    if any(name in text.lower() for name in ['тётя роза', 'тетя роза', 'тёте розе', 'тете розе', 'тётю розу', 'тетю розу']):
        await handle_bot_mention_or_reply(message)
        return True
    
    return False


@router.message(F.sticker)
async def collect_stickers(message: Message):
    """Сбор стикеров + сохранение в коллекцию мемов + обновление профиля"""
    if message.chat.type == "private":
        return
    
    # Проверяем реплай на бота (кэшировано)
    if await is_reply_to_bot(message):
        await handle_bot_mention_or_reply(message)
    
    sticker = message.sticker
    reply_to_user_id = None
    if message.reply_to_message and message.reply_to_message.from_user:
        reply_to_user_id = message.reply_to_message.from_user.id
    
    await save_chat_message(
        chat_id=message.chat.id,
        user_id=message.from_user.id,
        username=message.from_user.username or "",
        first_name=message.from_user.first_name or "Аноним",
        message_text="",
        message_type="sticker",
        sticker_emoji=sticker.emoji if sticker else "🎭",
        file_id=sticker.file_id if sticker else None,
        file_unique_id=sticker.file_unique_id if sticker else None
    )
    
    # ОБНОВЛЯЕМ ПРОФИЛЬ (per-chat)
    if USE_POSTGRES:
        try:
            await update_user_profile_comprehensive(
                user_id=message.from_user.id,
                chat_id=message.chat.id,
                message_text=sticker.emoji if sticker and sticker.emoji else "",
                timestamp=int(time.time()),
                first_name=message.from_user.first_name or "",
                username=message.from_user.username or "",
                reply_to_user_id=reply_to_user_id,
                message_type="sticker",
                sticker_emoji=sticker.emoji if sticker else None
            )
        except Exception as e:
            logger.warning(f"Profile update error (sticker): {e}", exc_info=True)
    
    # Сохраняем стикер в коллекцию (если это не анимированный/видео стикер)
    if sticker and not sticker.is_video and not sticker.is_animated:
        await save_media(
            chat_id=message.chat.id,
            user_id=message.from_user.id,
            file_id=sticker.file_id,
            file_type="sticker",
            file_unique_id=sticker.file_unique_id,
            description=sticker.emoji
        )


@router.message(F.photo)
async def collect_photos(message: Message):
    """Сбор фото с анализом через Claude Vision"""
    if message.chat.type == "private":
        return
    
    # Проверяем реплай на бота (кэшировано)
    if await is_reply_to_bot(message):
        await handle_bot_mention_or_reply(message)
    
    caption = message.caption[:200] if message.caption else ""
    image_description = None
    
    # Анализируем фото через Vision API (только если есть API URL)
    vision_api_url = os.getenv("VISION_API_URL")
    if vision_api_url:
        try:
            # Получаем файл фото (берём самое большое разрешение)
            photo = message.photo[-1]
            file = await bot.get_file(photo.file_id)
            
            # Скачиваем фото
            photo_bytes = await bot.download_file(file.file_path)
            
            # Конвертируем в base64
            import base64
            import io
            
            if isinstance(photo_bytes, io.BytesIO):
                photo_data = photo_bytes.getvalue()
            else:
                photo_data = photo_bytes
            
            image_base64 = base64.b64encode(photo_data).decode('utf-8')
            
            # Отправляем на анализ
            session = await get_http_session()
            async with session.post(
                vision_api_url,
                json={
                    "image_base64": image_base64,
                    "media_type": "image/jpeg"
                },
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    image_description = result.get("description", "")[:300]
                    logger.info(f"Image analyzed: {image_description[:50]}...")
        except Exception as e:
            logger.error(f"Error analyzing image: {e}")
            image_description = None
    
    photo = message.photo[-1]  # Самое большое разрешение
    reply_to_user_id = None
    if message.reply_to_message and message.reply_to_message.from_user:
        reply_to_user_id = message.reply_to_message.from_user.id
    
    await save_chat_message(
        chat_id=message.chat.id,
        user_id=message.from_user.id,
        username=message.from_user.username or "",
        first_name=message.from_user.first_name or "Аноним",
        message_text=caption,
        message_type="photo",
        image_description=image_description,
        file_id=photo.file_id,
        file_unique_id=photo.file_unique_id
    )
    
    # ОБНОВЛЯЕМ ПРОФИЛЬ (per-chat) — используем caption + описание картинки
    if USE_POSTGRES:
        try:
            # Объединяем caption и описание картинки для профилирования
            profile_text = caption
            if image_description:
                profile_text = f"{caption} [фото: {image_description}]" if caption else f"[фото: {image_description}]"
            
            await update_user_profile_comprehensive(
                user_id=message.from_user.id,
                chat_id=message.chat.id,
                message_text=profile_text,
                timestamp=int(time.time()),
                first_name=message.from_user.first_name or "",
                username=message.from_user.username or "",
                reply_to_user_id=reply_to_user_id,
                message_type="photo"
            )
        except Exception as e:
            logger.warning(f"Profile update error (photo): {e}", exc_info=True)
    
    # Сохраняем фото в коллекцию мемов
    await save_media(
        chat_id=message.chat.id,
        user_id=message.from_user.id,
        file_id=photo.file_id,
        file_type="photo",
        file_unique_id=photo.file_unique_id,
        description=image_description,
        caption=caption
    )
    
    # Шанс 15% для теста (потом вернуть на 2-3%)
    if random.random() < 0.15:
        try:
            await maybe_send_random_meme(message.chat.id, trigger="photo", target_user_id=message.from_user.id)
        except Exception as e:
            logger.warning(f"Failed to send random meme after photo: {e}")


@router.message(F.animation)
async def collect_animations(message: Message):
    """Сбор GIF/анимаций + сохранение в коллекцию + обновление профиля"""
    if message.chat.type == "private":
        return
    
    # Проверяем реплай на бота (кэшировано)
    if await is_reply_to_bot(message):
        await handle_bot_mention_or_reply(message)
    
    animation = message.animation
    caption = message.caption[:200] if message.caption else ""
    reply_to_user_id = None
    if message.reply_to_message and message.reply_to_message.from_user:
        reply_to_user_id = message.reply_to_message.from_user.id
    
    await save_chat_message(
        chat_id=message.chat.id,
        user_id=message.from_user.id,
        username=message.from_user.username or "",
        first_name=message.from_user.first_name or "Аноним",
        message_text=caption,
        message_type="animation",
        file_id=animation.file_id if animation else None,
        file_unique_id=animation.file_unique_id if animation else None
    )
    
    # ОБНОВЛЯЕМ ПРОФИЛЬ (per-chat)
    if USE_POSTGRES:
        try:
            await update_user_profile_comprehensive(
                user_id=message.from_user.id,
                chat_id=message.chat.id,
                message_text=caption if caption else "[GIF]",
                timestamp=int(time.time()),
                first_name=message.from_user.first_name or "",
                username=message.from_user.username or "",
                reply_to_user_id=reply_to_user_id,
                message_type="animation"
            )
        except Exception as e:
            logger.warning(f"Profile update error (animation): {e}", exc_info=True)
    
    # Сохраняем GIF в коллекцию
    if animation:
        await save_media(
            chat_id=message.chat.id,
            user_id=message.from_user.id,
            file_id=animation.file_id,
            file_type="animation",
            file_unique_id=animation.file_unique_id,
            caption=caption
        )
    
    # Шанс 15% для теста (потом вернуть на 2-3%)
    if random.random() < 0.15:
        try:
            await maybe_send_random_meme(message.chat.id, trigger="animation", target_user_id=message.from_user.id)
        except Exception as e:
            logger.warning(f"Failed to send random meme after animation: {e}")


@router.message(F.voice | F.video_note)
async def collect_voice(message: Message):
    """Сбор голосовых и кружочков + сохранение в коллекцию + ТРАНСКРИПЦИЯ В ПРОФИЛЬ"""
    if message.chat.type == "private":
        return
    
    # Проверяем реплай на бота (кэшировано)
    if await is_reply_to_bot(message):
        await handle_bot_mention_or_reply(message)
    
    msg_type = "voice" if message.voice else "video_note"
    media_obj = message.voice or message.video_note
    reply_to_user_id = None
    if message.reply_to_message and message.reply_to_message.from_user:
        reply_to_user_id = message.reply_to_message.from_user.id
    
    await save_chat_message(
        chat_id=message.chat.id,
        user_id=message.from_user.id,
        username=message.from_user.username or "",
        first_name=message.from_user.first_name or "Аноним",
        message_text="",
        message_type=msg_type,
        file_id=media_obj.file_id if media_obj else None,
        file_unique_id=media_obj.file_unique_id if media_obj else None
    )
    
    # ФОНОВАЯ ТРАНСКРИПЦИЯ + ОБНОВЛЕНИЕ ПРОФИЛЯ
    transcription = ""
    if USE_POSTGRES:
        try:
            # Пробуем транскрибировать голосовое/кружочек
            transcription = await transcribe_voice_message(message)
            
            # Обновляем профиль с транскрипцией (или без если не удалось)
            profile_text = transcription if transcription else f"[{msg_type}]"
            
            await update_user_profile_comprehensive(
                user_id=message.from_user.id,
                chat_id=message.chat.id,
                message_text=profile_text,
                timestamp=int(time.time()),
                first_name=message.from_user.first_name or "",
                username=message.from_user.username or "",
                reply_to_user_id=reply_to_user_id,
                message_type=msg_type
            )
            
            if transcription:
                logger.info(f"Voice transcribed and added to profile: {transcription[:50]}...")
        except Exception as e:
            logger.warning(f"Profile update error (voice): {e}", exc_info=True)
    
    # Сохраняем голосовое/кружочек в коллекцию
    if message.voice:
        voice = message.voice
        sender_name = message.from_user.first_name or "Аноним"
        # Добавляем транскрипцию в описание если есть
        description = f"Голосовое от {sender_name} ({voice.duration} сек)"
        if transcription:
            description += f": {transcription[:100]}"
        await save_media(
            chat_id=message.chat.id,
            user_id=message.from_user.id,
            file_id=voice.file_id,
            file_type="voice",
            file_unique_id=voice.file_unique_id,
            description=description
        )
        # Шанс 15% для теста (потом вернуть на 3%)
        if random.random() < 0.15:
            try:
                await maybe_send_random_meme(message.chat.id, trigger="voice", target_user_id=message.from_user.id)
            except Exception as e:
                logger.warning(f"Failed to send random meme after voice: {e}")
    
    elif message.video_note:
        video_note = message.video_note
        sender_name = message.from_user.first_name or "Аноним"
        # Добавляем транскрипцию в описание если есть
        description = f"Кружочек от {sender_name} ({video_note.duration} сек)"
        if transcription:
            description += f": {transcription[:100]}"
        await save_media(
            chat_id=message.chat.id,
            user_id=message.from_user.id,
            file_id=video_note.file_id,
            file_type="video_note",
            file_unique_id=video_note.file_unique_id,
            description=description
        )
        # Шанс 15% для теста (потом вернуть на 3%)
        if random.random() < 0.15:
            try:
                await maybe_send_random_meme(message.chat.id, trigger="video_note", target_user_id=message.from_user.id)
            except Exception as e:
                logger.warning(f"Failed to send random meme after video_note: {e}")


@router.message(F.video)
async def collect_videos(message: Message):
    """Сбор видео + сохранение в коллекцию + обновление профиля"""
    if message.chat.type == "private":
        return
    
    # Проверяем реплай на бота (кэшировано)
    if await is_reply_to_bot(message):
        await handle_bot_mention_or_reply(message)
    
    video = message.video
    caption = message.caption[:200] if message.caption else ""
    reply_to_user_id = None
    if message.reply_to_message and message.reply_to_message.from_user:
        reply_to_user_id = message.reply_to_message.from_user.id
    
    await save_chat_message(
        chat_id=message.chat.id,
        user_id=message.from_user.id,
        username=message.from_user.username or "",
        first_name=message.from_user.first_name or "Аноним",
        message_text=caption,
        message_type="video",
        file_id=video.file_id if video else None,
        file_unique_id=video.file_unique_id if video else None
    )
    
    # ОБНОВЛЯЕМ ПРОФИЛЬ (per-chat)
    if USE_POSTGRES:
        try:
            await update_user_profile_comprehensive(
                user_id=message.from_user.id,
                chat_id=message.chat.id,
                message_text=caption if caption else "[video]",
                timestamp=int(time.time()),
                first_name=message.from_user.first_name or "",
                username=message.from_user.username or "",
                reply_to_user_id=reply_to_user_id,
                message_type="video"
            )
        except Exception as e:
            logger.warning(f"Profile update error (video): {e}", exc_info=True)
    
    # Сохраняем видео в коллекцию
    if video:
        sender_name = message.from_user.first_name or "Аноним"
        duration = video.duration or 0
        await save_media(
            chat_id=message.chat.id,
            user_id=message.from_user.id,
            file_id=video.file_id,
            file_type="video",
            file_unique_id=video.file_unique_id,
            description=f"Видео от {sender_name} ({duration} сек)",
            caption=caption
        )
    
    # Шанс 15% для теста
    if random.random() < 0.15:
        try:
            await maybe_send_random_meme(message.chat.id, trigger="video", target_user_id=message.from_user.id)
        except Exception as e:
            logger.warning(f"Failed to send random meme after video: {e}")


@router.message(F.audio)
async def collect_audio(message: Message):
    """Сбор аудио/музыки + сохранение в коллекцию + обновление профиля"""
    if message.chat.type == "private":
        return
    
    # Проверяем реплай на бота (кэшировано)
    if await is_reply_to_bot(message):
        await handle_bot_mention_or_reply(message)
    
    audio = message.audio
    caption = message.caption[:200] if message.caption else ""
    reply_to_user_id = None
    if message.reply_to_message and message.reply_to_message.from_user:
        reply_to_user_id = message.reply_to_message.from_user.id
    
    await save_chat_message(
        chat_id=message.chat.id,
        user_id=message.from_user.id,
        username=message.from_user.username or "",
        first_name=message.from_user.first_name or "Аноним",
        message_text=caption,
        message_type="audio",
        file_id=audio.file_id if audio else None,
        file_unique_id=audio.file_unique_id if audio else None
    )
    
    # ОБНОВЛЯЕМ ПРОФИЛЬ (per-chat) — записываем музыкальные предпочтения
    if USE_POSTGRES:
        try:
            # Формируем текст с информацией о треке для профилирования
            profile_text = caption
            if audio:
                track_info = f"{audio.performer or 'Unknown'} - {audio.title or 'Unknown'}"
                profile_text = f"[музыка: {track_info}] {caption}".strip()
            
            await update_user_profile_comprehensive(
                user_id=message.from_user.id,
                chat_id=message.chat.id,
                message_text=profile_text if profile_text else "[audio]",
                timestamp=int(time.time()),
                first_name=message.from_user.first_name or "",
                username=message.from_user.username or "",
                reply_to_user_id=reply_to_user_id,
                message_type="audio"
            )
        except Exception as e:
            logger.warning(f"Profile update error (audio): {e}", exc_info=True)
    
    # Сохраняем аудио в коллекцию
    if audio:
        sender_name = message.from_user.first_name or "Аноним"
        # Собираем информацию о треке
        title = audio.title or "Без названия"
        performer = audio.performer or sender_name
        duration = audio.duration or 0
        await save_media(
            chat_id=message.chat.id,
            user_id=message.from_user.id,
            file_id=audio.file_id,
            file_type="audio",
            file_unique_id=audio.file_unique_id,
            description=f"{performer} - {title} ({duration} сек)",
            caption=caption
        )
    
    # Шанс 15% для теста
    if random.random() < 0.15:
        try:
            await maybe_send_random_meme(message.chat.id, trigger="audio", target_user_id=message.from_user.id)
        except Exception as e:
            logger.warning(f"Failed to send random meme after audio: {e}")


# ==================== СИСТЕМА МЕМОВ ====================

# Комментарии Тёти Розы к мемам
MEME_COMMENTS = [
    "О, вспомнила! Вот это было, блять... 🤔",
    "А помните эту хуйню? Я — да.",
    "Нашла в архивах. Классика жанра.",
    "Это вы скидывали. Я сохранила. Теперь страдайте.",
    "Из коллекции 'Лучшее'. Ну как лучшее... что было.",
    "Держите, чтоб не расслаблялись.",
    "Вот что бывает, когда форточку открываешь. Мемы залетают.",
    "Рандом выбрал именно это. Судьба.",
    "Тётя Роза делится культурой.",
    "Из личной коллекции. Цените.",
    "Это @кто-то кидал. Теперь все увидят снова.",
    "Мем дня. Или ночи. Хуй знает который час.",
    "Ваши мемы — моя боль. Вот.",
    "Архив открыт. Берите что дают.",
    "Культурная программа от Тёти Розы.",
]

# Комментарии к голосовым сообщениям
VOICE_COMMENTS = [
    "🎤 Нашла в архиве чьё-то пьяное бормотание. Наслаждайтесь.",
    "🎤 Кто-то это записывал. Теперь все послушают.",
    "🎤 Голосовуха из прошлого. Компромат навеки.",
    "🎤 Тётя Роза нашла аудиодоказательство вашей тупости.",
    "🎤 Это кто-то из вас наговорил. Теперь не отвертитесь.",
    "🎤 Архив голосовух открыт. Стыдитесь.",
    "🎤 Рандомная голосовуха. Возможно, пьяная. Скорее всего — да.",
    "🎤 Нашла это в закромах. Кто записывал — молодец. Нет.",
    "🎤 Голос из прошлого. Напоминание о ваших грехах.",
    "🎤 Кто-то думал, что это останется между нами. Ха-ха.",
    "🎤 Аудиопривет из архива Тёти Розы.",
    "🎤 Слушайте и плачьте. Или смейтесь. Мне похуй.",
    "🎤 Это записали трезвым? Сомневаюсь.",
    "🎤 Голосовое сообщение эпохи. Какой эпохи — хуй знает.",
    "🎤 Компромат дня. Или ночи. Зависит от того, когда записывали.",
]

# Комментарии к кружочкам
VIDEO_NOTE_COMMENTS = [
    "🔵 Кружочек из прошлого! Кто-то показал ебало.",
    "🔵 Нашла видосик. Лицо — огонь. В плохом смысле.",
    "🔵 Архивный кружок. Смотрите на это лицо и думайте о жизни.",
    "🔵 Кто-то записал это. Теперь не развидеть.",
]

# Комментарии к видео
VIDEO_COMMENTS = [
    "📹 Видосик из архива! Кто-то снял эту хуйню.",
    "📹 Нашла видео в закромах. Наслаждайтесь.",
    "📹 Архивное видео. Кинематограф уровня 'бог'.",
    "📹 Кто-то это снял и отправил. Теперь смотрите все.",
    "📹 Видео дня. Качество — говно, контент — огонь.",
    "📹 Из коллекции видосов Тёти Розы.",
    "📹 Рандомное видео. Судьба выбрала именно это.",
    "📹 Архив открыт. Видеосекция.",
    "📹 Кто снимал — молодец. Или нет. Смотрите сами.",
    "📹 Культурное наследие чата в видеоформате.",
]

# Комментарии к аудио
AUDIO_COMMENTS = [
    "🎵 Музычка из архива! Кто-то это слушал.",
    "🎵 Нашла трек в закромах. Врубайте.",
    "🎵 Аудио из коллекции. Вкусы у вас... интересные.",
    "🎵 Рандомный трек. DJ Тётя Роза в деле.",
    "🎵 Музыкальный привет из прошлого.",
    "🎵 Кто-то это кидал. Теперь слушайте все.",
    "🎵 Из плейлиста Тёти Розы. Цените.",
    "🎵 Аудиокультура чата. Наслаждайтесь.",
    "🎵 Трек дня. Или ночи. Зависит от настроения.",
    "🎵 Музыкальный архив открыт. Держите.",
]


async def maybe_send_random_meme(chat_id: int, trigger: str = "random", target_user_id: int = None):
    """Отправить случайный мем из коллекции (если есть). Комментарий персонализирован."""
    if not USE_POSTGRES:
        return
    
    try:
        media = await get_random_media(chat_id)
        if not media:
            return
        
        file_id = media['file_id']
        file_type = media['file_type']
        media_id = media['id']
        description = media.get('description', '')
        
        # Получаем профиль пользователя для персонализации комментария (per-chat!)
        user_profile = {}
        if target_user_id:
            try:
                user_profile = await get_user_profile_for_ai(target_user_id, chat_id, "", "")
            except Exception:
                pass
        
        # Выбираем комментарий в зависимости от типа
        if file_type == "voice":
            comment = random.choice(VOICE_COMMENTS)
        elif file_type == "video_note":
            comment = random.choice(VIDEO_NOTE_COMMENTS)
        elif file_type == "video":
            comment = random.choice(VIDEO_COMMENTS)
        elif file_type == "audio":
            comment = random.choice(AUDIO_COMMENTS)
        else:
            comment = random.choice(MEME_COMMENTS)
        
        # Персонализированные добавки к комментарию (20% шанс)
        if user_profile and random.random() < 0.20:
            interests = user_profile.get('interests', [])
            style = user_profile.get('communication_style', '')
            
            personalized_additions = []
            
            if 'gaming' in interests:
                personalized_additions.append(" Для геймера — самое то.")
            if 'crypto' in interests:
                personalized_additions.append(" Криптанам посвящается.")
            if 'memes' in interests:
                personalized_additions.append(" Знаток мемов оценит.")
            if style == 'humorous':
                personalized_additions.append(" Шутнику должно зайти.")
            if style == 'toxic':
                personalized_additions.append(" Для токсика в самый раз.")
            if user_profile.get('is_night_owl'):
                personalized_additions.append(" Ночным совам привет.")
            
            if personalized_additions:
                comment += random.choice(personalized_additions)
        
        # Отправляем в зависимости от типа
        if file_type == "photo":
            await bot.send_photo(chat_id, file_id, caption=comment)
        elif file_type == "sticker":
            await bot.send_sticker(chat_id, file_id)
            await bot.send_message(chat_id, comment)
        elif file_type == "animation":
            await bot.send_animation(chat_id, file_id, caption=comment)
        elif file_type == "voice":
            await bot.send_message(chat_id, comment)
            await bot.send_voice(chat_id, file_id)
        elif file_type == "video_note":
            await bot.send_message(chat_id, comment)
            await bot.send_video_note(chat_id, file_id)
        elif file_type == "video":
            await bot.send_video(chat_id, file_id, caption=comment)
        elif file_type == "audio":
            await bot.send_message(chat_id, comment)
            await bot.send_audio(chat_id, file_id)
        
        # Увеличиваем счётчик использования
        await increment_media_usage(media_id)
        logger.info(f"Sent random meme (type={file_type}) to chat {chat_id}, trigger={trigger}")
        
    except Exception as e:
        logger.warning(f"Could not send random meme: {e}")


@router.message(Command("meme", "мем", "мемас", "рандом"))
async def cmd_random_meme(message: Message):
    """Получить случайный мем из коллекции чата"""
    if message.chat.type == "private":
        await message.answer("❌ Мемы работают только в групповых чатах!")
        return
    
    chat_id = message.chat.id
    
    # Кулдаун 10 секунд
    can_do, remaining = check_cooldown(message.from_user.id, chat_id, "meme", 10)
    if not can_do:
        await message.answer(f"⏰ Подожди {remaining} сек, мемов не напасёшься!")
        return
    
    if not USE_POSTGRES:
        await message.answer("❌ Коллекция мемов недоступна")
        return
    
    # Получаем статистику
    stats = await get_media_stats(chat_id)
    
    if stats['total'] == 0:
        await message.answer(
            "📭 Коллекция мемов пуста!\n\n"
            "Кидайте картинки, стикеры, гифки, голосовые и кружочки — "
            "Тётя Роза всё запомнит и будет выдавать рандомно."
        )
        return
    
    # Определяем тип (если указан)
    args = message.text.split()
    file_type = None
    if len(args) > 1:
        type_map = {
            "фото": "photo", "photo": "photo", "картинка": "photo",
            "стикер": "sticker", "sticker": "sticker",
            "гиф": "animation", "gif": "animation", "гифка": "animation",
            "голосовое": "voice", "voice": "voice", "войс": "voice", "голосовуха": "voice",
            "кружок": "video_note", "кружочек": "video_note",
            "видео": "video", "video": "video", "видос": "video", "видосик": "video",
            "аудио": "audio", "audio": "audio", "музыка": "audio", "трек": "audio"
        }
        file_type = type_map.get(args[1].lower())
    
    media = await get_random_media(chat_id, file_type)
    
    if not media:
        await message.answer("📭 Мемов такого типа нет. Кидайте больше!")
        return
    
    file_id = media['file_id']
    media_type = media['file_type']
    media_id = media['id']
    
    try:
        # Выбираем комментарий по типу медиа
        if media_type == "voice":
            comment = random.choice(VOICE_COMMENTS)
        elif media_type == "video_note":
            comment = random.choice(VIDEO_NOTE_COMMENTS)
        else:
            comment = random.choice(MEME_COMMENTS)
        
        if media_type == "photo":
            await message.answer_photo(file_id, caption=comment)
        elif media_type == "sticker":
            await message.answer_sticker(file_id)
            await message.answer(comment)
        elif media_type == "animation":
            await message.answer_animation(file_id, caption=comment)
        elif media_type == "voice":
            await message.answer(comment)
            await message.answer_voice(file_id)
        elif media_type == "video_note":
            await message.answer(comment)
            await message.answer_video_note(file_id)
        elif media_type == "video":
            await message.answer_video(file_id, caption=comment)
        elif media_type == "audio":
            await message.answer(comment)
            await message.answer_audio(file_id)
        else:
            # Неизвестный тип — просто логируем
            logger.warning(f"Unknown media type: {media_type}")
            await message.answer("❓ Странный мем — не знаю как отправить.")
            return
        
        await increment_media_usage(media_id)
        metrics.track_command("meme")
        
    except TelegramBadRequest as e:
        # Обрабатываем устаревшие file_id или другие ошибки Telegram
        error_msg = str(e).lower()
        if "file" in error_msg or "invalid" in error_msg or "not found" in error_msg:
            logger.warning(f"Invalid/expired file_id for media {media_id}: {e}")
            await message.answer("❌ Этот мем протух. Попробуй ещё раз — выдам другой!")
        else:
            logger.error(f"Telegram error sending meme: {e}")
            await message.answer("❌ Telegram не принял мем. Попробуй позже.")
    except Exception as e:
        logger.error(f"Error sending meme: {e}")
        await message.answer("❌ Мем сломался. Попробуй ещё раз.")


@router.message(Command("мемчик", "makememe"))
async def cmd_ai_meme(message: Message, command: CommandObject):
    """Генерация мема через Supermeme AI — по теме или по участнику чата"""
    supermeme_key = os.getenv("SUPERMEME_API_KEY", "")
    if not supermeme_key:
        await message.answer("❌ SUPERMEME_API_KEY не настроен")
        return

    can_do, remaining = check_cooldown(message.from_user.id, message.chat.id, "aimeme", 30)
    if not can_do:
        await message.answer(f"⏳ Подожди {remaining:.0f} сек")
        return

    args = (command.args or "").strip()

    # Определяем цель: реплай или просто текст
    target_user = None
    if message.reply_to_message and message.reply_to_message.from_user:
        target_user = message.reply_to_message.from_user
    elif not args:
        await message.answer(
            "🤣 <b>Генератор мемов</b>\n\n"
            "По теме:\n"
            "<code>/мемчик когда пятница но завтра понедельник</code>\n\n"
            "Про участника (реплай на его сообщение):\n"
            "<code>/мемчик</code> → ответь на сообщение человека",
            parse_mode=ParseMode.HTML
        )
        return

    processing = await message.answer("🤣 Делаю мем...")
    try:
        session = await get_http_session()
        meme_text = args

        if target_user:
            # Режим персонального мема — читаем сообщения человека
            target_name = target_user.first_name or target_user.username or "Аноним"
            context = ""
            if USE_POSTGRES:
                try:
                    user_msgs = await get_user_messages(message.chat.id, target_user.id, limit=25)
                    if user_msgs:
                        context = "\n".join([m.get("message_text", "") for m in reversed(user_msgs) if m.get("message_text")])[:1500]
                except Exception:
                    pass

            ai_gateway_key = os.getenv("VERCEL_AI_GATEWAY_KEY", "")
            if not ai_gateway_key:
                await processing.edit_text("❌ VERCEL_AI_GATEWAY_KEY не настроен")
                return

            async with session.post(
                "https://ai-gateway.vercel.sh/v1/chat/completions",
                json={
                    "model": "anthropic/claude-sonnet-4-20250514",
                    "max_tokens": 100,
                    "messages": [{"role": "user", "content": (
                        f"Придумай одну смешную фразу для мема про человека по имени {target_name}. "
                        f"Используй его реальные высказывания и характер из переписки. "
                        f"Формат: короткая ситуация как в мемах (например: '{target_name} когда говорит щас и появляется через час'). "
                        f"ВАЖНО: пиши ТОЛЬКО на русском языке, никакого английского. "
                        f"Только сама фраза, без кавычек, скобок и объяснений.\n\n"
                        f"Сообщения {target_name}:\n{context}"
                    )}]
                },
                headers={
                    "Authorization": f"Bearer {ai_gateway_key}",
                    "Content-Type": "application/json"
                },
                timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                raw = await resp.text()
                if resp.status != 200:
                    await processing.edit_text(f"❌ Ошибка генерации текста: {raw[:150]}")
                    return
                gw_result = json.loads(raw)
            meme_text = gw_result.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
            if not meme_text:
                await processing.edit_text("❌ Текст для мема не придумался")
                return

        # Генерируем мем через Supermeme
        async with session.post(
            "https://app.supermeme.ai/api/v2/meme/image",
            json={"text": meme_text, "count": 1, "aspectRatio": "1:1"},
            headers={
                "Authorization": f"Bearer {supermeme_key}",
                "Content-Type": "application/json"
            },
            timeout=aiohttp.ClientTimeout(total=30)
        ) as resp:
            raw = await resp.text()
            logger.info(f"Supermeme status={resp.status} body={raw[:200]}")
            if resp.status != 200:
                await processing.edit_text(f"❌ Supermeme ошибка ({resp.status}): {raw[:150]}")
                return
            result = json.loads(raw)

        memes = result.get("memes", [])
        if not memes:
            await processing.edit_text("❌ Мем не сгенерировался")
            return

        async with session.get(memes[0], timeout=aiohttp.ClientTimeout(total=30)) as img_resp:
            img_data = await img_resp.read()

        await processing.delete()
        await message.answer_photo(
            BufferedInputFile(img_data, filename="meme.png"),
            caption=f"🤣 <i>{meme_text}</i>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"AI meme error: {e}")
        await processing.edit_text(f"❌ Ошибка: {e}")


@router.message(Command("зарисовка", "sketch", "illus"))
async def cmd_ai_visual(message: Message, command: CommandObject):
    """Минималистичная иллюстрация через Supermeme Text-to-visuals — по теме или про участника"""
    supermeme_key = os.getenv("SUPERMEME_API_KEY", "")
    if not supermeme_key:
        await message.answer("❌ SUPERMEME_API_KEY не настроен")
        return

    can_do, remaining = check_cooldown(message.from_user.id, message.chat.id, "visual", 30)
    if not can_do:
        await message.answer(f"⏳ Подожди {remaining:.0f} сек")
        return

    args = (command.args or "").strip()
    target_user = None
    if message.reply_to_message and message.reply_to_message.from_user:
        target_user = message.reply_to_message.from_user
    elif not args:
        await message.answer(
            "🎨 <b>Минималистичная иллюстрация</b>\n\n"
            "По теме (сравнения и метафоры работают лучше всего):\n"
            "<code>/зарисовка программист vs дедлайн</code>\n"
            "<code>/зарисовка ожидание vs реальность</code>\n\n"
            "Про участника (реплай на его сообщение):\n"
            "<code>/зарисовка</code> → ответь на сообщение человека",
            parse_mode=ParseMode.HTML
        )
        return

    processing = await message.answer("🎨 Рисую...")
    try:
        session = await get_http_session()
        visual_text = args

        if target_user:
            target_name = target_user.first_name or target_user.username or "Аноним"
            context = ""
            if USE_POSTGRES:
                try:
                    user_msgs = await get_user_messages(message.chat.id, target_user.id, limit=25)
                    if user_msgs:
                        context = "\n".join([m.get("message_text", "") for m in reversed(user_msgs) if m.get("message_text")])[:1500]
                except Exception:
                    pass

            ai_gateway_key = os.getenv("VERCEL_AI_GATEWAY_KEY", "")
            if not ai_gateway_key:
                await processing.edit_text("❌ VERCEL_AI_GATEWAY_KEY не настроен")
                return

            async with session.post(
                "https://ai-gateway.vercel.sh/v1/chat/completions",
                json={
                    "model": "anthropic/claude-sonnet-4-20250514",
                    "max_tokens": 80,
                    "messages": [{"role": "user", "content": (
                        f"Придумай тему для минималистичной иллюстрации про человека по имени {target_name}. "
                        f"Формат: короткое сравнение или метафора (например: '{target_name} vs дедлайны', '{target_name} ожидание vs реальность'). "
                        f"ТОЛЬКО на русском языке. Только сама тема, без объяснений.\n\n"
                        f"Сообщения {target_name}:\n{context}"
                    )}]
                },
                headers={
                    "Authorization": f"Bearer {ai_gateway_key}",
                    "Content-Type": "application/json"
                },
                timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                raw = await resp.text()
                if resp.status != 200:
                    await processing.edit_text(f"❌ Ошибка генерации темы: {raw[:150]}")
                    return
                gw_result = json.loads(raw)
            visual_text = gw_result.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
            if not visual_text:
                await processing.edit_text("❌ Тема не придумалась")
                return

        async with session.post(
            "https://app.supermeme.ai/api/v1/minimalist-visual",
            json={"text": visual_text, "count": 1, "aspectRatio": "1:1"},
            headers={
                "Authorization": f"Bearer {supermeme_key}",
                "Content-Type": "application/json"
            },
            timeout=aiohttp.ClientTimeout(total=40)
        ) as resp:
            raw = await resp.text()
            logger.info(f"Supermeme visual status={resp.status} body={raw[:200]}")
            if resp.status != 200:
                await processing.edit_text(f"❌ Ошибка ({resp.status}): {raw[:150]}")
                return
            result = json.loads(raw)

        images = result.get("images", [])
        if not images:
            await processing.edit_text("❌ Иллюстрация не сгенерировалась")
            return

        async with session.get(images[0], timeout=aiohttp.ClientTimeout(total=30)) as img_resp:
            img_data = await img_resp.read()

        await processing.delete()
        await message.answer_photo(
            BufferedInputFile(img_data, filename="visual.png"),
            caption=f"🎨 <i>{visual_text}</i>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"AI visual error: {e}")
        await processing.edit_text(f"❌ Ошибка: {e}")


@router.message(Command("memestats", "мемы"))
async def cmd_meme_stats(message: Message):
    """Статистика коллекции мемов"""
    if message.chat.type == "private":
        return
    
    if not USE_POSTGRES:
        await message.answer("❌ Коллекция мемов недоступна")
        return
    
    stats = await get_media_stats(message.chat.id)
    
    text = f"""🎭 КОЛЛЕКЦИЯ МЕМОВ ЧАТА

📊 Всего: {stats.get('total', 0)} медиа

По типам:
🖼 Фото: {stats.get('photo', 0)}
😀 Стикеры: {stats.get('sticker', 0)}
🎬 Гифки: {stats.get('animation', 0)}
🎤 Голосовые: {stats.get('voice', 0)}
🔵 Кружочки: {stats.get('video_note', 0)}

💡 Кидайте мемы, голосовые, кружочки — бот запоминает и выдаёт!
Команда /мем — получить рандомный мем
"""
    await message.answer(text)


# ==================== ОЧИСТКА И МОНИТОРИНГ ====================

async def scheduled_cleanup():
    """Периодическая очистка старых данных (запускается каждые 6 часов)"""
    if not USE_POSTGRES:
        return
    
    try:
        results = await full_cleanup()
        logger.info(f"🧹 Автоочистка БД: {results}")
    except Exception as e:
        logger.error(f"❌ Ошибка очистки БД: {e}")


async def log_database_stats():
    """Логирование статистики БД (запускается каждый час)"""
    if not USE_POSTGRES:
        return
    
    try:
        stats = await get_database_stats()
        logger.info(
            f"📊 Статистика БД: "
            f"сообщений={stats.get('chat_messages_count', 0)}, "
            f"за 24ч={stats.get('messages_24h', 0)}, "
            f"чатов={stats.get('active_chats_24h', 0)}, "
            f"сводок={stats.get('chat_summaries_count', 0)}, "
            f"памяти={stats.get('chat_memories_count', 0)}"
        )
    except Exception as e:
        logger.error(f"❌ Ошибка статистики БД: {e}")


async def scheduled_auto_summaries():
    """
    Автоматическая генерация сводок для активных чатов (каждые 6 часов).
    Сводки НЕ отправляются в чат — только сохраняются в память для обучения.
    """
    if not USE_POSTGRES:
        return
    
    try:
        # Получаем активные чаты (50+ сообщений за 12 часов)
        active_chats = await get_active_chats_for_auto_summary(min_messages=50, hours=12)
        
        if not active_chats:
            logger.info("🧠 Авто-сводки: нет активных чатов")
            return
        
        logger.info(f"🧠 Авто-сводки: найдено {len(active_chats)} активных чатов")
        
        summaries_created = 0
        memories_created = 0
        
        for chat_info in active_chats:
            chat_id = chat_info['chat_id']
            message_count = chat_info['message_count']
            
            try:
                # Получаем статистику чата
                stats = await get_chat_statistics(chat_id, hours=12)
                if not stats or stats.get('total_messages', 0) < 30:
                    continue
                
                # Получаем предыдущие сводки и память
                previous_summaries = await get_previous_summaries(chat_id, limit=2)
                memories = await get_memories(chat_id, limit=15)
                
                # Получаем профили для персонализации
                enriched = await get_enriched_chat_data_for_ai(chat_id, hours=12)
                user_profiles = enriched.get('profiles', [])
                
                # Генерируем сводку через API
                session = await get_http_session()
                try:
                    async with session.post(
                        VERCEL_API_URL,
                        json={
                            "messages": stats.get('recent_messages', [])[:100],
                            "stats": {
                                "total_messages": stats.get('total_messages', 0),
                                "unique_users": stats.get('unique_users', 0),
                                "top_authors": stats.get('top_authors', [])[:10],
                                "reply_pairs": stats.get('reply_pairs', [])[:5]
                            },
                            "chat_title": f"Чат {chat_id}",
                            "hours": 12,
                            "previous_summaries": previous_summaries,
                            "memories": memories,
                            "user_profiles": user_profiles,
                            "auto_mode": True  # Флаг для API — более краткая сводка
                        },
                        timeout=aiohttp.ClientTimeout(total=60)
                    ) as response:
                        if response.status == 200:
                            result = await response.json()
                            summary = result.get("summary", "")
                            
                            if summary and len(summary) > 50:
                                # Сохраняем сводку в старую систему
                                top_author = stats.get('top_authors', [{}])[0]
                                await save_summary(
                                    chat_id=chat_id,
                                    summary_text=summary[:2000],
                                    top_talker_username=top_author.get('username'),
                                    top_talker_name=top_author.get('first_name'),
                                    top_talker_count=top_author.get('msg_count'),
                                )
                                summaries_created += 1
                                
                                # УМНАЯ ПАМЯТЬ: Сохраняем в context_summaries
                                try:
                                    from database_postgres import save_context_summary
                                    now = int(time.time())
                                    
                                    # Извлекаем ключевые темы из топ-авторов
                                    key_topics = []
                                    for author in stats.get('top_authors', [])[:3]:
                                        name = author.get('first_name') or author.get('username')
                                        if name:
                                            key_topics.append(name)
                                    
                                    await save_context_summary(
                                        chat_id=chat_id,
                                        summary_type="hourly",
                                        summary_text=summary[:1500],
                                        period_start=now - 12*3600,  # 12 часов назад
                                        period_end=now,
                                        messages_count=stats.get('total_messages', 0),
                                        active_users=stats.get('unique_users', 0),
                                        key_topics=key_topics,
                                        mood_score=0.0  # TODO: вычислять из сообщений
                                    )
                                    logger.debug(f"✅ Context summary saved for chat {chat_id}")
                                except Exception as e:
                                    logger.debug(f"Could not save context summary: {e}")
                                
                                # Сохраняем воспоминания об активных участниках
                                for author in stats.get('top_authors', [])[:5]:
                                    author_user_id = author.get('user_id')
                                    if author_user_id and author.get('msg_count', 0) >= 10:
                                        await save_memory(
                                            chat_id=chat_id,
                                            user_id=author_user_id,
                                            username=author.get('username') or '',
                                            first_name=author.get('first_name') or '',
                                            memory_type="activity",
                                            memory_text=f"Был активен: {author.get('msg_count')} сообщений за 12ч",
                                            relevance_score=min(author.get('msg_count', 0) // 10, 10)
                                        )
                                        memories_created += 1
                                
                                # Сохраняем воспоминания о взаимодействиях
                                for pair in stats.get('reply_pairs', [])[:3]:
                                    pair_user_id = pair.get('user_id')
                                    if pair_user_id and pair.get('replies', 0) >= 5:
                                        await save_memory(
                                            chat_id=chat_id,
                                            user_id=pair_user_id,
                                            username=pair.get('username') or '',
                                            first_name=pair.get('first_name') or '',
                                            memory_type="relationship",
                                            memory_text=f"Общался с {pair.get('target_name', '???')}: {pair.get('replies')} реплаев",
                                            relevance_score=min(pair.get('replies', 0) // 5, 10)
                                        )
                                        memories_created += 1
                                
                except asyncio.TimeoutError:
                    logger.warning(f"⏰ Авто-сводка для чата {chat_id}: таймаут")
                except Exception as e:
                    logger.warning(f"❌ Авто-сводка для чата {chat_id}: {e}")
                
                # Небольшая пауза между чатами
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.warning(f"❌ Ошибка авто-сводки чата {chat_id}: {e}")
        
        logger.info(f"🧠 Авто-сводки завершены: {summaries_created} сводок, {memories_created} воспоминаний")
        
    except Exception as e:
        logger.error(f"❌ Ошибка авто-сводок: {e}")


async def cleanup_memory():
    """Очистка памяти (cooldowns и api_calls) — запускается каждые 10 минут"""
    try:
        cooldowns_before = len(cooldowns)
        api_calls_before = len(api_calls)
        
        cleanup_cooldowns()
        cleanup_api_calls()
        
        cooldowns_after = len(cooldowns)
        api_calls_after = len(api_calls)
        
        if cooldowns_before != cooldowns_after or api_calls_before != api_calls_after:
            logger.info(
                f"🧹 Очистка памяти: cooldowns {cooldowns_before}→{cooldowns_after}, "
                f"api_calls {api_calls_before}→{api_calls_after}"
            )
    except Exception as e:
        logger.error(f"❌ Ошибка очистки памяти: {e}")


# ==================== АДМИНКА ====================

# ID администраторов (добавь свой Telegram ID)
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}


def admin_only(func):
    """Декоратор для админских команд"""
    @functools.wraps(func)
    async def wrapper(message: Message, *args, **kwargs):
        if message.chat.type != "private":
            return
        if not is_admin(message.from_user.id):
            return
        return await func(message, *args, **kwargs)
    return wrapper


def admin_postgres_only(func):
    """Декоратор для админских команд, требующих PostgreSQL"""
    @functools.wraps(func)
    async def wrapper(message: Message, *args, **kwargs):
        if message.chat.type != "private":
            return
        if not is_admin(message.from_user.id):
            return
        if not USE_POSTGRES:
            await message.answer("❌ Доступно только с PostgreSQL")
            return
        return await func(message, *args, **kwargs)
    return wrapper


def is_admin(user_id: int) -> bool:
    """Проверить, является ли пользователь админом"""
    # Если ADMIN_IDS не настроен — запрещаем всем (безопасность)
    if not ADMIN_IDS:
        logger.warning(f"ADMIN_IDS не настроен! Пользователь {user_id} пытался использовать админку.")
        return False
    return user_id in ADMIN_IDS


@router.message(Command("admin", "админ", "panel"))
async def cmd_admin(message: Message):
    """Главное меню админки"""
    if message.chat.type != "private":
        await message.answer("❌ Админка работает только в личке!")
        return
    
    if not is_admin(message.from_user.id):
        await message.answer("❌ У тебя нет прав админа!")
        return
    
    text = """🔐 *АДМИН-ПАНЕЛЬ ТЁТИ РОЗЫ*

📊 *Статистика:*
/dbstats — общая статистика БД
/chats — список всех чатов
/topusers — топ пользователей
/metrics — метрики бота (аптайм, команды)
/userstats — статистика профилей

🔍 *Поиск:*
/chat `<id>` — инфо о чате
/finduser `<имя>` — поиск пользователя
/rawprofile `<id>` — сырой профиль юзера

🛠 *Управление:*
/cleanup — очистка старых данных
/health — проверка состояния системы
/migrate\_media — миграция медиа в коллекцию
/migrate\_users — миграция пользователей в реестр
/vk\_import — импорт мемов из VK

💡 _Твой ID:_ `{}`
""".format(message.from_user.id)
    
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)


@router.message(Command("dbstats", "stats_db"))
async def cmd_dbstats(message: Message):
    """Расширенная статистика базы данных"""
    if message.chat.type != "private":
        return
    
    if not is_admin(message.from_user.id):
        return
    
    if not USE_POSTGRES:
        await message.answer("❌ Статистика доступна только с PostgreSQL")
        return
    
    try:
        processing = await message.answer("📊 Собираю статистику...")
        stats = await get_database_stats()
        
        # Получаем статистику реестра пользователей
        chat_users_count = 0
        unique_users_in_messages = 0
        try:
            from database_postgres import get_pool
            async with (await get_pool()).acquire() as conn:
                chat_users_count = await conn.fetchval("SELECT COUNT(*) FROM chat_users") or 0
                unique_users_in_messages = await conn.fetchval(
                    "SELECT COUNT(DISTINCT (chat_id, user_id)) FROM chat_messages"
                ) or 0
        except Exception:
            pass
        
        text = f"""📊 *ПОЛНАЯ СТАТИСТИКА БОТА*

🌐 *Охват:*
• Всего чатов: *{stats.get('total_chats', 0):,}*
• Активных чатов (24ч): *{stats.get('active_chats_24h', 0)}*
• Всего пользователей: *{stats.get('total_users', 0):,}*

👥 *Реестр пользователей:*
• В chat\_users: *{chat_users_count:,}*
• Уникальных в сообщениях: *{unique_users_in_messages:,}*

📝 *Сообщения:*
• Всего в БД: {stats.get('chat_messages_count', 0):,}
• За 24 часа: {stats.get('messages_24h', 0):,}
• Хранятся: {stats.get('oldest_message_days', 0)} дней

🧠 *Память:*
• Сводок: {stats.get('chat_summaries_count', 0):,}
• Воспоминаний: {stats.get('chat_memories_count', 0):,}

🎮 *RPG система:*
• Игроков: {stats.get('players_count', 0):,}
• Достижений: {stats.get('achievements_count', 0):,}
• Событий в логе: {stats.get('event_log_count', 0):,}

💰 *Экономика:*
• Общак всех чатов: {stats.get('total_treasury', 0):,} 💎
"""
        await processing.edit_text(text, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


@router.message(Command("chats", "чаты"))
async def cmd_chats(message: Message):
    """Список всех чатов с статистикой"""
    if message.chat.type != "private" or not is_admin(message.from_user.id):
        return
    
    if not USE_POSTGRES:
        await message.answer("❌ Доступно только с PostgreSQL")
        return
    
    try:
        processing = await message.answer("📋 Загружаю список чатов...")
        chats = await get_all_chats_stats()
        
        if not chats:
            await processing.edit_text("📭 Нет данных о чатах")
            return
        
        from datetime import datetime
        
        lines = ["📋 СПИСОК ЧАТОВ\n"]
        for i, chat in enumerate(chats[:20], 1):
            chat_id = chat['chat_id']
            title = chat.get('chat_title')
            username = chat.get('chat_username')
            total = chat['total_messages']
            users = chat['unique_users']
            today = chat['messages_24h']
            last = chat['last_activity']
            
            # Если нет инфо — получаем из Telegram API
            if not title and not username:
                try:
                    tg_chat = await bot.get_chat(chat_id)
                    title = tg_chat.title
                    username = tg_chat.username
                    # Сохраняем в БД на будущее
                    await save_chat_info(chat_id, title, username, tg_chat.type)
                except Exception:
                    title = None
                    username = None
            
            # Форматируем время последней активности
            if last:
                last_dt = datetime.fromtimestamp(last)
                last_str = last_dt.strftime("%d.%m %H:%M")
            else:
                last_str = "—"
            
            # Определяем активность
            if today > 100:
                status = "🔥"
            elif today > 20:
                status = "✅"
            elif today > 0:
                status = "💤"
            else:
                status = "💀"
            
            # Формируем название чата
            if username:
                chat_name = f"@{username}"
            elif title:
                chat_name = title[:30].replace('_', ' ').replace('*', '')
            else:
                chat_name = f"Чат {chat_id}"
            
            lines.append(
                f"{status} {chat_name}\n"
                f"   📝 {total:,} | 👥 {users} | 🕐 {last_str}"
            )
        
        if len(chats) > 20:
            lines.append(f"\n...и ещё {len(chats) - 20} чатов")
        
        lines.append(f"\n💡 Детали: /chat <id>")
        
        await processing.edit_text("\n".join(lines))
    except Exception as e:
        logger.error(f"Error in chats: {e}")
        await message.answer(f"❌ Ошибка: {e}")


@router.message(Command("chat"))
async def cmd_chat_details(message: Message):
    """Детальная информация о чате"""
    if message.chat.type != "private" or not is_admin(message.from_user.id):
        return
    
    if not USE_POSTGRES:
        await message.answer("❌ Доступно только с PostgreSQL")
        return
    
    # Парсим chat_id из команды
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("❌ Укажи ID чата: `/chat -1001234567890`", parse_mode=ParseMode.MARKDOWN)
        return
    
    try:
        chat_id = int(parts[1])
    except ValueError:
        await message.answer("❌ Неверный ID чата!")
        return
    
    try:
        processing = await message.answer(f"🔍 Загружаю данные чата {chat_id}...")
        stats = await get_chat_details(chat_id)
        
        if not stats or not stats.get('total_messages'):
            await processing.edit_text(f"📭 Чат {chat_id} не найден")
            return
        
        from datetime import datetime
        
        # Название чата — получаем из БД или Telegram API
        chat_title = stats.get('chat_title')
        chat_username = stats.get('chat_username')
        
        if not chat_title and not chat_username:
            try:
                tg_chat = await bot.get_chat(chat_id)
                chat_title = tg_chat.title
                chat_username = tg_chat.username
                await save_chat_info(chat_id, chat_title, chat_username, tg_chat.type)
            except Exception:
                pass
        
        chat_name = f"@{chat_username}" if chat_username else (chat_title or f"Чат {chat_id}").replace('_', ' ')
        
        first = stats.get('first_message')
        last = stats.get('last_message')
        first_str = datetime.fromtimestamp(first).strftime("%d.%m.%Y") if first else "—"
        last_str = datetime.fromtimestamp(last).strftime("%d.%m.%Y %H:%M") if last else "—"
        
        text = f"""📊 ЧАТ: {chat_name}
ID: {chat_id}

📝 Сообщения:
• Всего: {stats.get('total_messages', 0):,}
• За 24ч: {stats.get('messages_24h', 0):,}

👥 Пользователи:
• Уникальных: {stats.get('unique_users', 0)}
• Игроков RPG: {stats.get('players_count', 0)}

🧠 Память:
• Сводок: {stats.get('summaries_count', 0)}
• Воспоминаний: {stats.get('memories_count', 0)}

💰 Общак: {stats.get('treasury', 0):,} 💎

📅 Период:
• Первое сообщение: {first_str}
• Последнее: {last_str}
"""
        
        # Топ пользователей
        top_users = stats.get('top_users', [])
        if top_users:
            text += "\n🏆 Топ пользователей:\n"
            for i, u in enumerate(top_users[:5], 1):
                name = u.get('first_name', '?').replace('_', ' ')
                username = u.get('username')
                count = u.get('msg_count', 0)
                user_str = f"@{username}" if username else name
                text += f"{i}. {user_str} — {count:,}\n"
        
        await processing.edit_text(text)
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


@router.message(Command("topusers", "топюзеры"))
async def cmd_top_users(message: Message):
    """Топ пользователей по всем чатам"""
    if message.chat.type != "private" or not is_admin(message.from_user.id):
        return
    
    if not USE_POSTGRES:
        await message.answer("❌ Доступно только с PostgreSQL")
        return
    
    try:
        processing = await message.answer("🏆 Загружаю топ пользователей...")
        users = await get_top_users_global(20)
        
        if not users:
            await processing.edit_text("📭 Нет данных")
            return
        
        lines = ["🏆 *ТОП ПОЛЬЗОВАТЕЛЕЙ (все чаты)*\n"]
        for i, u in enumerate(users, 1):
            name = u.get('first_name', '?')
            username = u.get('username')
            total = u.get('total_messages', 0)
            chats = u.get('chats_count', 0)
            
            user_str = f"@{username}" if username else name
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            
            lines.append(f"{medal} {user_str}\n   📝 {total:,} сообщ. в {chats} чатах")
        
        await processing.edit_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


@router.message(Command("finduser", "найти"))
async def cmd_find_user(message: Message):
    """Поиск пользователя по имени"""
    if message.chat.type != "private" or not is_admin(message.from_user.id):
        return
    
    if not USE_POSTGRES:
        await message.answer("❌ Доступно только с PostgreSQL")
        return
    
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("❌ Укажи имя: `/finduser Вася`", parse_mode=ParseMode.MARKDOWN)
        return
    
    query = parts[1].strip()
    
    try:
        users = await search_user(query)
        
        if not users:
            await message.answer(f"📭 Пользователи по запросу '{query}' не найдены")
            return
        
        lines = [f"🔍 *Результаты поиска:* _{query}_\n"]
        for u in users[:15]:
            user_id = u.get('user_id')
            name = u.get('first_name', '?')
            username = u.get('username')
            msgs = u.get('messages', 0)
            
            user_str = f"@{username}" if username else name
            lines.append(f"• {user_str} (`{user_id}`)\n  📝 {msgs:,} сообщений")
        
        await message.answer("\n".join(lines), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


@router.message(Command("health", "здоровье"))
async def cmd_health(message: Message):
    """Проверка состояния системы"""
    if message.chat.type != "private" or not is_admin(message.from_user.id):
        return
    
    processing = await message.answer("🔍 Проверяю системы...")
    
    checks = []
    
    # Проверка БД
    if USE_POSTGRES:
        try:
            db_ok = await health_check()
            checks.append(f"{'✅' if db_ok else '❌'} PostgreSQL: {'OK' if db_ok else 'FAIL'}")
        except Exception as e:
            err_msg = str(e)[:50].replace('_', ' ')
            checks.append(f"❌ PostgreSQL: {err_msg}")
    else:
        checks.append("⚠️ PostgreSQL: не используется (SQLite)")
    
    # Проверка бота
    try:
        me = await bot.get_me()
        checks.append(f"✅ Бот: @{me.username} (ID: {me.id})")
    except Exception as e:
        err_msg = str(e)[:50].replace('_', ' ')
        checks.append(f"❌ Бот: {err_msg}")
    
    # Проверка планировщика
    if scheduler.running:
        jobs = len(scheduler.get_jobs())
        checks.append(f"✅ Планировщик: {jobs} задач")
    else:
        checks.append("⚠️ Планировщик: не запущен")
    
    # Память cooldowns
    checks.append(f"📊 Кулдауны в памяти: {len(cooldowns)}")
    
    # Платформа
    import platform
    plat_info = f"{platform.system()} {platform.release()}".replace('_', '-')
    checks.append(f"🖥 Платформа: {plat_info}")
    
    text = "🏥 СОСТОЯНИЕ СИСТЕМЫ\n\n" + "\n".join(checks)
    await processing.edit_text(text)


@router.message(Command("metrics", "метрики"))
async def cmd_metrics(message: Message):
    """Показать метрики бота"""
    if message.chat.type != "private" or not is_admin(message.from_user.id):
        return
    
    stats = metrics.get_stats()
    
    top_cmds = "\n".join([f"  • {cmd}: {count}" for cmd, count in stats['top_commands']]) or "  Нет данных"
    api_calls_text = "\n".join([f"  • {api}: {count}" for api, count in stats['api_calls'].items()]) or "  Нет данных"
    
    text = f"""📈 МЕТРИКИ БОТА

⏱ Аптайм: {stats['uptime_human']}

📊 Команды ({stats['total_commands']} всего):
{top_cmds}

🌐 API вызовы ({stats['total_api_calls']} всего):
{api_calls_text}

❌ Ошибок: {stats['errors']}
📦 Cooldowns в памяти: {len(cooldowns)}
🔄 Rate limits: {len(api_calls)} записей
"""
    await message.answer(text)


@router.message(Command("cleanup", "clean_db"))
async def cmd_cleanup(message: Message):
    """Ручная очистка БД"""
    if message.chat.type != "private" or not is_admin(message.from_user.id):
        return
    
    if not USE_POSTGRES:
        await message.answer("❌ Очистка доступна только с PostgreSQL")
        return
    
    try:
        processing = await message.answer("🧹 Запускаю очистку...")
        results = await full_cleanup()
        
        await processing.edit_text(
            f"✅ *Очистка завершена!*\n\n"
            f"🗑 Сообщений: {results.get('messages_deleted', 0):,}\n"
            f"📜 Сводок: {results.get('summaries_deleted', 0)}\n"
            f"🧠 Воспоминаний: {results.get('memories_deleted', 0)}\n"
            f"📋 Событий: {results.get('events_deleted', 0)}",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка очистки: {e}")


@router.message(Command("userstats", "user_stats"))
async def cmd_userstats(message: Message):
    """Статистика профилей пользователей"""
    if message.chat.type != "private" or not is_admin(message.from_user.id):
        return
    
    if not USE_POSTGRES:
        await message.answer("❌ Статистика доступна только с PostgreSQL")
        return
    
    try:
        from database_postgres import get_pool
        async with (await get_pool()).acquire() as conn:
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_profiles,
                    COUNT(CASE WHEN detected_gender = 'мужской' THEN 1 END) as males,
                    COUNT(CASE WHEN detected_gender = 'женский' THEN 1 END) as females,
                    COUNT(CASE WHEN detected_gender = 'unknown' THEN 1 END) as unknown,
                    COUNT(CASE WHEN activity_level = 'hyperactive' THEN 1 END) as hyperactive,
                    COUNT(CASE WHEN activity_level = 'very_active' THEN 1 END) as very_active,
                    COUNT(CASE WHEN activity_level = 'active' THEN 1 END) as active,
                    COUNT(CASE WHEN activity_level = 'normal' THEN 1 END) as normal,
                    COUNT(CASE WHEN activity_level = 'lurker' THEN 1 END) as lurkers,
                    COUNT(CASE WHEN communication_style = 'toxic' THEN 1 END) as toxic,
                    COUNT(CASE WHEN communication_style = 'humorous' THEN 1 END) as humorous,
                    COUNT(CASE WHEN communication_style = 'positive' THEN 1 END) as positive,
                    AVG(total_messages) as avg_messages,
                    SUM(total_messages) as total_messages,
                    COUNT(CASE WHEN is_night_owl THEN 1 END) as night_owls,
                    COUNT(CASE WHEN is_early_bird THEN 1 END) as early_birds
                FROM user_profiles
            """)
            
            interests_stats = await conn.fetch("""
                SELECT topic, COUNT(*) as users, SUM(message_count) as mentions
                FROM user_interests
                GROUP BY topic
                ORDER BY users DESC
                LIMIT 10
            """)
            
            interactions = await conn.fetchrow("""
                SELECT COUNT(*) as total, SUM(interaction_count) as interactions
                FROM user_interactions
            """)
        
        interests_text = "\n".join([
            f"  • {row['topic']}: {row['users']} юзеров, {row['mentions']} упоминаний"
            for row in interests_stats
        ]) or "Нет данных"
        
        text = f"""📊 *СТАТИСТИКА ПРОФИЛЕЙ*

👥 *Всего профилей:* {stats['total_profiles']}
└ 👨 Мужчины: {stats['males']}
└ 👩 Женщины: {stats['females']}
└ 🤷 Неизвестно: {stats['unknown']}

🔥 *Активность:*
└ 🔥🔥🔥 Гиперактивные: {stats['hyperactive']}
└ 🔥🔥 Очень активные: {stats['very_active']}
└ 🔥 Активные: {stats['active']}
└ 🙂 Обычные: {stats['normal']}
└ 👀 Лурки: {stats['lurkers']}

🎭 *Стиль общения:*
└ ☠️ Токсичные: {stats['toxic']}
└ 😂 Юмористы: {stats['humorous']}
└ 😊 Позитивные: {stats['positive']}

⏰ *Режим сна:*
└ 🦉 Совы: {stats['night_owls']}
└ 🐓 Жаворонки: {stats['early_birds']}

📝 *Сообщения:*
└ Всего: {stats['total_messages'] or 0:,.0f}
└ Среднее: {stats['avg_messages'] or 0:,.1f} на юзера

🕸️ *Социальные связи:*
└ Уникальных: {interactions['total'] or 0}
└ Взаимодействий: {interactions['interactions'] or 0:,}

🎯 *Топ интересов:*
{interests_text}
"""
        await message.answer(text, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"User stats error: {e}")
        await message.answer(f"❌ Ошибка: {e}")


@router.message(Command("rawprofile", "raw_profile"))
async def cmd_rawprofile(message: Message):
    """Показать сырой JSON профиля пользователя"""
    if message.chat.type != "private" or not is_admin(message.from_user.id):
        return
    
    if not USE_POSTGRES:
        await message.answer("❌ Профили доступны только с PostgreSQL")
        return
    
    # Получаем ID из аргумента: /rawprofile <user_id> <chat_id>
    args = message.text.split()
    if len(args) < 3:
        await message.answer("Использование: /rawprofile <user_id> <chat_id>")
        return
    
    try:
        user_id = int(args[1])
        chat_id = int(args[2])
    except ValueError:
        await message.answer("❌ ID должны быть числами")
        return
    
    try:
        profile = await get_user_full_profile(user_id, chat_id)
        if not profile:
            await message.answer(f"❌ Профиль {user_id} не найден")
            return
        
        # Конвертируем все в строку
        import json
        
        # Обрабатываем специальные типы
        for key, value in profile.items():
            if isinstance(value, (dict, list)):
                continue
            if value is None:
                profile[key] = None
        
        json_text = json.dumps(profile, ensure_ascii=False, indent=2, default=str)
        
        # Разбиваем на части если слишком длинное
        if len(json_text) > 4000:
            json_text = json_text[:4000] + "\n... (обрезано)"
        
        await message.answer(f"```json\n{json_text}\n```", parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Raw profile error: {e}")
        await message.answer(f"❌ Ошибка: {e}")


@router.message(Command("allprofiles", "all_profiles", "профили"))
async def cmd_all_profiles(message: Message):
    """Показать все профили пользователей чата"""
    if not USE_POSTGRES:
        await message.answer("⚠️ Профили доступны только в полной версии")
        return
    
    chat_id = message.chat.id
    
    # В личке требуем chat_id как аргумент (только для админов)
    if message.chat.type == "private":
        if not is_admin(message.from_user.id):
            await message.answer("❌ Эта команда только для админов в личке")
            return
        
        args = message.text.split()
        if len(args) < 2:
            await message.answer("Использование в личке: /allprofiles <chat_id>")
            return
        
        try:
            chat_id = int(args[1])
        except ValueError:
            await message.answer("❌ chat_id должен быть числом")
            return
    
    try:
        profiles = await get_all_chat_profiles(chat_id, limit=30)
        
        if not profiles:
            await message.answer("📭 В этом чате пока нет профилей")
            return
        
        # Формируем красивый вывод
        lines = [f"👥 *Профили чата* (`{chat_id}`)\n"]
        
        for i, p in enumerate(profiles, 1):
            # Определяем иконки
            gender_icon = "👨" if p.get('detected_gender') == 'мужской' else "👩" if p.get('detected_gender') == 'женский' else "🤷"
            
            # Стиль общения
            style = p.get('communication_style', 'нейтральный')
            style_icons = {
                'токсик': '☠️', 'матершинник': '🤬', 'шутник': '😂',
                'позитивный': '😊', 'негативный': '😤', 'крикун': '📢',
                'молодёжный': '🔥', 'нейтральный': '😐'
            }
            style_icon = style_icons.get(style, '😐')
            
            # Активность
            activity = p.get('activity_level', 'normal')
            activity_icons = {
                'hyperactive': '🚀', 'very_active': '⚡', 'active': '💪',
                'normal': '👤', 'lurker': '👻'
            }
            activity_icon = activity_icons.get(activity, '👤')
            
            # Время активности
            time_icon = "🌙" if p.get('is_night_owl') else "🌅" if p.get('is_early_bird') else "☀️"
            
            # Имя
            name = p.get('first_name') or p.get('username') or f"id{p.get('user_id')}"
            name = name[:15] + "…" if len(name) > 15 else name
            
            # Метрики
            msgs = p.get('total_messages', 0)
            toxicity = p.get('toxicity_score', 0) or 0
            humor = p.get('humor_score', 0) or 0
            
            # Любимые эмодзи
            fav_emojis = p.get('favorite_emojis') or []
            emojis_str = "".join(fav_emojis[:3]) if fav_emojis else ""
            
            lines.append(
                f"{i}. {gender_icon}{activity_icon}{style_icon}{time_icon} *{name}*\n"
                f"   📝 {msgs} | 🧪 {toxicity:.0%} | 😂 {humor:.0%} {emojis_str}"
            )
        
        # Добавляем легенду
        lines.append("\n*Легенда:*")
        lines.append("👨👩🤷 пол | 🚀⚡💪👤👻 активность")
        lines.append("☠️🤬😂😊😤📢🔥😐 стиль | 🌙🌅☀️ время")
        lines.append("📝 сообщений | 🧪 токсичность | 😂 юмор")
        
        await message.answer("\n".join(lines), parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        logger.error(f"All profiles error: {e}")
        await message.answer(f"❌ Ошибка: {e}")


@router.message(Command("migrate_media", "миграция_медиа"))
async def cmd_migrate_media(message: Message):
    """Миграция медиа из chat_messages в chat_media"""
    if message.chat.type != "private" or not is_admin(message.from_user.id):
        return
    
    if not USE_POSTGRES:
        await message.answer("❌ Миграция доступна только с PostgreSQL")
        return
    
    try:
        processing = await message.answer("🔄 Запускаю миграцию медиа...\n\nЭто может занять некоторое время.")
        results = await migrate_media_from_messages()
        
        await processing.edit_text(
            f"✅ *Миграция завершена!*\n\n"
            f"📥 Мигрировано: {results.get('migrated', 0):,}\n"
            f"⏭ Пропущено (уже есть): {results.get('skipped', 0):,}\n"
            f"❌ Ошибок: {results.get('errors', 0)}",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка миграции: {e}")


@router.message(Command("migrate_users"))
async def cmd_migrate_users(message: Message):
    """Миграция пользователей из chat_messages в chat_users"""
    if message.chat.type != "private" or not is_admin(message.from_user.id):
        return
    
    if not USE_POSTGRES:
        await message.answer("❌ Миграция доступна только с PostgreSQL")
        return
    
    try:
        from database_postgres import migrate_chat_users_from_messages
        
        processing = await message.answer(
            "🔄 Запускаю миграцию пользователей в реестр...\n\n"
            "Это заполнит таблицу chat_users данными из всех сообщений."
        )
        
        results = await migrate_chat_users_from_messages()
        
        await processing.edit_text(
            f"✅ *Миграция пользователей завершена!*\n\n"
            f"📊 Было записей: {results.get('before', 0):,}\n"
            f"📊 Стало записей: {results.get('after', 0):,}\n"
            f"➕ Добавлено: {results.get('added', 0):,}\n"
            f"💬 Чатов с пользователями: {results.get('total_chats', 0):,}",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Migration error: {e}")
        await message.answer(f"❌ Ошибка миграции: {e}")


@dp.message(Command("admin"))
async def admin_rebuild_profiles(message: Message):
    """Миграция профилей на per-chat архитектуру"""
    if message.chat.type != "private" or not is_admin(message.from_user.id):
        return
    
    args = message.text.split()
    if len(args) < 2 or args[1] not in ("rebuild_profiles", "reset_corrupted"):
        await message.answer(
            "📋 *Доступные admin-команды:*\n\n"
            "`/admin rebuild_profiles [chat_id]` — пересобрать профили из истории\n"
            "`/admin reset_corrupted [chat_id]` — сбросить и пересобрать битые профили",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if not USE_POSTGRES:
        await message.answer("❌ Команда доступна только с PostgreSQL")
        return

    # --- reset_corrupted ---
    if args[1] == "reset_corrupted":
        try:
            from database_postgres import reset_corrupted_profiles
            target_chat_id = int(args[2]) if len(args) >= 3 else None
            scope = f"чата {target_chat_id}" if target_chat_id else "всех чатов"
            processing = await message.answer(f"🔧 Ищу и сбрасываю битые профили {scope}...")

            results = await reset_corrupted_profiles(target_chat_id)

            await processing.edit_text(
                f"✅ *Сброс битых профилей завершён!*\n\n"
                f"🗑️ Удалено битых профилей: {results.get('deleted', 0):,}\n"
                f"🔨 Пересоздано профилей: {results.get('rebuilt', 0):,}\n"
                f"❌ Ошибок: {len(results.get('errors', []))}",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            logger.error(f"reset_corrupted_profiles error: {e}")
            await message.answer(f"❌ Ошибка: {e}")
        return

    try:
        from database_postgres import rebuild_all_profiles, rebuild_profiles_from_messages
        
        # Проверяем, указан ли конкретный чат
        if len(args) >= 3:
            try:
                target_chat_id = int(args[2])
                processing = await message.answer(
                    f"🔄 Пересобираю профили для чата {target_chat_id}...\n\n"
                    "Это создаст per-chat профили из существующих сообщений."
                )
                
                results = await rebuild_profiles_from_messages(target_chat_id)
                
                await processing.edit_text(
                    f"✅ *Пересборка профилей завершена!*\n\n"
                    f"👥 Пользователей обработано: {results.get('users_processed', 0):,}\n"
                    f"📝 Профилей создано: {results.get('profiles_created', 0):,}\n"
                    f"💬 Сообщений проанализировано: {results.get('messages_analyzed', 0):,}\n"
                    f"❌ Ошибок: {len(results.get('errors', []))}",
                    parse_mode=ParseMode.MARKDOWN
                )
            except ValueError:
                await message.answer("❌ Неверный chat_id")
        else:
            # Пересборка для всех чатов
            processing = await message.answer(
                "🔄 Пересобираю профили для ВСЕХ чатов...\n\n"
                "⚠️ Это может занять долгое время!\n"
                "Создаются per-chat профили из существующих сообщений."
            )
            
            results = await rebuild_all_profiles()
            
            await processing.edit_text(
                f"✅ *Глобальная пересборка профилей завершена!*\n\n"
                f"🏠 Чатов обработано: {results.get('chats_processed', 0):,}\n"
                f"👥 Пользователей: {results.get('total_users', 0):,}\n"
                f"📝 Профилей создано: {results.get('total_profiles', 0):,}\n"
                f"💬 Сообщений: {results.get('total_messages', 0):,}\n"
                f"❌ Ошибок: {len(results.get('errors', []))}",
                parse_mode=ParseMode.MARKDOWN
            )
    except Exception as e:
        logger.error(f"Profile rebuild error: {e}")
        await message.answer(f"❌ Ошибка пересборки профилей: {e}")


# ==================== VK ИНТЕГРАЦИЯ ====================

VK_API_TOKEN = os.getenv("VK_API_TOKEN", "")
VK_API_VERSION = "5.199"

# Популярные паблики с мемами
VK_MEME_COMMUNITIES = {
    "mdk": "MDK",
    "borsch": "Борщ",
    "mudakoff": "Мудакофф", 
    "leprum": "Лепра",
    "memes": "Мемы",
    "igm": "IGM",
    "tproger_official": "Типичный программист",
    "oldlentach": "Лентач",
    "cat": "Коты",
}

# Чат для автоматического сбора мемов (установи через /vk_auto)
VK_AUTO_CHAT_ID = None


async def fetch_vk_memes(community: str, count: int = 50, min_likes: int = 100) -> List[Dict]:
    """Получить ПОПУЛЯРНЫЕ мемы из VK паблика (фильтр по лайкам)"""
    if not VK_API_TOKEN:
        return []
    
    memes = []
    session = await get_http_session()
    
    try:
        # Запрашиваем много постов, чтобы отфильтровать по лайкам
        fetch_count = 100  # Максимум VK API
        
        # Получаем посты со стены
        async with session.get(
            "https://api.vk.com/method/wall.get",
            params={
                "domain": community,
                "count": fetch_count,
                "filter": "owner",
                "extended": 0,
                "access_token": VK_API_TOKEN,
                "v": VK_API_VERSION
            }
        ) as response:
            data = await response.json()
            
            if "error" in data:
                logger.error(f"VK API error: {data['error']}")
                return []
            
            items = data.get("response", {}).get("items", [])
            logger.info(f"VK returned {len(items)} posts from {community}")
            
            # Собираем все посты с картинками и лайками
            candidates = []
            
            for item in items:
                # Пропускаем репосты
                if item.get("copy_history"):
                    continue
                
                # Получаем количество лайков
                likes = item.get("likes", {}).get("count", 0)
                reposts = item.get("reposts", {}).get("count", 0)
                views = item.get("views", {}).get("count", 0)
                
                # Рассчитываем "популярность" (лайки + репосты*3)
                popularity = likes + (reposts * 3)
                
                attachments = item.get("attachments", [])
                
                for att in attachments:
                    if att["type"] == "photo":
                        photo = att["photo"]
                        sizes = photo.get("sizes", [])
                        
                        if not sizes:
                            continue
                        
                        best = max(sizes, key=lambda x: x.get("width", 0) * x.get("height", 0))
                        width = best.get("width", 0)
                        height = best.get("height", 0)
                        
                        # Фильтр по размеру
                        if width < 400 or height < 300:
                            continue
                        if width == height and width < 500:
                            continue
                        
                        candidates.append({
                            "type": "photo",
                            "url": best["url"],
                            "text": item.get("text", "")[:200],
                            "width": width,
                            "height": height,
                            "likes": likes,
                            "popularity": popularity
                        })
                        break  # Одно фото с поста
                        
                    elif att["type"] == "doc":
                        doc = att["doc"]
                        if doc.get("ext") == "gif":
                            candidates.append({
                                "type": "animation",
                                "url": doc["url"],
                                "text": item.get("text", "")[:200],
                                "likes": likes,
                                "popularity": popularity
                            })
                            break
            
            # Сортируем по популярности (больше лайков = лучше)
            candidates.sort(key=lambda x: x["popularity"], reverse=True)
            
            # Фильтруем по минимальному количеству лайков
            memes = [m for m in candidates if m["likes"] >= min_likes]
            
            # Если мало постов с нужным количеством лайков — берём топ по популярности
            if len(memes) < count:
                memes = candidates[:count * 2]
            
            logger.info(f"Found {len(candidates)} candidates, {len(memes)} with {min_likes}+ likes")
                    
    except Exception as e:
        logger.error(f"Error fetching VK memes: {e}")
    
    return memes[:count]


async def import_vk_memes_to_chat(chat_id: int, community: str, count: int = 30, min_likes: int = 100) -> Dict[str, int]:
    """Импортировать ПОПУЛЯРНЫЕ мемы из VK в коллекцию чата"""
    stats = {"imported": 0, "errors": 0, "skipped": 0, "already_exists": 0}
    
    memes = await fetch_vk_memes(community, count * 2, min_likes)  # Берём больше, т.к. часть пропустим
    if not memes:
        return stats
    
    session = await get_http_session()
    
    # Получаем существующие URL хеши из описаний (для дедупликации)
    existing_hashes = set()
    if USE_POSTGRES:
        from database_postgres import get_pool
        async with (await get_pool()).acquire() as conn:
            rows = await conn.fetch("""
                SELECT caption FROM chat_media 
                WHERE chat_id = $1 AND description LIKE 'VK:%'
            """, chat_id)
            for row in rows:
                if row['caption']:
                    # Храним хеш URL в caption
                    existing_hashes.add(row['caption'][:50])
    
    imported_count = 0
    
    for meme in memes:
        if imported_count >= count:
            break
            
        try:
            # Создаём хеш URL для проверки дубликатов
            url_hash = meme["url"].split("?")[0][-50:]  # Последние 50 символов URL без параметров
            
            if url_hash in existing_hashes:
                stats["already_exists"] += 1
                continue
            
            # Скачиваем файл
            async with session.get(meme["url"]) as response:
                if response.status != 200:
                    stats["errors"] += 1
                    continue
                
                file_data = await response.read()
                
                # Проверяем размер — слишком маленькие пропускаем
                if len(file_data) < 10000:  # < 10KB
                    stats["skipped"] += 1
                    continue
            
            # Отправляем в чат (чтобы получить file_id)
            if meme["type"] == "photo":
                from aiogram.types import BufferedInputFile
                input_file = BufferedInputFile(file_data, filename="meme.jpg")
                sent = await bot.send_photo(chat_id, input_file)
                file_id = sent.photo[-1].file_id
                file_unique_id = sent.photo[-1].file_unique_id
                # Удаляем отправленное сообщение
                await sent.delete()
            elif meme["type"] == "animation":
                from aiogram.types import BufferedInputFile
                input_file = BufferedInputFile(file_data, filename="meme.gif")
                sent = await bot.send_animation(chat_id, input_file)
                file_id = sent.animation.file_id
                file_unique_id = sent.animation.file_unique_id
                await sent.delete()
            else:
                continue
            
            # Сохраняем в коллекцию (caption = url_hash для дедупликации)
            saved = await save_media(
                chat_id=chat_id,
                user_id=0,  # VK import
                file_id=file_id,
                file_type=meme["type"],
                file_unique_id=file_unique_id,
                description=f"VK: {community}",
                caption=url_hash  # Храним хеш для проверки дубликатов
            )
            
            if saved:
                stats["imported"] += 1
                imported_count += 1
                existing_hashes.add(url_hash)
            else:
                stats["skipped"] += 1
            
            # Небольшая задержка чтобы не спамить
            await asyncio.sleep(0.3)
            
        except Exception as e:
            logger.error(f"Error importing meme: {e}")
            stats["errors"] += 1
    
    return stats


async def fetch_trending_vk_memes(min_likes: int = 500, count: int = 20) -> List[Dict]:
    """Получить трендовые мемы со всего VK через поиск"""
    if not VK_API_TOKEN:
        return []
    
    memes = []
    session = await get_http_session()
    
    # Поисковые запросы для мемов
    search_queries = ["мем", "смешно", "ржака", "прикол", "угар", "юмор"]
    
    try:
        for query in search_queries:
            if len(memes) >= count:
                break
                
            async with session.get(
                "https://api.vk.com/method/newsfeed.search",
                params={
                    "q": query,
                    "count": 50,
                    "extended": 0,
                    "access_token": VK_API_TOKEN,
                    "v": VK_API_VERSION
                }
            ) as response:
                data = await response.json()
                
                if "error" in data:
                    logger.warning(f"VK search error: {data['error']}")
                    continue
                
                items = data.get("response", {}).get("items", [])
                
                for item in items:
                    if len(memes) >= count:
                        break
                    
                    likes = item.get("likes", {}).get("count", 0)
                    if likes < min_likes:
                        continue
                    
                    attachments = item.get("attachments", [])
                    for att in attachments:
                        if att["type"] == "photo":
                            photo = att["photo"]
                            sizes = photo.get("sizes", [])
                            if not sizes:
                                continue
                            
                            best = max(sizes, key=lambda x: x.get("width", 0) * x.get("height", 0))
                            width = best.get("width", 0)
                            height = best.get("height", 0)
                            
                            if width < 400 or height < 300:
                                continue
                            
                            memes.append({
                                "type": "photo",
                                "url": best["url"],
                                "text": item.get("text", "")[:100],
                                "likes": likes
                            })
                            break
            
            await asyncio.sleep(0.5)  # Задержка между запросами
                
    except Exception as e:
        logger.error(f"Error fetching trending memes: {e}")
    
    # Сортируем по лайкам
    memes.sort(key=lambda x: x["likes"], reverse=True)
    return memes[:count]


async def auto_fetch_vk_memes():
    """Автоматический сбор популярных мемов (вызывается по расписанию)"""
    global VK_AUTO_CHAT_ID
    
    if not VK_API_TOKEN or not VK_AUTO_CHAT_ID:
        return
    
    logger.info(f"🤖 Автосбор мемов для чата {VK_AUTO_CHAT_ID}")
    
    try:
        # Собираем из топовых пабликов
        total_imported = 0
        
        for community in ["mdk", "borsch", "mudakoff", "oldlentach"]:
            stats = await import_vk_memes_to_chat(VK_AUTO_CHAT_ID, community, 5, 500)
            total_imported += stats.get("imported", 0)
            await asyncio.sleep(2)
        
        # Собираем трендовые
        trending = await fetch_trending_vk_memes(min_likes=1000, count=10)
        if trending:
            for meme in trending[:5]:
                try:
                    session = await get_http_session()
                    async with session.get(meme["url"]) as response:
                        if response.status != 200:
                            continue
                        file_data = await response.read()
                    
                    from aiogram.types import BufferedInputFile
                    input_file = BufferedInputFile(file_data, filename="meme.jpg")
                    sent = await bot.send_photo(VK_AUTO_CHAT_ID, input_file)
                    file_id = sent.photo[-1].file_id
                    file_unique_id = sent.photo[-1].file_unique_id
                    await sent.delete()
                    
                    url_hash = meme["url"].split("?")[0][-50:]
                    await save_media(
                        chat_id=VK_AUTO_CHAT_ID,
                        user_id=0,
                        file_id=file_id,
                        file_type="photo",
                        file_unique_id=file_unique_id,
                        description="VK: trending",
                        caption=url_hash
                    )
                    total_imported += 1
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(f"Error importing trending meme: {e}")
        
        logger.info(f"✅ Автосбор завершён: {total_imported} мемов")
        
    except Exception as e:
        logger.error(f"Auto-fetch error: {e}")


@router.message(Command("vk_auto", "автомемы"))
async def cmd_vk_auto(message: Message):
    """Настроить автоматический сбор мемов"""
    global VK_AUTO_CHAT_ID
    
    if not is_admin(message.from_user.id):
        await message.answer("❌ Только для админов!")
        return
    
    if not VK_API_TOKEN:
        await message.answer("❌ VK API токен не настроен!")
        return
    
    if message.chat.type == "private":
        await message.answer(
            "❌ Используй эту команду в групповом чате!\n\n"
            "Бот будет автоматически собирать популярные мемы в этот чат."
        )
        return
    
    VK_AUTO_CHAT_ID = message.chat.id
    
    # Добавляем задачу в планировщик (каждые 6 часов)
    job_id = "vk_auto_memes"
    
    # Удаляем старую задачу если есть
    existing = scheduler.get_job(job_id)
    if existing:
        scheduler.remove_job(job_id)
    
    scheduler.add_job(
        auto_fetch_vk_memes,
        'interval',
        hours=6,
        id=job_id,
        replace_existing=True
    )
    
    await message.answer(
        f"✅ *Автосбор мемов включён!*\n\n"
        f"📍 Чат: {message.chat.title or 'этот чат'}\n"
        f"⏰ Расписание: каждые 6 часов\n"
        f"📥 Источники: MDK, Борщ, Мудакофф, Лентач + тренды\n"
        f"🔥 Фильтр: 500+ лайков\n\n"
        f"Для ручного запуска: `/vk_now`",
        parse_mode=ParseMode.MARKDOWN
    )


@router.message(Command("vk_now", "мемы_сейчас"))
async def cmd_vk_now(message: Message):
    """Запустить сбор мемов прямо сейчас"""
    global VK_AUTO_CHAT_ID
    
    if not is_admin(message.from_user.id):
        return
    
    if message.chat.type != "private":
        VK_AUTO_CHAT_ID = message.chat.id
    
    if not VK_AUTO_CHAT_ID:
        await message.answer("❌ Сначала укажи чат командой /vk_auto в групповом чате!")
        return
    
    processing = await message.answer("🔄 Собираю популярные мемы со всего VK...")
    
    await auto_fetch_vk_memes()
    
    await processing.edit_text("✅ Сбор мемов завершён! Проверь /мемы")


@router.message(Command("vk_import", "vk", "импорт_вк"))
async def cmd_vk_import(message: Message):
    """Импортировать мемы из VK паблика"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Только для админов!")
        return
    
    if not VK_API_TOKEN:
        await message.answer(
            "❌ VK API токен не настроен!\n\n"
            "Добавь `VK_API_TOKEN` в переменные окружения.\n"
            "Получить: https://vk.com/dev → Создать приложение → Сервисный ключ"
        )
        return
    
    # Парсим аргументы: /vk_import mdk 30
    args = message.text.split()
    
    if len(args) < 2:
        communities_list = "\n".join([f"• `{k}` — {v}" for k, v in VK_MEME_COMMUNITIES.items()])
        await message.answer(
            f"📥 *Импорт ПОПУЛЯРНЫХ мемов из VK*\n\n"
            f"Использование: `/vk_import <паблик> [кол-во] [мин_лайков]`\n\n"
            f"Примеры:\n"
            f"• `/vk_import mdk` — топ мемов из MDK\n"
            f"• `/vk_import borsch 30` — 30 мемов из Борща\n"
            f"• `/vk_import mdk 20 500` — мемы с 500+ лайками\n\n"
            f"*Доступные паблики:*\n{communities_list}\n\n"
            f"Или укажи любой домен паблика!\n"
            f"_По умолчанию: 100+ лайков, сортировка по популярности_",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    community = args[1].lower().replace("@", "").replace("https://vk.com/", "")
    count = int(args[2]) if len(args) > 2 and args[2].isdigit() else 30
    count = min(count, 100)  # Максимум 100
    min_likes = int(args[3]) if len(args) > 3 and args[3].isdigit() else 100
    
    # Определяем chat_id куда импортировать
    if message.chat.type == "private":
        await message.answer(
            "❓ В какой чат импортировать?\n\n"
            "Используй эту команду в групповом чате, куда хочешь загрузить мемы."
        )
        return
    
    chat_id = message.chat.id
    community_name = VK_MEME_COMMUNITIES.get(community, community)
    
    processing = await message.answer(
        f"🔄 Импортирую ПОПУЛЯРНЫЕ мемы из VK/{community_name}...\n"
        f"Количество: до {count} шт.\n"
        f"Фильтр: {min_likes}+ лайков\n\n"
        f"⏳ Это может занять несколько минут..."
    )
    
    try:
        stats = await import_vk_memes_to_chat(chat_id, community, count, min_likes)
        
        await processing.edit_text(
            f"✅ *Импорт завершён!*\n\n"
            f"📥 Импортировано: {stats['imported']}\n"
            f"🔄 Уже были: {stats.get('already_exists', 0)}\n"
            f"⏭ Пропущено: {stats['skipped']}\n"
            f"❌ Ошибок: {stats['errors']}\n\n"
            f"Источник: VK/{community_name}",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        await processing.edit_text(f"❌ Ошибка импорта: {e}")


# ==================== ПРИВЕТСТВИЯ ПО РАСПИСАНИЮ ====================

# Московское время = UTC+3
GREETING_MESSAGES = {
    # Ночь (0-5)
    "night": [
        "не спите, дегенераты? правильно, ночью только и жить",
        "3 ночи. нормальные люди спят. вы здесь. всё понятно про вас",
        "чё не спите, упыри? завтра снова будете ныть что устали",
        "ночной дозор педиков собрался. приятного бдения",
        "тихая ночь, блять. жаль вы её портите своим присутствием",
        "полночь. самое время сидеть в телефоне и гробить здоровье. молодцы",
        "ночь, а вы не спите. диагноз — хроническое слабоумие",
    ],
    # Утро (6-10)
    "morning": [
        "доброе утро, педики 🌅",
        "вставайте, дармоеды. солнце уже встало, а вы всё дрыхнете небось",
        "утро добрым не бывает, но вы его сделали ещё хуже. с добрым утром",
        "доброе утро. день обещает быть таким же бессмысленным как и вы",
        "просыпайтесь, уроды. новый день — новые разочарования",
        "утро! время делать вид что вы что-то планируете на день",
        "с добрым утром, дегенераты. кофе пейте, мозг всё равно не включится",
    ],
    # День (11-14)
    "day": [
        "обед скоро, жиробасы. не забудьте поесть — хотя вы никогда не забываете",
        "день в разгаре. чем занимаетесь? ничем полезным, понятно",
        "добрый день. хотя что в нём доброго — непонятно",
        "полдень. половина дня прошла впустую. поздравляю",
        "дневной привет дармоедам. как дела? всё так же плохо? логично",
        "день добрый. хотя вам лично ничего доброго не желаю",
        "обеденный час. надеюсь вы хоть что-то успели сделать. нет? конечно нет",
    ],
    # Вечер (15-20)
    "evening": [
        "добрый вечер, отбросы 🌆",
        "вечер. день прошёл зря? молодцы, держите темп",
        "вечерний привет. как прожили день — так же бездарно как обычно?",
        "смеркается. самое время собраться и поныть про усталость",
        "добрый вечер. вечер добрый, вы — нет",
        "вечер наступил. пора ужинать и тупить в телефон. всё как вы любите",
        "вечерний сбор дегенератов объявляется открытым",
    ],
    # Поздний вечер (21-23)
    "late_evening": [
        "поздний вечер. скоро спать, а вы всё тут сидите",
        "ночь близко. последний шанс сделать что-то полезное. не используете? логично",
        "почти ночь. что, не устали существовать? странно",
        "поздновато для нормальных людей. но вас это не касается",
        "вечер заканчивается, бездельники. итоги дня: снова ничего",
        "скоро полночь. закрывайте свои помойные рты и идите спать",
        "поздний привет ночным уродам. завтра снова всё по новой",
    ],
}


async def scheduled_greeting():
    """Отправляет случайное приветствие в активные чаты каждые 2 часа"""
    if not USE_POSTGRES:
        return

    try:
        from datetime import datetime, timezone, timedelta
        moscow_tz = timezone(timedelta(hours=3))
        now = datetime.now(moscow_tz)
        hour = now.hour

        if 0 <= hour <= 5:
            bucket = "night"
        elif 6 <= hour <= 10:
            bucket = "morning"
        elif 11 <= hour <= 14:
            bucket = "day"
        elif 15 <= hour <= 20:
            bucket = "evening"
        else:
            bucket = "late_evening"

        text = random.choice(GREETING_MESSAGES[bucket])

        active_chats = await get_active_chats_for_auto_summary(min_messages=10, hours=4)
        if not active_chats:
            return

        for chat_info in active_chats:
            chat_id = chat_info['chat_id']
            try:
                await bot.send_message(chat_id, text)
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.warning(f"Greeting: не смог отправить в {chat_id}: {e}")

        logger.info(f"⏰ Приветствие [{bucket}] отправлено в {len(active_chats)} чатов")

    except Exception as e:
        logger.error(f"❌ Ошибка scheduled_greeting: {e}")


# ==================== ЗАПУСК ====================

async def on_shutdown():
    """Graceful shutdown — закрытие соединений"""
    logger.info("🛑 Останавливаю бота...")
    
    # Останавливаем планировщик
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("⏰ Планировщик остановлен")
    
    # Закрываем HTTP сессию
    await close_http_session()
    logger.info("🌐 HTTP сессия закрыта")
    
    # Закрываем пул соединений с БД
    if close_db:
        await close_db()
        logger.info("🗄 Соединение с БД закрыто")
    
    # Логируем итоговую статистику
    stats = metrics.get_stats()
    logger.info(f"📊 Итоги сессии: {stats['total_commands']} команд, {stats['total_api_calls']} API вызовов, {stats['errors']} ошибок")
    
    logger.info("✅ Бот остановлен корректно")


class CommandReplyInterceptMiddleware(BaseMiddleware):
    """Middleware для перехвата команд в реплай на бота"""
    
    async def __call__(self, handler, event: TelegramObject, data: dict):
        # Проверяем только Message
        if isinstance(event, Message):
            # Если это команда в реплай на бота — блокируем
            if await check_command_reply_to_bot(event):
                return  # Не вызываем handler — команда заблокирована
        
        # Иначе — выполняем обычную обработку
        return await handler(event, data)


# ==================== РЕГИСТРАЦИЯ КОМАНД В МЕНЮ TELEGRAM ====================

async def setup_bot_commands():
    """
    Регистрирует команды бота в меню Telegram (при нажатии /).
    Разные команды для групп, личных сообщений и админов.
    """
    
    # ===== КОМАНДЫ ДЛЯ ГРУППОВЫХ ЧАТОВ =====
    group_commands = [
        # RPG система
        BotCommand(command="help", description="📖 Справка по командам"),
        BotCommand(command="profile", description="👤 Мой игровой профиль"),
        BotCommand(command="top", description="🏆 Топ игроков"),
        BotCommand(command="crime", description="🔫 Пойти на дело"),
        BotCommand(command="attack", description="💥 Наехать на игрока"),
        BotCommand(command="treasury", description="💰 Общак чата"),
        BotCommand(command="achievements", description="🏅 Мои достижения"),
        
        # AI-развлечения
        BotCommand(command="poem", description="📜 Стих-унижение про юзера"),
        BotCommand(command="diagnosis", description="🏥 Диагноз юзеру"),
        BotCommand(command="burn", description="🔥 Сжечь на костре правды"),
        BotCommand(command="drink", description="🍺 Бухнуть и слить секреты"),
        BotCommand(command="suck", description="🍭 Послать сосать"),
        BotCommand(command="dream", description="💤 Грязный сон про юзера"),
        BotCommand(command="ventilate", description="🪟 Проветрить чат"),
        BotCommand(command="нарисуй", description="🎨 Нарисовать портрет через Flux"),
        BotCommand(command="видео", description="🎬 Снять видео через Kling AI"),
        BotCommand(command="оживи", description="✨ Анимировать фото (реплай на фото)"),
        BotCommand(command="улучши", description="🔍 Улучшить фото 4x (реплай на фото)"),
        BotCommand(command="музыка", description="🎵 Трек по мотивам чата"),
        BotCommand(command="подкаст", description="🎙️ AI подкаст про чат — два ведущих"),

        # Анализ и профили
        BotCommand(command="dossier", description="📋 AI-досье на юзера"),
        BotCommand(command="psycho", description="🧠 Психоанализ личности"),
        BotCommand(command="memory", description="🧠 Что бот помнит о юзере"),
        BotCommand(command="learnme", description="📚 Быстрое обучение (30 сообщений)"),
        BotCommand(command="deeplearn", description="🧠 Глубокое обучение (все сообщения)"),
        BotCommand(command="social", description="🕸️ Социальный граф чата"),
        BotCommand(command="allprofiles", description="👥 Все профили чата"),
        
        # Утилиты
        BotCommand(command="describe", description="🖼️ Описать фото (реплай)"),
        BotCommand(command="say", description="🎤 Сказать голосом"),
        BotCommand(command="pic", description="🔍 Найти картинку"),
        BotCommand(command="svodka", description="📊 Сводка чата за 5 часов"),
        
        # Мемы
        BotCommand(command="meme", description="🎭 Случайный мем"),
        BotCommand(command="memestats", description="📈 Статистика мемов"),
    ]
    
    # ===== КОМАНДЫ ДЛЯ ЛИЧНЫХ СООБЩЕНИЙ =====
    private_commands = [
        BotCommand(command="start", description="🚀 Начать"),
        BotCommand(command="help", description="📖 Справка по командам"),
        BotCommand(command="say", description="🎤 Сказать голосом"),
        BotCommand(command="pic", description="🔍 Найти картинку"),
        BotCommand(command="describe", description="🖼️ Описать фото"),
    ]
    
    # ===== КОМАНДЫ ДЛЯ АДМИНОВ (в личке) =====
    admin_commands = [
        BotCommand(command="start", description="🚀 Начать"),
        BotCommand(command="help", description="📖 Справка"),
        BotCommand(command="admin", description="⚙️ Админ-панель"),
        BotCommand(command="dbstats", description="📊 Статистика БД"),
        BotCommand(command="chats", description="💬 Список чатов"),
        BotCommand(command="chat", description="🔍 Инфо о чате"),
        BotCommand(command="topusers", description="👥 Топ юзеров глобально"),
        BotCommand(command="finduser", description="🔎 Найти юзера"),
        BotCommand(command="health", description="💚 Здоровье системы"),
        BotCommand(command="metrics", description="📈 Метрики бота"),
        BotCommand(command="cleanup", description="🧹 Очистка БД"),
        BotCommand(command="userstats", description="👤 Статистика профилей"),
        BotCommand(command="rawprofile", description="📄 Сырой профиль юзера"),
        BotCommand(command="allprofiles", description="👥 Профили чата"),
        BotCommand(command="say", description="🎤 Сказать голосом"),
        BotCommand(command="pic", description="🔍 Найти картинку"),
        BotCommand(command="vk_import", description="📥 Импорт мемов из VK"),
    ]
    
    try:
        # Устанавливаем команды для групповых чатов
        await bot.set_my_commands(
            commands=group_commands,
            scope=BotCommandScopeAllGroupChats()
        )
        logger.info(f"✅ Зарегистрировано {len(group_commands)} команд для групп")
        
        # Устанавливаем команды для личных сообщений (обычные юзеры)
        await bot.set_my_commands(
            commands=private_commands,
            scope=BotCommandScopeAllPrivateChats()
        )
        logger.info(f"✅ Зарегистрировано {len(private_commands)} команд для личных сообщений")
        
        # Устанавливаем расширенные команды для админов
        admin_ids = []
        for x in os.getenv("ADMIN_IDS", "").split(","):
            x = x.strip()
            if not x:
                continue
            try:
                admin_ids.append(int(x))
            except ValueError:
                logger.warning("Некорректный ADMIN_ID в переменной окружения: %r — пропускаю", x)
        for admin_id in admin_ids:
            try:
                await bot.set_my_commands(
                    commands=admin_commands,
                    scope=BotCommandScopeChat(chat_id=admin_id)
                )
                logger.info(f"✅ Админ-команды установлены для {admin_id}")
            except Exception as e:
                logger.warning(f"⚠️ Не удалось установить команды для админа {admin_id}: {e}")
        
        logger.info("✅ Меню команд бота успешно настроено!")
        
    except Exception as e:
        logger.error(f"❌ Ошибка настройки команд: {e}")


async def main():
    """Главная функция запуска бота"""
    # Инициализация БД
    await init_db()
    
    # Регистрируем middleware для перехвата команд в реплай на бота
    dp.message.outer_middleware(CommandReplyInterceptMiddleware())
    
    # Подключаем роутер
    dp.include_router(router)
    
    # Настраиваем меню команд в Telegram
    await setup_bot_commands()
    
    # Регистрируем shutdown handler
    dp.shutdown.register(on_shutdown)
    
    # Запуск планировщика для очистки и мониторинга
    if USE_POSTGRES:
        scheduler.add_job(scheduled_cleanup, 'interval', hours=6, id='cleanup')
        scheduler.add_job(log_database_stats, 'interval', hours=1, id='stats')
        scheduler.add_job(scheduled_auto_summaries, 'interval', hours=6, id='auto_summaries')
        scheduler.add_job(scheduled_greeting, 'interval', hours=2, id='greeting')

    # Очистка памяти (cooldowns и api_calls) каждые 10 минут
    scheduler.add_job(cleanup_memory, 'interval', minutes=10, id='memory_cleanup')
    scheduler.start()

    if USE_POSTGRES:
        logger.info("⏰ Планировщик запущен: очистка БД (6ч), статистика (1ч), авто-сводки (6ч), приветствия (2ч), память (10м)")
    else:
        logger.info("⏰ Планировщик запущен: очистка памяти (10м)")
    
    logger.info("🔫 Гильдия Беспредела запущена!")
    
    # Первичное логирование статистики
    if USE_POSTGRES:
        await log_database_stats()
    
    # Запуск бота
    try:
        await dp.start_polling(bot)
    finally:
        await on_shutdown()


if __name__ == "__main__":
    asyncio.run(main())
