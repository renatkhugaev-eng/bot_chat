import asyncio
import logging
import random
import re
import time
from typing import Optional, List, Dict

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ChatMemberUpdated, BufferedInputFile
)
from aiogram.filters import Command, CommandStart
from aiogram.enums import ParseMode
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


async def get_http_session() -> aiohttp.ClientSession:
    """Получить глобальную HTTP сессию (создаёт если нет)"""
    global _http_session
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
        update_user_gender_incrementally
    )
else:
    from database import (
        init_db, get_player, create_player, set_player_class, update_player_stats,
        get_top_players, is_in_jail, put_in_jail, get_all_active_players,
        add_to_treasury, get_treasury, log_event, add_achievement,
        save_chat_message, get_chat_statistics, get_player_achievements,
        save_summary, get_previous_summaries, save_memory, get_memories,
        get_user_messages
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
    # Заглушки для определения пола (только PostgreSQL)
    async def get_user_profile(user_id): return None
    async def get_user_gender(user_id): return 'unknown'
    async def analyze_and_update_user_gender(user_id, first_name="", username=""): return {'gender': 'unknown', 'confidence': 0.0, 'female_score': 0, 'male_score': 0, 'messages_analyzed': 0}
    async def update_user_gender_incrementally(user_id, new_message, first_name="", username=""): return {'gender': 'unknown', 'confidence': 0.0, 'female_score': 0, 'male_score': 0, 'messages_analyzed': 0}
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


def check_cooldown(user_id: int, chat_id: int, action: str, cooldown_seconds: int) -> tuple[bool, int]:
    """Проверить кулдаун. Возвращает (можно_ли, оставшееся_время)"""
    key = (user_id, chat_id, action)
    current_time = time.time()
    
    if key in cooldowns:
        remaining = cooldowns[key] - current_time
        if remaining > 0:
            return False, int(remaining)
    
    cooldowns[key] = current_time + cooldown_seconds
    
    # Очистка старых записей (раз в 100 проверок)
    if len(cooldowns) > 1000:
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


# ==================== СБОР КОНТЕКСТА (DRY) ====================

async def gather_user_context(chat_id: int, user_id: int, limit: int = 100) -> tuple[str, int]:
    """
    Собирает контекст сообщений пользователя для AI-команд.
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
                # Берём интересные (длинные) + последние
                interesting = sorted(texts, key=len, reverse=True)[:15]
                recent = texts[:15]
                all_texts = list(dict.fromkeys(interesting + recent))[:20]
                
                for i, text in enumerate(all_texts, 1):
                    truncated = text[:200] + "..." if len(text) > 200 else text
                    context_parts.append(f'{i}. "{truncated}"')
    except Exception as e:
        logger.warning(f"Could not fetch user messages: {e}")
    
    if context_parts:
        return "\n".join(context_parts), messages_found
    else:
        return "Сообщений нет — молчит как партизан", 0


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
    
    welcome = random.choice(WELCOME_MESSAGES).format(name=callback.from_user.first_name)
    
    await callback.message.edit_text(
        f"🎉 *ПОЗДРАВЛЯЕМ!*\n\n"
        f"{welcome}\n\n"
        f"Твой класс: {class_data['emoji']} *{class_data['name']}*\n"
        f"_{class_data['starter_phrase']}_\n\n"
        f"💰 Стартовый капитал: 100 лавэ\n"
        f"🎯 Теперь ты можешь:\n"
        f"• /crime — пойти на дело\n"
        f"• /attack @username — наехать на лоха\n"
        f"• /profile — глянуть досье\n"
        f"• /top — топ авторитетов\n"
        f"• /casino — испытать удачу\n\n"
        f"Да начнётся беспредел! 😈",
        parse_mode=ParseMode.MARKDOWN
    )
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
    if message.reply_to_message:
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
    
    await callback.message.edit_text(text, reply_markup=keyboard)
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
    crime_index = int(callback.data.replace("crime_", ""))
    
    if crime_index >= len(CRIMES):
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
    
    await callback.message.edit_text(result_text, parse_mode=ParseMode.MARKDOWN)
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
    
    if message.reply_to_message:
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


@router.message(Command("casino", "bet", "gamble"))
async def cmd_casino(message: Message):
    """Казино"""
    if message.chat.type == "private":
        await message.answer("❌ Казино работает только в группах!")
        return
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    player = await get_player(user_id, chat_id)
    if not player or not player['player_class']:
        await message.answer("❌ Сначала вступи в гильдию! /start")
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🎰 Слоты (50 лавэ)", callback_data="casino_slots_50"),
            InlineKeyboardButton(text="🎰 Слоты (200 лавэ)", callback_data="casino_slots_200")
        ],
        [
            InlineKeyboardButton(text="🎲 Кости (100 лавэ)", callback_data="casino_dice_100"),
            InlineKeyboardButton(text="🎲 Кости (500 лавэ)", callback_data="casino_dice_500")
        ],
        [
            InlineKeyboardButton(text="🃏 Рулетка (ВСЁ!)", callback_data="casino_roulette_all")
        ]
    ])
    
    await message.answer(
        f"🎰 *КАЗИНО 'БЕСПРЕДЕЛ'*\n\n"
        f"💰 Твой баланс: {player['money']} лавэ\n\n"
        f"Выбирай игру, братиш:",
        reply_markup=keyboard,
        parse_mode=ParseMode.MARKDOWN
    )


@router.callback_query(F.data.startswith("casino_"))
async def casino_game(callback: CallbackQuery):
    """Игра в казино"""
    data = callback.data.split("_")
    game_type = data[1]
    bet = data[2]
    
    user_id = callback.from_user.id
    chat_id = callback.message.chat.id
    
    player = await get_player(user_id, chat_id)
    if not player:
        await callback.answer("❌ Ты не в игре!", show_alert=True)
        return
    
    # Определяем ставку
    if bet == "all":
        bet_amount = player['money']
    else:
        bet_amount = int(bet)
    
    if player['money'] < bet_amount:
        await callback.answer("💸 Не хватает лавэ, нищеброд!", show_alert=True)
        return
    
    if bet_amount < 10:
        await callback.answer("❌ Минимальная ставка 10 лавэ!", show_alert=True)
        return
    
    # Проверка кулдауна
    can_do, cooldown_remaining = check_cooldown(user_id, chat_id, "casino", 10)
    if not can_do:
        await callback.answer(f"⏰ Подожди {cooldown_remaining} сек!", show_alert=True)
        return
    
    result_text = ""
    
    if game_type == "slots":
        # Слоты
        symbols = ["🍋", "🍒", "🍀", "💎", "7️⃣", "💰"]
        weights = [30, 25, 20, 15, 7, 3]  # Вероятности
        
        result = random.choices(symbols, weights=weights, k=3)
        
        if result[0] == result[1] == result[2]:
            # Джекпот!
            if result[0] == "💰":
                multiplier = 10
                result_text = f"🎰 [ {' '.join(result)} ]\n\n💰💰💰 МЕГА ДЖЕКПОТ!!! x{multiplier}"
            elif result[0] == "7️⃣":
                multiplier = 7
                result_text = f"🎰 [ {' '.join(result)} ]\n\n🔥 ДЖЕКПОТ!!! x{multiplier}"
            elif result[0] == "💎":
                multiplier = 5
                result_text = f"🎰 [ {' '.join(result)} ]\n\n💎 БРИЛЛИАНТОВЫЙ ВЫИГРЫШ! x{multiplier}"
            else:
                multiplier = 3
                result_text = f"🎰 [ {' '.join(result)} ]\n\n🎉 ТРИ В РЯД! x{multiplier}"
            
            winnings = bet_amount * multiplier
            await update_player_stats(user_id, chat_id, money=f"+{winnings - bet_amount}")
            result_text += f"\n\n💰 +{winnings} лавэ!"
        
        elif result[0] == result[1] or result[1] == result[2]:
            # Два совпадения
            winnings = int(bet_amount * 1.5)
            await update_player_stats(user_id, chat_id, money=f"+{winnings - bet_amount}")
            result_text = f"🎰 [ {' '.join(result)} ]\n\n✨ Две одинаковых!\n💰 +{winnings} лавэ"
        
        else:
            # Проигрыш
            await update_player_stats(user_id, chat_id, money=f"-{bet_amount}")
            result_text = f"🎰 [ {' '.join(result)} ]\n\n😭 Мимо! -{bet_amount} лавэ"
    
    elif game_type == "dice":
        # Кости
        player_roll = random.randint(1, 6) + random.randint(1, 6)
        dealer_roll = random.randint(1, 6) + random.randint(1, 6)
        
        dice_emoji = ["⚀", "⚁", "⚂", "⚃", "⚄", "⚅"]
        
        result_text = f"🎲 Ты выкинул: {player_roll}\n🎲 Крупье выкинул: {dealer_roll}\n\n"
        
        if player_roll > dealer_roll:
            winnings = bet_amount * 2
            await update_player_stats(user_id, chat_id, money=f"+{bet_amount}")
            result_text += f"🎉 ПОБЕДА! +{winnings} лавэ"
        elif player_roll < dealer_roll:
            await update_player_stats(user_id, chat_id, money=f"-{bet_amount}")
            result_text += f"💀 Крупье победил! -{bet_amount} лавэ"
        else:
            result_text += "🤝 Ничья! Ставка возвращена"
    
    elif game_type == "roulette":
        # Рулетка — всё или ничего
        if random.random() < 0.45:  # 45% шанс на победу
            winnings = bet_amount * 2
            await update_player_stats(user_id, chat_id, money=f"+{bet_amount}")
            result_text = f"🎡 Рулетка крутится...\n\n🔴 КРАСНОЕ!\n\n🎉 ТЫ УДВОИЛСЯ! +{winnings} лавэ!"
        else:
            await update_player_stats(user_id, chat_id, money=f"-{bet_amount}")
            result_text = f"🎡 Рулетка крутится...\n\n⚫ ЧЁРНОЕ!\n\n💀 ВСЁ ПОТЕРЯЛ! -{bet_amount} лавэ"
    
    # Часть проигрышей идёт в общак
    if "-" in result_text:
        treasury_cut = int(bet_amount * 0.1)
        await add_to_treasury(chat_id, treasury_cut)
    
    await callback.message.edit_text(result_text)
    await callback.answer()


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
"""
    await message.answer(help_text, parse_mode=ParseMode.MARKDOWN)


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
VERCEL_API_URL = os.getenv("VERCEL_API_URL", "https://your-vercel-app.vercel.app/api/generate-summary")
VISION_API_URL = os.getenv("VISION_API_URL", "")
POEM_API_URL = os.getenv("POEM_API_URL", "")


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
        
        # Отправляем на анализ
        async with aiohttp.ClientSession() as session:
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
        await processing_msg.edit_text(f"❌ Ошибка: {str(e)[:100]}")


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
    poem_api_url = os.getenv("POEM_API_URL") or VERCEL_API_URL.replace("/summary", "/poem")
    
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
        # Собираем контекст (используем новую функцию)
        context_parts = []
        if target_user:
            context_parts.append(f"Ник: @{target_user.username}" if target_user.username else "Ник: нет")
        
        if target_user_id:
            user_context, messages_found = await gather_user_context(chat_id, target_user_id)
            if messages_found > 0:
                context_parts.append(f"\n=== СООБЩЕНИЯ ({messages_found} шт) ===")
                context_parts.append(user_context)
                context_parts.append("=== ИСПОЛЬЗУЙ ЭТО ДЛЯ УНИЖЕНИЯ! ===")
        else:
            messages_found = 0
        
        context = "\n".join(context_parts) if context_parts else "Обычный участник чата"
        logger.info(f"Poem: {target_name}, {messages_found} msgs")
        
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
        await processing_msg.edit_text(f"❌ Ошибка: {str(e)[:100]}")


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
    diagnosis_api_url = VERCEL_API_URL.replace("/summary", "/diagnosis")
    
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
        # Собираем контекст
        context, messages_found = await gather_user_context(chat_id, target_user_id) if target_user_id else ("Пациент молчалив — это симптом", 0)
        logger.info(f"Diagnosis: {target_name}, {messages_found} msgs")
        
        metrics.track_api_call("diagnosis")
        session = await get_http_session()
        async with session.post(
                diagnosis_api_url,
                json={"name": target_name, "username": target_username or "", "context": context}
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
        await processing_msg.edit_text(f"❌ Ошибка: {str(e)[:100]}")


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
    
    burn_api_url = VERCEL_API_URL.replace("/summary", "/burn")
    
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
        # Собираем контекст
        context, messages_found = await gather_user_context(chat_id, target_user_id) if target_user_id else ("Горел молча, как и жил", 0)
        logger.info(f"Burn: {target_name}, {messages_found} msgs")
        
        metrics.track_api_call("burn")
        session = await get_http_session()
        async with session.post(
                burn_api_url,
                json={"name": target_name, "username": target_username or "", "context": context}
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
        await processing_msg.edit_text(f"❌ Ошибка: {str(e)[:100]}")


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
    
    drink_api_url = VERCEL_API_URL.replace("/summary", "/drink")
    
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
        # Собираем контекст
        context, messages_found = await gather_user_context(chat_id, target_user_id) if target_user_id else ("Молчал как партизан", 0)
        logger.info(f"Drink: {target_name}, {messages_found} msgs")
        
        metrics.track_api_call("drink")
        session = await get_http_session()
        async with session.post(
                drink_api_url,
                json={"name": target_name, "username": target_username or "", "context": context}
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
        await processing_msg.edit_text(f"❌ Ошибка: {str(e)[:100]}")


# ==================== ПОСОСИ ====================

SUCK_API_URL = os.getenv("SUCK_API_URL", "")

@router.message(Command("suck", "пососи", "соси", "сосни"))
async def cmd_suck(message: Message):
    """Послать человека сосать — AI генерация"""
    if message.chat.type == "private":
        await message.answer("❌ Сосать только публично!")
        return
    
    target_name = None
    
    if message.reply_to_message and message.reply_to_message.from_user:
        target_name = message.reply_to_message.from_user.first_name
    else:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            target_name = parts[1].strip().replace("@", "")
        else:
            await message.answer("🍭 Кому сосать? Ответь на сообщение или укажи имя!")
            return
    
    if not target_name:
        target_name = "Эй ты"
    
    if not SUCK_API_URL:
        # Fallback если API не настроен
        await message.answer(f"🍭 {target_name}, пососи, пожалуйста. Вселенная ждёт. Соси, блять.")
        return
    
    processing_msg = await message.answer("🍭 Готовлю послание...")
    metrics.track_command("suck")
    
    try:
        metrics.track_api_call("suck")
        session = await get_http_session()
        async with session.post(SUCK_API_URL, json={"name": target_name}) as response:
                if response.status == 200:
                    result = await response.json()
                    text = result.get("text", f"🍭 {target_name}, соси. Тётя Роза так сказала.")
                    await processing_msg.edit_text(text)
                else:
                    error_text = await response.text()
                    logger.error(f"Suck API error: {response.status} - {error_text}")
                    await processing_msg.edit_text(f"🍭 {target_name}, пососи. API сломался, но посыл остался.")
    
    except asyncio.TimeoutError:
        await processing_msg.edit_text(f"🍭 {target_name}, пососи. Тётя Роза задумалась, но посыл ясен.")
    except Exception as e:
        logger.error(f"Error in suck command: {e}")
        await processing_msg.edit_text(f"🍭 {target_name}, соси. Ошибка, но соси.")


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
    try:
        if USE_POSTGRES and victim_id:
            # Берём больше сообщений для точного определения пола по глаголам
            messages = await get_user_messages(chat_id, victim_id, limit=30)
            victim_messages = [m.get('text', '') for m in messages if m.get('text')]
    except Exception as e:
        logger.warning(f"Could not get victim messages: {e}")
    
    # Определяем пол по имени (базовое определение для склонения)
    # API определит более точно по сообщениям
    is_female = False
    name_lower = victim_name.lower() if victim_name else ""
    female_endings = ['а', 'я', 'ия', 'ья']
    male_with_a = ['никита', 'илья', 'кузьма', 'фома', 'лука', 'саша', 'женя']
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
    ventilate_url = VENTILATE_API_URL or VERCEL_API_URL.replace("/summary", "/ventilate")
    
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
                    "initial_gender": gender  # Передаём начальное определение пола
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
                    fallback_events = [
                        f"🪟 Тётя Роза открыла форточку в чате.\n\nЗалетел голубь. Насрал на {mentions['acc']}. Улетел.\n\nПроветрено.",
                        f"🪟 Тётя Роза открыла форточку в чате.\n\nСквозняком сдуло {mentions['acc']} куда-то в угол чата. {mentions['nom']} там теперь сидит.\n\nСвежо.",
                        f"🪟 Тётя Роза открыла форточку в чате.\n\nВорвался холод. {mentions['nom']} {'замёрзла' if api_gender == 'женский' else 'замёрз'} нахуй.\n\nЗакрываю."
                    ]
                    await processing_msg.edit_text(random.choice(fallback_events), parse_mode=ParseMode.HTML)
    
    except asyncio.TimeoutError:
        await processing_msg.edit_text("🪟 Форточка заклинила. Попробуй позже.")
    except Exception as e:
        logger.error(f"Error in ventilate command: {e}")
        metrics.track_error()
        await processing_msg.edit_text(f"🪟 Форточка сломалась: {str(e)[:50]}")


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
        
        async with aiohttp.ClientSession() as session:
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
    query = message.text.split(maxsplit=1)
    
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
        for result in top_results:
            image_url = result.get('original') or result.get('thumbnail')
            if not image_url:
                continue
            
            try:
                # Скачиваем картинку
                async with aiohttp.ClientSession() as session:
                    headers = {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                    }
                    async with session.get(
                        image_url, 
                        timeout=aiohttp.ClientTimeout(total=15),
                        headers=headers
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
        await processing_msg.edit_text(f"❌ Ошибка поиска: {str(e)[:100]}")


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
    
    # Отправляем запрос к Vercel API с памятью
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
                    "memories": memories
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
                    
                    # Разбиваем на части если слишком длинное
                    if len(summary) > 4000:
                        parts = [summary[i:i+4000] for i in range(0, len(summary), 4000)]
                        for part in parts:
                            await message.answer(part)
                    else:
                        await message.answer(summary)
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
        await message.answer(f"❌ Ошибка: {str(e)}")
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


@router.message(F.text, ~F.text.startswith("/"))
async def who_is_this_handler(message: Message):
    """Обработчик 'это кто?' с реплаем или упоминанием"""
    if message.chat.type == "private":
        return
    
    text_lower = message.text.lower().strip()
    
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
    
    # Определяем пол: сначала из БД, потом fallback на простой анализ
    gender = "мужской"  # default
    if USE_POSTGRES and target_id:
        try:
            db_gender = await get_user_gender(target_id)
            if db_gender and db_gender != 'unknown':
                gender = db_gender
                logger.debug(f"Got gender from DB for {target_name}: {gender}")
            else:
                # Если в БД нет — анализируем сообщения пользователя
                result = await analyze_and_update_user_gender(
                    target_id, target_name, target_username or ""
                )
                if result['gender'] != 'unknown':
                    gender = result['gender']
                    logger.debug(f"Analyzed gender for {target_name}: {gender} (confidence: {result['confidence']})")
                else:
                    # Fallback на простой анализ по имени
                    gender = detect_gender_simple(target_name)
        except Exception as e:
            logger.debug(f"Gender detection error: {e}")
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
    
    # Выбираем рандомный ответ и подставляем склонения
    response = random.choice(WHO_IS_THIS_RESPONSES)
    response = response.format(
        name=clickable_name,
        name_gen=clickable_gen,
        name_acc=clickable_acc,
        name_dat=clickable_dat
    )
    
    await message.reply(response, parse_mode=ParseMode.HTML)


@router.message(F.text, ~F.text.startswith("/"))
async def collect_messages_and_exp(message: Message):
    """Сбор всех сообщений + пассивный опыт (кроме команд)"""
    if message.chat.type == "private":
        return
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Сохраняем информацию о чате
    await save_chat_info(
        chat_id=chat_id,
        title=message.chat.title,
        username=message.chat.username,
        chat_type=message.chat.type
    )
    
    # Сохраняем сообщение для аналитики
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
        message_text=message.text[:500] if message.text else "",  # Лимит 500 символов
        message_type="text",
        reply_to_user_id=reply_to_user_id,
        reply_to_first_name=reply_to_first_name,
        reply_to_username=reply_to_username
    )
    
    # Обновляем профиль пользователя (пол) инкрементально
    if USE_POSTGRES and message.text:
        try:
            await update_user_gender_incrementally(
                user_id=user_id,
                new_message=message.text,
                first_name=message.from_user.first_name or "",
                username=message.from_user.username or ""
            )
        except Exception as e:
            logger.debug(f"Gender update error: {e}")
    
    # Пассивный опыт для игроков
    player = await get_player(user_id, chat_id)
    if not player or not player['player_class']:
        return
    
    # Опыт за сообщения с кулдауном 30 сек
    can_get_exp, _ = check_cooldown(user_id, chat_id, "message_exp", 30)
    if can_get_exp:
        exp_gain = random.randint(1, 3)
        money_gain = random.randint(0, 2)
        
        await update_player_stats(
            user_id, chat_id,
            experience=f"+{exp_gain}",
            money=f"+{money_gain}"
        )


@router.message(F.sticker)
async def collect_stickers(message: Message):
    """Сбор стикеров + сохранение в коллекцию мемов"""
    if message.chat.type == "private":
        return
    
    sticker = message.sticker
    
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
            async with aiohttp.ClientSession() as session:
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
            await maybe_send_random_meme(message.chat.id, trigger="photo")
        except Exception as e:
            logger.warning(f"Failed to send random meme after photo: {e}")


@router.message(F.animation)
async def collect_animations(message: Message):
    """Сбор GIF/анимаций + сохранение в коллекцию"""
    if message.chat.type == "private":
        return
    
    animation = message.animation
    caption = message.caption[:200] if message.caption else ""
    
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
            await maybe_send_random_meme(message.chat.id, trigger="animation")
        except Exception as e:
            logger.warning(f"Failed to send random meme after animation: {e}")


@router.message(F.voice | F.video_note)
async def collect_voice(message: Message):
    """Сбор голосовых и кружочков + сохранение в коллекцию"""
    if message.chat.type == "private":
        return
    
    msg_type = "voice" if message.voice else "video_note"
    media_obj = message.voice or message.video_note
    
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
    
    # Сохраняем голосовое/кружочек в коллекцию
    if message.voice:
        voice = message.voice
        sender_name = message.from_user.first_name or "Аноним"
        await save_media(
            chat_id=message.chat.id,
            user_id=message.from_user.id,
            file_id=voice.file_id,
            file_type="voice",
            file_unique_id=voice.file_unique_id,
            description=f"Голосовое от {sender_name} ({voice.duration} сек)"
        )
        # Шанс 15% для теста (потом вернуть на 3%)
        if random.random() < 0.15:
            try:
                await maybe_send_random_meme(message.chat.id, trigger="voice")
            except Exception as e:
                logger.warning(f"Failed to send random meme after voice: {e}")
    
    elif message.video_note:
        video_note = message.video_note
        sender_name = message.from_user.first_name or "Аноним"
        await save_media(
            chat_id=message.chat.id,
            user_id=message.from_user.id,
            file_id=video_note.file_id,
            file_type="video_note",
            file_unique_id=video_note.file_unique_id,
            description=f"Кружочек от {sender_name} ({video_note.duration} сек)"
        )
        # Шанс 15% для теста (потом вернуть на 3%)
        if random.random() < 0.15:
            try:
                await maybe_send_random_meme(message.chat.id, trigger="video_note")
            except Exception as e:
                logger.warning(f"Failed to send random meme after video_note: {e}")


@router.message(F.video)
async def collect_videos(message: Message):
    """Сбор видео + сохранение в коллекцию"""
    if message.chat.type == "private":
        return
    
    video = message.video
    caption = message.caption[:200] if message.caption else ""
    
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
            await maybe_send_random_meme(message.chat.id, trigger="video")
        except Exception as e:
            logger.warning(f"Failed to send random meme after video: {e}")


@router.message(F.audio)
async def collect_audio(message: Message):
    """Сбор аудио/музыки + сохранение в коллекцию"""
    if message.chat.type == "private":
        return
    
    audio = message.audio
    caption = message.caption[:200] if message.caption else ""
    
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
            await maybe_send_random_meme(message.chat.id, trigger="audio")
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
    "🔵 Тётя Роза делится видеокомпроматом.",
    "🔵 Кружочек позора. Наслаждайтесь.",
    "🔵 Это записывали добровольно. Вдумайтесь.",
    "🔵 Лицо из архива. Возможно, ваше. Возможно, нет.",
    "🔵 Видеопривет из прошлого. Кринж обеспечен.",
    "🔵 Рандомный кружок. Рандомное ебало.",
]


async def maybe_send_random_meme(chat_id: int, trigger: str = "random"):
    """Отправить случайный мем из коллекции (если есть)"""
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
        
        await increment_media_usage(media_id)
        metrics.track_command("meme")
        
    except Exception as e:
        logger.error(f"Error sending meme: {e}")
        await message.answer("❌ Мем сломался. Попробуй ещё раз.")


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
    async def wrapper(message: Message, *args, **kwargs):
        if message.chat.type != "private":
            return
        if not is_admin(message.from_user.id):
            return
        return await func(message, *args, **kwargs)
    return wrapper


def admin_postgres_only(func):
    """Декоратор для админских команд, требующих PostgreSQL"""
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
    # Если ADMIN_IDS не настроен — разрешаем всем в приватке
    if not ADMIN_IDS:
        return True
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

🔍 *Поиск:*
/chat `<id>` — инфо о чате
/finduser `<имя>` — поиск пользователя

🛠 *Управление:*
/cleanup — очистка старых данных
/health — проверка состояния системы
/migrate\_media — миграция медиа в коллекцию
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
        
        text = f"""📊 *ПОЛНАЯ СТАТИСТИКА БОТА*

🌐 *Охват:*
• Всего чатов: *{stats.get('total_chats', 0):,}*
• Активных чатов (24ч): *{stats.get('active_chats_24h', 0)}*
• Всего пользователей: *{stats.get('total_users', 0):,}*

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
    api_calls = "\n".join([f"  • {api}: {count}" for api, count in stats['api_calls'].items()]) or "  Нет данных"
    
    text = f"""📈 МЕТРИКИ БОТА

⏱ Аптайм: {stats['uptime_human']}

📊 Команды ({stats['total_commands']} всего):
{top_cmds}

🌐 API вызовы ({stats['total_api_calls']} всего):
{api_calls}

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


async def main():
    """Главная функция запуска бота"""
    # Инициализация БД
    await init_db()
    
    # Подключаем роутер
    dp.include_router(router)
    
    # Регистрируем shutdown handler
    dp.shutdown.register(on_shutdown)
    
    # Запуск планировщика для очистки и мониторинга
    if USE_POSTGRES:
        scheduler.add_job(scheduled_cleanup, 'interval', hours=6, id='cleanup')
        scheduler.add_job(log_database_stats, 'interval', hours=1, id='stats')
    
    # Очистка памяти (cooldowns и api_calls) каждые 10 минут
    scheduler.add_job(cleanup_memory, 'interval', minutes=10, id='memory_cleanup')
    scheduler.start()
    
    if USE_POSTGRES:
        logger.info("⏰ Планировщик запущен: очистка БД (6ч), статистика (1ч), память (10м)")
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
