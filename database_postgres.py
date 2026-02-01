"""
База данных PostgreSQL (Neon/Vercel Postgres)
Замена SQLite на PostgreSQL для продакшена
v3.1 - Production-ready: SSL, retry logic, connection health checks
"""
import asyncpg
import asyncio
import time
import os
import logging
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# URL базы данных из переменных окружения
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL")

# Пул соединений
pool: Optional[asyncpg.Pool] = None


def _ensure_ssl_in_url(url: str) -> str:
    """Добавить SSL параметры для Neon если их нет"""
    if not url:
        return url
    if "sslmode=" not in url:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}sslmode=require"
    return url


async def _execute_with_retry(coro_func, *args, max_retries: int = 3, **kwargs):
    """Выполнить запрос с повторными попытками при сбое соединения"""
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            return await coro_func(*args, **kwargs)
        except (asyncpg.ConnectionDoesNotExistError, 
                asyncpg.InterfaceError,
                asyncpg.ConnectionFailureError) as e:
            last_exception = e
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 0.5  # 0.5, 1.0, 1.5 сек
                logger.warning(f"DB connection error, retry {attempt + 1}/{max_retries} in {wait_time}s: {e}")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"DB connection failed after {max_retries} retries: {e}")
    
    raise last_exception


async def get_pool():
    """Получить пул соединений с проверкой инициализации"""
    global pool
    if pool is None:
        raise RuntimeError("Database pool not initialized! Call init_db() first.")
    return pool


async def init_db():
    """Инициализация базы данных и создание таблиц"""
    global pool
    
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL не установлен! Добавь его в .env")
    
    # Добавляем SSL для Neon
    db_url = _ensure_ssl_in_url(DATABASE_URL)
    
    # Создаём пул соединений с оптимальными настройками для Neon serverless
    pool = await asyncpg.create_pool(
        db_url,
        min_size=1,           # Минимум соединений (Neon serverless режим)
        max_size=10,          # Максимум соединений
        max_inactive_connection_lifetime=60,  # Закрывать неактивные через 60 сек
        command_timeout=60,   # Таймаут команды
        statement_cache_size=100  # Кэш подготовленных запросов
    )
    
    logger.info("🗄 Подключение к PostgreSQL установлено")
    
    async with (await get_pool()).acquire() as conn:
        # Таблица сообщений чата (для сводок)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS chat_messages (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                username TEXT,
                first_name TEXT,
                message_text TEXT,
                message_type TEXT DEFAULT 'text',
                reply_to_user_id BIGINT,
                reply_to_first_name TEXT,
                reply_to_username TEXT,
                sticker_emoji TEXT,
                image_description TEXT,
                file_id TEXT,
                file_unique_id TEXT,
                created_at BIGINT NOT NULL
            )
        """)
        
        # Индекс для быстрого поиска по времени
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_time 
            ON chat_messages(chat_id, created_at)
        """)
        
        # Индекс для поиска сообщений пользователя
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_user 
            ON chat_messages(chat_id, user_id, created_at DESC)
        """)
        
        # Миграция: добавляем колонку reply_to_username если её нет
        try:
            await conn.execute("""
                ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS reply_to_username TEXT
            """)
        except Exception:
            pass  # Колонка уже существует
        
        # Миграция: добавляем колонку image_description для описаний фото
        try:
            await conn.execute("""
                ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS image_description TEXT
            """)
        except Exception:
            pass
        
        # Добавляем file_id для хранения медиа
        try:
            await conn.execute("""
                ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS file_id TEXT
            """)
        except Exception:
            pass
        
        try:
            await conn.execute("""
                ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS file_unique_id TEXT
            """)
        except Exception:
            pass  # Колонка уже существует
        
        # Таблица сводок (память между сессиями)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS chat_summaries (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                summary_text TEXT NOT NULL,
                key_facts TEXT,
                top_talker_username TEXT,
                top_talker_name TEXT,
                top_talker_count INTEGER,
                drama_pairs TEXT,
                memorable_quotes TEXT,
                created_at BIGINT NOT NULL
            )
        """)
        
        # Индекс для быстрого поиска сводок по чату
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_summaries_chat 
            ON chat_summaries(chat_id, created_at DESC)
        """)
        
        # Таблица воспоминаний о участниках (долгосрочная память)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS chat_memories (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                username TEXT,
                first_name TEXT,
                memory_type TEXT NOT NULL,
                memory_text TEXT NOT NULL,
                relevance_score INTEGER DEFAULT 5,
                created_at BIGINT NOT NULL,
                expires_at BIGINT,
                UNIQUE(chat_id, user_id, memory_type, memory_text)
            )
        """)
        
        # Индекс для поиска воспоминаний
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_memories_chat_user 
            ON chat_memories(chat_id, user_id)
        """)
        
        # Таблица игроков
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS players (
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                username TEXT,
                first_name TEXT,
                player_class TEXT DEFAULT NULL,
                experience INTEGER DEFAULT 0,
                money INTEGER DEFAULT 100,
                health INTEGER DEFAULT 100,
                attack INTEGER DEFAULT 10,
                luck INTEGER DEFAULT 10,
                crimes_success INTEGER DEFAULT 0,
                crimes_fail INTEGER DEFAULT 0,
                pvp_wins INTEGER DEFAULT 0,
                pvp_losses INTEGER DEFAULT 0,
                jail_until BIGINT DEFAULT 0,
                last_crime_time BIGINT DEFAULT 0,
                last_attack_time BIGINT DEFAULT 0,
                last_work_time BIGINT DEFAULT 0,
                total_stolen BIGINT DEFAULT 0,
                total_lost BIGINT DEFAULT 0,
                created_at BIGINT DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                PRIMARY KEY (user_id, chat_id)
            )
        """)
        
        # Таблица инвентаря (с chat_id для разделения по чатам)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS inventory (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL DEFAULT 0,
                item_name TEXT NOT NULL,
                item_type TEXT NOT NULL,
                bonus_attack INTEGER DEFAULT 0,
                bonus_luck INTEGER DEFAULT 0,
                bonus_steal INTEGER DEFAULT 0,
                acquired_at BIGINT DEFAULT 0
            )
        """)
        
        # Миграция: добавляем chat_id в inventory если его нет
        try:
            await conn.execute("""
                ALTER TABLE inventory ADD COLUMN IF NOT EXISTS chat_id BIGINT DEFAULT 0
            """)
        except Exception:
            pass
        
        # Таблица достижений
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS achievements (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                achievement_name TEXT NOT NULL,
                achieved_at BIGINT DEFAULT 0,
                UNIQUE(user_id, achievement_name)
            )
        """)
        
        # Таблица логов событий
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS event_log (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                event_type TEXT NOT NULL,
                user_id BIGINT,
                target_id BIGINT,
                amount INTEGER DEFAULT 0,
                details TEXT,
                created_at BIGINT DEFAULT 0
            )
        """)
        
        # Общак чата
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS chat_treasury (
                chat_id BIGINT PRIMARY KEY,
                money BIGINT DEFAULT 0,
                last_raid_time BIGINT DEFAULT 0
            )
        """)
        
        # Таблица информации о чатах
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS chats (
                chat_id BIGINT PRIMARY KEY,
                title TEXT,
                username TEXT,
                chat_type TEXT,
                first_seen BIGINT,
                last_activity BIGINT
            )
        """)
        
        # Индекс для логов событий (новый!)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_event_log_chat 
            ON event_log(chat_id, created_at DESC)
        """)
        
        # Индекс для инвентаря по пользователю и чату
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_inventory_user 
            ON inventory(user_id, chat_id)
        """)
        
        # Индекс для быстрой очистки истёкших воспоминаний
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_memories_expires 
            ON chat_memories(expires_at) WHERE expires_at IS NOT NULL
        """)
        
        # Индекс для players по chat_id (для get_all_active_players и других)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_players_chat 
            ON players(chat_id) WHERE is_active = 1
        """)
        
        # Индекс для achievements по user_id
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_achievements_user 
            ON achievements(user_id)
        """)
        
        # Таблица медиа (мемы, картинки, стикеры, гифки)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS chat_media (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                file_id TEXT NOT NULL,
                file_type TEXT NOT NULL,
                file_unique_id TEXT,
                description TEXT,
                caption TEXT,
                usage_count INTEGER DEFAULT 0,
                is_approved INTEGER DEFAULT 1,
                created_at BIGINT NOT NULL,
                last_used_at BIGINT,
                UNIQUE(chat_id, file_unique_id)
            )
        """)
        
        # Индекс для быстрого поиска медиа по чату
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_media_chat 
            ON chat_media(chat_id, file_type, created_at DESC)
        """)
        
        # Таблица профилей пользователей (глобальное определение пола и характеристик)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_profiles (
                user_id BIGINT PRIMARY KEY,
                detected_gender TEXT DEFAULT 'unknown',
                gender_confidence REAL DEFAULT 0.0,
                gender_female_score INTEGER DEFAULT 0,
                gender_male_score INTEGER DEFAULT 0,
                messages_analyzed INTEGER DEFAULT 0,
                last_analysis_at BIGINT,
                first_name TEXT,
                username TEXT,
                created_at BIGINT NOT NULL,
                updated_at BIGINT NOT NULL
            )
        """)
        
        # Индекс для профилей
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_profiles_gender 
            ON user_profiles(detected_gender)
        """)
    
    logger.info("✅ PostgreSQL database initialized!")


async def close_db():
    """Закрыть пул соединений"""
    global pool
    if pool:
        await pool.close()
        pool = None
        logger.info("🗄 PostgreSQL connection pool closed")


async def health_check() -> bool:
    """Проверить соединение с БД"""
    try:
        p = await get_pool()
        async with p.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return True
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False


async def get_player(user_id: int, chat_id: int) -> Optional[Dict[str, Any]]:
    """Получить данные игрока"""
    p = await get_pool()
    async with p.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM players WHERE user_id = $1 AND chat_id = $2",
            user_id, chat_id
        )
        if row:
            return dict(row)
    return None


async def create_player(user_id: int, chat_id: int, username: str, first_name: str) -> Dict[str, Any]:
    """Создать нового игрока"""
    p = await get_pool()
    async with p.acquire() as conn:
        await conn.execute("""
            INSERT INTO players (user_id, chat_id, username, first_name, created_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (user_id, chat_id) DO NOTHING
        """, user_id, chat_id, username, first_name, int(time.time()))
    return await get_player(user_id, chat_id)


async def set_player_class(user_id: int, chat_id: int, player_class: str, bonuses: dict):
    """Установить класс игрока"""
    p = await get_pool()
    async with p.acquire() as conn:
        await conn.execute("""
            UPDATE players 
            SET player_class = $1,
                attack = attack + $2,
                luck = luck + $3
            WHERE user_id = $4 AND chat_id = $5
        """, player_class, bonuses.get('bonus_attack', 0), bonuses.get('bonus_luck', 0), user_id, chat_id)


async def update_player_stats(user_id: int, chat_id: int, **kwargs):
    """Обновить статистику игрока с защитой от SQL injection"""
    if not kwargs:
        return
    
    # Защита от SQL injection — только разрешённые поля
    allowed_fields = {
        'experience', 'money', 'health', 'attack', 'luck',
        'crimes_success', 'crimes_fail', 'pvp_wins', 'pvp_losses',
        'jail_until', 'last_crime_time', 'last_attack_time', 'last_work_time',
        'total_stolen', 'total_lost', 'is_active', 'username', 'first_name'
    }
    
    set_clauses = []
    values = []
    param_num = 1
    
    for key, value in kwargs.items():
        if key not in allowed_fields:
            continue  # Пропускаем неразрешённые поля
            
        if isinstance(value, str) and value.startswith('+'):
            set_clauses.append(f"{key} = {key} + ${param_num}")
            values.append(int(value[1:]))
        elif isinstance(value, str) and value.startswith('-'):
            set_clauses.append(f"{key} = {key} - ${param_num}")
            values.append(int(value[1:]))
        else:
            set_clauses.append(f"{key} = ${param_num}")
            values.append(value)
        param_num += 1
    
    if not set_clauses:
        return
    
    values.extend([user_id, chat_id])
    
    query = f"""
        UPDATE players 
        SET {', '.join(set_clauses)}
        WHERE user_id = ${param_num} AND chat_id = ${param_num + 1}
    """
    
    p = await get_pool()
    async with p.acquire() as conn:
        await conn.execute(query, *values)


async def get_top_players(chat_id: int, limit: int = 10, sort_by: str = "experience") -> List[Dict[str, Any]]:
    """Получить топ игроков чата"""
    # Защита от SQL injection - только разрешённые поля
    allowed_fields = ["experience", "money", "crimes_success", "pvp_wins"]
    if sort_by not in allowed_fields:
        sort_by = "experience"
    
    async with (await get_pool()).acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT * FROM players 
            WHERE chat_id = $1 AND is_active = 1 AND player_class IS NOT NULL
            ORDER BY {sort_by} DESC
            LIMIT $2
        """, chat_id, limit)
        return [dict(row) for row in rows]


async def get_all_active_players(chat_id: int) -> List[Dict[str, Any]]:
    """Получить всех активных игроков чата"""
    async with (await get_pool()).acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM players 
            WHERE chat_id = $1 AND is_active = 1 AND player_class IS NOT NULL
        """, chat_id)
        return [dict(row) for row in rows]


async def put_in_jail(user_id: int, chat_id: int, seconds: int):
    """Посадить игрока в тюрьму"""
    jail_until = int(time.time()) + seconds
    await update_player_stats(user_id, chat_id, jail_until=jail_until)


async def is_in_jail(user_id: int, chat_id: int) -> tuple:
    """Проверить, в тюрьме ли игрок"""
    player = await get_player(user_id, chat_id)
    if not player:
        return False, 0
    
    jail_until = player.get('jail_until', 0)
    current_time = int(time.time())
    
    if jail_until > current_time:
        return True, jail_until - current_time
    return False, 0


async def add_to_treasury(chat_id: int, amount: int):
    """Добавить деньги в общак чата"""
    async with (await get_pool()).acquire() as conn:
        await conn.execute("""
            INSERT INTO chat_treasury (chat_id, money)
            VALUES ($1, $2)
            ON CONFLICT(chat_id) DO UPDATE SET money = chat_treasury.money + $2
        """, chat_id, amount)


async def get_treasury(chat_id: int) -> int:
    """Получить общак чата"""
    async with (await get_pool()).acquire() as conn:
        row = await conn.fetchrow(
            "SELECT money FROM chat_treasury WHERE chat_id = $1",
            chat_id
        )
        return row['money'] if row else 0


async def log_event(chat_id: int, event_type: str, user_id: int = None, 
                    target_id: int = None, amount: int = 0, details: str = None):
    """Записать событие в лог"""
    async with (await get_pool()).acquire() as conn:
        await conn.execute("""
            INSERT INTO event_log (chat_id, event_type, user_id, target_id, amount, details, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """, chat_id, event_type, user_id, target_id, amount, details, int(time.time()))


async def add_achievement(user_id: int, achievement_name: str) -> bool:
    """Добавить достижение игроку"""
    async with (await get_pool()).acquire() as conn:
        try:
            await conn.execute("""
                INSERT INTO achievements (user_id, achievement_name, achieved_at)
                VALUES ($1, $2, $3)
            """, user_id, achievement_name, int(time.time()))
            return True
        except asyncpg.UniqueViolationError:
            return False


async def get_player_achievements(user_id: int) -> List[str]:
    """Получить все достижения игрока"""
    async with (await get_pool()).acquire() as conn:
        rows = await conn.fetch(
            "SELECT achievement_name FROM achievements WHERE user_id = $1",
            user_id
        )
        return [row['achievement_name'] for row in rows]


# ==================== СООБЩЕНИЯ ЧАТА ====================

async def save_chat_message(
    chat_id: int,
    user_id: int,
    username: str,
    first_name: str,
    message_text: str,
    message_type: str = "text",
    reply_to_user_id: int = None,
    reply_to_first_name: str = None,
    reply_to_username: str = None,
    sticker_emoji: str = None,
    image_description: str = None,
    file_id: str = None,
    file_unique_id: str = None
):
    """Сохранить сообщение чата для аналитики"""
    async with (await get_pool()).acquire() as conn:
        await conn.execute("""
            INSERT INTO chat_messages 
            (chat_id, user_id, username, first_name, message_text, message_type,
             reply_to_user_id, reply_to_first_name, reply_to_username, sticker_emoji, 
             image_description, file_id, file_unique_id, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        """, chat_id, user_id, username, first_name, message_text, message_type,
             reply_to_user_id, reply_to_first_name, reply_to_username, sticker_emoji, 
             image_description, file_id, file_unique_id, int(time.time()))


async def get_chat_messages(chat_id: int, hours: int = 5) -> List[Dict[str, Any]]:
    """Получить сообщения чата за последние N часов"""
    since_time = int(time.time()) - (hours * 3600)
    
    async with (await get_pool()).acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM chat_messages 
            WHERE chat_id = $1 AND created_at >= $2
            ORDER BY created_at ASC
        """, chat_id, since_time)
        return [dict(row) for row in rows]


async def get_user_messages(chat_id: int, user_id: int, limit: int = 100) -> List[Dict[str, Any]]:
    """Получить последние N сообщений конкретного пользователя"""
    async with (await get_pool()).acquire() as conn:
        rows = await conn.fetch("""
            SELECT message_text, message_type, sticker_emoji, created_at
            FROM chat_messages 
            WHERE chat_id = $1 AND user_id = $2 AND message_text IS NOT NULL
            ORDER BY created_at DESC
            LIMIT $3
        """, chat_id, user_id, limit)
        return [dict(row) for row in rows]


async def get_chat_statistics(chat_id: int, hours: int = 5) -> Dict[str, Any]:
    """Получить статистику чата за последние N часов"""
    since_time = int(time.time()) - (hours * 3600)
    
    async with (await get_pool()).acquire() as conn:
        # Общее количество сообщений
        row = await conn.fetchrow("""
            SELECT COUNT(*) as total FROM chat_messages 
            WHERE chat_id = $1 AND created_at >= $2
        """, chat_id, since_time)
        total_messages = row['total'] if row else 0
        
        # Топ авторов
        top_authors = await conn.fetch("""
            SELECT user_id, first_name, username, COUNT(*) as msg_count
            FROM chat_messages 
            WHERE chat_id = $1 AND created_at >= $2
            GROUP BY user_id, first_name, username
            ORDER BY msg_count DESC
            LIMIT 10
        """, chat_id, since_time)
        
        # Типы сообщений
        msg_types_rows = await conn.fetch("""
            SELECT message_type, COUNT(*) as count
            FROM chat_messages 
            WHERE chat_id = $1 AND created_at >= $2
            GROUP BY message_type
        """, chat_id, since_time)
        message_types = {row['message_type']: row['count'] for row in msg_types_rows}
        
        # Reply pairs с username
        reply_pairs = await conn.fetch("""
            SELECT first_name, username, reply_to_first_name, reply_to_username, COUNT(*) as replies
            FROM chat_messages 
            WHERE chat_id = $1 AND created_at >= $2 AND reply_to_user_id IS NOT NULL
            GROUP BY user_id, reply_to_user_id, first_name, username, reply_to_first_name, reply_to_username
            ORDER BY replies DESC
            LIMIT 10
        """, chat_id, since_time)
        
        # Активность по часам
        hourly_rows = await conn.fetch("""
            SELECT EXTRACT(HOUR FROM TO_TIMESTAMP(created_at))::TEXT as hour,
                   COUNT(*) as count
            FROM chat_messages 
            WHERE chat_id = $1 AND created_at >= $2
            GROUP BY hour
            ORDER BY hour
        """, chat_id, since_time)
        hourly_activity = {row['hour']: row['count'] for row in hourly_rows}
        
        # Последние сообщения
        recent_messages = await conn.fetch("""
            SELECT first_name, username, message_text, message_type, sticker_emoji,
                   reply_to_first_name, reply_to_username, image_description, created_at
            FROM chat_messages 
            WHERE chat_id = $1 AND created_at >= $2 AND message_type IN ('text', 'photo')
            ORDER BY created_at DESC
            LIMIT 50
        """, chat_id, since_time)
        
        return {
            "total_messages": total_messages,
            "top_authors": [dict(row) for row in top_authors],
            "message_types": message_types,
            "reply_pairs": [dict(row) for row in reply_pairs],
            "hourly_activity": hourly_activity,
            "recent_messages": [dict(row) for row in recent_messages][::-1],
            "hours_analyzed": hours
        }


async def cleanup_old_messages(days: int = 7) -> int:
    """Удалить старые сообщения, возвращает количество удалённых"""
    cutoff_time = int(time.time()) - (days * 24 * 3600)
    
    async with (await get_pool()).acquire() as conn:
        # Сначала считаем сколько удалим
        row = await conn.fetchrow("""
            SELECT COUNT(*) as count FROM chat_messages WHERE created_at < $1
        """, cutoff_time)
        count = row['count'] if row else 0
        
        # Удаляем
        await conn.execute("""
            DELETE FROM chat_messages WHERE created_at < $1
        """, cutoff_time)
        
        return count


# ==================== СИСТЕМА ПАМЯТИ ====================

async def save_summary(
    chat_id: int,
    summary_text: str,
    key_facts: str = None,
    top_talker_username: str = None,
    top_talker_name: str = None,
    top_talker_count: int = None,
    drama_pairs: str = None,
    memorable_quotes: str = None
):
    """Сохранить сводку в память"""
    async with (await get_pool()).acquire() as conn:
        await conn.execute("""
            INSERT INTO chat_summaries 
            (chat_id, summary_text, key_facts, top_talker_username, top_talker_name, 
             top_talker_count, drama_pairs, memorable_quotes, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """, chat_id, summary_text, key_facts, top_talker_username, top_talker_name,
             top_talker_count, drama_pairs, memorable_quotes, int(time.time()))


async def get_previous_summaries(chat_id: int, limit: int = 3) -> List[Dict[str, Any]]:
    """Получить предыдущие сводки для контекста"""
    async with (await get_pool()).acquire() as conn:
        rows = await conn.fetch("""
            SELECT summary_text, key_facts, top_talker_username, top_talker_name,
                   top_talker_count, drama_pairs, memorable_quotes, created_at
            FROM chat_summaries 
            WHERE chat_id = $1
            ORDER BY created_at DESC
            LIMIT $2
        """, chat_id, limit)
        return [dict(row) for row in rows]


async def save_memory(
    chat_id: int,
    user_id: int,
    username: str,
    first_name: str,
    memory_type: str,
    memory_text: str,
    relevance_score: int = 5,
    expires_days: int = 30
):
    """Сохранить воспоминание о участнике"""
    expires_at = int(time.time()) + (expires_days * 24 * 3600) if expires_days else None
    
    async with (await get_pool()).acquire() as conn:
        # Upsert - обновляем если такое воспоминание уже есть
        await conn.execute("""
            INSERT INTO chat_memories 
            (chat_id, user_id, username, first_name, memory_type, memory_text, 
             relevance_score, created_at, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (chat_id, user_id, memory_type, memory_text) 
            DO UPDATE SET relevance_score = chat_memories.relevance_score + 1,
                          created_at = $8
        """, chat_id, user_id, username, first_name, memory_type, memory_text,
             relevance_score, int(time.time()), expires_at)


async def get_memories(chat_id: int, limit: int = 20) -> List[Dict[str, Any]]:
    """Получить воспоминания о чате"""
    current_time = int(time.time())
    
    async with (await get_pool()).acquire() as conn:
        rows = await conn.fetch("""
            SELECT user_id, username, first_name, memory_type, memory_text, 
                   relevance_score, created_at
            FROM chat_memories 
            WHERE chat_id = $1 
              AND (expires_at IS NULL OR expires_at > $2)
            ORDER BY relevance_score DESC, created_at DESC
            LIMIT $3
        """, chat_id, current_time, limit)
        return [dict(row) for row in rows]


async def get_user_memories(chat_id: int, user_id: int) -> List[Dict[str, Any]]:
    """Получить воспоминания о конкретном участнике"""
    current_time = int(time.time())
    
    async with (await get_pool()).acquire() as conn:
        rows = await conn.fetch("""
            SELECT memory_type, memory_text, relevance_score, created_at
            FROM chat_memories 
            WHERE chat_id = $1 AND user_id = $2
              AND (expires_at IS NULL OR expires_at > $3)
            ORDER BY relevance_score DESC
            LIMIT 10
        """, chat_id, user_id, current_time)
        return [dict(row) for row in rows]


async def cleanup_expired_memories() -> int:
    """Удалить истёкшие воспоминания, возвращает количество удалённых"""
    current_time = int(time.time())
    
    async with (await get_pool()).acquire() as conn:
        # Сначала считаем
        row = await conn.fetchrow("""
            SELECT COUNT(*) as count FROM chat_memories 
            WHERE expires_at IS NOT NULL AND expires_at < $1
        """, current_time)
        count = row['count'] if row else 0
        
        # Удаляем
        await conn.execute("""
            DELETE FROM chat_memories WHERE expires_at IS NOT NULL AND expires_at < $1
        """, current_time)
        
        return count


async def cleanup_old_summaries(days: int = 30) -> int:
    """Удалить сводки старше N дней, возвращает количество удалённых"""
    cutoff_time = int(time.time()) - (days * 24 * 3600)
    
    async with (await get_pool()).acquire() as conn:
        # Сначала считаем
        row = await conn.fetchrow("""
            SELECT COUNT(*) as count FROM chat_summaries WHERE created_at < $1
        """, cutoff_time)
        count = row['count'] if row else 0
        
        # Удаляем
        await conn.execute("""
            DELETE FROM chat_summaries WHERE created_at < $1
        """, cutoff_time)
        
        return count


async def get_database_stats() -> Dict[str, Any]:
    """Получить статистику базы данных для мониторинга (ОПТИМИЗИРОВАНО)"""
    async with (await get_pool()).acquire() as conn:
        day_ago = int(time.time()) - 86400
        current_time = int(time.time())
        
        # Один большой запрос вместо 8+ мелких
        row = await conn.fetchrow("""
            SELECT 
                (SELECT COUNT(*) FROM chat_messages) as chat_messages_count,
                (SELECT COUNT(*) FROM chat_summaries) as chat_summaries_count,
                (SELECT COUNT(*) FROM chat_memories) as chat_memories_count,
                (SELECT COUNT(*) FROM players) as players_count,
                (SELECT COUNT(*) FROM achievements) as achievements_count,
                (SELECT COUNT(*) FROM event_log) as event_log_count,
                (SELECT COUNT(*) FROM chat_messages WHERE created_at >= $1) as messages_24h,
                (SELECT COUNT(DISTINCT chat_id) FROM chat_messages WHERE created_at >= $1) as active_chats_24h,
                (SELECT COUNT(DISTINCT chat_id) FROM chat_messages) as total_chats,
                (SELECT COUNT(DISTINCT user_id) FROM chat_messages) as total_users,
                (SELECT COALESCE(SUM(money), 0) FROM chat_treasury) as total_treasury,
                (SELECT MIN(created_at) FROM chat_messages) as oldest_message
        """, day_ago)
        
        stats = {
            'chat_messages_count': row['chat_messages_count'] or 0,
            'chat_summaries_count': row['chat_summaries_count'] or 0,
            'chat_memories_count': row['chat_memories_count'] or 0,
            'players_count': row['players_count'] or 0,
            'achievements_count': row['achievements_count'] or 0,
            'event_log_count': row['event_log_count'] or 0,
            'messages_24h': row['messages_24h'] or 0,
            'active_chats_24h': row['active_chats_24h'] or 0,
            'total_chats': row['total_chats'] or 0,
            'total_users': row['total_users'] or 0,
            'total_treasury': row['total_treasury'] or 0,
        }
        
        # Старейшее сообщение
        oldest = row['oldest_message']
        stats['oldest_message_days'] = (current_time - oldest) // 86400 if oldest else 0
        
        return stats


async def save_chat_info(chat_id: int, title: str = None, username: str = None, chat_type: str = None):
    """Сохранить или обновить информацию о чате"""
    async with (await get_pool()).acquire() as conn:
        await conn.execute("""
            INSERT INTO chats (chat_id, title, username, chat_type, first_seen, last_activity)
            VALUES ($1, $2, $3, $4, $5, $5)
            ON CONFLICT (chat_id) DO UPDATE SET 
                title = COALESCE($2, chats.title),
                username = COALESCE($3, chats.username),
                chat_type = COALESCE($4, chats.chat_type),
                last_activity = $5
        """, chat_id, title, username, chat_type, int(time.time()))


async def get_chat_info(chat_id: int) -> Optional[Dict[str, Any]]:
    """Получить информацию о чате"""
    async with (await get_pool()).acquire() as conn:
        row = await conn.fetchrow("""
            SELECT * FROM chats WHERE chat_id = $1
        """, chat_id)
        return dict(row) if row else None


async def get_all_chats_stats() -> List[Dict[str, Any]]:
    """Получить статистику по всем чатам с названиями"""
    async with (await get_pool()).acquire() as conn:
        day_ago = int(time.time()) - 86400
        week_ago = int(time.time()) - (7 * 86400)
        
        rows = await conn.fetch("""
            SELECT 
                m.chat_id,
                c.title as chat_title,
                c.username as chat_username,
                COUNT(*) as total_messages,
                COUNT(DISTINCT m.user_id) as unique_users,
                COUNT(*) FILTER (WHERE m.created_at >= $1) as messages_24h,
                COUNT(*) FILTER (WHERE m.created_at >= $2) as messages_7d,
                MAX(m.created_at) as last_activity
            FROM chat_messages m
            LEFT JOIN chats c ON m.chat_id = c.chat_id
            GROUP BY m.chat_id, c.title, c.username
            ORDER BY messages_24h DESC, total_messages DESC
            LIMIT 50
        """, day_ago, week_ago)
        
        return [dict(row) for row in rows]


async def get_chat_details(chat_id: int) -> Dict[str, Any]:
    """Получить детальную статистику по конкретному чату"""
    async with (await get_pool()).acquire() as conn:
        day_ago = int(time.time()) - 86400
        
        # Информация о чате
        chat_info = await conn.fetchrow("""
            SELECT title, username, chat_type FROM chats WHERE chat_id = $1
        """, chat_id)
        
        # Основная статистика
        row = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total_messages,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(*) FILTER (WHERE created_at >= $2) as messages_24h,
                MIN(created_at) as first_message,
                MAX(created_at) as last_message
            FROM chat_messages
            WHERE chat_id = $1
        """, chat_id, day_ago)
        
        stats = dict(row) if row else {}
        
        # Добавляем инфо о чате
        if chat_info:
            stats['chat_title'] = chat_info['title']
            stats['chat_username'] = chat_info['username']
            stats['chat_type'] = chat_info['chat_type']
        
        # Топ пользователей
        top_users = await conn.fetch("""
            SELECT 
                user_id, 
                first_name, 
                username,
                COUNT(*) as msg_count
            FROM chat_messages
            WHERE chat_id = $1
            GROUP BY user_id, first_name, username
            ORDER BY msg_count DESC
            LIMIT 10
        """, chat_id)
        stats['top_users'] = [dict(u) for u in top_users]
        
        # Количество сводок
        row = await conn.fetchrow("""
            SELECT COUNT(*) as count FROM chat_summaries WHERE chat_id = $1
        """, chat_id)
        stats['summaries_count'] = row['count'] if row else 0
        
        # Количество воспоминаний
        row = await conn.fetchrow("""
            SELECT COUNT(*) as count FROM chat_memories WHERE chat_id = $1
        """, chat_id)
        stats['memories_count'] = row['count'] if row else 0
        
        # Игроки в чате
        row = await conn.fetchrow("""
            SELECT COUNT(*) as count FROM players WHERE chat_id = $1 AND player_class IS NOT NULL
        """, chat_id)
        stats['players_count'] = row['count'] if row else 0
        
        # Общак чата
        row = await conn.fetchrow("""
            SELECT money FROM chat_treasury WHERE chat_id = $1
        """, chat_id)
        stats['treasury'] = row['money'] if row else 0
        
        return stats


async def get_top_users_global(limit: int = 20) -> List[Dict[str, Any]]:
    """Получить топ самых активных пользователей по всем чатам"""
    async with (await get_pool()).acquire() as conn:
        rows = await conn.fetch("""
            SELECT 
                user_id,
                first_name,
                username,
                COUNT(*) as total_messages,
                COUNT(DISTINCT chat_id) as chats_count
            FROM chat_messages
            GROUP BY user_id, first_name, username
            ORDER BY total_messages DESC
            LIMIT $1
        """, limit)
        
        return [dict(row) for row in rows]


async def search_user(query: str) -> List[Dict[str, Any]]:
    """Поиск пользователя по имени или username"""
    async with (await get_pool()).acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT
                user_id,
                first_name,
                username,
                COUNT(*) as messages
            FROM chat_messages
            WHERE LOWER(first_name) LIKE LOWER($1) 
               OR LOWER(username) LIKE LOWER($1)
            GROUP BY user_id, first_name, username
            ORDER BY messages DESC
            LIMIT 20
        """, f"%{query}%")
        
        return [dict(row) for row in rows]


async def full_cleanup() -> Dict[str, int]:
    """Полная очистка устаревших данных"""
    results = {}
    
    # Очистка сообщений старше 7 дней
    results['messages_deleted'] = await cleanup_old_messages(days=7)
    
    # Очистка сводок старше 30 дней
    results['summaries_deleted'] = await cleanup_old_summaries(days=30)
    
    # Очистка истёкших воспоминаний
    results['memories_deleted'] = await cleanup_expired_memories()
    
    # Очистка старых событий логов (старше 14 дней)
    results['events_deleted'] = await cleanup_old_events(days=14)
    
    return results


async def cleanup_old_events(days: int = 14) -> int:
    """Удалить старые события из лога"""
    cutoff_time = int(time.time()) - (days * 24 * 3600)
    
    async with (await get_pool()).acquire() as conn:
        # Считаем
        row = await conn.fetchrow("""
            SELECT COUNT(*) as count FROM event_log WHERE created_at < $1
        """, cutoff_time)
        count = row['count'] if row else 0
        
        # Удаляем
        await conn.execute("""
            DELETE FROM event_log WHERE created_at < $1
        """, cutoff_time)
        
        return count


# ==================== СИСТЕМА МЕМОВ ====================

async def save_media(
    chat_id: int,
    user_id: int,
    file_id: str,
    file_type: str,
    file_unique_id: str = None,
    description: str = None,
    caption: str = None
) -> bool:
    """Сохранить медиа (мем, стикер, гифку, голосовое) в коллекцию чата"""
    async with (await get_pool()).acquire() as conn:
        try:
            # Если нет file_unique_id — используем file_id как уникальный ключ
            unique_key = file_unique_id or file_id
            
            # Проверяем, есть ли уже такой медиа
            existing = await conn.fetchrow("""
                SELECT id FROM chat_media 
                WHERE chat_id = $1 AND (file_unique_id = $2 OR file_id = $3)
            """, chat_id, unique_key, file_id)
            
            if existing:
                # Уже есть — не дублируем
                return False
            
            await conn.execute("""
                INSERT INTO chat_media 
                (chat_id, user_id, file_id, file_type, file_unique_id, description, caption, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """, chat_id, user_id, file_id, file_type, unique_key, description, caption, int(time.time()))
            logger.info(f"Saved media: type={file_type}, chat={chat_id}")
            return True
        except Exception as e:
            logger.warning(f"Could not save media: {e}")
            return False


async def get_random_media(chat_id: int, file_type: str = None) -> Optional[Dict[str, Any]]:
    """Получить случайное медиа из коллекции чата"""
    async with (await get_pool()).acquire() as conn:
        if file_type:
            row = await conn.fetchrow("""
                SELECT * FROM chat_media 
                WHERE chat_id = $1 AND file_type = $2 AND is_approved = 1
                ORDER BY RANDOM()
                LIMIT 1
            """, chat_id, file_type)
        else:
            row = await conn.fetchrow("""
                SELECT * FROM chat_media 
                WHERE chat_id = $1 AND is_approved = 1
                ORDER BY RANDOM()
                LIMIT 1
            """, chat_id)
        
        return dict(row) if row else None


async def get_media_stats(chat_id: int) -> Dict[str, int]:
    """Получить статистику медиа в чате"""
    async with (await get_pool()).acquire() as conn:
        rows = await conn.fetch("""
            SELECT file_type, COUNT(*) as count
            FROM chat_media
            WHERE chat_id = $1 AND is_approved = 1
            GROUP BY file_type
        """, chat_id)
        
        stats = {row['file_type']: row['count'] for row in rows}
        stats['total'] = sum(stats.values())
        return stats


async def increment_media_usage(media_id: int):
    """Увеличить счётчик использования медиа"""
    async with (await get_pool()).acquire() as conn:
        await conn.execute("""
            UPDATE chat_media 
            SET usage_count = usage_count + 1, last_used_at = $2
            WHERE id = $1
        """, media_id, int(time.time()))


async def get_top_media(chat_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    """Получить самые используемые медиа"""
    async with (await get_pool()).acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM chat_media
            WHERE chat_id = $1 AND is_approved = 1
            ORDER BY usage_count DESC, created_at DESC
            LIMIT $2
        """, chat_id, limit)
        
        return [dict(row) for row in rows]


async def migrate_media_from_messages() -> Dict[str, int]:
    """Мигрировать медиа из chat_messages в chat_media (только те, у которых есть file_id)"""
    async with (await get_pool()).acquire() as conn:
        stats = {'migrated': 0, 'skipped': 0, 'errors': 0}
        
        # Получаем все сообщения с file_id
        rows = await conn.fetch("""
            SELECT chat_id, user_id, message_type, file_id, file_unique_id, 
                   image_description, sticker_emoji, created_at, first_name
            FROM chat_messages 
            WHERE file_id IS NOT NULL 
              AND message_type IN ('photo', 'sticker', 'animation', 'voice', 'video_note')
        """)
        
        for row in rows:
            try:
                # Проверяем, есть ли уже в chat_media
                existing = await conn.fetchrow("""
                    SELECT id FROM chat_media 
                    WHERE chat_id = $1 AND file_unique_id = $2
                """, row['chat_id'], row['file_unique_id'] or row['file_id'])
                
                if existing:
                    stats['skipped'] += 1
                    continue
                
                # Формируем описание
                description = row.get('image_description') or row.get('sticker_emoji') or ''
                if row['message_type'] in ('voice', 'video_note'):
                    description = f"{row['message_type']} от {row.get('first_name', 'Аноним')}"
                
                # Добавляем в chat_media
                await conn.execute("""
                    INSERT INTO chat_media 
                    (chat_id, user_id, file_id, file_type, file_unique_id, description, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                """, row['chat_id'], row['user_id'], row['file_id'], row['message_type'],
                     row['file_unique_id'] or row['file_id'], description, row['created_at'])
                
                stats['migrated'] += 1
            except Exception as e:
                logger.warning(f"Migration error for message: {e}")
                stats['errors'] += 1
        
        return stats


# ==================== ПРОФИЛИ ПОЛЬЗОВАТЕЛЕЙ И ОПРЕДЕЛЕНИЕ ПОЛА ====================

# Расширенные маркеры для определения пола
FEMALE_VERB_MARKERS = [
    # Глаголы прошедшего времени (-ла, -лась)
    'сделала', 'пошла', 'была', 'хотела', 'могла', 'знала', 'видела',
    'написала', 'сказала', 'думала', 'решила', 'поняла', 'взяла',
    'пришла', 'ушла', 'нашла', 'потеряла', 'купила', 'продала',
    'пила', 'ела', 'спала', 'читала', 'смотрела', 'слушала',
    'работала', 'училась', 'жила', 'любила', 'ненавидела',
    'ходила', 'бегала', 'летала', 'ездила', 'плавала',
    'играла', 'пела', 'танцевала', 'рисовала', 'писала',
    'готовила', 'убирала', 'стирала', 'мыла', 'чистила',
    'звонила', 'отвечала', 'спрашивала', 'просила', 'требовала',
    'ждала', 'надеялась', 'верила', 'мечтала', 'планировала',
    'смеялась', 'плакала', 'радовалась', 'грустила', 'злилась',
    'боялась', 'волновалась', 'переживала', 'успокоилась',
    'проснулась', 'уснула', 'легла', 'встала', 'села',
    'оделась', 'разделась', 'помылась', 'накрасилась', 'причесалась',
    'влюбилась', 'развелась', 'родила', 'забеременела',
    'заболела', 'выздоровела', 'похудела', 'поправилась',
    'опоздала', 'успела', 'забыла', 'вспомнила', 'узнала',
    'поехала', 'приехала', 'уехала', 'вернулась', 'осталась',
    'начала', 'закончила', 'продолжила', 'бросила', 'попробовала',
    'получила', 'отдала', 'подарила', 'выиграла', 'проиграла',
    'удивилась', 'обрадовалась', 'расстроилась', 'разозлилась',
    'испугалась', 'обиделась', 'влюбилась', 'разлюбила',
    'ошиблась', 'исправилась', 'изменилась', 'согласилась',
    'отказалась', 'решилась', 'постаралась', 'устроилась',
    'уволилась', 'заработала', 'потратила', 'сэкономила',
    'познакомилась', 'поссорилась', 'помирилась', 'расставалась',
    'скучала', 'соскучилась', 'дождалась', 'надоела', 'достала',
]

FEMALE_ADJ_MARKERS = [
    'рада', 'устала', 'готова', 'довольна', 'счастлива', 'несчастна',
    'злая', 'добрая', 'весёлая', 'грустная', 'красивая', 'умная',
    'глупая', 'сильная', 'слабая', 'больная', 'здоровая',
    'молодая', 'старая', 'высокая', 'низкая', 'толстая', 'худая',
    'беременна', 'замужняя', 'разведённая', 'одинокая',
    'голодная', 'сытая', 'пьяная', 'трезвая', 'уставшая',
    'занятая', 'свободная', 'богатая', 'бедная', 'влюблена',
    'занята', 'увлечена', 'одета', 'раздета', 'накрашена',
    'расстроена', 'раздражена', 'удивлена', 'шокирована',
    'замужем', 'разведена', 'помолвлена', 'беременная',
]

FEMALE_PHRASES = [
    'я девушка', 'я женщина', 'я мама', 'я жена', 'я бабушка',
    'я девочка', 'я тётя', 'я сестра', 'я дочь', 'я подруга',
    'как баба', 'как девка', 'как женщина', 'как мама',
    'мой муж', 'мой парень', 'мой мужчина', 'мой бывший',
    'мой молодой человек', 'мой мч', 'мой бойфренд',
    'у меня месячные', 'критические дни', 'пмс', 'кд',
    'маникюр', 'педикюр', 'эпиляция', 'макияж', 'косметика',
    'платье', 'юбка', 'каблуки', 'туфли', 'сумочка', 'клатч',
    'рожала', 'кормила грудью', 'беременная', 'роды',
    'гинеколог', 'женская консультация', 'узи беременность',
    'декрет', 'в декрете', 'декретный отпуск',
    'подруга сказала', 'подруги', 'с подругами', 'девичник',
    'женский день', 'восьмое марта', 'цветы подарили',
    'кольцо подарил', 'замуж позвал', 'предложение сделал',
]

MALE_VERB_MARKERS = [
    # Глаголы прошедшего времени (-л, -лся)
    'сделал', 'пошёл', 'пошел', 'был', 'хотел', 'мог', 'знал', 'видел',
    'написал', 'сказал', 'думал', 'решил', 'понял', 'взял',
    'пришёл', 'пришел', 'ушёл', 'ушел', 'нашёл', 'нашел', 'потерял',
    'пил', 'ел', 'спал', 'читал', 'смотрел', 'слушал',
    'работал', 'учился', 'жил', 'любил', 'ненавидел',
    'ходил', 'бегал', 'летал', 'ездил', 'плавал',
    'играл', 'пел', 'танцевал', 'рисовал', 'писал',
    'готовил', 'убирал', 'чинил', 'строил', 'ремонтировал',
    'звонил', 'отвечал', 'спрашивал', 'просил', 'требовал',
    'ждал', 'надеялся', 'верил', 'мечтал', 'планировал',
    'смеялся', 'плакал', 'радовался', 'грустил', 'злился',
    'боялся', 'волновался', 'переживал', 'успокоился',
    'проснулся', 'уснул', 'лёг', 'лег', 'встал', 'сел',
    'оделся', 'разделся', 'помылся', 'побрился',
    'влюбился', 'женился', 'развёлся', 'развелся', 'расстался',
    'заболел', 'выздоровел', 'похудел', 'поправился',
    'опоздал', 'успел', 'забыл', 'вспомнил', 'узнал',
    'поехал', 'приехал', 'уехал', 'вернулся', 'остался',
    'начал', 'закончил', 'продолжил', 'бросил', 'попробовал',
    'получил', 'отдал', 'подарил', 'выиграл', 'проиграл',
    'удивился', 'обрадовался', 'расстроился', 'разозлился',
    'испугался', 'обиделся', 'влюбился', 'разлюбил',
    'ошибся', 'исправился', 'изменился', 'согласился',
    'отказался', 'решился', 'постарался', 'устроился',
    'уволился', 'заработал', 'потратил', 'сэкономил',
    'познакомился', 'поссорился', 'помирился', 'расставался',
    'скучал', 'соскучился', 'дождался', 'надоел', 'достал',
]

MALE_ADJ_MARKERS = [
    'рад', 'устал', 'готов', 'доволен', 'счастлив', 'несчастен',
    'злой', 'добрый', 'весёлый', 'грустный', 'красивый', 'умный',
    'глупый', 'сильный', 'слабый', 'больной', 'здоровый',
    'молодой', 'старый', 'высокий', 'низкий', 'толстый', 'худой',
    'женатый', 'разведённый', 'холостой', 'одинокий',
    'голодный', 'сытый', 'пьяный', 'трезвый', 'уставший',
    'занятый', 'свободный', 'богатый', 'бедный', 'влюблён',
    'занят', 'увлечён', 'одет', 'раздет', 'побрит',
    'расстроен', 'раздражён', 'удивлён', 'шокирован',
    'женат', 'разведён', 'помолвлен', 'холост',
]

MALE_PHRASES = [
    'я парень', 'я мужик', 'я муж', 'я отец', 'я папа', 'я дед',
    'я мальчик', 'я дядя', 'я брат', 'я сын', 'я друг', 'я пацан',
    'как мужик', 'как пацан', 'как батя', 'как отец', 'как мужчина',
    'моя жена', 'моя девушка', 'моя женщина', 'моя бывшая',
    'моя подруга', 'моя тёлка', 'моя баба',
    'у меня борода', 'побрился', 'бреюсь', 'борода растёт',
    'служил в армии', 'армия', 'военкомат', 'повестка', 'призыв',
    'качалка', 'штанга', 'гантели', 'бицепс', 'качаюсь', 'жму',
    'с пацанами', 'с друзьями пиво', 'мальчишник', 'на рыбалку',
    'на охоту', 'в гараж', 'машину чинил', 'под машиной',
    'предложение сделал', 'кольцо купил', 'замуж позвал',
    'отец ребёнка', 'дети мои', 'сын родился', 'дочь родилась',
]

# Женские имена для fallback
FEMALE_NAMES = [
    'анна', 'аня', 'мария', 'маша', 'екатерина', 'катя', 'ольга', 'оля',
    'наталья', 'наташа', 'елена', 'лена', 'татьяна', 'таня', 'ирина', 'ира',
    'светлана', 'света', 'юлия', 'юля', 'анастасия', 'настя', 'дарья', 'даша',
    'полина', 'алина', 'виктория', 'вика', 'кристина', 'александра',
    'софья', 'софия', 'алёна', 'алена', 'ксения', 'ксюша', 'вероника', 'марина',
    'валерия', 'лера', 'диана', 'карина', 'арина', 'милана', 'ева', 'яна',
    'регина', 'ангелина', 'валентина', 'людмила', 'люда', 'надежда', 'надя',
    'галина', 'галя', 'лилия', 'лиля', 'жанна', 'инна', 'эльвира', 'элина',
    'оксана', 'лариса', 'вера', 'любовь', 'люба', 'нина', 'зоя', 'рита',
    'алиса', 'соня', 'варя', 'варвара', 'ульяна', 'лиза', 'елизавета',
]

# Мужские имена на -а/-я (исключения)
MALE_NAMES_ENDING_A = [
    'никита', 'илья', 'кузьма', 'фома', 'лука', 'саша', 'женя', 'валя',
    'миша', 'гоша', 'паша', 'лёша', 'леша', 'гриша', 'коля', 'толя',
    'вася', 'петя', 'ваня', 'дима', 'стёпа', 'степа', 'лёня', 'леня',
    'гена', 'боря', 'федя', 'сеня', 'костя', 'витя', 'вова', 'серёжа',
    'сережа', 'андрюша', 'данила', 'данька', 'тёма', 'тема', 'лёва',
]


def analyze_gender_from_text(text: str, name: str = "") -> dict:
    """
    Анализ пола по тексту сообщений.
    Возвращает: {'gender': str, 'confidence': float, 'female_score': int, 'male_score': int}
    """
    text_lower = text.lower()
    
    female_score = 0
    male_score = 0
    
    # Считаем глаголы (вес 3)
    for marker in FEMALE_VERB_MARKERS:
        if f' {marker}' in text_lower or text_lower.startswith(marker):
            female_score += 3
    
    for marker in MALE_VERB_MARKERS:
        if f' {marker}' in text_lower or text_lower.startswith(marker):
            male_score += 3
    
    # Считаем прилагательные (вес 2)
    for marker in FEMALE_ADJ_MARKERS:
        if f' {marker}' in text_lower or text_lower.startswith(marker):
            female_score += 2
    
    for marker in MALE_ADJ_MARKERS:
        if f' {marker}' in text_lower or text_lower.startswith(marker):
            male_score += 2
    
    # Считаем фразы (вес 10 — очень значимо)
    for phrase in FEMALE_PHRASES:
        if phrase in text_lower:
            female_score += 10
    
    for phrase in MALE_PHRASES:
        if phrase in text_lower:
            male_score += 10
    
    # Определяем результат
    total = female_score + male_score
    if total == 0:
        # Fallback по имени
        name_lower = name.lower().strip()
        if name_lower in FEMALE_NAMES:
            return {'gender': 'женский', 'confidence': 0.6, 'female_score': 1, 'male_score': 0}
        elif name_lower in MALE_NAMES_ENDING_A:
            return {'gender': 'мужской', 'confidence': 0.6, 'female_score': 0, 'male_score': 1}
        elif name_lower.endswith(('а', 'я')) and len(name_lower) > 2:
            return {'gender': 'женский', 'confidence': 0.4, 'female_score': 1, 'male_score': 0}
        return {'gender': 'unknown', 'confidence': 0.0, 'female_score': 0, 'male_score': 0}
    
    # Вычисляем уверенность
    if female_score > male_score:
        confidence = female_score / total
        gender = 'женский' if confidence >= 0.6 else 'unknown'
    elif male_score > female_score:
        confidence = male_score / total
        gender = 'мужской' if confidence >= 0.6 else 'unknown'
    else:
        confidence = 0.5
        gender = 'unknown'
    
    return {
        'gender': gender,
        'confidence': round(confidence, 3),
        'female_score': female_score,
        'male_score': male_score
    }


async def get_user_profile(user_id: int) -> Optional[Dict[str, Any]]:
    """Получить профиль пользователя с определённым полом"""
    async with (await get_pool()).acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM user_profiles WHERE user_id = $1",
            user_id
        )
        return dict(row) if row else None


async def get_user_gender(user_id: int) -> str:
    """Быстро получить пол пользователя (или 'unknown')"""
    profile = await get_user_profile(user_id)
    if profile and profile.get('detected_gender') and profile['detected_gender'] != 'unknown':
        return profile['detected_gender']
    return 'unknown'


async def analyze_and_update_user_gender(user_id: int, first_name: str = "", username: str = "") -> dict:
    """
    Проанализировать все сообщения пользователя и обновить его пол в профиле.
    Возвращает результат анализа.
    """
    async with (await get_pool()).acquire() as conn:
        # Получаем все сообщения пользователя
        rows = await conn.fetch("""
            SELECT message_text FROM chat_messages 
            WHERE user_id = $1 AND message_text IS NOT NULL AND message_text != ''
            ORDER BY created_at DESC
            LIMIT 1000
        """, user_id)
        
        messages_count = len(rows)
        
        if messages_count == 0:
            # Нет сообщений — анализируем только по имени
            result = analyze_gender_from_text("", first_name)
        else:
            # Объединяем все сообщения
            all_text = " ".join([row['message_text'] for row in rows])
            result = analyze_gender_from_text(all_text, first_name)
        
        now = int(time.time())
        
        # Обновляем или создаём профиль
        await conn.execute("""
            INSERT INTO user_profiles 
            (user_id, detected_gender, gender_confidence, gender_female_score, 
             gender_male_score, messages_analyzed, last_analysis_at, 
             first_name, username, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)
            ON CONFLICT (user_id) DO UPDATE SET
                detected_gender = $2,
                gender_confidence = $3,
                gender_female_score = $4,
                gender_male_score = $5,
                messages_analyzed = $6,
                last_analysis_at = $7,
                first_name = COALESCE($8, user_profiles.first_name),
                username = COALESCE($9, user_profiles.username),
                updated_at = $10
        """, user_id, result['gender'], result['confidence'], 
             result['female_score'], result['male_score'], messages_count,
             now, first_name or None, username or None, now)
        
        result['messages_analyzed'] = messages_count
        return result


async def update_user_gender_incrementally(user_id: int, new_message: str, first_name: str = "", username: str = "") -> dict:
    """
    Инкрементально обновить пол пользователя на основе нового сообщения.
    Более эффективно чем полный анализ.
    """
    async with (await get_pool()).acquire() as conn:
        # Получаем текущий профиль
        profile = await conn.fetchrow(
            "SELECT * FROM user_profiles WHERE user_id = $1", user_id
        )
        
        # Анализируем новое сообщение
        new_result = analyze_gender_from_text(new_message, first_name)
        
        now = int(time.time())
        
        if profile:
            # Добавляем к существующим очкам
            new_female = profile['gender_female_score'] + new_result['female_score']
            new_male = profile['gender_male_score'] + new_result['male_score']
            messages_count = profile['messages_analyzed'] + 1
            
            # Пересчитываем пол
            total = new_female + new_male
            if total > 0:
                if new_female > new_male:
                    confidence = new_female / total
                    gender = 'женский' if confidence >= 0.55 else 'unknown'
                elif new_male > new_female:
                    confidence = new_male / total
                    gender = 'мужской' if confidence >= 0.55 else 'unknown'
                else:
                    confidence = 0.5
                    gender = 'unknown'
            else:
                confidence = 0.0
                gender = profile['detected_gender']
            
            await conn.execute("""
                UPDATE user_profiles SET
                    detected_gender = $2,
                    gender_confidence = $3,
                    gender_female_score = $4,
                    gender_male_score = $5,
                    messages_analyzed = $6,
                    last_analysis_at = $7,
                    first_name = COALESCE($8, first_name),
                    username = COALESCE($9, username),
                    updated_at = $7
                WHERE user_id = $1
            """, user_id, gender, confidence, new_female, new_male,
                 messages_count, now, first_name or None, username or None)
            
            return {
                'gender': gender, 
                'confidence': round(confidence, 3),
                'female_score': new_female,
                'male_score': new_male,
                'messages_analyzed': messages_count
            }
        else:
            # Создаём новый профиль
            gender = new_result['gender']
            confidence = new_result['confidence']
            
            await conn.execute("""
                INSERT INTO user_profiles 
                (user_id, detected_gender, gender_confidence, gender_female_score,
                 gender_male_score, messages_analyzed, last_analysis_at,
                 first_name, username, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8, $6, $6)
            """, user_id, gender, confidence, new_result['female_score'],
                 new_result['male_score'], now, first_name or None, username or None)
            
            return {
                'gender': gender,
                'confidence': confidence,
                'female_score': new_result['female_score'],
                'male_score': new_result['male_score'],
                'messages_analyzed': 1
            }