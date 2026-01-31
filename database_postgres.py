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
        
        # Индекс для очистки старых сообщений
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_cleanup 
            ON chat_messages(created_at) WHERE created_at < EXTRACT(EPOCH FROM NOW())::BIGINT
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
    image_description: str = None
):
    """Сохранить сообщение чата для аналитики"""
    async with (await get_pool()).acquire() as conn:
        await conn.execute("""
            INSERT INTO chat_messages 
            (chat_id, user_id, username, first_name, message_text, message_type,
             reply_to_user_id, reply_to_first_name, reply_to_username, sticker_emoji, image_description, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """, chat_id, user_id, username, first_name, message_text, message_type,
             reply_to_user_id, reply_to_first_name, reply_to_username, sticker_emoji, image_description, int(time.time()))


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
    """Получить статистику базы данных для мониторинга"""
    async with (await get_pool()).acquire() as conn:
        stats = {}
        
        # Количество записей в таблицах
        tables = ['chat_messages', 'chat_summaries', 'chat_memories', 'players', 'achievements', 'event_log']
        for table in tables:
            try:
                row = await conn.fetchrow(f"SELECT COUNT(*) as count FROM {table}")
                stats[f'{table}_count'] = row['count'] if row else 0
            except Exception:
                stats[f'{table}_count'] = -1
        
        # Размер сообщений за последние сутки
        day_ago = int(time.time()) - 86400
        row = await conn.fetchrow("""
            SELECT COUNT(*) as count FROM chat_messages WHERE created_at >= $1
        """, day_ago)
        stats['messages_24h'] = row['count'] if row else 0
        
        # Старейшее сообщение
        row = await conn.fetchrow("""
            SELECT MIN(created_at) as oldest FROM chat_messages
        """)
        if row and row['oldest']:
            stats['oldest_message_days'] = (int(time.time()) - row['oldest']) // 86400
        else:
            stats['oldest_message_days'] = 0
        
        # Активные чаты за сутки
        row = await conn.fetchrow("""
            SELECT COUNT(DISTINCT chat_id) as count FROM chat_messages WHERE created_at >= $1
        """, day_ago)
        stats['active_chats_24h'] = row['count'] if row else 0
        
        # ВСЕГО уникальных чатов
        row = await conn.fetchrow("""
            SELECT COUNT(DISTINCT chat_id) as count FROM chat_messages
        """)
        stats['total_chats'] = row['count'] if row else 0
        
        # Всего уникальных пользователей
        row = await conn.fetchrow("""
            SELECT COUNT(DISTINCT user_id) as count FROM chat_messages
        """)
        stats['total_users'] = row['count'] if row else 0
        
        # Общак всех чатов
        row = await conn.fetchrow("""
            SELECT COALESCE(SUM(money), 0) as total FROM chat_treasury
        """)
        stats['total_treasury'] = row['total'] if row else 0
        
        return stats


async def get_all_chats_stats() -> List[Dict[str, Any]]:
    """Получить статистику по всем чатам"""
    async with (await get_pool()).acquire() as conn:
        day_ago = int(time.time()) - 86400
        week_ago = int(time.time()) - (7 * 86400)
        
        rows = await conn.fetch("""
            SELECT 
                chat_id,
                COUNT(*) as total_messages,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(*) FILTER (WHERE created_at >= $1) as messages_24h,
                COUNT(*) FILTER (WHERE created_at >= $2) as messages_7d,
                MAX(created_at) as last_activity
            FROM chat_messages
            GROUP BY chat_id
            ORDER BY messages_24h DESC, total_messages DESC
            LIMIT 50
        """, day_ago, week_ago)
        
        return [dict(row) for row in rows]


async def get_chat_details(chat_id: int) -> Dict[str, Any]:
    """Получить детальную статистику по конкретному чату"""
    async with (await get_pool()).acquire() as conn:
        day_ago = int(time.time()) - 86400
        
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