"""
База данных PostgreSQL (Neon/Vercel Postgres)
Замена SQLite на PostgreSQL для продакшена
"""
import asyncpg
import time
import os
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

load_dotenv()

# URL базы данных из переменных окружения
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL")

# Пул соединений
pool: Optional[asyncpg.Pool] = None


async def init_db():
    """Инициализация базы данных и создание таблиц"""
    global pool
    
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL не установлен! Добавь его в .env")
    
    # Создаём пул соединений
    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=2,
        max_size=10,
        command_timeout=60
    )
    
    async with pool.acquire() as conn:
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
                created_at BIGINT NOT NULL
            )
        """)
        
        # Индекс для быстрого поиска по времени
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_time 
            ON chat_messages(chat_id, created_at)
        """)
        
        # Миграция: добавляем колонку reply_to_username если её нет
        try:
            await conn.execute("""
                ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS reply_to_username TEXT
            """)
        except Exception:
            pass  # Колонка уже существует
        
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
        
        # Таблица инвентаря
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS inventory (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                item_name TEXT NOT NULL,
                item_type TEXT NOT NULL,
                bonus_attack INTEGER DEFAULT 0,
                bonus_luck INTEGER DEFAULT 0,
                bonus_steal INTEGER DEFAULT 0,
                acquired_at BIGINT DEFAULT 0
            )
        """)
        
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
    
    print("[OK] PostgreSQL database initialized!")


async def close_db():
    """Закрыть пул соединений"""
    global pool
    if pool:
        await pool.close()


async def get_player(user_id: int, chat_id: int) -> Optional[Dict[str, Any]]:
    """Получить данные игрока"""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM players WHERE user_id = $1 AND chat_id = $2",
            user_id, chat_id
        )
        if row:
            return dict(row)
    return None


async def create_player(user_id: int, chat_id: int, username: str, first_name: str) -> Dict[str, Any]:
    """Создать нового игрока"""
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO players (user_id, chat_id, username, first_name, created_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (user_id, chat_id) DO NOTHING
        """, user_id, chat_id, username, first_name, int(time.time()))
    return await get_player(user_id, chat_id)


async def set_player_class(user_id: int, chat_id: int, player_class: str, bonuses: dict):
    """Установить класс игрока"""
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE players 
            SET player_class = $1,
                attack = attack + $2,
                luck = luck + $3
            WHERE user_id = $4 AND chat_id = $5
        """, player_class, bonuses.get('bonus_attack', 0), bonuses.get('bonus_luck', 0), user_id, chat_id)


async def update_player_stats(user_id: int, chat_id: int, **kwargs):
    """Обновить статистику игрока"""
    if not kwargs:
        return
    
    set_clauses = []
    values = []
    param_num = 1
    
    for key, value in kwargs.items():
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
    
    values.extend([user_id, chat_id])
    
    query = f"""
        UPDATE players 
        SET {', '.join(set_clauses)}
        WHERE user_id = ${param_num} AND chat_id = ${param_num + 1}
    """
    
    async with pool.acquire() as conn:
        await conn.execute(query, *values)


async def get_top_players(chat_id: int, limit: int = 10, sort_by: str = "experience") -> List[Dict[str, Any]]:
    """Получить топ игроков чата"""
    # Защита от SQL injection - только разрешённые поля
    allowed_fields = ["experience", "money", "crimes_success", "pvp_wins"]
    if sort_by not in allowed_fields:
        sort_by = "experience"
    
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT * FROM players 
            WHERE chat_id = $1 AND is_active = 1 AND player_class IS NOT NULL
            ORDER BY {sort_by} DESC
            LIMIT $2
        """, chat_id, limit)
        return [dict(row) for row in rows]


async def get_all_active_players(chat_id: int) -> List[Dict[str, Any]]:
    """Получить всех активных игроков чата"""
    async with pool.acquire() as conn:
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
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO chat_treasury (chat_id, money)
            VALUES ($1, $2)
            ON CONFLICT(chat_id) DO UPDATE SET money = chat_treasury.money + $2
        """, chat_id, amount)


async def get_treasury(chat_id: int) -> int:
    """Получить общак чата"""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT money FROM chat_treasury WHERE chat_id = $1",
            chat_id
        )
        return row['money'] if row else 0


async def log_event(chat_id: int, event_type: str, user_id: int = None, 
                    target_id: int = None, amount: int = 0, details: str = None):
    """Записать событие в лог"""
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO event_log (chat_id, event_type, user_id, target_id, amount, details, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """, chat_id, event_type, user_id, target_id, amount, details, int(time.time()))


async def add_achievement(user_id: int, achievement_name: str) -> bool:
    """Добавить достижение игроку"""
    async with pool.acquire() as conn:
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
    async with pool.acquire() as conn:
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
    sticker_emoji: str = None
):
    """Сохранить сообщение чата для аналитики"""
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO chat_messages 
            (chat_id, user_id, username, first_name, message_text, message_type,
             reply_to_user_id, reply_to_first_name, reply_to_username, sticker_emoji, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        """, chat_id, user_id, username, first_name, message_text, message_type,
             reply_to_user_id, reply_to_first_name, reply_to_username, sticker_emoji, int(time.time()))


async def get_chat_messages(chat_id: int, hours: int = 5) -> List[Dict[str, Any]]:
    """Получить сообщения чата за последние N часов"""
    since_time = int(time.time()) - (hours * 3600)
    
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM chat_messages 
            WHERE chat_id = $1 AND created_at >= $2
            ORDER BY created_at ASC
        """, chat_id, since_time)
        return [dict(row) for row in rows]


async def get_chat_statistics(chat_id: int, hours: int = 5) -> Dict[str, Any]:
    """Получить статистику чата за последние N часов"""
    since_time = int(time.time()) - (hours * 3600)
    
    async with pool.acquire() as conn:
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
                   reply_to_first_name, reply_to_username, created_at
            FROM chat_messages 
            WHERE chat_id = $1 AND created_at >= $2 AND message_type = 'text'
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


async def cleanup_old_messages(days: int = 7):
    """Удалить старые сообщения"""
    cutoff_time = int(time.time()) - (days * 24 * 3600)
    
    async with pool.acquire() as conn:
        await conn.execute("""
            DELETE FROM chat_messages WHERE created_at < $1
        """, cutoff_time)
