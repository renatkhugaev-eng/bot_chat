import aiosqlite
import time
from typing import Optional, Dict, Any, List

DATABASE_PATH = "guild_of_crime.db"


async def init_db():
    """Инициализация базы данных"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        # Таблица сообщений чата (для сводок)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT,
                first_name TEXT,
                message_text TEXT,
                message_type TEXT DEFAULT 'text',
                reply_to_user_id INTEGER,
                reply_to_first_name TEXT,
                reply_to_username TEXT,
                sticker_emoji TEXT,
                image_description TEXT,
                created_at INTEGER NOT NULL
            )
        """)
        
        # Индекс для быстрого поиска по времени
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_time 
            ON chat_messages(chat_id, created_at)
        """)
        
        # Таблица игроков
        await db.execute("""
            CREATE TABLE IF NOT EXISTS players (
                user_id INTEGER PRIMARY KEY,
                chat_id INTEGER NOT NULL,
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
                jail_until INTEGER DEFAULT 0,
                last_crime_time INTEGER DEFAULT 0,
                last_attack_time INTEGER DEFAULT 0,
                last_work_time INTEGER DEFAULT 0,
                total_stolen INTEGER DEFAULT 0,
                total_lost INTEGER DEFAULT 0,
                created_at INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1
            )
        """)
        
        # Таблица инвентаря
        await db.execute("""
            CREATE TABLE IF NOT EXISTS inventory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                item_name TEXT NOT NULL,
                item_type TEXT NOT NULL,
                bonus_attack INTEGER DEFAULT 0,
                bonus_luck INTEGER DEFAULT 0,
                bonus_steal INTEGER DEFAULT 0,
                acquired_at INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES players(user_id)
            )
        """)
        
        # Таблица достижений
        await db.execute("""
            CREATE TABLE IF NOT EXISTS achievements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                achievement_name TEXT NOT NULL,
                achieved_at INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES players(user_id),
                UNIQUE(user_id, achievement_name)
            )
        """)
        
        # Таблица логов событий (для статистики и приколов)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS event_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                user_id INTEGER,
                target_id INTEGER,
                amount INTEGER DEFAULT 0,
                details TEXT,
                created_at INTEGER DEFAULT 0
            )
        """)
        
        # Общак чата
        await db.execute("""
            CREATE TABLE IF NOT EXISTS chat_treasury (
                chat_id INTEGER PRIMARY KEY,
                money INTEGER DEFAULT 0,
                last_raid_time INTEGER DEFAULT 0
            )
        """)
        
        await db.commit()


async def get_player(user_id: int, chat_id: int) -> Optional[Dict[str, Any]]:
    """Получить данные игрока"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM players WHERE user_id = ? AND chat_id = ?",
            (user_id, chat_id)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return dict(row)
    return None


async def create_player(user_id: int, chat_id: int, username: str, first_name: str) -> Dict[str, Any]:
    """Создать нового игрока"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            INSERT OR IGNORE INTO players 
            (user_id, chat_id, username, first_name, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, chat_id, username, first_name, int(time.time())))
        await db.commit()
    return await get_player(user_id, chat_id)


async def set_player_class(user_id: int, chat_id: int, player_class: str, bonuses: dict):
    """Установить класс игрока"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            UPDATE players 
            SET player_class = ?,
                attack = attack + ?,
                luck = luck + ?
            WHERE user_id = ? AND chat_id = ?
        """, (
            player_class,
            bonuses.get('bonus_attack', 0),
            bonuses.get('bonus_luck', 0),
            user_id,
            chat_id
        ))
        await db.commit()


async def update_player_stats(user_id: int, chat_id: int, **kwargs):
    """Обновить статистику игрока"""
    if not kwargs:
        return
    
    set_clauses = []
    values = []
    
    for key, value in kwargs.items():
        if isinstance(value, str) and value.startswith('+'):
            set_clauses.append(f"{key} = {key} + ?")
            values.append(int(value[1:]))
        elif isinstance(value, str) and value.startswith('-'):
            set_clauses.append(f"{key} = {key} - ?")
            values.append(int(value[1:]))
        else:
            set_clauses.append(f"{key} = ?")
            values.append(value)
    
    values.extend([user_id, chat_id])
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(f"""
            UPDATE players 
            SET {', '.join(set_clauses)}
            WHERE user_id = ? AND chat_id = ?
        """, values)
        await db.commit()


async def get_top_players(chat_id: int, limit: int = 10, sort_by: str = "experience") -> List[Dict[str, Any]]:
    """Получить топ игроков чата"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(f"""
            SELECT * FROM players 
            WHERE chat_id = ? AND is_active = 1 AND player_class IS NOT NULL
            ORDER BY {sort_by} DESC
            LIMIT ?
        """, (chat_id, limit)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_all_active_players(chat_id: int) -> List[Dict[str, Any]]:
    """Получить всех активных игроков чата"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM players 
            WHERE chat_id = ? AND is_active = 1 AND player_class IS NOT NULL
        """, (chat_id,)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def put_in_jail(user_id: int, chat_id: int, seconds: int):
    """Посадить игрока в тюрьму"""
    jail_until = int(time.time()) + seconds
    await update_player_stats(user_id, chat_id, jail_until=jail_until)


async def is_in_jail(user_id: int, chat_id: int) -> tuple[bool, int]:
    """Проверить, в тюрьме ли игрок. Возвращает (в_тюрьме, оставшееся_время)"""
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
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            INSERT INTO chat_treasury (chat_id, money)
            VALUES (?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET money = money + ?
        """, (chat_id, amount, amount))
        await db.commit()


async def get_treasury(chat_id: int) -> int:
    """Получить общак чата"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        async with db.execute(
            "SELECT money FROM chat_treasury WHERE chat_id = ?",
            (chat_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0


async def log_event(chat_id: int, event_type: str, user_id: int = None, 
                    target_id: int = None, amount: int = 0, details: str = None):
    """Записать событие в лог"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            INSERT INTO event_log (chat_id, event_type, user_id, target_id, amount, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (chat_id, event_type, user_id, target_id, amount, details, int(time.time())))
        await db.commit()


async def add_achievement(user_id: int, achievement_name: str) -> bool:
    """Добавить достижение игроку. Возвращает True если это новое достижение"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        try:
            await db.execute("""
                INSERT INTO achievements (user_id, achievement_name, achieved_at)
                VALUES (?, ?, ?)
            """, (user_id, achievement_name, int(time.time())))
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False


async def get_player_achievements(user_id: int) -> List[str]:
    """Получить все достижения игрока"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        async with db.execute(
            "SELECT achievement_name FROM achievements WHERE user_id = ?",
            (user_id,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [row[0] for row in rows]


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
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            INSERT INTO chat_messages 
            (chat_id, user_id, username, first_name, message_text, message_type,
             reply_to_user_id, reply_to_first_name, reply_to_username, sticker_emoji, 
             image_description, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            chat_id, user_id, username, first_name, message_text, message_type,
            reply_to_user_id, reply_to_first_name, reply_to_username, sticker_emoji,
            image_description, int(time.time())
        ))
        await db.commit()


async def get_chat_messages(chat_id: int, hours: int = 5) -> List[Dict[str, Any]]:
    """Получить сообщения чата за последние N часов"""
    since_time = int(time.time()) - (hours * 3600)
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM chat_messages 
            WHERE chat_id = ? AND created_at >= ?
            ORDER BY created_at ASC
        """, (chat_id, since_time)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_chat_statistics(chat_id: int, hours: int = 5) -> Dict[str, Any]:
    """Получить статистику чата за последние N часов"""
    since_time = int(time.time()) - (hours * 3600)
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        
        # Общее количество сообщений
        async with db.execute("""
            SELECT COUNT(*) as total FROM chat_messages 
            WHERE chat_id = ? AND created_at >= ?
        """, (chat_id, since_time)) as cursor:
            row = await cursor.fetchone()
            total_messages = row['total'] if row else 0
        
        # Топ авторов по количеству сообщений
        async with db.execute("""
            SELECT user_id, first_name, username, COUNT(*) as msg_count
            FROM chat_messages 
            WHERE chat_id = ? AND created_at >= ?
            GROUP BY user_id
            ORDER BY msg_count DESC
            LIMIT 10
        """, (chat_id, since_time)) as cursor:
            top_authors = [dict(row) for row in await cursor.fetchall()]
        
        # Статистика по типам сообщений
        async with db.execute("""
            SELECT message_type, COUNT(*) as count
            FROM chat_messages 
            WHERE chat_id = ? AND created_at >= ?
            GROUP BY message_type
        """, (chat_id, since_time)) as cursor:
            message_types = {row['message_type']: row['count'] for row in await cursor.fetchall()}
        
        # Кто с кем больше общался (reply connections)
        async with db.execute("""
            SELECT first_name, reply_to_first_name, COUNT(*) as replies
            FROM chat_messages 
            WHERE chat_id = ? AND created_at >= ? AND reply_to_user_id IS NOT NULL
            GROUP BY user_id, reply_to_user_id
            ORDER BY replies DESC
            LIMIT 10
        """, (chat_id, since_time)) as cursor:
            reply_pairs = [dict(row) for row in await cursor.fetchall()]
        
        # Активность по часам
        async with db.execute("""
            SELECT strftime('%H', datetime(created_at, 'unixepoch', 'localtime')) as hour,
                   COUNT(*) as count
            FROM chat_messages 
            WHERE chat_id = ? AND created_at >= ?
            GROUP BY hour
            ORDER BY hour
        """, (chat_id, since_time)) as cursor:
            hourly_activity = {row['hour']: row['count'] for row in await cursor.fetchall()}
        
        # Выборка последних сообщений для контекста (до 50)
        async with db.execute("""
            SELECT first_name, message_text, message_type, sticker_emoji,
                   reply_to_first_name, created_at
            FROM chat_messages 
            WHERE chat_id = ? AND created_at >= ? AND message_type = 'text'
            ORDER BY created_at DESC
            LIMIT 50
        """, (chat_id, since_time)) as cursor:
            recent_messages = [dict(row) for row in await cursor.fetchall()]
        
        return {
            "total_messages": total_messages,
            "top_authors": top_authors,
            "message_types": message_types,
            "reply_pairs": reply_pairs,
            "hourly_activity": hourly_activity,
            "recent_messages": recent_messages[::-1],  # Обратный порядок (старые сначала)
            "hours_analyzed": hours
        }


async def cleanup_old_messages(days: int = 7):
    """Удалить старые сообщения (для экономии места)"""
    cutoff_time = int(time.time()) - (days * 24 * 3600)
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            DELETE FROM chat_messages WHERE created_at < ?
        """, (cutoff_time,))
        await db.commit()


async def get_user_messages(chat_id: int, user_id: int, limit: int = 100) -> List[Dict[str, Any]]:
    """Получить последние N сообщений конкретного пользователя"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT message_text, message_type, sticker_emoji, created_at
            FROM chat_messages 
            WHERE chat_id = ? AND user_id = ? AND message_text IS NOT NULL
            ORDER BY created_at DESC
            LIMIT ?
        """, (chat_id, user_id, limit)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


# ==================== СИСТЕМА ПАМЯТИ (ЗАГЛУШКИ ДЛЯ SQLITE) ====================

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
    """Сохранить сводку в память (заглушка для SQLite)"""
    pass  # В SQLite версии не сохраняем


async def get_previous_summaries(chat_id: int, limit: int = 3) -> List[Dict[str, Any]]:
    """Получить предыдущие сводки (заглушка для SQLite)"""
    return []  # В SQLite версии память не работает


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
    """Сохранить воспоминание (заглушка для SQLite)"""
    pass  # В SQLite версии не сохраняем


async def get_memories(chat_id: int, limit: int = 20) -> List[Dict[str, Any]]:
    """Получить воспоминания (заглушка для SQLite)"""
    return []  # В SQLite версии память не работает
