"""
SQLite база данных (для локальной разработки)
v3.0 - Full feature parity with PostgreSQL
"""
import aiosqlite
import time
from typing import Optional, Dict, Any, List

DATABASE_PATH = "guild_of_crime.db"


async def init_db():
    """Инициализация базы данных с полной схемой"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        # Таблица сообщений чата (для сводок) - синхронизировано с PostgreSQL
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
                file_id TEXT,
                file_unique_id TEXT,
                voice_transcription TEXT,
                created_at INTEGER NOT NULL
            )
        """)
        
        # Миграция: добавляем колонки file_id, file_unique_id, voice_transcription если их нет
        for col_name in ['file_id', 'file_unique_id', 'voice_transcription']:
            try:
                await db.execute(f"ALTER TABLE chat_messages ADD COLUMN {col_name} TEXT")
            except Exception:
                pass  # Колонка уже существует
        
        # Индекс для быстрого поиска по времени
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_time 
            ON chat_messages(chat_id, created_at)
        """)
        
        # Индекс для поиска сообщений пользователя
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_user 
            ON chat_messages(chat_id, user_id, created_at DESC)
        """)
        
        # Таблица сводок (память между сессиями)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS chat_summaries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                summary_text TEXT NOT NULL,
                key_facts TEXT,
                top_talker_username TEXT,
                top_talker_name TEXT,
                top_talker_count INTEGER,
                drama_pairs TEXT,
                memorable_quotes TEXT,
                created_at INTEGER NOT NULL
            )
        """)
        
        # Индекс для быстрого поиска сводок по чату
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_summaries_chat 
            ON chat_summaries(chat_id, created_at DESC)
        """)
        
        # Таблица воспоминаний о участниках (долгосрочная память)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS chat_memories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT,
                first_name TEXT,
                memory_type TEXT NOT NULL,
                memory_text TEXT NOT NULL,
                relevance_score INTEGER DEFAULT 5,
                created_at INTEGER NOT NULL,
                expires_at INTEGER,
                UNIQUE(chat_id, user_id, memory_type, memory_text)
            )
        """)
        
        # Индекс для поиска воспоминаний
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_memories_chat_user 
            ON chat_memories(chat_id, user_id)
        """)
        
        # Таблица игроков — ВАЖНО: composite PRIMARY KEY!
        await db.execute("""
            CREATE TABLE IF NOT EXISTS players (
                user_id INTEGER NOT NULL,
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
                is_active INTEGER DEFAULT 1,
                PRIMARY KEY (user_id, chat_id)
            )
        """)
        
        # Таблица инвентаря
        await db.execute("""
            CREATE TABLE IF NOT EXISTS inventory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                item_name TEXT NOT NULL,
                item_type TEXT NOT NULL,
                bonus_attack INTEGER DEFAULT 0,
                bonus_luck INTEGER DEFAULT 0,
                bonus_steal INTEGER DEFAULT 0,
                acquired_at INTEGER DEFAULT 0
            )
        """)
        
        # Индекс для инвентаря
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_inventory_user 
            ON inventory(user_id, chat_id)
        """)
        
        # Таблица достижений
        await db.execute("""
            CREATE TABLE IF NOT EXISTS achievements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                achievement_name TEXT NOT NULL,
                achieved_at INTEGER DEFAULT 0,
                UNIQUE(user_id, chat_id, achievement_name)
            )
        """)
        
        # Таблица логов событий
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
        
        # Индекс для логов событий
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_event_log_chat 
            ON event_log(chat_id, created_at DESC)
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
        
    print("[OK] SQLite database initialized!")


async def close_db():
    """Закрыть соединение (для совместимости с PostgreSQL)"""
    pass  # SQLite не требует закрытия пула


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
        # Защита от SQL injection — только разрешённые поля
        allowed_fields = {
            'experience', 'money', 'health', 'attack', 'luck',
            'crimes_success', 'crimes_fail', 'pvp_wins', 'pvp_losses',
            'jail_until', 'last_crime_time', 'last_attack_time', 'last_work_time',
            'total_stolen', 'total_lost', 'is_active', 'username', 'first_name'
        }
        if key not in allowed_fields:
            continue
            
        if isinstance(value, str) and value.startswith('+'):
            set_clauses.append(f"{key} = {key} + ?")
            values.append(int(value[1:]))
        elif isinstance(value, str) and value.startswith('-'):
            set_clauses.append(f"{key} = {key} - ?")
            values.append(int(value[1:]))
        else:
            set_clauses.append(f"{key} = ?")
            values.append(value)
    
    if not set_clauses:
        return
    
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
    # Защита от SQL injection — только разрешённые поля
    allowed_fields = ["experience", "money", "crimes_success", "pvp_wins"]
    if sort_by not in allowed_fields:
        sort_by = "experience"
    
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


async def is_in_jail(user_id: int, chat_id: int) -> tuple:
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
    image_description: str = None,
    file_id: str = None,
    file_unique_id: str = None,
    voice_transcription: str = None
):
    """Сохранить сообщение чата для аналитики - синхронизировано с PostgreSQL"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            INSERT INTO chat_messages 
            (chat_id, user_id, username, first_name, message_text, message_type,
             reply_to_user_id, reply_to_first_name, reply_to_username, sticker_emoji, 
             image_description, file_id, file_unique_id, voice_transcription, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            chat_id, user_id, username, first_name, message_text, message_type,
            reply_to_user_id, reply_to_first_name, reply_to_username, sticker_emoji,
            image_description, file_id, file_unique_id, voice_transcription, int(time.time())
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


async def get_user_messages(chat_id: int, user_id: int, limit: int = 1000) -> List[Dict[str, Any]]:
    """Получить последние N сообщений конкретного пользователя (по умолчанию 1000)"""
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


async def get_chat_statistics(chat_id: int, hours: int = 5) -> Dict[str, Any]:
    """Получить статистику чата за последние N часов (синхронизировано с PostgreSQL)"""
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
        
        # Топ авторов по количеству сообщений (с username!)
        async with db.execute("""
            SELECT user_id, first_name, username, COUNT(*) as msg_count
            FROM chat_messages 
            WHERE chat_id = ? AND created_at >= ?
            GROUP BY user_id, first_name, username
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
        
        # Кто с кем больше общался (reply connections — ИСПРАВЛЕНО: добавлены user_id)
        async with db.execute("""
            SELECT user_id, reply_to_user_id, first_name, username, 
                   reply_to_first_name, reply_to_username, COUNT(*) as replies
            FROM chat_messages 
            WHERE chat_id = ? AND created_at >= ? AND reply_to_user_id IS NOT NULL
            GROUP BY user_id, reply_to_user_id, first_name, username, reply_to_first_name, reply_to_username
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
        
        # Выборка последних сообщений (включая voice с транскрипцией)
        async with db.execute("""
            SELECT first_name, username, message_text, message_type, sticker_emoji,
                   reply_to_first_name, reply_to_username, image_description, 
                   voice_transcription, created_at
            FROM chat_messages 
            WHERE chat_id = ? AND created_at >= ? 
            AND (message_type IN ('text', 'photo') OR (message_type = 'voice' AND voice_transcription IS NOT NULL))
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


async def cleanup_old_messages(days: int = 7) -> int:
    """Удалить старые сообщения (для экономии места)"""
    cutoff_time = int(time.time()) - (days * 24 * 3600)
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("""
            DELETE FROM chat_messages WHERE created_at < ?
        """, (cutoff_time,))
        deleted = cursor.rowcount
        await db.commit()
        return deleted


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
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            INSERT INTO chat_summaries 
            (chat_id, summary_text, key_facts, top_talker_username, top_talker_name, 
             top_talker_count, drama_pairs, memorable_quotes, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (chat_id, summary_text, key_facts, top_talker_username, top_talker_name,
              top_talker_count, drama_pairs, memorable_quotes, int(time.time())))
        await db.commit()


async def get_previous_summaries(chat_id: int, limit: int = 3) -> List[Dict[str, Any]]:
    """Получить предыдущие сводки для контекста"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT summary_text, key_facts, top_talker_username, top_talker_name,
                   top_talker_count, drama_pairs, memorable_quotes, created_at
            FROM chat_summaries 
            WHERE chat_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        """, (chat_id, limit)) as cursor:
            rows = await cursor.fetchall()
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
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        # Upsert — обновляем если такое воспоминание уже есть
        await db.execute("""
            INSERT INTO chat_memories 
            (chat_id, user_id, username, first_name, memory_type, memory_text, 
             relevance_score, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (chat_id, user_id, memory_type, memory_text) 
            DO UPDATE SET relevance_score = relevance_score + 1,
                          created_at = ?
        """, (chat_id, user_id, username, first_name, memory_type, memory_text,
              relevance_score, int(time.time()), expires_at, int(time.time())))
        await db.commit()


async def get_memories(chat_id: int, limit: int = 20) -> List[Dict[str, Any]]:
    """Получить воспоминания о чате"""
    current_time = int(time.time())
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT user_id, username, first_name, memory_type, memory_text, 
                   relevance_score, created_at
            FROM chat_memories 
            WHERE chat_id = ? 
              AND (expires_at IS NULL OR expires_at > ?)
            ORDER BY relevance_score DESC, created_at DESC
            LIMIT ?
        """, (chat_id, current_time, limit)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_user_memories(chat_id: int, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    """Получить воспоминания о конкретном участнике"""
    current_time = int(time.time())
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT memory_type, memory_text, relevance_score, created_at
            FROM chat_memories 
            WHERE chat_id = ? AND user_id = ?
              AND (expires_at IS NULL OR expires_at > ?)
            ORDER BY relevance_score DESC
            LIMIT ?
        """, (chat_id, user_id, current_time, limit)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def cleanup_expired_memories() -> int:
    """Удалить истёкшие воспоминания"""
    current_time = int(time.time())
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("""
            DELETE FROM chat_memories WHERE expires_at IS NOT NULL AND expires_at < ?
        """, (current_time,))
        deleted = cursor.rowcount
        await db.commit()
        return deleted


async def cleanup_old_summaries(days: int = 30) -> int:
    """Удалить сводки старше N дней"""
    cutoff_time = int(time.time()) - (days * 24 * 3600)
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("""
            DELETE FROM chat_summaries WHERE created_at < ?
        """, (cutoff_time,))
        deleted = cursor.rowcount
        await db.commit()
        return deleted


async def get_database_stats() -> Dict[str, Any]:
    """Получить статистику базы данных для мониторинга"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        stats = {}
        
        # Количество записей в таблицах
        tables = ['chat_messages', 'chat_summaries', 'chat_memories', 'players', 'achievements']
        for table in tables:
            try:
                async with db.execute(f"SELECT COUNT(*) as count FROM {table}") as cursor:
                    row = await cursor.fetchone()
                    stats[f'{table}_count'] = row[0] if row else 0
            except Exception:
                stats[f'{table}_count'] = 0
        
        # Размер сообщений за последние сутки
        day_ago = int(time.time()) - 86400
        async with db.execute("""
            SELECT COUNT(*) as count FROM chat_messages WHERE created_at >= ?
        """, (day_ago,)) as cursor:
            row = await cursor.fetchone()
            stats['messages_24h'] = row[0] if row else 0
        
        # Старейшее сообщение
        async with db.execute("""
            SELECT MIN(created_at) as oldest FROM chat_messages
        """) as cursor:
            row = await cursor.fetchone()
            if row and row[0]:
                stats['oldest_message_days'] = (int(time.time()) - row[0]) // 86400
            else:
                stats['oldest_message_days'] = 0
        
        # Активные чаты за сутки
        async with db.execute("""
            SELECT COUNT(DISTINCT chat_id) as count FROM chat_messages WHERE created_at >= ?
        """, (day_ago,)) as cursor:
            row = await cursor.fetchone()
            stats['active_chats_24h'] = row[0] if row else 0
        
        return stats


async def cleanup_old_events(days: int = 30) -> int:
    """Очистка старых записей из event_log"""
    threshold = int(time.time()) - (days * 24 * 60 * 60)
    db = await get_db()
    async with db.execute(
        "DELETE FROM event_log WHERE created_at < ?",
        (threshold,)
    ) as cursor:
        deleted = cursor.rowcount
    await db.commit()
    return deleted


async def full_cleanup() -> Dict[str, int]:
    """Полная очистка устаревших данных"""
    results = {}
    
    # Очистка сообщений старше 7 дней
    results['messages_deleted'] = await cleanup_old_messages(days=7)
    
    # Очистка сводок старше 30 дней
    results['summaries_deleted'] = await cleanup_old_summaries(days=30)
    
    # Очистка истёкших воспоминаний
    results['memories_deleted'] = await cleanup_expired_memories()
    
    # Очистка старых событий (30 дней)
    results['events_deleted'] = await cleanup_old_events(days=30)
    
    return results


# ==================== ПРОФИЛИРОВАНИЕ (STUB для SQLite) ====================
# Эти функции не реализованы в SQLite версии, используются только в PostgreSQL

async def update_user_profile_comprehensive(
    user_id: int,
    chat_id: int,
    message_text: str,
    timestamp: int,
    first_name: str = "",
    username: str = "",
    reply_to_user_id: int = None,
    message_type: str = "text",
    sticker_emoji: str = None
):
    """Stub для SQLite - профилирование v2 не поддерживается"""
    pass


async def get_user_full_profile(user_id: int, chat_id: int) -> Optional[Dict[str, Any]]:
    """Stub для SQLite - профилирование не поддерживается"""
    return None


async def get_user_profile_for_ai(user_id: int, chat_id: int, first_name: str = "", username: str = "") -> Dict[str, Any]:
    """Stub для SQLite - возвращает минимальный профиль"""
    return {
        'user_id': user_id,
        'name': first_name or username or 'Аноним',
        'username': username,
        'gender': 'unknown',
        'description': f"{first_name or username or 'Аноним'} — информация недоступна (SQLite)",
        'traits': [],
        'interests': [],
        'social': {}
    }


async def get_enriched_chat_data_for_ai(chat_id: int, hours: int = 5) -> Dict[str, Any]:
    """Stub для SQLite - возвращает минимальные данные"""
    return {
        'profiles': [],
        'profiles_text': "Профилирование недоступно (SQLite)",
        'social': {'relationships': [], 'conflicts': [], 'friendships': [], 'description': ''},
        'social_text': "Социальные связи недоступны (SQLite)"
    }


async def get_chat_users_profiles_for_ai(chat_id: int, user_ids: List[int] = None) -> List[Dict[str, Any]]:
    """Stub для SQLite - профилирование не поддерживается"""
    return []


async def get_chat_social_data_for_ai(chat_id: int) -> Dict[str, Any]:
    """Stub для SQLite - профилирование не поддерживается"""
    return {
        'relationships': [],
        'conflicts': [],
        'friendships': [],
        'description': "Социальные связи недоступны (SQLite)"
    }


async def update_or_create_chat_user(
    chat_id: int,
    user_id: int,
    first_name: str = None,
    last_name: str = None,
    username: str = None,
    is_bot: bool = False
):
    """Stub для SQLite - chat_users не поддерживается"""
    pass


async def find_user_in_chat(chat_id: int, search_term: str) -> Optional[Dict[str, Any]]:
    """Stub для SQLite - возвращает None"""
    return None


async def get_all_chat_profiles(chat_id: int) -> List[Dict[str, Any]]:
    """Stub для SQLite - профилирование не поддерживается"""
    return []


async def get_active_chats_for_auto_summary() -> List[Dict[str, Any]]:
    """Stub для SQLite - автосводки не поддерживаются"""
    return []
