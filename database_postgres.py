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
import json
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
        
        # Миграция: добавляем voice_transcription для голосовых
        try:
            await conn.execute("""
                ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS voice_transcription TEXT
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
        
        # ========== МИГРАЦИЯ user_profiles на PER-CHAT ==========
        # Проверяем структуру существующей таблицы user_profiles
        # Если она существует с user_id PRIMARY KEY (старая версия), пересоздаём
        try:
            existing_pk = await conn.fetchval("""
                SELECT COUNT(*) FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage ccu USING (constraint_name, table_schema)
                WHERE tc.table_name = 'user_profiles' 
                AND tc.constraint_type = 'PRIMARY KEY'
                AND ccu.column_name = 'chat_id'
            """)
            
            if existing_pk == 0:
                # Старая структура без chat_id в PRIMARY KEY - нужно пересоздать
                logger.info("Миграция: пересоздание user_profiles для per-chat архитектуры...")
                
                # Сохраняем старые данные во временную таблицу
                await conn.execute("DROP TABLE IF EXISTS user_profiles_old CASCADE")
                
                # Проверяем существует ли таблица
                table_exists = await conn.fetchval("""
                    SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'user_profiles')
                """)
                
                if table_exists:
                    await conn.execute("ALTER TABLE user_profiles RENAME TO user_profiles_old")
                    logger.info("Миграция: старая таблица user_profiles переименована в user_profiles_old")
        except Exception as e:
            logger.debug(f"Проверка миграции user_profiles: {e}")
        
        # Таблица профилей пользователей (PER-CHAT! Каждый чат имеет свой профиль)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_profiles (
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                -- Базовая информация
                first_name TEXT,
                username TEXT,
                -- Определение пола
                detected_gender TEXT DEFAULT 'unknown',
                gender_confidence REAL DEFAULT 0.0,
                gender_female_score INTEGER DEFAULT 0,
                gender_male_score INTEGER DEFAULT 0,
                -- Активность в этом чате
                total_messages INTEGER DEFAULT 0,
                messages_analyzed INTEGER DEFAULT 0,
                first_seen_at BIGINT,
                last_seen_at BIGINT,
                last_analysis_at BIGINT,
                -- Временные паттерны (часы активности 0-23)
                active_hours JSONB DEFAULT '{}',
                peak_hour INTEGER,
                is_night_owl BOOLEAN DEFAULT FALSE,
                is_early_bird BOOLEAN DEFAULT FALSE,
                -- Тональность и эмоции в этом чате
                sentiment_score REAL DEFAULT 0.0,
                positive_messages INTEGER DEFAULT 0,
                negative_messages INTEGER DEFAULT 0,
                neutral_messages INTEGER DEFAULT 0,
                emoji_usage_rate REAL DEFAULT 0.0,
                avg_message_length REAL DEFAULT 0.0,
                -- Характер общения в этом чате
                toxicity_score REAL DEFAULT 0.0,
                humor_score REAL DEFAULT 0.0,
                activity_level TEXT DEFAULT 'normal',
                communication_style TEXT DEFAULT 'neutral',
                -- Интересы (топ тем) в этом чате
                interests JSONB DEFAULT '[]',
                frequent_words JSONB DEFAULT '[]',
                -- Социальные связи (в этом чате)
                friends JSONB DEFAULT '[]',
                frequent_replies_to JSONB DEFAULT '[]',
                frequent_replies_from JSONB DEFAULT '[]',
                
                -- ========== НОВЫЕ ПОЛЯ ДЛЯ ГЛУБОКОЙ ПЕРСОНАЛИЗАЦИИ ==========
                
                -- Любимые фразы и словечки
                favorite_phrases JSONB DEFAULT '[]',
                vocabulary_richness REAL DEFAULT 0.0,
                unique_words_count INTEGER DEFAULT 0,
                
                -- Паттерны ответов
                avg_reply_time_seconds INTEGER DEFAULT 0,
                reply_rate REAL DEFAULT 0.0,
                initiates_conversations BOOLEAN DEFAULT FALSE,
                messages_as_reply INTEGER DEFAULT 0,
                conversations_started INTEGER DEFAULT 0,
                
                -- Языковой стиль
                caps_rate REAL DEFAULT 0.0,
                mat_rate REAL DEFAULT 0.0,
                slang_rate REAL DEFAULT 0.0,
                typo_rate REAL DEFAULT 0.0,
                question_rate REAL DEFAULT 0.0,
                exclamation_rate REAL DEFAULT 0.0,
                
                -- Настроение по времени
                mood_by_day JSONB DEFAULT '{}',
                mood_by_hour JSONB DEFAULT '{}',
                worst_mood_day TEXT,
                best_mood_day TEXT,
                worst_mood_hour INTEGER,
                best_mood_hour INTEGER,
                
                -- Медиа предпочтения
                favorite_stickers JSONB DEFAULT '[]',
                favorite_emojis JSONB DEFAULT '[]',
                meme_style TEXT DEFAULT 'unknown',
                voice_messages_rate REAL DEFAULT 0.0,
                photo_rate REAL DEFAULT 0.0,
                sticker_rate REAL DEFAULT 0.0,
                video_rate REAL DEFAULT 0.0,
                
                -- Эмоциональные триггеры и связи
                trigger_topics JSONB DEFAULT '[]',
                emotional_triggers JSONB DEFAULT '[]',
                enemies JSONB DEFAULT '[]',
                crushes JSONB DEFAULT '[]',
                admirers JSONB DEFAULT '[]',
                
                -- Цитаты и мемы
                memorable_quotes JSONB DEFAULT '[]',
                catchphrases JSONB DEFAULT '[]',
                
                -- Поведенческие паттерны
                avg_messages_per_day REAL DEFAULT 0.0,
                longest_streak_days INTEGER DEFAULT 0,
                current_streak_days INTEGER DEFAULT 0,
                last_streak_date TEXT,
                lurk_periods JSONB DEFAULT '[]',
                burst_periods JSONB DEFAULT '[]',
                
                -- Социальный статус в чате
                influence_score REAL DEFAULT 0.0,
                popularity_score REAL DEFAULT 0.0,
                controversy_score REAL DEFAULT 0.0,
                
                -- Метаданные
                profile_version INTEGER DEFAULT 2,
                created_at BIGINT NOT NULL,
                updated_at BIGINT NOT NULL,
                PRIMARY KEY (user_id, chat_id)
            )
        """)
        
        # Добавляем недостающие колонки если таблица уже существует
        # (для миграции старых баз данных)
        migration_columns = [
            # Базовые поля v1
            ("activity_level", "TEXT DEFAULT 'normal'"),
            ("communication_style", "TEXT DEFAULT 'neutral'"),
            ("toxicity_score", "REAL DEFAULT 0.0"),
            ("humor_score", "REAL DEFAULT 0.0"),
            ("sentiment_score", "REAL DEFAULT 0.0"),
            ("positive_messages", "INTEGER DEFAULT 0"),
            ("negative_messages", "INTEGER DEFAULT 0"),
            ("neutral_messages", "INTEGER DEFAULT 0"),
            ("emoji_usage_rate", "REAL DEFAULT 0.0"),
            ("avg_message_length", "REAL DEFAULT 0.0"),
            ("peak_hour", "INTEGER"),
            ("is_night_owl", "BOOLEAN DEFAULT FALSE"),
            ("is_early_bird", "BOOLEAN DEFAULT FALSE"),
            ("active_hours", "JSONB DEFAULT '{}'"),
            ("interests", "JSONB DEFAULT '[]'"),
            ("frequent_words", "JSONB DEFAULT '[]'"),
            ("friends", "JSONB DEFAULT '[]'"),
            ("frequent_replies_to", "JSONB DEFAULT '[]'"),
            ("frequent_replies_from", "JSONB DEFAULT '[]'"),
            
            # ========== НОВЫЕ ПОЛЯ v2 - Глубокая персонализация ==========
            
            # Любимые фразы и словечки
            ("favorite_phrases", "JSONB DEFAULT '[]'"),
            ("vocabulary_richness", "REAL DEFAULT 0.0"),
            ("unique_words_count", "INTEGER DEFAULT 0"),
            
            # Паттерны ответов
            ("avg_reply_time_seconds", "INTEGER DEFAULT 0"),
            ("reply_rate", "REAL DEFAULT 0.0"),
            ("initiates_conversations", "BOOLEAN DEFAULT FALSE"),
            ("messages_as_reply", "INTEGER DEFAULT 0"),
            ("conversations_started", "INTEGER DEFAULT 0"),
            
            # Языковой стиль
            ("caps_rate", "REAL DEFAULT 0.0"),
            ("mat_rate", "REAL DEFAULT 0.0"),
            ("slang_rate", "REAL DEFAULT 0.0"),
            ("typo_rate", "REAL DEFAULT 0.0"),
            ("question_rate", "REAL DEFAULT 0.0"),
            ("exclamation_rate", "REAL DEFAULT 0.0"),
            
            # Настроение по времени
            ("mood_by_day", "JSONB DEFAULT '{}'"),
            ("mood_by_hour", "JSONB DEFAULT '{}'"),
            ("worst_mood_day", "TEXT"),
            ("best_mood_day", "TEXT"),
            ("worst_mood_hour", "INTEGER"),
            ("best_mood_hour", "INTEGER"),
            
            # Медиа предпочтения
            ("favorite_stickers", "JSONB DEFAULT '[]'"),
            ("favorite_emojis", "JSONB DEFAULT '[]'"),
            ("meme_style", "TEXT DEFAULT 'unknown'"),
            ("voice_messages_rate", "REAL DEFAULT 0.0"),
            ("photo_rate", "REAL DEFAULT 0.0"),
            ("sticker_rate", "REAL DEFAULT 0.0"),
            ("video_rate", "REAL DEFAULT 0.0"),
            
            # Эмоциональные триггеры и связи
            ("trigger_topics", "JSONB DEFAULT '[]'"),
            ("emotional_triggers", "JSONB DEFAULT '[]'"),
            ("enemies", "JSONB DEFAULT '[]'"),
            ("crushes", "JSONB DEFAULT '[]'"),
            ("admirers", "JSONB DEFAULT '[]'"),
            
            # Цитаты и мемы
            ("memorable_quotes", "JSONB DEFAULT '[]'"),
            ("catchphrases", "JSONB DEFAULT '[]'"),
            
            # Поведенческие паттерны
            ("avg_messages_per_day", "REAL DEFAULT 0.0"),
            ("longest_streak_days", "INTEGER DEFAULT 0"),
            ("current_streak_days", "INTEGER DEFAULT 0"),
            ("last_streak_date", "TEXT"),
            ("lurk_periods", "JSONB DEFAULT '[]'"),
            ("burst_periods", "JSONB DEFAULT '[]'"),
            
            # Социальный статус в чате
            ("influence_score", "REAL DEFAULT 0.0"),
            ("popularity_score", "REAL DEFAULT 0.0"),
            ("controversy_score", "REAL DEFAULT 0.0"),
            
            # Версия профиля
            ("profile_version", "INTEGER DEFAULT 2"),
        ]
        
        for col_name, col_type in migration_columns:
            try:
                await conn.execute(f"""
                    ALTER TABLE user_profiles 
                    ADD COLUMN IF NOT EXISTS {col_name} {col_type}
                """)
            except Exception as e:
                # Колонка уже существует или другая ошибка - пропускаем
                pass
        
        # Индексы для профилей (PER-CHAT)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_profiles_chat 
            ON user_profiles(chat_id, user_id)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_profiles_gender_chat 
            ON user_profiles(chat_id, detected_gender)
        """)
        
        # Проверяем наличие колонки activity_level перед созданием индекса
        try:
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_profiles_activity_chat
                ON user_profiles(chat_id, activity_level, last_seen_at DESC)
            """)
        except Exception as e:
            logger.warning(f"Could not create activity index: {e}")
        
        # ========== GIN ИНДЕКСЫ для быстрого поиска по JSONB полям ==========
        gin_indexes = [
            ("idx_profiles_interests_gin", "interests"),
            ("idx_profiles_friends_gin", "friends"),
            ("idx_profiles_active_hours_gin", "active_hours"),
            ("idx_profiles_favorite_phrases_gin", "favorite_phrases"),
            ("idx_profiles_trigger_topics_gin", "trigger_topics"),
            ("idx_profiles_crushes_gin", "crushes"),
            ("idx_profiles_enemies_gin", "enemies"),
            ("idx_profiles_catchphrases_gin", "catchphrases"),
            ("idx_profiles_favorite_emojis_gin", "favorite_emojis"),
        ]
        for idx_name, col_name in gin_indexes:
            try:
                await conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS {idx_name}
                    ON user_profiles USING GIN ({col_name})
                """)
            except Exception as e:
                logger.debug(f"Could not create GIN index {idx_name}: {e}")
        
        # Таблица пользователей чата (для быстрого поиска по имени/username)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS chat_users (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                first_name TEXT,
                last_name TEXT,
                username TEXT,
                is_bot BOOLEAN DEFAULT FALSE,
                message_count INTEGER DEFAULT 1,
                first_seen_at BIGINT NOT NULL,
                last_seen_at BIGINT NOT NULL,
                UNIQUE(chat_id, user_id)
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_chat_users_lookup
            ON chat_users(chat_id, LOWER(username), LOWER(first_name))
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_chat_users_by_name
            ON chat_users(chat_id, first_name)
        """)
        
        # Таблица социальных связей (кто с кем общается)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_interactions (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                target_user_id BIGINT NOT NULL,
                interaction_type TEXT NOT NULL,
                interaction_count INTEGER DEFAULT 1,
                last_interaction_at BIGINT NOT NULL,
                sentiment_avg REAL DEFAULT 0.0,
                UNIQUE(chat_id, user_id, target_user_id, interaction_type)
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_interactions_users
            ON user_interactions(chat_id, user_id, target_user_id)
        """)
        
        # ========== МИГРАЦИЯ user_interests на PER-CHAT ==========
        try:
            # Проверяем есть ли chat_id в UNIQUE constraint
            existing_constraint = await conn.fetchval("""
                SELECT COUNT(*) FROM information_schema.constraint_column_usage
                WHERE table_name = 'user_interests' 
                AND constraint_name LIKE '%unique%'
                AND column_name = 'chat_id'
            """)
            
            if existing_constraint == 0:
                # Старая структура - пересоздаём
                logger.info("Миграция: пересоздание user_interests для per-chat архитектуры...")
                
                table_exists = await conn.fetchval("""
                    SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'user_interests')
                """)
                
                if table_exists:
                    await conn.execute("DROP TABLE IF EXISTS user_interests_old CASCADE")
                    await conn.execute("ALTER TABLE user_interests RENAME TO user_interests_old")
                    logger.info("Миграция: старая таблица user_interests переименована в user_interests_old")
        except Exception as e:
            logger.debug(f"Проверка миграции user_interests: {e}")
        
        # Таблица тематических интересов (PER-CHAT!)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_interests (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                topic TEXT NOT NULL,
                score REAL DEFAULT 1.0,
                message_count INTEGER DEFAULT 1,
                last_mentioned_at BIGINT NOT NULL,
                UNIQUE(user_id, chat_id, topic)
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_interests_user_chat
            ON user_interests(user_id, chat_id, score DESC)
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
    now = int(time.time())
    async with (await get_pool()).acquire() as conn:
        await conn.execute("""
            INSERT INTO chat_messages 
            (chat_id, user_id, username, first_name, message_text, message_type,
             reply_to_user_id, reply_to_first_name, reply_to_username, sticker_emoji, 
             image_description, file_id, file_unique_id, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        """, chat_id, user_id, username, first_name, message_text, message_type,
             reply_to_user_id, reply_to_first_name, reply_to_username, sticker_emoji, 
             image_description, file_id, file_unique_id, now)
        
        # Обновляем реестр пользователей чата для быстрого поиска
        await conn.execute("""
            INSERT INTO chat_users (chat_id, user_id, first_name, username, message_count, first_seen_at, last_seen_at)
            VALUES ($1, $2, $3, $4, 1, $5, $5)
            ON CONFLICT (chat_id, user_id) DO UPDATE SET
                first_name = COALESCE(EXCLUDED.first_name, chat_users.first_name),
                username = COALESCE(EXCLUDED.username, chat_users.username),
                message_count = chat_users.message_count + 1,
                last_seen_at = EXCLUDED.last_seen_at
        """, chat_id, user_id, first_name, username, now)


async def find_user_in_chat(chat_id: int, search_term: str) -> Optional[Dict[str, Any]]:
    """
    Найти пользователя в чате по имени или username.
    Возвращает user_id, first_name, username если найден.
    """
    if not search_term:
        return None
    
    search_lower = search_term.lower().strip()
    
    async with (await get_pool()).acquire() as conn:
        # Сначала ищем точное совпадение по username
        row = await conn.fetchrow("""
            SELECT user_id, first_name, username
            FROM chat_users
            WHERE chat_id = $1 AND LOWER(username) = $2
            ORDER BY last_seen_at DESC
            LIMIT 1
        """, chat_id, search_lower)
        
        if row:
            return dict(row)
        
        # Затем ищем по имени (точное)
        row = await conn.fetchrow("""
            SELECT user_id, first_name, username
            FROM chat_users
            WHERE chat_id = $1 AND LOWER(first_name) = $2
            ORDER BY last_seen_at DESC
            LIMIT 1
        """, chat_id, search_lower)
        
        if row:
            return dict(row)
        
        # Ищем частичное совпадение
        row = await conn.fetchrow("""
            SELECT user_id, first_name, username
            FROM chat_users
            WHERE chat_id = $1 
              AND (LOWER(first_name) LIKE $2 OR LOWER(username) LIKE $2)
            ORDER BY last_seen_at DESC
            LIMIT 1
        """, chat_id, f"%{search_lower}%")
        
        return dict(row) if row else None


async def get_all_chat_users(chat_id: int) -> List[Dict[str, Any]]:
    """Получить всех известных пользователей чата"""
    async with (await get_pool()).acquire() as conn:
        rows = await conn.fetch("""
            SELECT user_id, first_name, username, message_count, last_seen_at
            FROM chat_users
            WHERE chat_id = $1
            ORDER BY message_count DESC
        """, chat_id)
        return [dict(row) for row in rows]


async def migrate_chat_users_from_messages() -> Dict[str, Any]:
    """
    Миграция пользователей из chat_messages в chat_users.
    Заполняет реестр пользователей старыми данными.
    """
    async with (await get_pool()).acquire() as conn:
        # Считаем сколько уже есть
        before = await conn.fetchval("SELECT COUNT(*) FROM chat_users")
        
        # Миграция с агрегацией
        result = await conn.execute("""
            INSERT INTO chat_users (chat_id, user_id, first_name, username, message_count, first_seen_at, last_seen_at)
            SELECT 
                chat_id, 
                user_id, 
                MAX(first_name) as first_name,
                MAX(username) as username,
                COUNT(*) as message_count, 
                MIN(created_at) as first_seen_at, 
                MAX(created_at) as last_seen_at
            FROM chat_messages
            WHERE user_id IS NOT NULL
            GROUP BY chat_id, user_id
            ON CONFLICT (chat_id, user_id) DO UPDATE SET
                first_name = COALESCE(EXCLUDED.first_name, chat_users.first_name),
                username = COALESCE(EXCLUDED.username, chat_users.username),
                message_count = EXCLUDED.message_count,
                first_seen_at = LEAST(chat_users.first_seen_at, EXCLUDED.first_seen_at),
                last_seen_at = GREATEST(chat_users.last_seen_at, EXCLUDED.last_seen_at)
        """)
        
        # Считаем после
        after = await conn.fetchval("SELECT COUNT(*) FROM chat_users")
        
        # Уникальные чаты
        chats = await conn.fetchval("SELECT COUNT(DISTINCT chat_id) FROM chat_users")
        
        return {
            'before': before,
            'after': after,
            'added': after - before,
            'total_chats': chats,
            'status': 'success'
        }


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
        
        # Reply pairs с username (ИСПРАВЛЕНО: добавлены user_id в SELECT)
        reply_pairs = await conn.fetch("""
            SELECT user_id, reply_to_user_id, first_name, username, 
                   reply_to_first_name, reply_to_username, COUNT(*) as replies
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
        
        # Последние сообщения (включая voice с транскрипцией)
        recent_messages = await conn.fetch("""
            SELECT first_name, username, message_text, message_type, sticker_emoji,
                   reply_to_first_name, reply_to_username, image_description, 
                   voice_transcription, created_at
            FROM chat_messages 
            WHERE chat_id = $1 AND created_at >= $2 
            AND (message_type IN ('text', 'photo') OR (message_type = 'voice' AND voice_transcription IS NOT NULL))
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
                 first_name, username, first_seen_at, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8, $6, $6, $6)
            """, user_id, gender, confidence, new_result['female_score'],
                 new_result['male_score'], now, first_name or None, username or None)
            
            return {
                'gender': gender,
                'confidence': confidence,
                'female_score': new_result['female_score'],
                'male_score': new_result['male_score'],
                'messages_analyzed': 1
            }


# ==================== РАСШИРЕННОЕ ПРОФИЛИРОВАНИЕ ПОЛЬЗОВАТЕЛЕЙ ====================

# Темы для автоматического определения интересов
TOPIC_KEYWORDS = {
    'gaming': ['игра', 'играть', 'геймер', 'стрим', 'twitch', 'ps5', 'xbox', 'steam', 'dota', 'cs', 'valorant', 'minecraft', 'fortnite', 'рейд', 'босс', 'лвл', 'скилл', 'респ', 'фраг', 'читер', 'нуб', 'про', 'ранкед', 'катка', 'матч'],
    'tech': ['код', 'программ', 'python', 'javascript', 'react', 'api', 'сервер', 'баг', 'фикс', 'девелоп', 'it', 'айти', 'комп', 'ноут', 'софт', 'хард', 'линукс', 'винда', 'мак', 'андроид', 'ios', 'нейросеть', 'gpt', 'ai', 'ии'],
    'crypto': ['крипт', 'биткоин', 'btc', 'eth', 'эфир', 'блокчейн', 'nft', 'токен', 'альткоин', 'холд', 'трейд', 'памп', 'дамп', 'бычий', 'медвежий', 'defi', 'web3', 'метамаск', 'binance', 'байбит'],
    'finance': ['деньги', 'зарплат', 'кредит', 'ипотек', 'банк', 'инвест', 'акци', 'дивиденд', 'вклад', 'процент', 'курс', 'доллар', 'рубл', 'евро', 'сбер', 'тинькофф', 'альфа', 'брокер'],
    'food': ['еда', 'готов', 'рецепт', 'вкусн', 'ресторан', 'кафе', 'пицц', 'суши', 'бургер', 'кофе', 'чай', 'пиво', 'вино', 'водка', 'завтрак', 'обед', 'ужин', 'доставк', 'яндекс еда', 'деливери'],
    'fitness': ['спорт', 'качал', 'тренир', 'зал', 'фитнес', 'бег', 'йога', 'кардио', 'протеин', 'диет', 'похуд', 'мышц', 'пресс', 'бицепс', 'штанг', 'гантел', 'тренер'],
    'travel': ['путешеств', 'поезд', 'полёт', 'самолёт', 'отель', 'виза', 'загран', 'турц', 'египет', 'тайланд', 'бали', 'европ', 'пляж', 'море', 'горы', 'поход', 'отпуск', 'каникул'],
    'cars': ['машин', 'авто', 'тачк', 'бмв', 'мерс', 'ауди', 'тойот', 'лексус', 'порш', 'ламб', 'феррари', 'двигател', 'масло', 'шины', 'колёс', 'права', 'гаи', 'штраф', 'парков'],
    'music': ['музык', 'песн', 'трек', 'альбом', 'концерт', 'группа', 'рэп', 'хип-хоп', 'рок', 'поп', 'электро', 'диджей', 'spotify', 'яндекс музык', 'плейлист', 'слушаю', 'наушник'],
    'movies': ['фильм', 'кино', 'сериал', 'netflix', 'кинопоиск', 'актёр', 'режиссёр', 'смотрел', 'смотрю', 'трейлер', 'премьер', 'оскар', 'марвел', 'dc', 'хоррор', 'комеди', 'драма'],
    'politics': ['политик', 'путин', 'навальн', 'выбор', 'депутат', 'закон', 'госдум', 'правительств', 'санкци', 'война', 'украин', 'сво', 'мобилизац', 'протест', 'митинг', 'оппозиц'],
    'work': ['работ', 'офис', 'начальник', 'коллег', 'зарплат', 'уволи', 'повыш', 'проект', 'дедлайн', 'созвон', 'митинг', 'отчёт', 'клиент', 'задач', 'kpi', 'hr', 'резюме', 'собеседован'],
    'relationships': ['отношен', 'девушк', 'парень', 'муж', 'жена', 'свадьб', 'развод', 'любов', 'секс', 'свидан', 'тиндер', 'баду', 'встреч', 'романтик', 'измен', 'ревност', 'расстал'],
    'memes': ['мем', 'лол', 'кек', 'рофл', 'хаха', 'ахах', 'смешно', 'угар', 'ору', 'орнул', 'прикол', 'жиза', 'база', 'кринж', 'зашквар', 'вайб', 'рил', 'факт'],
}

# Позитивные и негативные слова для анализа тональности
POSITIVE_WORDS = [
    'круто', 'класс', 'супер', 'отлично', 'прекрасно', 'замечательно', 'восхитительно',
    'люблю', 'обожаю', 'нравится', 'рад', 'рада', 'счастлив', 'счастлива', 'доволен', 'довольна',
    'спасибо', 'благодарю', 'молодец', 'умница', 'красавчик', 'красотка', 'гений',
    'лучший', 'лучшая', 'топ', 'огонь', 'бомба', 'шик', 'кайф', 'красота', 'прелесть',
    'ура', 'йес', 'yes', 'вау', 'wow', 'охуенно', 'заебись', 'пиздато', 'ништяк',
    'победа', 'успех', 'удача', 'везение', 'праздник', 'радость', 'счастье',
    '❤️', '😍', '🥰', '😊', '🔥', '👍', '💪', '🎉', '✨', '💯', '👏', '🙌',
]

NEGATIVE_WORDS = [
    'плохо', 'ужас', 'кошмар', 'отстой', 'дерьмо', 'говно', 'хуйня', 'пиздец', 'блять',
    'ненавижу', 'бесит', 'раздражает', 'достало', 'надоело', 'заебало', 'устал', 'устала',
    'грустно', 'печально', 'депрессия', 'тоска', 'скука', 'одиноко', 'плохо',
    'злой', 'злая', 'злюсь', 'бешусь', 'в ярости', 'ненависть', 'презираю',
    'проблема', 'беда', 'катастрофа', 'провал', 'неудача', 'облом', 'фиаско',
    'больно', 'страшно', 'тревожно', 'паника', 'стресс', 'выгорание',
    'мудак', 'идиот', 'дебил', 'тупой', 'тупая', 'урод', 'уродина', 'тварь',
    '😢', '😭', '😡', '🤬', '💔', '👎', '😤', '😠', '🙄', '😒', '😞', '😔',
]

# Токсичные слова/фразы
TOXIC_MARKERS = [
    'убей себя', 'сдохни', 'иди нахуй', 'пошёл нахуй', 'пошла нахуй', 'ебал',
    'ёбаный', 'пидор', 'пидорас', 'петух', 'чмо', 'лох', 'даун', 'дебил',
    'уёбок', 'уебан', 'мразь', 'тварь', 'сука', 'блядь', 'шлюха', 'проститутка',
    'нигер', 'хач', 'чурка', 'жид', 'ненавижу тебя', 'умри',
]

# Юмор-маркеры
HUMOR_MARKERS = [
    'хаха', 'ахах', 'лол', 'lol', 'кек', 'рофл', 'rofl', 'ору', 'орнул',
    'смешно', 'угар', 'прикол', 'ржу', 'ржунимагу', '😂', '🤣', '😹',
    'шутка', 'анекдот', 'стёб', 'троллинг', 'сарказм', 'ирония',
]


def analyze_message_sentiment(text: str) -> dict:
    """Анализ тональности сообщения"""
    text_lower = text.lower()
    
    positive_count = sum(1 for word in POSITIVE_WORDS if word in text_lower)
    negative_count = sum(1 for word in NEGATIVE_WORDS if word in text_lower)
    toxic_count = sum(1 for marker in TOXIC_MARKERS if marker in text_lower)
    humor_count = sum(1 for marker in HUMOR_MARKERS if marker in text_lower)
    
    # Считаем эмодзи
    import re
    emoji_pattern = re.compile(r'[\U0001F300-\U0001F9FF]')
    emoji_count = len(emoji_pattern.findall(text))
    
    # Определяем тональность
    total = positive_count + negative_count
    if total == 0:
        sentiment = 0.0  # нейтральный
        sentiment_label = 'neutral'
    elif positive_count > negative_count:
        sentiment = positive_count / total
        sentiment_label = 'positive'
    else:
        sentiment = -negative_count / total
        sentiment_label = 'negative'
    
    return {
        'sentiment': round(sentiment, 3),
        'sentiment_label': sentiment_label,
        'positive_count': positive_count,
        'negative_count': negative_count,
        'toxic_count': toxic_count,
        'humor_count': humor_count,
        'emoji_count': emoji_count,
        'message_length': len(text)
    }


def detect_topics(text: str) -> List[str]:
    """Определить темы в сообщении"""
    text_lower = text.lower()
    detected_topics = []
    
    for topic, keywords in TOPIC_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text_lower:
                detected_topics.append(topic)
                break
    
    return detected_topics


def get_hour_from_timestamp(timestamp: int) -> int:
    """Получить час из timestamp"""
    from datetime import datetime
    return datetime.fromtimestamp(timestamp).hour


def get_day_of_week(timestamp: int) -> str:
    """Получить день недели из timestamp"""
    from datetime import datetime
    days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    return days[datetime.fromtimestamp(timestamp).weekday()]


# ========== РАСШИРЕННЫЙ АНАЛИЗ СООБЩЕНИЙ v2 ==========

# Слова молодёжного сленга
SLANG_WORDS = [
    'кринж', 'краш', 'рофл', 'лол', 'кек', 'чел', 'чилить', 'вайб', 'рил', 'факт',
    'изи', 'хард', 'имба', 'нуб', 'пруф', 'чекать', 'юзать', 'агриться', 'хейтить',
    'флексить', 'шипперить', 'стримить', 'донатить', 'бустить', 'нерфить', 'баффить',
    'гоу', 'го', 'норм', 'оки', 'лан', 'спс', 'пж', 'плс', 'пжлст', 'хз', 'кст',
    'бро', 'сис', 'чувак', 'чувиха', 'тян', 'кун', 'сенпай', 'вайфу',
    'база', 'базово', 'на базе', 'жиза', 'факап', 'зашквар', 'душнила', 'токсик',
    'сигма', 'альфа', 'бета', 'омега', 'грайндсет', 'скуф', 'скибиди',
    'войс', 'репост', 'шерить', 'лайкать', 'хайп', 'вайрал', 'трендовый',
]

# Матерные слова/корни
MAT_PATTERNS = [
    r'бля', r'хуй', r'хуя', r'хуе', r'пизд', r'ебат', r'ебан', r'ёбан', r'еба', r'ёба',
    r'сука', r'сучк', r'мудак', r'мудил', r'пидор', r'пидар', r'гандон', r'залуп',
    r'шлюх', r'блядь', r'блять', r'нахуй', r'нахуя', r'похуй', r'охуе', r'охуи',
    r'заеб', r'заёб', r'уёб', r'долбо', r'ёбарь', r'ебарь', r'хуёв', r'хуев',
    r'пиздец', r'пиздат', r'пиздос', r'распизд', r'выеб', r'въеб', r'отъеб',
    r'ёбтв', r'ебтв', r'сцук', r'ссук', r'хер[^а-я]', r'херов', r'херн',
]

# Распространённые опечатки и паттерны торопливого набора
TYPO_PATTERNS = [
    r'\b(\w)\1{3,}\b',  # буквы повторяются 3+ раза (привееет, даааа)
    r'\b[а-яa-z]{1,2}\b',  # очень короткие слова могут быть опечатками
    r'[,.!?]{2,}',  # повторяющиеся знаки препинания
]

# Эмоциональные триггер-темы
EMOTIONAL_TRIGGER_TOPICS = {
    'политика': ['политика', 'путин', 'навальный', 'выборы', 'правительство', 'депутат', 'госдума', 'кремль'],
    'деньги': ['зарплата', 'деньги', 'кредит', 'ипотека', 'долг', 'бедность', 'богатый'],
    'работа': ['начальник', 'работа', 'увольнение', 'собеседование', 'зп', 'оклад'],
    'отношения': ['бывший', 'бывшая', 'развод', 'измена', 'изменил', 'изменила', 'расстались'],
    'семья': ['родители', 'мать', 'отец', 'брат', 'сестра', 'семья', 'дети'],
    'здоровье': ['болезнь', 'врач', 'больница', 'лечение', 'операция', 'диагноз'],
    'внешность': ['толстый', 'худой', 'некрасив', 'уродл', 'внешность', 'фигура'],
    'возраст': ['старый', 'молодой', 'возраст', 'лет', 'годы'],
}


def analyze_language_style(text: str) -> dict:
    """
    Анализ языкового стиля сообщения.
    Возвращает метрики: капс, мат, сленг, опечатки, вопросы, восклицания.
    """
    import re
    
    if not text or len(text) < 2:
        return {
            'caps_ratio': 0.0,
            'mat_count': 0,
            'slang_count': 0,
            'typo_score': 0.0,
            'question_count': 0,
            'exclamation_count': 0,
            'word_count': 0,
            'unique_words': [],
        }
    
    text_lower = text.lower()
    
    # Подсчёт слов
    words = re.findall(r'[а-яёa-z]+', text_lower)
    word_count = len(words)
    unique_words = list(set(words))
    
    # Капс (только для слов длиннее 2 символов)
    alpha_chars = [c for c in text if c.isalpha()]
    upper_chars = [c for c in alpha_chars if c.isupper()]
    caps_ratio = len(upper_chars) / max(len(alpha_chars), 1)
    
    # Мат
    mat_count = 0
    for pattern in MAT_PATTERNS:
        mat_count += len(re.findall(pattern, text_lower))
    
    # Сленг
    slang_count = sum(1 for word in SLANG_WORDS if word in text_lower)
    
    # Опечатки (упрощённая эвристика)
    typo_score = 0.0
    for pattern in TYPO_PATTERNS:
        typo_score += len(re.findall(pattern, text)) * 0.1
    typo_score = min(typo_score, 1.0)
    
    # Вопросы и восклицания
    question_count = text.count('?')
    exclamation_count = text.count('!')
    
    return {
        'caps_ratio': round(caps_ratio, 3),
        'mat_count': mat_count,
        'slang_count': slang_count,
        'typo_score': round(typo_score, 3),
        'question_count': question_count,
        'exclamation_count': exclamation_count,
        'word_count': word_count,
        'unique_words': unique_words[:50],  # ограничиваем для экономии памяти
    }


def detect_emotional_triggers(text: str, sentiment_score: float) -> List[str]:
    """
    Определить эмоциональные триггеры в сообщении.
    Триггер засчитывается если тема упомянута И сообщение эмоциональное (не нейтральное).
    """
    if abs(sentiment_score) < 0.2:  # слишком нейтральное сообщение
        return []
    
    text_lower = text.lower()
    triggers = []
    
    for topic, keywords in EMOTIONAL_TRIGGER_TOPICS.items():
        for keyword in keywords:
            if keyword in text_lower:
                triggers.append(topic)
                break
    
    return triggers


def extract_catchphrases(text: str, min_length: int = 5) -> List[str]:
    """
    Извлечь потенциальные коронные фразы из сообщения.
    Ищем фразы с характерным началом или эмоциональной окраской.
    """
    import re
    
    phrases = []
    text_lower = text.lower().strip()
    
    # Фразы начинающиеся с характерных слов
    catchphrase_starters = [
        r'^(короче|ну|типа|вообще|честно|реально|серьёзно|кстати|между прочим|слушай|знаешь)',
        r'^(я думаю|я считаю|мне кажется|по-моему|имхо)',
        r'^(а вот|но вот|зато|однако)',
    ]
    
    for pattern in catchphrase_starters:
        if re.match(pattern, text_lower):
            # Берём первые слова как потенциальную фразу
            words = text.split()[:7]  # макс 7 слов
            phrase = ' '.join(words)
            if len(phrase) >= min_length and len(phrase) <= 100:
                phrases.append(phrase)
    
    # Фразы с восклицаниями или многоточиями
    if '!' in text or '...' in text:
        sentences = re.split(r'[.!?]+', text)
        for sentence in sentences:
            sentence = sentence.strip()
            if min_length <= len(sentence) <= 80:
                phrases.append(sentence)
    
    return phrases[:3]  # максимум 3 фразы за сообщение


def extract_emojis(text: str) -> List[str]:
    """Извлечь все эмодзи из текста"""
    import re
    emoji_pattern = re.compile(
        r'[\U0001F300-\U0001F9FF'  # Symbols & Pictographs
        r'\U0001FA00-\U0001FA6F'   # Chess Symbols
        r'\U0001FA70-\U0001FAFF'   # Symbols Extended-A
        r'\U00002702-\U000027B0'   # Dingbats
        r'\U0001F600-\U0001F64F'   # Emoticons
        r']+', 
        flags=re.UNICODE
    )
    return emoji_pattern.findall(text)


def calculate_vocabulary_richness(unique_words_count: int, total_words: int) -> float:
    """
    Рассчитать богатство словарного запаса (Type-Token Ratio).
    0.0 = очень бедный, 1.0 = очень богатый
    """
    if total_words == 0:
        return 0.0
    # Модифицированный TTR для длинных текстов
    ttr = unique_words_count / total_words
    # Нормализуем (обычно TTR падает с увеличением текста)
    normalized = min(ttr * 2, 1.0)  # умножаем на 2 и ограничиваем 1.0
    return round(normalized, 3)


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
    """
    Комплексное обновление профиля пользователя v2.
    Вызывается при каждом сообщении для инкрементального обучения.
    Собирает расширенные метрики для глубокой персонализации.
    """
    async with (await get_pool()).acquire() as conn:
        now = int(time.time())
        hour = get_hour_from_timestamp(timestamp)
        day_of_week = get_day_of_week(timestamp)
        
        # ========== БАЗОВЫЙ АНАЛИЗ ==========
        sentiment = analyze_message_sentiment(message_text)
        topics = detect_topics(message_text)
        gender_result = analyze_gender_from_text(message_text, first_name)
        
        # ========== РАСШИРЕННЫЙ АНАЛИЗ v2 ==========
        lang_style = analyze_language_style(message_text)
        emotional_triggers = detect_emotional_triggers(message_text, sentiment['sentiment'])
        catchphrases = extract_catchphrases(message_text)
        emojis_in_message = extract_emojis(message_text)
        
        # Получаем текущий профиль для ЭТОГО ЧАТА (per-chat!)
        profile = await conn.fetchrow(
            "SELECT * FROM user_profiles WHERE user_id = $1 AND chat_id = $2", user_id, chat_id
        )
        
        if profile:
            # ========== ОБНОВЛЯЕМ СУЩЕСТВУЮЩИЙ ПРОФИЛЬ ==========
            total_msgs = profile['total_messages'] + 1
            
            # --- Часы активности ---
            active_hours = dict(profile.get('active_hours') or {})
            hour_str = str(hour)
            active_hours[hour_str] = active_hours.get(hour_str, 0) + 1
            peak_hour = int(max(active_hours, key=active_hours.get)) if active_hours else hour
            
            # Паттерн активности
            night_hours = sum(active_hours.get(str(h), 0) for h in [0, 1, 2, 3, 4, 5])
            morning_hours = sum(active_hours.get(str(h), 0) for h in [6, 7, 8, 9])
            total_hours = sum(active_hours.values()) or 1
            is_night_owl = night_hours / total_hours > 0.3
            is_early_bird = morning_hours / total_hours > 0.3
            
            # --- Настроение по дням/часам ---
            mood_by_day = dict(profile.get('mood_by_day') or {})
            old_day_mood = mood_by_day.get(day_of_week, 0)
            day_count = mood_by_day.get(f'{day_of_week}_count', 0) + 1
            mood_by_day[day_of_week] = (old_day_mood * (day_count - 1) + sentiment['sentiment']) / day_count
            mood_by_day[f'{day_of_week}_count'] = day_count
            
            mood_by_hour = dict(profile.get('mood_by_hour') or {})
            old_hour_mood = mood_by_hour.get(hour_str, 0)
            hour_count = mood_by_hour.get(f'{hour_str}_count', 0) + 1
            mood_by_hour[hour_str] = (old_hour_mood * (hour_count - 1) + sentiment['sentiment']) / hour_count
            mood_by_hour[f'{hour_str}_count'] = hour_count
            
            # Лучший/худший день
            day_moods = {k: v for k, v in mood_by_day.items() if not k.endswith('_count')}
            best_mood_day = max(day_moods, key=day_moods.get) if day_moods else None
            worst_mood_day = min(day_moods, key=day_moods.get) if day_moods else None
            
            # Лучший/худший час
            hour_moods = {k: v for k, v in mood_by_hour.items() if not k.endswith('_count')}
            best_mood_hour = int(max(hour_moods, key=hour_moods.get)) if hour_moods else None
            worst_mood_hour = int(min(hour_moods, key=hour_moods.get)) if hour_moods else None
            
            # --- Счётчики тональности ---
            positive = profile['positive_messages'] + (1 if sentiment['sentiment_label'] == 'positive' else 0)
            negative = profile['negative_messages'] + (1 if sentiment['sentiment_label'] == 'negative' else 0)
            neutral = profile['neutral_messages'] + (1 if sentiment['sentiment_label'] == 'neutral' else 0)
            
            # Средние значения (скользящее среднее)
            old_sentiment = profile['sentiment_score'] or 0
            new_sentiment = (old_sentiment * (total_msgs - 1) + sentiment['sentiment']) / total_msgs
            
            old_toxicity = profile['toxicity_score'] or 0
            msg_toxicity = min(sentiment['toxic_count'] / 3, 1.0)
            new_toxicity = (old_toxicity * (total_msgs - 1) + msg_toxicity) / total_msgs
            
            old_humor = profile['humor_score'] or 0
            msg_humor = min(sentiment['humor_count'] / 2, 1.0)
            new_humor = (old_humor * (total_msgs - 1) + msg_humor) / total_msgs
            
            old_avg_len = profile['avg_message_length'] or 0
            new_avg_len = (old_avg_len * (total_msgs - 1) + len(message_text)) / total_msgs
            
            old_emoji_rate = profile['emoji_usage_rate'] or 0
            msg_emoji_rate = sentiment['emoji_count'] / max(len(message_text), 1) * 100
            new_emoji_rate = (old_emoji_rate * (total_msgs - 1) + msg_emoji_rate) / total_msgs
            
            # --- НОВОЕ v2: Языковой стиль ---
            old_caps = profile.get('caps_rate') or 0
            new_caps = (old_caps * (total_msgs - 1) + lang_style['caps_ratio']) / total_msgs
            
            old_mat = profile.get('mat_rate') or 0
            msg_mat = min(lang_style['mat_count'] / 3, 1.0)
            new_mat = (old_mat * (total_msgs - 1) + msg_mat) / total_msgs
            
            old_slang = profile.get('slang_rate') or 0
            msg_slang = min(lang_style['slang_count'] / 5, 1.0)
            new_slang = (old_slang * (total_msgs - 1) + msg_slang) / total_msgs
            
            old_typo = profile.get('typo_rate') or 0
            new_typo = (old_typo * (total_msgs - 1) + lang_style['typo_score']) / total_msgs
            
            old_question = profile.get('question_rate') or 0
            msg_question = min(lang_style['question_count'], 1.0)
            new_question = (old_question * (total_msgs - 1) + msg_question) / total_msgs
            
            old_exclaim = profile.get('exclamation_rate') or 0
            msg_exclaim = min(lang_style['exclamation_count'], 1.0)
            new_exclaim = (old_exclaim * (total_msgs - 1) + msg_exclaim) / total_msgs
            
            # --- НОВОЕ v2: Уникальные слова ---
            old_unique_count = profile.get('unique_words_count') or 0
            new_unique_count = old_unique_count + len(set(lang_style['unique_words']))
            # Богатство словаря (примерная оценка)
            vocabulary_richness = calculate_vocabulary_richness(new_unique_count, total_msgs * 10)  # примерно 10 слов на сообщение
            
            # --- НОВОЕ v2: Любимые фразы ---
            favorite_phrases = list(profile.get('favorite_phrases') or [])
            for phrase in catchphrases:
                if phrase not in favorite_phrases:
                    favorite_phrases.append(phrase)
            favorite_phrases = favorite_phrases[-50:]  # храним последние 50
            
            # --- НОВОЕ v2: Любимые эмодзи ---
            favorite_emojis = list(profile.get('favorite_emojis') or [])
            for emoji in emojis_in_message:
                if emoji not in favorite_emojis:
                    favorite_emojis.append(emoji)
            favorite_emojis = favorite_emojis[-30:]  # храним последние 30
            
            # --- НОВОЕ v2: Эмоциональные триггеры ---
            trigger_topics = list(profile.get('trigger_topics') or [])
            for trigger in emotional_triggers:
                if trigger not in trigger_topics:
                    trigger_topics.append(trigger)
            trigger_topics = trigger_topics[-20:]
            
            # --- НОВОЕ v2: Паттерны ответов ---
            messages_as_reply = (profile.get('messages_as_reply') or 0) + (1 if reply_to_user_id else 0)
            reply_rate = messages_as_reply / total_msgs
            
            # --- НОВОЕ v2: Медиа предпочтения ---
            old_voice_rate = profile.get('voice_messages_rate') or 0
            is_voice = 1 if message_type == 'voice' else 0
            new_voice_rate = (old_voice_rate * (total_msgs - 1) + is_voice) / total_msgs
            
            old_photo_rate = profile.get('photo_rate') or 0
            is_photo = 1 if message_type == 'photo' else 0
            new_photo_rate = (old_photo_rate * (total_msgs - 1) + is_photo) / total_msgs
            
            old_sticker_rate = profile.get('sticker_rate') or 0
            is_sticker = 1 if message_type == 'sticker' else 0
            new_sticker_rate = (old_sticker_rate * (total_msgs - 1) + is_sticker) / total_msgs
            
            old_video_rate = profile.get('video_rate') or 0
            is_video = 1 if message_type in ('video', 'video_note') else 0
            new_video_rate = (old_video_rate * (total_msgs - 1) + is_video) / total_msgs
            
            # --- Уровень активности ---
            if total_msgs > 1000:
                activity_level = 'hyperactive'
            elif total_msgs > 500:
                activity_level = 'very_active'
            elif total_msgs > 100:
                activity_level = 'active'
            elif total_msgs > 20:
                activity_level = 'normal'
            else:
                activity_level = 'lurker'
            
            # --- Стиль общения (расширенный) ---
            if new_mat > 0.3:
                communication_style = 'матершинник'
            elif new_toxicity > 0.3:
                communication_style = 'токсик'
            elif new_humor > 0.3:
                communication_style = 'шутник'
            elif new_caps > 0.3:
                communication_style = 'крикун'
            elif new_slang > 0.3:
                communication_style = 'молодёжный'
            elif new_sentiment > 0.3:
                communication_style = 'позитивный'
            elif new_sentiment < -0.3:
                communication_style = 'негативный'
            else:
                communication_style = 'нейтральный'
            
            # --- Гендер ---
            new_female_score = profile['gender_female_score'] + gender_result['female_score']
            new_male_score = profile['gender_male_score'] + gender_result['male_score']
            gender_total = new_female_score + new_male_score
            if gender_total > 0:
                if new_female_score > new_male_score:
                    gender_confidence = new_female_score / gender_total
                    detected_gender = 'женский' if gender_confidence >= 0.55 else 'unknown'
                elif new_male_score > new_female_score:
                    gender_confidence = new_male_score / gender_total
                    detected_gender = 'мужской' if gender_confidence >= 0.55 else 'unknown'
                else:
                    gender_confidence = 0.5
                    detected_gender = profile['detected_gender']
            else:
                gender_confidence = profile['gender_confidence']
                detected_gender = profile['detected_gender']
            
            # ========== ОБНОВЛЯЕМ ПРОФИЛЬ (per-chat!) ==========
            await conn.execute("""
                UPDATE user_profiles SET
                    first_name = COALESCE($3, first_name),
                    username = COALESCE($4, username),
                    detected_gender = $5,
                    gender_confidence = $6,
                    gender_female_score = $7,
                    gender_male_score = $8,
                    total_messages = $9,
                    messages_analyzed = $9,
                    last_seen_at = $10,
                    last_analysis_at = $10,
                    active_hours = $11,
                    peak_hour = $12,
                    is_night_owl = $13,
                    is_early_bird = $14,
                    sentiment_score = $15,
                    positive_messages = $16,
                    negative_messages = $17,
                    neutral_messages = $18,
                    emoji_usage_rate = $19,
                    avg_message_length = $20,
                    toxicity_score = $21,
                    humor_score = $22,
                    activity_level = $23,
                    communication_style = $24,
                    -- НОВЫЕ ПОЛЯ v2
                    caps_rate = $25,
                    mat_rate = $26,
                    slang_rate = $27,
                    typo_rate = $28,
                    question_rate = $29,
                    exclamation_rate = $30,
                    mood_by_day = $31,
                    mood_by_hour = $32,
                    best_mood_day = $33,
                    worst_mood_day = $34,
                    best_mood_hour = $35,
                    worst_mood_hour = $36,
                    favorite_phrases = $37,
                    favorite_emojis = $38,
                    trigger_topics = $39,
                    messages_as_reply = $40,
                    reply_rate = $41,
                    voice_messages_rate = $42,
                    photo_rate = $43,
                    sticker_rate = $44,
                    video_rate = $45,
                    unique_words_count = $46,
                    vocabulary_richness = $47,
                    profile_version = 2,
                    updated_at = $10
                WHERE user_id = $1 AND chat_id = $2
            """, user_id, chat_id, first_name or None, username or None,
                 detected_gender, gender_confidence, new_female_score, new_male_score,
                 total_msgs, now, json.dumps(active_hours), peak_hour,
                 is_night_owl, is_early_bird, new_sentiment, positive, negative, neutral,
                 new_emoji_rate, new_avg_len, new_toxicity, new_humor,
                 activity_level, communication_style,
                 # Новые поля v2
                 new_caps, new_mat, new_slang, new_typo, new_question, new_exclaim,
                 json.dumps(mood_by_day), json.dumps(mood_by_hour),
                 best_mood_day, worst_mood_day, best_mood_hour, worst_mood_hour,
                 json.dumps(favorite_phrases), json.dumps(favorite_emojis),
                 json.dumps(trigger_topics),
                 messages_as_reply, reply_rate,
                 new_voice_rate, new_photo_rate, new_sticker_rate, new_video_rate,
                 new_unique_count, vocabulary_richness)
        
        else:
            # ========== СОЗДАЁМ НОВЫЙ ПРОФИЛЬ (per-chat!) ==========
            active_hours = {str(hour): 1}
            mood_by_day = {day_of_week: sentiment['sentiment'], f'{day_of_week}_count': 1}
            mood_by_hour = {str(hour): sentiment['sentiment'], f'{str(hour)}_count': 1}
            
            detected_gender = gender_result['gender']
            gender_confidence = gender_result['confidence']
            
            sentiment_label = sentiment['sentiment_label']
            positive = 1 if sentiment_label == 'positive' else 0
            negative = 1 if sentiment_label == 'negative' else 0
            neutral = 1 if sentiment_label == 'neutral' else 0
            
            # Начальные значения для новых полей
            initial_caps = lang_style['caps_ratio']
            initial_mat = min(lang_style['mat_count'] / 3, 1.0)
            initial_slang = min(lang_style['slang_count'] / 5, 1.0)
            initial_typo = lang_style['typo_score']
            initial_question = min(lang_style['question_count'], 1.0)
            initial_exclaim = min(lang_style['exclamation_count'], 1.0)
            
            is_reply = 1 if reply_to_user_id else 0
            is_voice = 1 if message_type == 'voice' else 0
            is_photo = 1 if message_type == 'photo' else 0
            is_sticker = 1 if message_type == 'sticker' else 0
            is_video = 1 if message_type in ('video', 'video_note') else 0
            
            initial_vocab_richness = calculate_vocabulary_richness(len(lang_style['unique_words']), lang_style['word_count'])
            
            await conn.execute("""
                INSERT INTO user_profiles (
                    user_id, chat_id, first_name, username,
                    detected_gender, gender_confidence, gender_female_score, gender_male_score,
                    total_messages, messages_analyzed, first_seen_at, last_seen_at, last_analysis_at,
                    active_hours, peak_hour, is_night_owl, is_early_bird,
                    sentiment_score, positive_messages, negative_messages, neutral_messages,
                    emoji_usage_rate, avg_message_length, toxicity_score, humor_score,
                    activity_level, communication_style,
                    -- НОВЫЕ ПОЛЯ v2
                    caps_rate, mat_rate, slang_rate, typo_rate, question_rate, exclamation_rate,
                    mood_by_day, mood_by_hour, best_mood_day, worst_mood_day, best_mood_hour, worst_mood_hour,
                    favorite_phrases, favorite_emojis, trigger_topics,
                    messages_as_reply, reply_rate,
                    voice_messages_rate, photo_rate, sticker_rate, video_rate,
                    unique_words_count, vocabulary_richness,
                    profile_version, created_at, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, 1, 1, $9, $9, $9,
                    $10, $11, FALSE, FALSE,
                    $12, $13, $14, $15, $16, $17, $18, $19,
                    'lurker', 'нейтральный',
                    -- Новые значения
                    $20, $21, $22, $23, $24, $25,
                    $26, $27, $28, $28, $11, $11,
                    $29, $30, $31,
                    $32, $33,
                    $34, $35, $36, $37,
                    $38, $39,
                    2, $9, $9
                )
            """, user_id, chat_id, first_name or None, username or None,
                 detected_gender, gender_confidence, 
                 gender_result['female_score'], gender_result['male_score'],
                 now, json.dumps(active_hours), hour,
                 sentiment['sentiment'], positive, negative, neutral,
                 sentiment['emoji_count'] / max(len(message_text), 1) * 100,
                 len(message_text),
                 min(sentiment['toxic_count'] / 3, 1.0),
                 min(sentiment['humor_count'] / 2, 1.0),
                 # Новые поля v2
                 initial_caps, initial_mat, initial_slang, initial_typo, initial_question, initial_exclaim,
                 json.dumps(mood_by_day), json.dumps(mood_by_hour), day_of_week,
                 json.dumps(catchphrases[:10]), json.dumps(emojis_in_message[:10]), json.dumps(emotional_triggers),
                 is_reply, float(is_reply),
                 float(is_voice), float(is_photo), float(is_sticker), float(is_video),
                 len(lang_style['unique_words']), initial_vocab_richness)
        
        # Обновляем темы/интересы (per-chat!)
        for topic in topics:
            await conn.execute("""
                INSERT INTO user_interests (user_id, chat_id, topic, score, message_count, last_mentioned_at)
                VALUES ($1, $2, $3, 1.0, 1, $4)
                ON CONFLICT (user_id, chat_id, topic) DO UPDATE SET
                    score = user_interests.score + 0.1,
                    message_count = user_interests.message_count + 1,
                    last_mentioned_at = $4
            """, user_id, chat_id, topic, now)
        
        # Обновляем социальные связи (если это реплай)
        if reply_to_user_id and reply_to_user_id != user_id:
            await conn.execute("""
                INSERT INTO user_interactions 
                (chat_id, user_id, target_user_id, interaction_type, interaction_count, last_interaction_at, sentiment_avg)
                VALUES ($1, $2, $3, 'reply', 1, $4, $5)
                ON CONFLICT (chat_id, user_id, target_user_id, interaction_type) DO UPDATE SET
                    interaction_count = user_interactions.interaction_count + 1,
                    last_interaction_at = $4,
                    sentiment_avg = (user_interactions.sentiment_avg * user_interactions.interaction_count + $5) / (user_interactions.interaction_count + 1)
            """, chat_id, user_id, reply_to_user_id, now, sentiment['sentiment'])


async def get_user_full_profile(user_id: int, chat_id: int) -> Optional[Dict[str, Any]]:
    """Получить полный профиль пользователя со всеми данными для конкретного чата (per-chat!)"""
    async with (await get_pool()).acquire() as conn:
        # Основной профиль для ЭТОГО ЧАТА
        profile = await conn.fetchrow(
            "SELECT * FROM user_profiles WHERE user_id = $1 AND chat_id = $2", user_id, chat_id
        )
        
        if not profile:
            return None
        
        result = dict(profile)
        
        # Получаем интересы для ЭТОГО ЧАТА (per-chat!)
        interests = await conn.fetch("""
            SELECT topic, score, message_count 
            FROM user_interests 
            WHERE user_id = $1 AND chat_id = $2
            ORDER BY score DESC 
            LIMIT 10
        """, user_id, chat_id)
        result['top_interests'] = [dict(i) for i in interests]
        
        # Получаем топ собеседников для ЭТОГО ЧАТА (per-chat!)
        interactions = await conn.fetch("""
            SELECT target_user_id, interaction_count, sentiment_avg
            FROM user_interactions
            WHERE user_id = $1 AND chat_id = $2 AND interaction_type = 'reply'
            ORDER BY interaction_count DESC
            LIMIT 10
        """, user_id, chat_id)
        result['top_interactions'] = [dict(i) for i in interactions]
        
        return result


async def get_chat_social_graph(chat_id: int) -> List[Dict[str, Any]]:
    """Получить социальный граф чата (кто с кем общается)"""
    async with (await get_pool()).acquire() as conn:
        rows = await conn.fetch("""
            SELECT 
                user_id, target_user_id, 
                SUM(interaction_count) as total_interactions,
                AVG(sentiment_avg) as avg_sentiment
            FROM user_interactions
            WHERE chat_id = $1
            GROUP BY user_id, target_user_id
            ORDER BY total_interactions DESC
            LIMIT 100
        """, chat_id)
        return [dict(row) for row in rows]


async def get_user_activity_report(user_id: int, chat_id: int) -> Dict[str, Any]:
    """Получить отчёт об активности пользователя в конкретном чате (per-chat!)"""
    profile = await get_user_full_profile(user_id, chat_id)
    
    if not profile:
        return {'error': 'Profile not found'}
    
    # Формируем читаемый отчёт
    report = {
        'user_id': user_id,
        'name': profile.get('first_name') or profile.get('username') or 'Unknown',
        'gender': profile.get('detected_gender', 'unknown'),
        'gender_confidence': f"{(profile.get('gender_confidence', 0) * 100):.0f}%",
        'total_messages': profile.get('total_messages', 0),
        'activity_level': profile.get('activity_level', 'unknown'),
        'communication_style': profile.get('communication_style', 'neutral'),
        'sentiment': {
            'score': profile.get('sentiment_score', 0),
            'positive': profile.get('positive_messages', 0),
            'negative': profile.get('negative_messages', 0),
            'neutral': profile.get('neutral_messages', 0),
        },
        'behavior': {
            'is_night_owl': profile.get('is_night_owl', False),
            'is_early_bird': profile.get('is_early_bird', False),
            'peak_hour': profile.get('peak_hour'),
            'avg_message_length': round(profile.get('avg_message_length', 0)),
            'emoji_rate': f"{profile.get('emoji_usage_rate', 0):.1f}%",
            'toxicity': f"{(profile.get('toxicity_score', 0) * 100):.0f}%",
            'humor': f"{(profile.get('humor_score', 0) * 100):.0f}%",
        },
        'interests': [i['topic'] for i in profile.get('top_interests', [])],
        'top_interacted_users': [i['target_user_id'] for i in profile.get('top_interactions', [])],
        'first_seen': profile.get('first_seen_at'),
        'last_seen': profile.get('last_seen_at'),
    }
    
    return report


# ==================== ДАННЫЕ ДЛЯ AI-ГЕНЕРАЦИИ ====================

async def get_user_profile_for_ai(user_id: int, chat_id: int, first_name: str = "", username: str = "") -> Dict[str, Any]:
    """
    Получить профиль пользователя в формате, оптимизированном для AI-генерации.
    Возвращает человекочитаемое описание для промптов.
    Per-chat! Каждый чат имеет свой профиль.
    """
    profile = await get_user_full_profile(user_id, chat_id)
    
    if not profile:
        # Минимальный профиль для новых пользователей
        return {
            'user_id': user_id,
            'name': first_name or username or 'Аноним',
            'username': username,
            'gender': 'unknown',
            'description': f"{first_name or username or 'Аноним'} — новый персонаж, о нём пока ничего не известно",
            'traits': [],
            'interests': [],
            'social': {}
        }
    
    name = profile.get('first_name') or profile.get('username') or first_name or username or 'Аноним'
    gender = profile.get('detected_gender', 'unknown')
    
    # Собираем характеристики
    traits = []
    
    # Пол
    if gender == 'мужской':
        traits.append('мужчина')
    elif gender == 'женский':
        traits.append('женщина')
    
    # Активность
    activity = profile.get('activity_level', 'normal')
    activity_map = {
        'hyperactive': 'гиперактивный графоман',
        'very_active': 'очень активный болтун',
        'active': 'активный участник',
        'normal': 'обычный пользователь',
        'lurker': 'молчаливый наблюдатель'
    }
    traits.append(activity_map.get(activity, 'обычный'))
    
    # Стиль общения
    style = profile.get('communication_style', 'neutral')
    style_map = {
        'toxic': 'токсичный агрессор',
        'humorous': 'шутник и юморист',
        'positive': 'позитивный оптимист',
        'negative': 'вечно недовольный нытик',
        'neutral': 'нейтральный'
    }
    if style != 'neutral':
        traits.append(style_map.get(style, ''))
    
    # Режим сна
    if profile.get('is_night_owl'):
        traits.append('ночная сова')
    elif profile.get('is_early_bird'):
        traits.append('ранняя пташка')
    
    # Токсичность
    toxicity = profile.get('toxicity_score', 0)
    if toxicity > 0.5:
        traits.append('крайне токсичен')
    elif toxicity > 0.3:
        traits.append('склонен к токсичности')
    
    # Юмор
    humor = profile.get('humor_score', 0)
    if humor > 0.4:
        traits.append('постоянно шутит')
    elif humor > 0.2:
        traits.append('любит пошутить')
    
    # Эмодзи
    emoji_rate = profile.get('emoji_usage_rate', 0)
    if emoji_rate > 5:
        traits.append('засыпает эмодзи')
    elif emoji_rate > 2:
        traits.append('любит эмодзи')
    
    # Интересы
    interests = [i['topic'] for i in profile.get('top_interests', [])][:5]
    interests_map = {
        'gaming': 'геймер',
        'tech': 'технарь/айтишник',
        'crypto': 'криптан',
        'finance': 'финансист',
        'food': 'гурман',
        'fitness': 'качок/спортсмен',
        'travel': 'путешественник',
        'cars': 'автомобилист',
        'music': 'меломан',
        'movies': 'киноман',
        'politics': 'политолог',
        'work': 'трудоголик',
        'relationships': 'романтик',
        'memes': 'мемолог'
    }
    interests_readable = [interests_map.get(i, i) for i in interests]
    
    # ========== НОВЫЕ ДАННЫЕ v2 ==========
    
    # Языковой стиль
    mat_rate = profile.get('mat_rate', 0)
    if mat_rate > 0.4:
        traits.append('матерится как сапожник')
    elif mat_rate > 0.2:
        traits.append('любит крепкое словцо')
    
    caps_rate = profile.get('caps_rate', 0)
    if caps_rate > 0.3:
        traits.append('ОРЁТ КАПСОМ')
    
    slang_rate = profile.get('slang_rate', 0)
    if slang_rate > 0.3:
        traits.append('говорит на молодёжном сленге')
    
    question_rate = profile.get('question_rate', 0)
    if question_rate > 0.4:
        traits.append('вечно задаёт вопросы')
    
    # Настроение по времени
    best_mood_day = profile.get('best_mood_day')
    worst_mood_day = profile.get('worst_mood_day')
    mood_patterns = []
    if best_mood_day:
        day_names = {'monday': 'понедельник', 'tuesday': 'вторник', 'wednesday': 'среда',
                     'thursday': 'четверг', 'friday': 'пятница', 'saturday': 'суббота', 'sunday': 'воскресенье'}
        mood_patterns.append(f"в {day_names.get(best_mood_day, best_mood_day)} в хорошем настроении")
    if worst_mood_day:
        day_names = {'monday': 'понедельник', 'tuesday': 'вторник', 'wednesday': 'среда',
                     'thursday': 'четверг', 'friday': 'пятница', 'saturday': 'суббота', 'sunday': 'воскресенье'}
        mood_patterns.append(f"в {day_names.get(worst_mood_day, worst_mood_day)} бывает мрачен")
    
    best_mood_hour = profile.get('best_mood_hour')
    worst_mood_hour = profile.get('worst_mood_hour')
    if worst_mood_hour is not None and 0 <= worst_mood_hour <= 5:
        mood_patterns.append("ночью бывает подавлен")
    
    # Любимые фразы (коронные выражения)
    favorite_phrases = profile.get('favorite_phrases') or []
    catchphrases = favorite_phrases[:5] if favorite_phrases else []
    
    # Любимые эмодзи
    favorite_emojis = profile.get('favorite_emojis') or []
    top_emojis = favorite_emojis[:5] if favorite_emojis else []
    
    # Эмоциональные триггеры
    trigger_topics = profile.get('trigger_topics') or []
    triggers_text = ', '.join(trigger_topics[:3]) if trigger_topics else None
    if triggers_text:
        traits.append(f"бурно реагирует на темы: {triggers_text}")
    
    # Медиа предпочтения
    voice_rate = profile.get('voice_messages_rate', 0)
    if voice_rate > 0.2:
        traits.append('часто записывает голосовые')
    
    sticker_rate = profile.get('sticker_rate', 0)
    if sticker_rate > 0.3:
        traits.append('обожает стикеры')
    
    # Словарный запас
    vocabulary = profile.get('vocabulary_richness', 0)
    if vocabulary > 0.7:
        traits.append('богатый словарный запас')
    elif vocabulary < 0.3:
        traits.append('скудный словарь')
    
    # Паттерн реплаев
    reply_rate = profile.get('reply_rate', 0)
    if reply_rate > 0.7:
        traits.append('в основном отвечает другим')
    elif reply_rate < 0.2:
        traits.append('редко отвечает, чаще пишет сам')
    
    # Социальные связи
    top_interactions = profile.get('top_interactions', [])
    social = {
        'frequently_talks_to': [i['target_user_id'] for i in top_interactions[:3]],
        'interaction_count': sum(i['interaction_count'] for i in top_interactions)
    }
    
    # Crushes и enemies из профиля (если есть)
    crushes = profile.get('crushes') or []
    enemies = profile.get('enemies') or []
    
    # Формируем текстовое описание для AI
    traits_text = ', '.join(traits) if traits else 'обычный пользователь'
    interests_text = ', '.join(interests_readable) if interests_readable else 'интересы не определены'
    
    # Пиковый час
    peak = profile.get('peak_hour')
    peak_text = f"активен около {peak}:00" if peak is not None else ""
    
    # Тональность
    sentiment = profile.get('sentiment_score', 0)
    if sentiment > 0.3:
        mood = "обычно позитивен"
    elif sentiment < -0.3:
        mood = "обычно негативен"
    else:
        mood = "эмоционально нейтрален"
    
    # Добавляем паттерны настроения
    mood_text = ". ".join(mood_patterns) if mood_patterns else ""
    
    # Коронные фразы
    catchphrases_text = f"Коронные фразы: {', '.join(catchphrases[:3])}" if catchphrases else ""
    
    description = f"{name} — {traits_text}. Интересы: {interests_text}. {mood}. {peak_text}. {mood_text}. {catchphrases_text}".strip()
    # Убираем лишние точки и пробелы
    description = ' '.join(description.split())
    description = description.replace('. .', '.').replace('..', '.').rstrip('.')
    
    return {
        'user_id': user_id,
        'name': name,
        'username': profile.get('username', username),
        'gender': gender,
        'activity_level': activity,
        'communication_style': style,
        'toxicity': toxicity,
        'humor': humor,
        'sentiment': sentiment,
        'is_night_owl': profile.get('is_night_owl', False),
        'is_early_bird': profile.get('is_early_bird', False),
        'peak_hour': peak,
        'total_messages': profile.get('total_messages', 0),
        'traits': traits,
        'interests': interests,
        'interests_readable': interests_readable,
        'social': social,
        'description': description,  # Готовое описание для промпта
        # ========== НОВЫЕ ПОЛЯ v2 ==========
        'mat_rate': mat_rate,
        'caps_rate': caps_rate,
        'slang_rate': slang_rate,
        'vocabulary_richness': vocabulary,
        'favorite_phrases': catchphrases,
        'favorite_emojis': top_emojis,
        'trigger_topics': trigger_topics[:5],
        'mood_patterns': mood_patterns,
        'best_mood_day': best_mood_day,
        'worst_mood_day': worst_mood_day,
        'reply_rate': reply_rate,
        'voice_rate': voice_rate,
        'crushes': crushes[:3],
        'enemies': enemies[:3],
    }


async def get_chat_users_profiles_for_ai(chat_id: int, user_ids: List[int] = None) -> List[Dict[str, Any]]:
    """
    Получить профили всех пользователей чата для AI-генерации.
    Если user_ids указан, получает только эти профили.
    """
    async with (await get_pool()).acquire() as conn:
        if user_ids:
            # Получаем конкретных пользователей
            rows = await conn.fetch("""
                SELECT DISTINCT user_id, first_name, username 
                FROM chat_messages 
                WHERE chat_id = $1 AND user_id = ANY($2)
            """, chat_id, user_ids)
        else:
            # Получаем активных пользователей за последние 24 часа
            day_ago = int(time.time()) - 86400
            rows = await conn.fetch("""
                SELECT user_id, first_name, username, COUNT(*) as msg_count
                FROM chat_messages 
                WHERE chat_id = $1 AND created_at >= $2
                GROUP BY user_id, first_name, username
                ORDER BY msg_count DESC
                LIMIT 20
            """, chat_id, day_ago)
    
    profiles = []
    for row in rows:
        profile = await get_user_profile_for_ai(
            row['user_id'],
            chat_id,  # per-chat!
            row.get('first_name', ''), 
            row.get('username', '')
        )
        profiles.append(profile)
    
    return profiles


async def get_chat_social_data_for_ai(chat_id: int) -> Dict[str, Any]:
    """
    Получить социальные данные чата для AI: кто с кем общается, конфликты, дружба.
    Per-chat! Профили теперь привязаны к конкретному чату.
    """
    async with (await get_pool()).acquire() as conn:
        # Топ взаимодействий (per-chat!)
        # JOIN с user_profiles теперь по (user_id, chat_id)
        interactions = await conn.fetch("""
            SELECT 
                ui.user_id, ui.target_user_id, 
                ui.interaction_count, ui.sentiment_avg,
                COALESCE(up1.first_name, cu1.first_name) as from_name, 
                COALESCE(up1.username, cu1.username) as from_username,
                COALESCE(up2.first_name, cu2.first_name) as to_name, 
                COALESCE(up2.username, cu2.username) as to_username
            FROM user_interactions ui
            LEFT JOIN user_profiles up1 ON ui.user_id = up1.user_id AND ui.chat_id = up1.chat_id
            LEFT JOIN user_profiles up2 ON ui.target_user_id = up2.user_id AND ui.chat_id = up2.chat_id
            LEFT JOIN chat_users cu1 ON ui.user_id = cu1.user_id AND ui.chat_id = cu1.chat_id
            LEFT JOIN chat_users cu2 ON ui.target_user_id = cu2.user_id AND ui.chat_id = cu2.chat_id
            WHERE ui.chat_id = $1
            ORDER BY ui.interaction_count DESC
            LIMIT 15
        """, chat_id)
        
        # Формируем читаемые связи
        relationships = []
        conflicts = []
        friendships = []
        
        for row in interactions:
            from_name = row['from_name'] or row['from_username'] or f"User_{row['user_id']}"
            to_name = row['to_name'] or row['to_username'] or f"User_{row['target_user_id']}"
            sentiment = row['sentiment_avg'] or 0
            count = row['interaction_count']
            
            rel = {
                'from': from_name,
                'to': to_name,
                'count': count,
                'sentiment': sentiment
            }
            relationships.append(rel)
            
            if sentiment < -0.2 and count > 5:
                conflicts.append(f"{from_name} часто конфликтует с {to_name}")
            elif sentiment > 0.2 and count > 10:
                friendships.append(f"{from_name} дружит с {to_name}")
        
        return {
            'relationships': relationships,
            'conflicts': conflicts,
            'friendships': friendships,
            'description': _format_social_for_prompt(relationships, conflicts, friendships)
        }


def _format_social_for_prompt(relationships: list, conflicts: list, friendships: list) -> str:
    """Форматировать социальные данные в текст для промпта"""
    lines = []
    
    if friendships:
        lines.append("ДРУЖЕСКИЕ СВЯЗИ:")
        lines.extend([f"  • {f}" for f in friendships[:5]])
    
    if conflicts:
        lines.append("КОНФЛИКТЫ:")
        lines.extend([f"  • {c}" for c in conflicts[:5]])
    
    if relationships:
        lines.append("КТО КОМУ ЧАЩЕ ОТВЕЧАЕТ:")
        for r in relationships[:10]:
            mood = "😊" if r['sentiment'] > 0.2 else "😠" if r['sentiment'] < -0.2 else "😐"
            lines.append(f"  • {r['from']} → {r['to']}: {r['count']}x {mood}")
    
    return "\n".join(lines) if lines else "Социальные связи не определены"


async def get_enriched_chat_data_for_ai(chat_id: int, hours: int = 5) -> Dict[str, Any]:
    """
    Получить полные обогащённые данные чата для AI-генерации.
    Включает: статистику, профили пользователей, социальные связи.
    """
    since_time = int(time.time()) - (hours * 3600)
    
    async with (await get_pool()).acquire() as conn:
        # Получаем активных пользователей за период
        users = await conn.fetch("""
            SELECT user_id, first_name, username, COUNT(*) as msg_count
            FROM chat_messages 
            WHERE chat_id = $1 AND created_at >= $2
            GROUP BY user_id, first_name, username
            ORDER BY msg_count DESC
            LIMIT 15
        """, chat_id, since_time)
    
    # Получаем профили (per-chat!)
    profiles = []
    for user in users:
        profile = await get_user_profile_for_ai(
            user['user_id'],
            chat_id,  # per-chat!
            user['first_name'] or '',
            user['username'] or ''
        )
        profile['messages_in_period'] = user['msg_count']
        profiles.append(profile)
    
    # Получаем социальные данные
    social = await get_chat_social_data_for_ai(chat_id)
    
    # Форматируем профили для промпта
    profiles_text = []
    for p in profiles:
        profiles_text.append(f"@{p['username'] or p['name']} ({p['name']}): {p['description']}")
    
    return {
        'profiles': profiles,
        'profiles_text': "\n".join(profiles_text),
        'social': social,
        'social_text': social['description']
    }


# ==================== МИГРАЦИЯ ПРОФИЛЕЙ ====================

async def rebuild_profiles_from_messages(chat_id: int, limit: int = 500) -> Dict[str, Any]:
    """
    Пересобрать per-chat профили из существующих сообщений.
    Используется после миграции на новую архитектуру.
    
    Args:
        chat_id: ID чата для пересборки профилей
        limit: Максимум сообщений на пользователя для анализа
    
    Returns:
        Статистика миграции
    """
    stats = {
        'users_processed': 0,
        'profiles_created': 0,
        'messages_analyzed': 0,
        'errors': []
    }
    
    async with (await get_pool()).acquire() as conn:
        # Получаем всех уникальных пользователей чата
        users = await conn.fetch("""
            SELECT DISTINCT user_id, first_name, username, COUNT(*) as msg_count
            FROM chat_messages
            WHERE chat_id = $1 AND message_text IS NOT NULL AND message_text != ''
            GROUP BY user_id, first_name, username
            ORDER BY msg_count DESC
        """, chat_id)
        
        for user in users:
            user_id = user['user_id']
            first_name = user['first_name'] or ''
            username = user['username'] or ''
            
            try:
                # Получаем последние сообщения пользователя
                messages = await conn.fetch("""
                    SELECT message_text, created_at, reply_to_user_id
                    FROM chat_messages
                    WHERE chat_id = $1 AND user_id = $2 
                    AND message_text IS NOT NULL AND message_text != ''
                    ORDER BY created_at DESC
                    LIMIT $3
                """, chat_id, user_id, limit)
                
                if not messages:
                    continue
                
                stats['users_processed'] += 1
                
                # Анализируем каждое сообщение и обновляем профиль
                for msg in messages:
                    try:
                        await update_user_profile_comprehensive(
                            user_id=user_id,
                            chat_id=chat_id,
                            message_text=msg['message_text'],
                            timestamp=msg['created_at'],
                            first_name=first_name,
                            username=username,
                            reply_to_user_id=msg.get('reply_to_user_id')
                        )
                        stats['messages_analyzed'] += 1
                    except Exception as e:
                        pass  # Продолжаем при ошибках отдельных сообщений
                
                stats['profiles_created'] += 1
                
            except Exception as e:
                stats['errors'].append(f"User {user_id}: {str(e)}")
    
    return stats


async def rebuild_all_profiles(limit_per_user: int = 200) -> Dict[str, Any]:
    """
    Пересобрать профили для ВСЕХ чатов.
    
    Args:
        limit_per_user: Максимум сообщений на пользователя
    
    Returns:
        Статистика по всем чатам
    """
    global_stats = {
        'chats_processed': 0,
        'total_users': 0,
        'total_profiles': 0,
        'total_messages': 0,
        'errors': []
    }
    
    async with (await get_pool()).acquire() as conn:
        # Получаем все уникальные чаты
        chats = await conn.fetch("""
            SELECT DISTINCT chat_id, COUNT(*) as msg_count
            FROM chat_messages
            WHERE message_text IS NOT NULL AND message_text != ''
            GROUP BY chat_id
            ORDER BY msg_count DESC
        """)
        
        for chat in chats:
            chat_id = chat['chat_id']
            try:
                stats = await rebuild_profiles_from_messages(chat_id, limit_per_user)
                
                global_stats['chats_processed'] += 1
                global_stats['total_users'] += stats['users_processed']
                global_stats['total_profiles'] += stats['profiles_created']
                global_stats['total_messages'] += stats['messages_analyzed']
                global_stats['errors'].extend(stats['errors'])
                
                logger.info(f"Миграция чата {chat_id}: {stats['profiles_created']} профилей")
            except Exception as e:
                global_stats['errors'].append(f"Chat {chat_id}: {str(e)}")
    
    return global_stats