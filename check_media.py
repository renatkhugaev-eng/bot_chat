"""Скрипт для подсчёта медиа в базе данных"""
import asyncio
import os
from dotenv import load_dotenv
load_dotenv()

async def count_media():
    import asyncpg
    db_url = os.getenv('DATABASE_URL') or os.getenv('POSTGRES_URL')
    if not db_url:
        print('No DATABASE_URL found')
        return
    
    conn = await asyncpg.connect(db_url)
    
    # Общая статистика по всем чатам
    rows = await conn.fetch('''
        SELECT file_type, COUNT(*) as count
        FROM chat_media
        WHERE is_approved = 1
        GROUP BY file_type
        ORDER BY count DESC
    ''')
    
    total = 0
    print('=== MEDIA IN DATABASE ===')
    for row in rows:
        ft = row['file_type']
        cnt = row['count']
        # ASCII names for Windows compatibility
        names = {
            'photo': 'Photos/Memes',
            'animation': 'GIF/Animations', 
            'sticker': 'Stickers',
            'voice': 'Voice messages',
            'video_note': 'Video notes (circles)'
        }
        name = names.get(ft, ft)
        print(f"  {name}: {cnt}")
        total += cnt
    
    print(f"\n  TOTAL: {total}")
    
    # По чатам
    chats = await conn.fetch('''
        SELECT chat_id, COUNT(*) as count
        FROM chat_media
        WHERE is_approved = 1
        GROUP BY chat_id
        ORDER BY count DESC
        LIMIT 5
    ''')
    print('\n=== TOP CHATS ===')
    for chat in chats:
        print(f"  Chat {chat['chat_id']}: {chat['count']} media")
    
    await conn.close()

if __name__ == '__main__':
    asyncio.run(count_media())
