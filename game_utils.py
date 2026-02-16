import random
import time
from typing import Dict, Any, Optional, Tuple
from config import RANKS, CLASSES, CRIMES, ATTACK_MESSAGES


def get_rank(experience: int) -> Dict[str, Any]:
    """Получить ранг по опыту"""
    current_rank = RANKS[0]
    for rank in RANKS:
        if experience >= rank['min_exp']:
            current_rank = rank
        else:
            break
    return current_rank


def get_next_rank(experience: int) -> Optional[Dict[str, Any]]:
    """Получить следующий ранг"""
    for i, rank in enumerate(RANKS):
        if experience < rank['min_exp']:
            return rank
    return None


def exp_to_next_rank(experience: int) -> Tuple[int, int]:
    """Вернуть (текущий опыт от ранга, нужно до следующего)"""
    current_rank = get_rank(experience)
    next_rank = get_next_rank(experience)
    
    if not next_rank:
        return experience - current_rank['min_exp'], 0
    
    exp_in_rank = experience - current_rank['min_exp']
    exp_needed = next_rank['min_exp'] - current_rank['min_exp']
    return exp_in_rank, exp_needed


def format_player_card(player: Dict[str, Any]) -> str:
    """Форматировать карточку игрока"""
    rank = get_rank(player['experience'])
    player_class = CLASSES.get(player['player_class'], {})
    class_emoji = player_class.get('emoji', '❓')
    class_name = player_class.get('name', 'Неизвестно')
    
    exp_current, exp_needed = exp_to_next_rank(player['experience'])
    next_rank = get_next_rank(player['experience'])
    
    # Прогресс-бар
    if exp_needed > 0:
        progress = int((exp_current / exp_needed) * 10)
        progress_bar = '█' * progress + '░' * (10 - progress)
        progress_text = f"[{progress_bar}] {exp_current}/{exp_needed}"
    else:
        progress_bar = '█' * 10
        progress_text = f"[{progress_bar}] МАКС"
    
    # Статус тюрьмы
    jail_status = ""
    if player['jail_until'] > time.time():
        remaining = int(player['jail_until'] - time.time())
        jail_status = f"\n⛓️ В ТЮРЬМЕ ещё {remaining} сек!"
    
    # Винрейт
    total_crimes = player['crimes_success'] + player['crimes_fail']
    winrate = (player['crimes_success'] / total_crimes * 100) if total_crimes > 0 else 0
    
    total_pvp = player['pvp_wins'] + player['pvp_losses']
    pvp_winrate = (player['pvp_wins'] / total_pvp * 100) if total_pvp > 0 else 0
    
    # Защита от None в first_name
    player_name = (player.get('first_name') or 'Аноним')[:20]
    
    card = f"""
╔══════════════════════════════╗
║  📋 КРИМИНАЛЬНОЕ ДОСЬЕ       ║
╠══════════════════════════════╣
║ 👤 {player_name}
║ {class_emoji} {class_name}
║ {rank['name']}
╠══════════════════════════════╣
║ 💰 Лавэ: {player['money']:,}
║ ⭐ Опыт: {player['experience']:,}
║ {progress_text}
╠══════════════════════════════╣
║ 🎯 Статы:
║ ⚔️ Атака: {player['attack']}
║ 🍀 Удача: {player['luck']}
║ ❤️ Здоровье: {player['health']}
╠══════════════════════════════╣
║ 📊 Криминал:
║ ✅ Удачных дел: {player['crimes_success']}
║ ❌ Провалов: {player['crimes_fail']}
║ 📈 Винрейт: {winrate:.1f}%
╠══════════════════════════════╣
║ ⚔️ PvP:
║ 🏆 Побед: {player['pvp_wins']}
║ 💀 Поражений: {player['pvp_losses']}
║ 📈 Винрейт: {pvp_winrate:.1f}%
╠══════════════════════════════╣
║ 💵 Всего украдено: {player['total_stolen']:,}
║ 💸 Всего потеряно: {player['total_lost']:,}
╚══════════════════════════════╝{jail_status}
"""
    return card


def format_top_players(players: list, sort_by: str = "experience") -> str:
    """Форматировать топ игроков"""
    if not players:
        return "🏜️ В этом чате ещё нет криминала... Пока что."
    
    titles = {
        "experience": "🏆 ТОП АВТОРИТЕТОВ",
        "money": "💰 ТОП БОГАЧЕЙ",
        "crimes_success": "🎯 ТОП КРИМИНАЛА",
        "pvp_wins": "⚔️ ТОП БОЙЦОВ"
    }
    
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
    
    text = f"\n{titles.get(sort_by, '🏆 ТОП')}\n"
    text += "═" * 30 + "\n\n"
    
    for i, player in enumerate(players):
        rank = get_rank(player['experience'])
        class_info = CLASSES.get(player['player_class'], {})
        class_emoji = class_info.get('emoji', '❓')
        medal = medals[i] if i < len(medals) else f"{i+1}."
        
        value = player.get(sort_by, 0)
        player_name = (player.get('first_name') or 'Аноним')[:15]
        
        text += f"{medal} {class_emoji} {player_name}\n"
        text += f"    {rank['name']} | {value:,} "
        
        if sort_by == "money":
            text += "лавэ"
        elif sort_by == "experience":
            text += "опыта"
        elif sort_by == "crimes_success":
            text += "дел"
        elif sort_by == "pvp_wins":
            text += "побед"
        
        text += "\n\n"
    
    return text


def calculate_crime_success(player: Dict[str, Any], crime: Dict[str, Any]) -> bool:
    """Рассчитать успех преступления"""
    base_chance = crime['success_rate']
    luck_bonus = player['luck'] * 0.5
    
    # Бонус класса
    class_info = CLASSES.get(player['player_class'], {})
    class_bonus = class_info.get('bonus_steal', 0) * 0.3
    
    total_chance = base_chance + luck_bonus + class_bonus
    total_chance = min(95, max(5, total_chance))  # Между 5% и 95%
    
    return random.randint(1, 100) <= total_chance


def calculate_crime_reward(crime: Dict[str, Any], player: Dict[str, Any]) -> int:
    """Рассчитать награду за преступление"""
    base_reward = random.randint(crime['min_reward'], crime['max_reward'])
    
    # Бонус за удачу
    luck_multiplier = 1 + (player['luck'] / 100)
    
    return int(base_reward * luck_multiplier)


def calculate_pvp_success(attacker: Dict[str, Any], victim: Dict[str, Any]) -> bool:
    """Рассчитать успех наезда"""
    attack_power = attacker['attack']
    defense_power = victim['attack'] * 0.7
    
    # Бонус класса атакующего
    attacker_class = CLASSES.get(attacker['player_class'], {})
    attack_bonus = attacker_class.get('bonus_attack', 0)
    
    # Разница в опыте влияет
    exp_diff = (attacker['experience'] - victim['experience']) / 100
    
    base_chance = 50 + (attack_power - defense_power) + attack_bonus + exp_diff
    base_chance = min(80, max(20, base_chance))  # Между 20% и 80%
    
    return random.randint(1, 100) <= base_chance


def calculate_pvp_steal_amount(victim: Dict[str, Any]) -> int:
    """Рассчитать сколько можно украсть при наезде"""
    money = victim.get('money', 0)
    
    # Защита от отрицательного и нулевого баланса
    if money <= 0:
        return 0
    
    max_steal = int(money * 0.3)  # Максимум 30% от денег жертвы
    min_steal = int(money * 0.1)  # Минимум 10%
    
    if max_steal < 10:
        return money  # Если совсем мало — забираем всё
    
    # Гарантируем min <= max для randint
    min_steal = max(1, min_steal)
    max_steal = max(min_steal, max_steal)
    
    return random.randint(min_steal, max_steal)


def get_random_crime_message(crime: Dict[str, Any], success: bool, **kwargs) -> str:
    """Получить случайное сообщение о преступлении"""
    try:
        messages = crime.get('messages', {}).get('success' if success else 'fail', [])
        if not messages:
            return "Операция завершена." if success else "Операция провалена."
        message = random.choice(messages)
        return message.format(**kwargs)
    except (KeyError, ValueError) as e:
        return "Операция завершена." if success else "Операция провалена."


def get_random_attack_message(success: bool, has_money: bool = True, **kwargs) -> str:
    """Получить случайное сообщение о наезде"""
    try:
        if not has_money:
            messages = ATTACK_MESSAGES.get('no_money', [])
        elif success:
            messages = ATTACK_MESSAGES.get('success', [])
        else:
            messages = ATTACK_MESSAGES.get('fail', [])
        
        if not messages:
            return "Наезд завершён." if success else "Наезд провалился."
        
        message = random.choice(messages)
        return message.format(**kwargs)
    except (KeyError, ValueError) as e:
        return "Наезд завершён." if success else "Наезд провалился."


def get_experience_for_action(action: str, success: bool = True) -> int:
    """Получить опыт за действие"""
    exp_table = {
        "crime_easy": (10, 2),      # (успех, провал)
        "crime_medium": (25, 5),
        "crime_hard": (50, 10),
        "crime_legendary": (100, 20),
        "pvp_win": (30, 0),
        "pvp_lose": (0, 5),
        "message": (1, 0),
        "daily": (20, 0),
        "event_participation": (15, 0)
    }
    
    if action in exp_table:
        return exp_table[action][0] if success else exp_table[action][1]
    return 0


# Достижения
ACHIEVEMENTS = {
    "first_blood": {
        "name": "🩸 Первая кровь",
        "description": "Совершить первое преступление",
        "check": lambda p: p['crimes_success'] + p['crimes_fail'] >= 1
    },
    "serial_criminal": {
        "name": "🔪 Серийный преступник", 
        "description": "Совершить 10 успешных преступлений",
        "check": lambda p: p['crimes_success'] >= 10
    },
    "crime_lord": {
        "name": "👑 Криминальный лорд",
        "description": "Совершить 50 успешных преступлений",
        "check": lambda p: p['crimes_success'] >= 50
    },
    "rich_bitch": {
        "name": "💰 Богатенький Буратино",
        "description": "Накопить 10,000 лавэ",
        "check": lambda p: p['money'] >= 10000
    },
    "millionaire": {
        "name": "🤑 Миллионер с района",
        "description": "Накопить 100,000 лавэ",
        "check": lambda p: p['money'] >= 100000
    },
    "fighter": {
        "name": "🥊 Боец",
        "description": "Выиграть 5 PvP схваток",
        "check": lambda p: p['pvp_wins'] >= 5
    },
    "bully": {
        "name": "😈 Местный террор",
        "description": "Выиграть 25 PvP схваток",
        "check": lambda p: p['pvp_wins'] >= 25
    },
    "loser": {
        "name": "🤡 Вечный лузер",
        "description": "Проиграть 10 PvP, не выиграв ни одного",
        "check": lambda p: p['pvp_losses'] >= 10 and p['pvp_wins'] == 0
    },
    "jailbird": {
        "name": "⛓️ Вечный неудачник",
        "description": "Провалить 10 преступлений",
        "check": lambda p: p['crimes_fail'] >= 10
    }
}


def check_achievements(player: Dict[str, Any]) -> list:
    """Проверить какие достижения заслужил игрок"""
    earned = []
    # Устанавливаем дефолтные значения для безопасной проверки
    safe_player = {
        'crimes_success': player.get('crimes_success', 0),
        'crimes_fail': player.get('crimes_fail', 0),
        'pvp_wins': player.get('pvp_wins', 0),
        'pvp_losses': player.get('pvp_losses', 0),
        'money': player.get('money', 0),
        'experience': player.get('experience', 0),
    }
    
    for key, achievement in ACHIEVEMENTS.items():
        try:
            if achievement['check'](safe_player):
                earned.append((key, achievement))
        except (KeyError, TypeError, ValueError) as e:
            # Пропускаем достижение если проверка упала
            pass
    return earned


# Прикольные фразы для разных ситуаций
RANDOM_PHRASES = {
    "no_money": [
        "💸 У тебя лавэ как у студента — ноль целых хрен десятых!",
        "🗑️ Твой кошелёк плачет от одиночества...",
        "😭 Даже бомжи на тебя с жалостью смотрят!",
        "💩 Финансовое положение: полная жопа"
    ],
    "in_jail": [
        "⛓️ Сиди тихо, а то срок добавят!",
        "🔒 Баланду принесут через 5 минут, потерпи",
        "👮 Не рыпайся, тут камеры везде!",
        "🏛️ Тюрьма — второй дом. Привыкай!"
    ],
    "cooldown": [
        "⏰ Братиш, не гони! Отдохни {time} сек",
        "🕐 Слишком часто крутишь дела! Подожди {time} сек",
        "⌛ Менты ещё не остыли. Выжди {time} сек"
    ],
    "level_up": [
        "🎉 LEVEL UP! Теперь ты {rank}! Уважуха, братиш!",
        "⬆️ Ты вырос до {rank}! Пацаны одобряют!",
        "🔥 Новый ранг: {rank}! Криминальный мир трепещет!"
    ]
}


def get_random_phrase(category: str, **kwargs) -> str:
    """Получить случайную фразу из категории"""
    try:
        if category in RANDOM_PHRASES:
            phrase = random.choice(RANDOM_PHRASES[category])
            return phrase.format(**kwargs)
    except (KeyError, ValueError) as e:
        pass
    return ""
