import asyncio
import logging
import random
import time
from typing import Optional

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ChatMemberUpdated
)
from aiogram.filters import Command, CommandStart
from aiogram.enums import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import BOT_TOKEN, CLASSES, CRIMES, RANDOM_EVENTS, WELCOME_MESSAGES, JAIL_PHRASES
import aiohttp
import json
import os

from database import (
    init_db, get_player, create_player, set_player_class, update_player_stats,
    get_top_players, is_in_jail, put_in_jail, get_all_active_players,
    add_to_treasury, get_treasury, log_event, add_achievement,
    save_chat_message, get_chat_statistics
)
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
    return True, 0


# ==================== КОМАНДЫ ====================

@router.message(CommandStart())
async def cmd_start(message: Message):
    """Начало игры"""
    if message.chat.type == "private":
        await message.answer(
            "🔫 *ГИЛЬДИЯ БЕСПРЕДЕЛА*\n\n"
            "Этот бот работает только в групповых чатах!\n"
            "Добавь меня в чат и начни криминальную карьеру!",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    player = await get_player(user_id, chat_id)
    
    if player and player['player_class']:
        await message.answer(
            f"😏 Йоу, {message.from_user.first_name}! Ты уже в деле!\n"
            f"Используй /profile чтобы глянуть своё досье."
        )
        return
    
    # Создаём игрока если его нет
    if not player:
        await create_player(
            user_id, chat_id,
            message.from_user.username or "",
            message.from_user.first_name
        )
    
    # Показываем выбор класса
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"{data['emoji']} {data['name']}",
            callback_data=f"class_{class_id}"
        )]
        for class_id, data in CLASSES.items()
    ])
    
    classes_text = "\n".join([
        f"{data['emoji']} *{data['name']}* — {data['description']}"
        for data in CLASSES.values()
    ])
    
    await message.answer(
        f"🔫 *ДОБРО ПОЖАЛОВАТЬ В ГИЛЬДИЮ БЕСПРЕДЕЛА!*\n\n"
        f"Выбери свой путь в криминальном мире:\n\n"
        f"{classes_text}\n\n"
        f"👇 Жми на свой класс:",
        reply_markup=keyboard,
        parse_mode=ParseMode.MARKDOWN
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
🔫 *ГИЛЬДИЯ БЕСПРЕДЕЛА — СПРАВКА*

*Основные команды:*
/start — Вступить в гильдию
/profile — Твоё криминальное досье
/top — Топ авторитетов чата

*Криминал:*
/crime — Пойти на дело
/attack — Наехать на игрока (ответь на сообщение)

*Развлечения:*
/casino — Казино для настоящих пацанов
/treasury — Воровской общак

*Аналитика:*
/svodka — 📺 Криминальная сводка чата за 5 часов (AI)

*Инфо:*
/help — Эта справка
/achievements — Твои достижения

_Будь осторожен — менты не дремлют!_ 🚔
"""
    await message.answer(help_text, parse_mode=ParseMode.MARKDOWN)


@router.message(Command("achievements", "ach"))
async def cmd_achievements(message: Message):
    """Показать достижения"""
    if message.chat.type == "private":
        return
    
    user_id = message.from_user.id
    
    from database import get_player_achievements
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
    from database import add_to_treasury
    await add_to_treasury(chat_id, -share)
    
    await message.answer(
        f"💸 {message.from_user.first_name} урвал {share} лавэ из общака! "
        f"({len(event['taken'])}/{event['max_takers']})"
    )


# ==================== СВОДКА ЧАТА ====================

# URL твоего Vercel API (замени на свой после деплоя)
VERCEL_API_URL = os.getenv("VERCEL_API_URL", "https://your-vercel-app.vercel.app/api/generate-summary")


@router.message(Command("svodka", "summary", "digest"))
async def cmd_svodka(message: Message):
    """Генерация сводки чата через AI"""
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
    
    # Отправляем запрос к Vercel API
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                VERCEL_API_URL,
                json={
                    "statistics": stats,
                    "chat_title": message.chat.title or "Чат",
                    "hours": 5
                },
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    summary = result.get("summary", "Ошибка генерации сводки")
                    
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

@router.message(F.text)
async def collect_messages_and_exp(message: Message):
    """Сбор всех сообщений + пассивный опыт"""
    if message.chat.type == "private":
        return
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Сохраняем сообщение для аналитики
    reply_to_user_id = None
    reply_to_first_name = None
    
    if message.reply_to_message and message.reply_to_message.from_user:
        reply_to_user_id = message.reply_to_message.from_user.id
        reply_to_first_name = message.reply_to_message.from_user.first_name
    
    await save_chat_message(
        chat_id=chat_id,
        user_id=user_id,
        username=message.from_user.username or "",
        first_name=message.from_user.first_name or "Аноним",
        message_text=message.text[:500] if message.text else "",  # Лимит 500 символов
        message_type="text",
        reply_to_user_id=reply_to_user_id,
        reply_to_first_name=reply_to_first_name
    )
    
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
    """Сбор стикеров"""
    if message.chat.type == "private":
        return
    
    await save_chat_message(
        chat_id=message.chat.id,
        user_id=message.from_user.id,
        username=message.from_user.username or "",
        first_name=message.from_user.first_name or "Аноним",
        message_text="",
        message_type="sticker",
        sticker_emoji=message.sticker.emoji if message.sticker else "🎭"
    )


@router.message(F.photo)
async def collect_photos(message: Message):
    """Сбор фото"""
    if message.chat.type == "private":
        return
    
    caption = message.caption[:200] if message.caption else ""
    
    await save_chat_message(
        chat_id=message.chat.id,
        user_id=message.from_user.id,
        username=message.from_user.username or "",
        first_name=message.from_user.first_name or "Аноним",
        message_text=caption,
        message_type="photo"
    )


@router.message(F.voice | F.video_note)
async def collect_voice(message: Message):
    """Сбор голосовых и кружочков"""
    if message.chat.type == "private":
        return
    
    msg_type = "voice" if message.voice else "video_note"
    
    await save_chat_message(
        chat_id=message.chat.id,
        user_id=message.from_user.id,
        username=message.from_user.username or "",
        first_name=message.from_user.first_name or "Аноним",
        message_text="",
        message_type=msg_type
    )


# ==================== ЗАПУСК ====================

async def scheduled_events():
    """Запланированные события"""
    # Получаем все чаты с активными игроками
    # Это упрощённая версия — в реальности нужен список активных чатов
    pass


async def main():
    """Главная функция запуска бота"""
    # Инициализация БД
    await init_db()
    
    # Подключаем роутер
    dp.include_router(router)
    
    # Запуск планировщика для случайных событий
    # scheduler.add_job(scheduled_events, 'interval', minutes=30)
    # scheduler.start()
    
    logger.info("🔫 Гильдия Беспредела запущена!")
    
    # Запуск бота
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
