import os
import asyncio
import logging
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

import aiosqlite
from aiohttp import web
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

try:
    from groq import AsyncGroq
except ImportError:
    AsyncGroq = None


# =============================================================================
# 1. КОНФИГУРАЦИЯ И ПЕРЕМЕННЫЕ
# =============================================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
ADMIN_ID = int(os.getenv("ADMIN_ID", "8916615782"))

DB_PATH = "language_exchange.db"
GROQ_MODEL = "openai/gpt-oss-120b"
SEARCH_TIMEOUT_SECONDS = 30
REPORTS_TO_BAN = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("language_exchange_bot")

if GROQ_API_KEY and GROQ_API_KEY != "PUT_YOUR_GROQ_API_KEY_HERE" and AsyncGroq is not None:
    groq_client = AsyncGroq(api_key=GROQ_API_KEY)
    logger.info("Groq client инициализирован.")
else:
    groq_client = None
    logger.warning(
        "GROQ_API_KEY не задан (или библиотека groq не установлена) — "
        "режим AI-Репетитора будет недоступен."
    )

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

LEVELS = [
    ("A1-A2", "A1-A2 (Beginner - Pre-Intermediate)"),
    ("B1-B2", "B1-B2 (Intermediate - Upper-Intermediate)"),
    ("C1-C2", "C1-C2 (Advanced - Proficiency)"),
]
TOPICS = [
    "☕️Разговоры обо всем",
    "🎬Кино и сериалы",
    "🎮Игры",
    "📚Книги",
    "✈️Путешествия",
    "🎵Музыка",
]

# Активные фоновые задачи ожидания собеседника: {user_id: asyncio.Task}
search_tasks: dict[int, asyncio.Task] = {}

# История сообщений в AI-чате: {user_id: [{"role": ..., "content": ...}, ...]}
ai_histories: dict[int, list] = {}

# ID последнего открытого inline-сообщения настроек: {user_id: message_id}
user_settings_msg: dict[int, int] = {}


# =============================================================================
# 2. БАЗА ДАННЫХ И МИГРАЦИИ
# =============================================================================

USERS_TABLE_SCHEMA = {
    "user_id": "INTEGER PRIMARY KEY",
    "username": "TEXT",
    "my_level": "TEXT DEFAULT 'B1-B2'",
    "partner_level": "TEXT DEFAULT 'B1-B2'",
    "topic": "TEXT DEFAULT '☕️Разговоры обо всем'",
    "status": "TEXT DEFAULT 'idle'",
    "partner_id": "INTEGER",
    "rating": "REAL DEFAULT 5.0",
    "rating_count": "INTEGER DEFAULT 0",
    "reports_count": "INTEGER DEFAULT 0",
    "is_banned": "INTEGER DEFAULT 0",
    "created_at": "TIMESTAMP",
}


async def init_db() -> None:
    """Создаёт таблицы и безопасно доводит структуру старых БД до актуальной."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reporter_id INTEGER,
                reported_id INTEGER,
                created_at TIMESTAMP
            )
            """
        )
        await db.commit()

        cursor = await db.execute("PRAGMA table_info(users)")
        existing_columns = {row[1] for row in await cursor.fetchall()}

        for column_name, column_def in USERS_TABLE_SCHEMA.items():
            if column_name == "user_id":
                continue
            if column_name not in existing_columns:
                try:
                    await db.execute(
                        f"ALTER TABLE users ADD COLUMN {column_name} {column_def}"
                    )
                    logger.info("Добавлена отсутствующая колонка users.%s", column_name)
                except aiosqlite.OperationalError as exc:
                    logger.warning("Не удалось добавить колонку %s: %s", column_name, exc)
        await db.commit()
    logger.info("База данных инициализирована: %s", DB_PATH)


async def ensure_user(user_id: int, username: str | None = None) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        if row is None:
            await db.execute(
                """
                INSERT INTO users (user_id, username, created_at)
                VALUES (?, ?, ?)
                """,
                (user_id, username or "", datetime.utcnow().isoformat()),
            )
            await db.commit()
        elif username:
            await db.execute(
                "UPDATE users SET username = ? WHERE user_id = ?", (username, user_id)
            )
            await db.commit()


async def get_user(user_id: int) -> aiosqlite.Row | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return await cursor.fetchone()


async def update_user(user_id: int, **fields) -> None:
    if not fields:
        return
    set_clause = ", ".join(f"{key} = ?" for key in fields)
    values = list(fields.values()) + [user_id]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"UPDATE users SET {set_clause} WHERE user_id = ?", values)
        await db.commit()


async def add_report(reporter_id: int, reported_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO reports (reporter_id, reported_id, created_at) VALUES (?, ?, ?)",
            (reporter_id, reported_id, datetime.utcnow().isoformat()),
        )
        await db.commit()
        cursor = await db.execute(
            "SELECT reports_count FROM users WHERE user_id = ?", (reported_id,)
        )
        row = await cursor.fetchone()
        new_count = (row[0] if row and row[0] else 0) + 1
        await db.execute(
            "UPDATE users SET reports_count = ? WHERE user_id = ?", (new_count, reported_id)
        )
        await db.commit()
        return new_count


async def find_partner(user_id: int, topic: str, partner_level: str) -> int | None:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT user_id FROM users
            WHERE status = 'searching'
              AND is_banned = 0
              AND user_id != ?
              AND topic = ?
              AND partner_level = ?
            LIMIT 1
            """,
            (user_id, topic, partner_level),
        )
        row = await cursor.fetchone()
        return row[0] if row else None


# =============================================================================
# 3. МЕНЮ И КЛАВИАТУРЫ
# =============================================================================

MENU_SEARCH = "🔍 Найти собеседника"
MENU_PROFILE = "👤 Профиль"
MENU_SETTINGS = "⚙️ Настройка собеседника"
MENU_STATS = "📊 Статистика"
MENU_STOP = "❌ Завершить диалог"


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=MENU_SEARCH)],
            [KeyboardButton(text=MENU_PROFILE), KeyboardButton(text=MENU_SETTINGS)],
            [KeyboardButton(text=MENU_STATS)],
        ],
        resize_keyboard=True,
    )


def active_chat_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=MENU_STOP)]],
        resize_keyboard=True,
    )


def level_keyboard(prefix: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=label, callback_data=f"setlevel_{prefix}_{code}")]
        for code, label in LEVELS
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def topics_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=topic, callback_data=f"settopic_{topic}")] for topic in TOPICS]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def back_to_settings_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад в настройки", callback_data="back_to_settings")]]
    )


def settings_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🧑‍🎓 Мой уровень", callback_data="settings_my_level")],
            [InlineKeyboardButton(text="🎯 Уровень собеседника", callback_data="settings_partner_level")],
            [InlineKeyboardButton(text="💬 Тема общения", callback_data="settings_topic")],
        ]
    )


def after_search_timeout_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🤖 Перейти к AI-Репетитору", callback_data="go_ai_tutor")],
            [InlineKeyboardButton(text="🔄 Искать снова", callback_data="search_again")],
        ]
    )


def feedback_keyboard(partner_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="👍 Отлично", callback_data=f"fb_like_{partner_id}"),
                InlineKeyboardButton(text="👎 Плохо", callback_data=f"fb_dislike_{partner_id}"),
            ],
            [InlineKeyboardButton(text="🚨 Пожаловаться", callback_data=f"fb_report_{partner_id}")],
        ]
    )


# =============================================================================
# 4. ЛОГИКА ПОИСКА С ФОНОВЫМ ТАЙМЕРОМ
# =============================================================================

def cancel_search_task(user_id: int) -> None:
    task = search_tasks.pop(user_id, None)
    if task and not task.done():
        task.cancel()


async def search_timeout_task(user_id: int) -> None:
    try:
        await asyncio.sleep(SEARCH_TIMEOUT_SECONDS)
        user = await get_user(user_id)
        if user is None or user["status"] != "searching":
            return
        await update_user(user_id, status="idle")
        try:
            await bot.send_message(
                user_id,
                "😔 Собеседник не найден за 30 секунд.\n"
                "Хотите попрактиковаться с AI-Репетитором или попробовать поиск ещё раз?",
                reply_markup=after_search_timeout_keyboard(),
            )
        except TelegramForbiddenError:
            logger.info("Пользователь %s заблокировал бота.", user_id)
    except asyncio.CancelledError:
        pass
    finally:
        search_tasks.pop(user_id, None)


async def start_search(user_id: int) -> None:
    cancel_search_task(user_id)
    user = await get_user(user_id)
    if user is None:
        return

    if user["is_banned"]:
        await bot.send_message(user_id, "🚫 Ваш аккаунт заблокирован за нарушения.")
        return

    await update_user(user_id, status="searching")

    partner_id = await find_partner(user_id, user["topic"], user["partner_level"])

    if partner_id:
        cancel_search_task(partner_id)
        await update_user(user_id, status="chatting", partner_id=partner_id)
        await update_user(partner_id, status="chatting", partner_id=user_id)

        partner = await get_user(partner_id)

        await bot.send_message(
            user_id,
            f"✅ Собеседник найден! Уровень: {partner['my_level'] or 'не указан'}. "
            f"Тема: {user['topic']}.\nМожете начинать общение — просто пишите сообщения.",
            reply_markup=active_chat_keyboard(),
        )
        await bot.send_message(
            partner_id,
            f"✅ Собеседник найден! Уровень: {user['my_level'] or 'не указан'}. "
            f"Тема: {user['topic']}.\nМожете начинать общение — просто пишите сообщения.",
            reply_markup=active_chat_keyboard(),
        )
    else:
        await bot.send_message(
            user_id,
            "🔍 Ищу собеседника... Это может занять до 30 секунд.",
        )
        task = asyncio.create_task(search_timeout_task(user_id))
        search_tasks[user_id] = task


async def end_chat(user_id: int, notify_partner: bool = True) -> None:
    user = await get_user(user_id)
    if user is None:
        return

    partner_id = user["partner_id"]
    was_chatting = user["status"] in ("chatting", "ai_chat") and partner_id

    await update_user(user_id, status="idle", partner_id=None)
    ai_histories.pop(user_id, None)

    if user["status"] == "ai_chat":
        await bot.send_message(user_id, "Диалог с AI-Репетитором завершён.", reply_markup=main_menu_keyboard())
        return

    if was_chatting:
        await update_user(partner_id, status="idle", partner_id=None)
        await bot.send_message(
            user_id,
            "Диалог завершён. Оцените, пожалуйста, вашего собеседника:",
            reply_markup=feedback_keyboard(partner_id),
        )
        if notify_partner:
            try:
                await bot.send_message(
                    partner_id,
                    "Собеседник завершил диалог. Оцените, пожалуйста, общение:",
                    reply_markup=feedback_keyboard(user_id),
                )
            except TelegramForbiddenError:
                logger.info("Пользователь %s заблокировал бота.", partner_id)
        await bot.send_message(partner_id, "Возврат в главное меню.", reply_markup=main_menu_keyboard())
        await bot.send_message(user_id, "Возврат в главное меню.", reply_markup=main_menu_keyboard())
    else:
        await bot.send_message(user_id, "Вы вернулись в главное меню.", reply_markup=main_menu_keyboard())


# =============================================================================
# 5. AI-РЕПЕТИТОР (Groq)
# =============================================================================

def build_ai_system_prompt(my_level: str) -> str:
    level = my_level or "B1-B2"
    return (
        "Ты — дружелюбный носитель языка и репетитор по разговорной практике. "
        f"Собеседник имеет уровень владения языком {level}. "
        "Общайся простыми фразами, соответствующими этому уровню, поддерживай "
        "диалог естественными вопросами. Отвечай кратко: 2-3 предложения. "
        "Если в сообщении собеседника есть ошибка, аккуратно поправь её в конце "
        "своего ответа в формате: '💡 Small tip: [краткая коррекция]'. "
        "Если ошибок нет, просто продолжай диалог без этой пометки."
    )


async def get_ai_reply(user_id: int, my_level: str, user_text: str) -> str:
    if groq_client is None:
        return "⚠️ AI-Репетитор временно недоступен: не настроен GROQ_API_KEY."

    history = ai_histories.setdefault(user_id, [])
    history.append({"role": "user", "content": user_text})
    history[:] = history[-10:]

    messages = [{"role": "system", "content": build_ai_system_prompt(my_level)}] + history

    try:
        response = await groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=messages,
            max_tokens=300,
            temperature=0.7,
        )
        reply_text = response.choices[0].message.content.strip()
    except Exception as exc:
        logger.exception("Ошибка запроса к Groq: %s", exc)
        return "⚠️ Произошла ошибка при обращении к AI. Попробуйте ещё раз чуть позже."

    history.append({"role": "assistant", "content": reply_text})
    history[:] = history[-10:]

    return reply_text


# =============================================================================
# 6. ОБРАБОТЧИКИ КОМАНД
# =============================================================================

@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    await ensure_user(message.from_user.id, message.from_user.username)
    await message.answer(
        "👋 Добро пожаловать в Language Exchange Bot!\n\n"
        "Здесь вы можете найти собеседника для языковой практики или "
        "потренироваться с AI-Репетитором.\n\n"
        "Настройте параметры поиска через «⚙️ Настройка собеседника» "
        "и нажмите «🔍 Найти собеседника», чтобы начать.",
        reply_markup=main_menu_keyboard(),
    )


@router.message(Command("reports"))
async def cmd_reports(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM reports ORDER BY id DESC LIMIT 10"
        )
        rows = await cursor.fetchall()

    if not rows:
        await message.answer("Жалоб пока нет.")
        return

    lines = ["📋 <b>Последние жалобы:</b>\n"]
    for row in rows:
        lines.append(
            f"#{row['id']} | Жалобщик: <code>{row['reporter_id']}</code> "
            f"→ Нарушитель: <code>{row['reported_id']}</code> | {row['created_at']}"
        )
    await message.answer("\n".join(lines))


@router.message(Command("ban"))
async def cmd_ban(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) != 2 or not parts[1].strip().lstrip("-").isdigit():
        await message.answer("Использование: /ban <user_id>")
        return

    target_id = int(parts[1].strip())
    await update_user(target_id, is_banned=1)

    target_user = await get_user(target_id)
    if target_user and target_user["status"] in ("chatting", "ai_chat") and target_user["partner_id"]:
        await end_chat(target_id, notify_partner=True)

    await message.answer(f"🚫 Пользователь {target_id} забанен.")
    try:
        await bot.send_message(target_id, "🚫 Вы были заблокированы администратором за нарушения.")
    except TelegramForbiddenError:
        pass


# =============================================================================
# 7. CALLBACK-ОБРАБОТЧИКИ (inline-кнопки)
# =============================================================================

@router.callback_query(F.data == "settings_my_level")
async def cb_settings_my_level(callback: CallbackQuery) -> None:
    await callback.answer()
    try:
        await callback.message.edit_text(
            "🧑‍🎓 Выберите ваш уровень владения языком:",
            reply_markup=level_keyboard("my"),
        )
    except TelegramBadRequest as exc:
        logger.warning("edit_text не сработал в cb_settings_my_level: %s", exc)


@router.callback_query(F.data == "settings_partner_level")
async def cb_settings_partner_level(callback: CallbackQuery) -> None:
    await callback.answer()
    try:
        await callback.message.edit_text(
            "🎯 Выберите желаемый уровень собеседника:",
            reply_markup=level_keyboard("partner"),
        )
    except TelegramBadRequest as exc:
        logger.warning("edit_text не сработал в cb_settings_partner_level: %s", exc)


@router.callback_query(F.data == "settings_topic")
async def cb_settings_topic(callback: CallbackQuery) -> None:
    await callback.answer()
    try:
        await callback.message.edit_text(
            "💬 Выберите тему общения:",
            reply_markup=topics_keyboard(),
        )
    except TelegramBadRequest as exc:
        logger.warning("edit_text не сработал в cb_settings_topic: %s", exc)


@router.callback_query(F.data.startswith("setlevel_"))
async def cb_set_level(callback: CallbackQuery) -> None:
    await callback.answer()

    _, target, level_code = callback.data.split("_")
    user_id = callback.from_user.id

    if target == "my":
        await update_user(user_id, my_level=level_code)
    else:
        await update_user(user_id, partner_level=level_code)

    try:
        await callback.message.edit_text(
            "✅ Уровень успешно сохранен!",
            reply_markup=back_to_settings_keyboard(),
        )
    except TelegramBadRequest as exc:
        logger.warning("edit_text не сработал в cb_set_level: %s", exc)


@router.callback_query(F.data.startswith("settopic_"))
async def cb_set_topic(callback: CallbackQuery) -> None:
    await callback.answer()

    topic = callback.data.removeprefix("settopic_")
    await update_user(callback.from_user.id, topic=topic)

    try:
        await callback.message.edit_text(
            "✅ Тема успешно сохранена!",
            reply_markup=back_to_settings_keyboard(),
        )
    except TelegramBadRequest as exc:
        logger.warning("edit_text не сработал в cb_set_topic: %s", exc)


@router.callback_query(F.data == "back_to_settings")
async def cb_back_to_settings(callback: CallbackQuery) -> None:
    await callback.answer()
    try:
        await callback.message.edit_text(
            "⚙️ Настройка параметров поиска собеседника:",
            reply_markup=settings_menu_keyboard(),
        )
    except TelegramBadRequest as exc:
        logger.warning("edit_text не сработал в cb_back_to_settings: %s", exc)


@router.callback_query(F.data == "go_ai_tutor")
async def cb_go_ai_tutor(callback: CallbackQuery) -> None:
    await callback.answer()

    user_id = callback.from_user.id
    await update_user(user_id, status="ai_chat", partner_id=None)
    ai_histories[user_id] = []

    try:
        await callback.message.edit_text("🤖 Переход к AI-Репетитору...", reply_markup=None)
    except TelegramBadRequest:
        pass

    await callback.message.answer(
        "🤖 Вы в режиме AI-Репетитора. Пишите сообщения на изучаемом языке — "
        "я буду поддерживать диалог и подсказывать, если замечу ошибки.",
        reply_markup=active_chat_keyboard(),
    )


@router.callback_query(F.data == "search_again")
async def cb_search_again(callback: CallbackQuery) -> None:
    await callback.answer()

    try:
        await callback.message.edit_text("🔄 Запускаю поиск заново...", reply_markup=None)
    except TelegramBadRequest:
        pass
    await start_search(callback.from_user.id)


@router.callback_query(F.data.startswith("fb_"))
async def cb_feedback(callback: CallbackQuery) -> None:
    await callback.answer()

    _, action, target_id_str = callback.data.split("_")
    target_id = int(target_id_str)
    rater_id = callback.from_user.id

    if action == "like" or action == "dislike":
        score = 5.0 if action == "like" else 1.0
        target_user = await get_user(target_id)
        if target_user:
            current_rating = target_user["rating"] or 5.0
            current_count = target_user["rating_count"] or 0
            new_count = current_count + 1
            new_rating = ((current_rating * current_count) + score) / new_count
            await update_user(target_id, rating=round(new_rating, 2), rating_count=new_count)

        thanks_text = "✅ Спасибо за оценку!" if action == "like" else "✅ Спасибо, ваша оценка учтена."
        try:
            await callback.message.edit_text(thanks_text, reply_markup=None)
        except TelegramBadRequest:
            pass

    elif action == "report":
        new_count = await add_report(rater_id, target_id)

        try:
            await bot.send_message(
                ADMIN_ID,
                "🚨 <b>Новая жалоба!</b>\n"
                f"Жалобщик: <code>{rater_id}</code>\n"
                f"Нарушитель: <code>{target_id}</code>\n"
                f"Всего жалоб на нарушителя: {new_count}\n\n"
                f"Забанить: <code>/ban {target_id}</code>",
            )
        except TelegramForbiddenError:
            logger.warning("Не удалось уведомить администратора: бот заблокирован админом.")

        if new_count >= REPORTS_TO_BAN:
            await update_user(target_id, is_banned=1)
            target_user = await get_user(target_id)
            if target_user and target_user["status"] in ("chatting", "ai_chat") and target_user["partner_id"]:
                await end_chat(target_id, notify_partner=True)
            try:
                await bot.send_message(
                    target_id, "🚫 Вы заблокированы автоматически за многократные жалобы."
                )
            except TelegramForbiddenError:
                pass

        try:
            await callback.message.edit_text("🚨 Жалоба отправлена администратору.", reply_markup=None)
        except TelegramBadRequest:
            pass


# =============================================================================
# 8. ЕДИНЫЙ ОБРАБОТЧИК ТЕКСТОВЫХ СООБЩЕНИЙ
# =============================================================================

async def remove_stale_settings_menu(user_id: int) -> None:
    """Удаляет зависшее inline-сообщение настроек."""
    old_msg_id = user_settings_msg.pop(user_id, None)
    if old_msg_id:
        try:
            await bot.delete_message(chat_id=user_id, message_id=old_msg_id)
        except TelegramBadRequest:
            pass


@router.message(F.text)
async def unified_text_handler(message: Message) -> None:
    user_id = message.from_user.id
    text = message.text or ""

    await ensure_user(user_id, message.from_user.username)
    user = await get_user(user_id)

    if user is None:
        return

    if user["is_banned"]:
        await message.answer("🚫 Ваш аккаунт заблокирован за нарушения правил.")
        return

    status = user["status"]

    if text == MENU_STOP and status in ("chatting", "ai_chat"):
        await remove_stale_settings_menu(user_id)
        await end_chat(user_id)
        return

    if status == "chatting":
        partner_id = user["partner_id"]
        if not partner_id:
            await update_user(user_id, status="idle")
            await message.answer("Диалог был прерван.", reply_markup=main_menu_keyboard())
            return
        try:
            await message.copy_to(chat_id=partner_id)
        except TelegramForbiddenError:
            await message.answer(
                "⚠️ Собеседник заблокировал бота. Диалог завершён.",
                reply_markup=main_menu_keyboard(),
            )
            await update_user(user_id, status="idle", partner_id=None)
            await update_user(partner_id, status="idle", partner_id=None)
        except TelegramBadRequest as exc:
            logger.warning("Не удалось переслать сообщение: %s", exc)
        return

    if status == "ai_chat":
        reply = await get_ai_reply(user_id, user["my_level"], text)
        await message.answer(reply)
        return

    if status == "searching":
        await message.answer("🔍 Идёт поиск собеседника, пожалуйста, подождите...")
        return

    await remove_stale_settings_menu(user_id)

    if "Найти собеседника" in text:
        await start_search(user_id)
        return

    if "Профиль" in text:
        await show_profile(message, user)
        return

    if "Настройка" in text:
        msg = await message.answer(
            "⚙️ Настройка параметров поиска собеседника:", reply_markup=settings_menu_keyboard()
        )
        user_settings_msg[user_id] = msg.message_id
        return

    if "Статистика" in text:
        await show_stats(message, user)
        return

    await message.answer(
        "Не понял вас 🤔 Используйте меню ниже.", reply_markup=main_menu_keyboard()
    )


async def show_profile(message: Message, user: aiosqlite.Row) -> None:
    ban_status = "🚫 Заблокирован" if user["is_banned"] else "✅ Активен"
    await message.answer(
        "👤 <b>Ваш профиль</b>\n\n"
        f"Мой уровень: <b>{user['my_level'] or 'не указан'}</b>\n"
        f"Уровень собеседника: <b>{user['partner_level'] or 'не указан'}</b>\n"
        f"Тема общения: <b>{user['topic']}</b>\n"
        f"Рейтинг: <b>{round(user['rating'] or 5.0, 2)} ⭐️</b> "
        f"({user['rating_count'] or 0} оценок)\n"
        f"Статус аккаунта: {ban_status}"
    )


async def show_stats(message: Message, user: aiosqlite.Row) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        total_users = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM users WHERE status = 'searching'")
        searching_now = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM users WHERE status = 'chatting'")
        chatting_now = (await cursor.fetchone())[0]

    await message.answer(
        "📊 <b>Статистика бота</b>\n\n"
        f"Всего пользователей: <b>{total_users}</b>\n"
        f"Сейчас ищут собеседника: <b>{searching_now}</b>\n"
        f"Сейчас общаются: <b>{chatting_now // 2 if chatting_now else 0}</b> пар\n\n"
        f"Ваш личный рейтинг: <b>{round(user['rating'] or 5.0, 2)} ⭐️</b>"
    )


# =============================================================================
# 9. МИКРО-СЕРВЕР ДЛЯ RENDER (Фоновый порт)
# =============================================================================

async def handle_ping(request):
    return web.Response(text="Bot is running!")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("Веб-сервер запущен на порту %s", port)


# =============================================================================
# 10. ЗАПУСК БОТА
# =============================================================================

async def main() -> None:
    await init_db()
    
    # Запускаем фоновый сервер для порта Render
    await start_web_server()
    
    logger.info("Бот запускается...")
    try:
        await bot.send_message(ADMIN_ID, "✅ Бот успешно запущен.")
    except Exception:
        logger.info("Не удалось отправить стартовое сообщение администратору.")

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен.")
