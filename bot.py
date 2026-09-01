from dotenv import load_dotenv
load_dotenv()

import asyncio
import csv
import io
import logging
from datetime import datetime

import aiosqlite
from aiohttp import web
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BufferedInputFile,
)

try:
    from groq import AsyncGroq
except ImportError:
    AsyncGroq = None


# =============================================================================
# 1. КОНФИГУРАЦИЯ И ПЕРЕМЕННЫЕ
# =============================================================================

import os

BOT_TOKEN = os.getenv("BOT_TOKEN")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# UPDATE: убран хардкод дефолтного ADMIN_ID. Раньше при отсутствии
# переменной в .env бот тихо запускался с чужим/тестовым ID — жалобы и
# админ-уведомления в этом случае уходили бы не туда. Теперь при отсутствии
# или некорректном значении ADMIN_ID бот падает при старте (fail-fast),
# не успев начать поллинг с неверной конфигурацией.
_admin_id_raw = os.getenv("ADMIN_ID")
if not _admin_id_raw:
    raise ValueError(
        "ADMIN_ID не задан в .env — укажите числовой Telegram ID администратора."
    )
try:
    ADMIN_ID = int(_admin_id_raw)
except ValueError as exc:
    raise ValueError(
        f"ADMIN_ID должен быть числом, получено: {_admin_id_raw!r}"
    ) from exc

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

# UPDATE: категории для системы обратной связи. Формат (code, label) —
# code уходит в callback_data и в колонку feedback.category, label — это
# то, что видит пользователь на кнопке и админ в /feedback_stats.
FEEDBACK_CATEGORIES = [
    ("usability", "⭐ Удобство"),
    ("search", "🔍 Поиск собеседника"),
    ("communication", "💬 Общение"),
    ("bug", "🐛 Сообщить об ошибке"),
    ("suggestion", "💡 Предложить улучшение"),
]


# UPDATE: FSM-состояния сценария "Обратная связь".
class FeedbackStates(StatesGroup):
    waiting_for_category = State()
    waiting_for_rating = State()
    waiting_for_comment = State()

# Активные фоновые задачи ожидания собеседника: {user_id: asyncio.Task}
search_tasks: dict[int, asyncio.Task] = {}

# История сообщений в AI-чате: {user_id: [{"role": ..., "content": ...}, ...]}
ai_histories: dict[int, list] = {}

# ID последнего открытого inline-сообщения настроек: {user_id: message_id}
# Нужен, чтобы подчищать зависшее меню настроек, если пользователь ушёл
# в другой раздел через reply-кнопки, не нажав "Назад".
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
    # UPDATE: новая колонка для системы статистики. У существующих БД её
    # нет — init_db() ниже находит недостающие колонки через
    # PRAGMA table_info и добавляет их через ALTER TABLE автоматически.
    "completed_chats": "INTEGER DEFAULT 0",
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
        # UPDATE: таблица для системы обратной связи.
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                category TEXT,
                rating INTEGER,
                comment TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await db.commit()

        # Проверка структуры users через PRAGMA table_info и добавление
        # отсутствующих колонок, чтобы не ловить KeyError на старых БД.
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

        # UPDATE: очистка "зомби-сессий" при рестарте бота.
        # Фоновые задачи search_timeout_task и партнёрские связи 'chatting' /
        # 'ai_chat' живут только в памяти процесса — после перезапуска они
        # теряются, но статус в БД остаётся прежним, и пользователь застревает
        # в несуществующем чате/поиске навсегда. Поэтому при каждом старте
        # принудительно сбрасываем все "активные" статусы обратно в 'idle'.
        cursor = await db.execute(
            "SELECT COUNT(*) FROM users WHERE status IN ('searching', 'chatting', 'ai_chat')"
        )
        zombie_count = (await cursor.fetchone())[0]
        if zombie_count:
            await db.execute(
                """
                UPDATE users
                SET status = 'idle', partner_id = NULL
                WHERE status IN ('searching', 'chatting', 'ai_chat')
                """
            )
            await db.commit()
            logger.info("Сброшено зомби-сессий при старте: %s", zombie_count)

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


# UPDATE: атомарный инкремент completed_chats через SQL (col = col + 1),
# а не через update_user() — тот умеет только присваивать значения, а
# read-then-write с округлением через Python дал бы гонку между двумя
# участниками, завершающими сессию почти одновременно.
async def increment_completed_chats(user_id: int) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET completed_chats = completed_chats + 1 WHERE user_id = ?",
            (user_id,),
        )
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


# UPDATE: сохранение отзыва в таблицу feedback.
async def save_feedback(user_id: int, category: str, rating: int, comment: str | None) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO feedback (user_id, category, rating, comment) VALUES (?, ?, ?, ?)",
            (user_id, category, rating, comment),
        )
        await db.commit()


# UPDATE: find_partner теперь ищет двусторонний матч по уровням.
# Раньше матч строился только по partner_level кандидата, из-за чего
# пара могла сойтись, даже если кандидату не подходил уровень ищущего.
# Теперь оба условия обязательны:
#   - candidate.my_level      == искомый уровень собеседника (partner_level ищущего)
#   - candidate.partner_level == собственный уровень ищущего (my_level ищущего)
async def find_partner(user_id: int, topic: str, my_level: str, partner_level: str) -> int | None:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT user_id FROM users
            WHERE status = 'searching'
              AND is_banned = 0
              AND user_id != ?
              AND topic = ?
              AND my_level = ?
              AND partner_level = ?
            LIMIT 1
            """,
            (user_id, topic, partner_level, my_level),
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
# UPDATE: новая кнопка обратной связи.
MENU_FEEDBACK = "📝 Обратная связь"


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=MENU_SEARCH)],
            [KeyboardButton(text=MENU_PROFILE), KeyboardButton(text=MENU_SETTINGS)],
            # UPDATE: MENU_FEEDBACK стоит рядом со Статистикой, как просили в ТЗ.
            [KeyboardButton(text=MENU_STATS), KeyboardButton(text=MENU_FEEDBACK)],
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


# UPDATE: лайк/дизлайк заменены на 5-звёздочную оценку.
# callback_data теперь имеет вид fb_rate_{partner_id}_{score}, где score от 1 до 5.
def feedback_keyboard(partner_id: int) -> InlineKeyboardMarkup:
    star_buttons = [
        InlineKeyboardButton(text=f"{score}⭐️", callback_data=f"fb_rate_{partner_id}_{score}")
        for score in range(1, 6)
    ]
    return InlineKeyboardMarkup(
        inline_keyboard=[
            star_buttons,
            [InlineKeyboardButton(text="🚨 Пожаловаться", callback_data=f"fb_report_{partner_id}")],
        ]
    )


# UPDATE: клавиатуры сценария "📝 Обратная связь".
# Важно: префикс callback_data здесь — "fbs_" (Feedback Survey), а не "fb_",
# чтобы НЕ пересекаться с уже существующим fb_rate_/fb_report_ из
# пост-чатового фидбека (тот обрабатывается отдельным хэндлером
# `F.data.startswith("fb_")`). "fbs_...".startswith("fb_") == False,
# так что оба обработчика гарантированно не конфликтуют.
def feedback_category_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=label, callback_data=f"fbs_cat_{code}")]
        for code, label in FEEDBACK_CATEGORIES
    ]
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="fbs_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def feedback_rating_keyboard() -> InlineKeyboardMarkup:
    stars = [
        InlineKeyboardButton(text=f"⭐ {score}", callback_data=f"fbs_rate_{score}")
        for score in range(1, 6)
    ]
    return InlineKeyboardMarkup(inline_keyboard=[stars])


def feedback_comment_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⏭️ Пропустить", callback_data="fbs_skip")]]
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

    # UPDATE: передаём и my_level, и partner_level ищущего — find_partner
    # теперь матчит по обоим условиям одновременно (двусторонний матч).
    partner_id = await find_partner(user_id, user["topic"], user["my_level"], user["partner_level"])

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

        # UPDATE: засчитываем завершённый диалог обеим сторонам. К этой
        # точке status уже гарантированно 'chatting' (ветка 'ai_chat' выше
        # уже сделала return), так что AI-сессии сюда никогда не попадают.
        await increment_completed_chats(user_id)
        await increment_completed_chats(partner_id)

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


# UPDATE: сводка по обратной связи для админа.
@router.message(Command("feedback_stats"))
async def cmd_feedback_stats(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*), AVG(rating) FROM feedback")
        total_count, avg_rating = await cursor.fetchone()

        cursor = await db.execute(
            """
            SELECT category, COUNT(*), AVG(rating)
            FROM feedback
            GROUP BY category
            ORDER BY COUNT(*) DESC
            """
        )
        category_rows = await cursor.fetchall()

    if not total_count:
        await message.answer("📭 Отзывов пока нет.")
        return

    category_labels = dict(FEEDBACK_CATEGORIES)
    global_rating = round(avg_rating, 2) if avg_rating is not None else 0.0

    lines = [
        "📊 <b>Статистика обратной связи</b>",
        f"📝 Всего отзывов: <b>{total_count}</b>",
        f"⭐ Средняя оценка: <b>{global_rating}</b>",
        "",
        "<b>По категориям:</b>",
    ]

    bug_count = 0
    suggestion_count = 0
    for category_code, count, cat_avg_rating in category_rows:
        label = category_labels.get(category_code, category_code)
        cat_avg = round(cat_avg_rating, 2) if cat_avg_rating is not None else 0.0
        lines.append(f"{label}: <b>{count}</b> (ср. {cat_avg}⭐)")
        if category_code == "bug":
            bug_count = count
        elif category_code == "suggestion":
            suggestion_count = count

    lines.append("")
    lines.append(f"🐛 Сообщений об ошибках: <b>{bug_count}</b>")
    lines.append(f"💡 Предложений по улучшению: <b>{suggestion_count}</b>")

    await message.answer("\n".join(lines))

    # UPDATE: ниже сводки — последние 10 отзывов с непустым текстовым
    # комментарием. Джойним users, чтобы показать username (если есть),
    # без него падать не должны — у части пользователей username может
    # отсутствовать.
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT
                feedback.id,
                feedback.user_id,
                feedback.category,
                feedback.rating,
                feedback.comment,
                feedback.created_at,
                users.username
            FROM feedback
            LEFT JOIN users ON users.user_id = feedback.user_id
            WHERE feedback.comment IS NOT NULL AND feedback.comment != ''
            ORDER BY feedback.id DESC
            LIMIT 10
            """
        )
        comment_rows = await cursor.fetchall()

    if not comment_rows:
        await message.answer("💬 Текстовых комментариев пока нет.")
        return

    await _send_feedback_comments(message, comment_rows, category_labels)


# UPDATE: константы и хелпер для безопасного вывода списка комментариев.
# Telegram режет сообщения на уровне ~4096 символов — при большом
# количестве/длине комментариев собранный текст может этот лимит
# превысить, поэтому собираем сообщения "пачками" и обрезаем длинные
# одиночные комментарии, чтобы один отзыв не мог сломать форматирование
# всего блока.
TELEGRAM_MESSAGE_LIMIT = 4096
FEEDBACK_COMMENT_MAX_LEN = 500


async def _send_feedback_comments(
    message: Message,
    comment_rows: list[aiosqlite.Row],
    category_labels: dict[str, str],
) -> None:
    header = "💬 <b>Последние комментарии (до 10):</b>\n"
    chunks: list[str] = []
    current = header

    for row in comment_rows:
        username = row["username"]
        user_ref = f"@{username}" if username else f"ID <code>{row['user_id']}</code>"
        label = category_labels.get(row["category"], row["category"] or "—")
        stars = "⭐" * max(0, min(5, row["rating"] or 0))

        comment_text = row["comment"] or ""
        if len(comment_text) > FEEDBACK_COMMENT_MAX_LEN:
            comment_text = comment_text[:FEEDBACK_COMMENT_MAX_LEN].rstrip() + "…"

        entry = (
            f"\n👤 {user_ref} | {label}\n"
            f"{stars} ({row['rating'] or 0}/5)\n"
            f"📝 {comment_text}\n"
            f"🕒 {row['created_at']}\n"
        )

        # Если добавление очередной записи превысит лимит Telegram —
        # отправляем накопленный кусок и начинаем новый.
        if len(current) + len(entry) > TELEGRAM_MESSAGE_LIMIT:
            chunks.append(current)
            current = ""
        current += entry

    if current:
        chunks.append(current)

    for chunk in chunks:
        await message.answer(chunk)


# UPDATE: экспорт всей таблицы feedback в CSV для админа. В отличие от
# /feedback_stats (агрегаты + последние 10 комментариев в тексте сообщения),
# эта команда отдаёт полный сырой дамп — включая записи с пустым
# comment — в виде файла, чтобы можно было открыть в Excel/Sheets и
# анализировать/фильтровать самостоятельно.
@router.message(Command("download_feedback"))
async def cmd_download_feedback(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT
                feedback.id,
                feedback.user_id,
                users.username,
                feedback.category,
                feedback.rating,
                feedback.comment,
                feedback.created_at
            FROM feedback
            LEFT JOIN users ON users.user_id = feedback.user_id
            ORDER BY feedback.id DESC
            """
        )
        rows = await cursor.fetchall()

    if not rows:
        await message.answer("📭 Отзывов пока нет — экспортировать нечего.")
        return

    category_labels = dict(FEEDBACK_CATEGORIES)

    # UTF-8 with BOM (utf-8-sig), чтобы Excel по умолчанию корректно
    # определял кодировку и не ломал кириллицу.
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(
        ["id", "user_id", "username", "category", "rating", "comment", "created_at"]
    )
    for row in rows:
        writer.writerow(
            [
                row["id"],
                row["user_id"],
                row["username"] or "",
                category_labels.get(row["category"], row["category"] or ""),
                row["rating"] if row["rating"] is not None else "",
                row["comment"] or "",
                row["created_at"],
            ]
        )

    csv_bytes = buffer.getvalue().encode("utf-8-sig")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    document = BufferedInputFile(csv_bytes, filename=f"feedback_export_{timestamp}.csv")

    await message.answer_document(
        document=document,
        caption=f"📥 Экспорт таблицы feedback — {len(rows)} записей.",
    )


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


# UPDATE: разбор callback_data переписан под 5-звёздочную систему.
# Форматы теперь:
#   fb_rate_{partner_id}_{score}  — оценка от 1 до 5
#   fb_report_{partner_id}        — жалоба
# Парсим через rsplit с фиксированным числом частей, чтобы не полагаться
# на отсутствие "_" внутри partner_id (id всегда числовой, но так надёжнее).
@router.callback_query(F.data.startswith("fb_"))
async def cb_feedback(callback: CallbackQuery) -> None:
    await callback.answer()

    parts = callback.data.split("_")
    action = parts[1]
    rater_id = callback.from_user.id

    if action == "rate":
        # fb_rate_{partner_id}_{score} -> parts = ["fb", "rate", partner_id, score]
        target_id = int(parts[2])
        score = int(parts[3])
        score = max(1, min(5, score))  # защита от некорректного значения в callback_data

        target_user = await get_user(target_id)
        if target_user:
            current_rating = target_user["rating"] or 5.0
            current_count = target_user["rating_count"] or 0
            new_count = current_count + 1
            new_rating = ((current_rating * current_count) + score) / new_count
            await update_user(target_id, rating=round(new_rating, 2), rating_count=new_count)

        try:
            await callback.message.edit_text(
                f"✅ Спасибо за оценку! Вы поставили {score}⭐️", reply_markup=None
            )
        except TelegramBadRequest:
            pass

    elif action == "report":
        target_id = int(parts[2])
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


# UPDATE: callback-хэндлеры сценария "📝 Обратная связь" (FSM).
# Шаг 1: выбор категории. Точка входа переводит состояние в
# waiting_for_category (см. unified_text_handler), эта функция — переход
# к следующему шагу или отмена по "◀️ Назад".
@router.callback_query(FeedbackStates.waiting_for_category, F.data.startswith("fbs_cat_"))
async def cb_feedback_category(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()

    code = callback.data.removeprefix("fbs_cat_")
    label = dict(FEEDBACK_CATEGORIES).get(code, code)

    await state.update_data(category_code=code, category_label=label)
    await state.set_state(FeedbackStates.waiting_for_rating)

    try:
        await callback.message.edit_text(
            f"Категория: <b>{label}</b>\n\nОцените от 1 до 5:",
            reply_markup=feedback_rating_keyboard(),
        )
    except TelegramBadRequest:
        pass


@router.callback_query(F.data == "fbs_cancel")
async def cb_feedback_cancel(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.clear()

    try:
        await callback.message.edit_text("Отменено.", reply_markup=None)
    except TelegramBadRequest:
        pass
    await callback.message.answer("Главное меню:", reply_markup=main_menu_keyboard())


# Шаг 2: выбор оценки 1-5.
@router.callback_query(FeedbackStates.waiting_for_rating, F.data.startswith("fbs_rate_"))
async def cb_feedback_rating(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()

    score = int(callback.data.removeprefix("fbs_rate_"))
    score = max(1, min(5, score))  # защита от некорректного значения в callback_data

    await state.update_data(rating=score)
    await state.set_state(FeedbackStates.waiting_for_comment)

    try:
        await callback.message.edit_text(
            f"Оценка: {'⭐' * score}\n\n"
            "Напишите короткий комментарий (необязательно) или нажмите «Пропустить»:",
            reply_markup=feedback_comment_keyboard(),
        )
    except TelegramBadRequest:
        pass


# Шаг 3 (skip-ветка): пользователь нажал "Пропустить" вместо ввода текста.
# Ветка с текстовым комментарием обрабатывается в unified_text_handler,
# т.к. это обычное текстовое сообщение, а не callback.
@router.callback_query(FeedbackStates.waiting_for_comment, F.data == "fbs_skip")
async def cb_feedback_skip(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()

    data = await state.get_data()
    await save_feedback(
        user_id=callback.from_user.id,
        category=data.get("category_code", "unknown"),
        rating=data.get("rating", 0),
        comment=None,
    )
    await state.clear()

    try:
        await callback.message.edit_text(
            "✅ Спасибо за feedback! Ваш отзыв сохранён и поможет улучшить Language Exchange Bot.",
            reply_markup=None,
        )
    except TelegramBadRequest:
        pass
    await callback.message.answer("Главное меню:", reply_markup=main_menu_keyboard())


# =============================================================================
# 8. ЕДИНЫЙ ОБРАБОТЧИК ТЕКСТОВЫХ СООБЩЕНИЙ
# =============================================================================

async def remove_stale_settings_menu(user_id: int) -> None:
    """Удаляет зависшее inline-сообщение настроек, если пользователь ушёл
    в другой раздел меню, не нажав кнопку "Назад"."""
    old_msg_id = user_settings_msg.pop(user_id, None)
    if old_msg_id:
        try:
            await bot.delete_message(chat_id=user_id, message_id=old_msg_id)
        except TelegramBadRequest:
            pass


@router.message(F.text)
async def unified_text_handler(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    text = message.text or ""

    # UPDATE: перехват шага "комментарий" сценария обратной связи.
    # Проверяем это ДО ensure_user/is_banned/status-веток, чтобы не зависеть
    # от статуса пользователя в БД (во время фидбека он всегда 'idle') и не
    # дать обычной маршрутизации по кнопкам меню случайно перехватить текст.
    current_fsm_state = await state.get_state()

    if current_fsm_state == FeedbackStates.waiting_for_comment.state:
        data = await state.get_data()
        await save_feedback(
            user_id=user_id,
            category=data.get("category_code", "unknown"),
            rating=data.get("rating", 0),
            comment=text.strip() or None,
        )
        await state.clear()
        await message.answer(
            "✅ Спасибо за feedback! Ваш отзыв сохранён и поможет улучшить Language Exchange Bot.",
            reply_markup=main_menu_keyboard(),
        )
        return

    if current_fsm_state in (
        FeedbackStates.waiting_for_category.state,
        FeedbackStates.waiting_for_rating.state,
    ):
        # Пользователь написал текст вместо нажатия inline-кнопки — мягко
        # напоминаем, а не проваливаемся в обычную маршрутизацию меню.
        await message.answer("Пожалуйста, используйте кнопки выше 👆 (или «◀️ Назад», чтобы отменить).")
        return

    await ensure_user(user_id, message.from_user.username)
    user = await get_user(user_id)

    if user is None:
        return

    if user["is_banned"]:
        await message.answer("🚫 Ваш аккаунт заблокирован за нарушения правил.")
        return

    status = user["status"]

    # --- Кнопка завершения активного диалога (чат или AI) ---
    if text == MENU_STOP and status in ("chatting", "ai_chat"):
        await remove_stale_settings_menu(user_id)
        await end_chat(user_id)
        return

    # --- Пользователь находится в обычном чате с собеседником ---
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

    # --- Пользователь общается с AI-Репетитором ---
    if status == "ai_chat":
        reply = await get_ai_reply(user_id, user["my_level"], text)
        await message.answer(reply)
        return

    # --- Пользователь ожидает собеседника ---
    if status == "searching":
        await message.answer("🔍 Идёт поиск собеседника, пожалуйста, подождите...")
        return

    # --- Статус 'idle' — распознаём кнопки главного меню по подстроке ---
    # Если открыт "хвост" от предыдущей сессии настроек (пользователь ушёл
    # в другой раздел, не нажав "Назад") — подчищаем его, чтобы не копился мусор.
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

    # UPDATE: точка входа в сценарий "📝 Обратная связь" — запускаем FSM.
    if "Обратная связь" in text:
        await state.set_state(FeedbackStates.waiting_for_category)
        await message.answer(
            "📝 Обратная связь\n\nВыберите категорию отзыва:",
            reply_markup=feedback_category_keyboard(),
        )
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


# UPDATE: show_stats переписан на реальную агрегацию по БД одним запросом
# вместо трёх отдельных SELECT'ов, плюс добавлены total_chats и global_rating.
async def show_stats(message: Message, user: aiosqlite.Row) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                COUNT(*) AS total_users,
                SUM(CASE WHEN status = 'searching' THEN 1 ELSE 0 END) AS searching_now,
                SUM(CASE WHEN status = 'chatting' THEN 1 ELSE 0 END) AS chatting_now,
                SUM(completed_chats) AS total_completed_chats,
                AVG(CASE WHEN rating_count > 0 THEN rating END) AS global_rating
            FROM users
            """
        )
        row = await cursor.fetchone()

    total_users = row[0] or 0
    searching_now = row[1] or 0
    # completed_chats и status='chatting' выставляются/инкрементируются
    # обеим сторонам диалога одновременно, поэтому реальное число пар
    # и реальное число завершённых диалогов — это сырое значение / 2.
    chatting_now = (row[2] or 0) // 2
    total_chats = (row[3] or 0) // 2
    global_rating = round(row[4], 2) if row[4] is not None else 0.0

    user_completed = user["completed_chats"] or 0
    user_rating = round(user["rating"] or 5.0, 2)
    user_ratings_count = user["rating_count"] or 0

    await message.answer(
        "📊 <b>Статистика бота</b>\n"
        f"👥 Всего пользователей: <b>{total_users}</b>\n"
        f"🔍 Сейчас ищут собеседника: <b>{searching_now}</b>\n"
        f"💬 Сейчас общаются: <b>{chatting_now}</b> пар\n"
        f"🗣️ Всего диалогов: <b>{total_chats}</b>\n"
        f"⭐ Средний рейтинг: <b>{global_rating}</b>\n"
        "👤 <b>Ваша статистика</b>\n"
        f"💬 Завершённых диалогов: <b>{user_completed}</b>\n"
        f"⭐ Ваш рейтинг: <b>{user_rating}</b>\n"
        f"📝 Получено оценок: <b>{user_ratings_count}</b>"
    )


# =============================================================================
# 9. ЗАПУСК БОТА
# =============================================================================

async def health_check(request: web.Request) -> web.Response:
    return web.Response(text="OK", status=200)


async def start_health_check() -> None:
    """Поднимает лёгкий HTTP-сервер для health-check'а Render.

    Render Web Service ожидает, что процесс забиндится на порт из
    переменной окружения PORT и будет отвечать на HTTP-запросы — иначе
    деплой падает с таймаутом. Сам бот работает через long-polling и
    никакого HTTP не поднимает, поэтому здесь запускается отдельный
    минимальный aiohttp-сервер параллельно с polling'ом (просто чтобы
    Render видел открытый порт и получал 200 OK на "/" и "/health").
    """
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    logger.info("Health-check сервер запущен на 0.0.0.0:%s", port)


async def main() -> None:
    await init_db()
    await start_health_check()
    logger.info("Бот запускается...")
    try:
        await bot.send_message(ADMIN_ID, "✅ Бот успешно запущен.")
    except Exception:
        logger.info("Не удалось отправить стартовое сообщение администратору (это нормально при первом запуске).")

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен.")
