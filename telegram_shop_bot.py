import asyncio
import ctypes
import html
import logging
import os
import random
import socket
import sqlite3
import sys
from typing import Any, Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime

import aiosqlite
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    Update,
    User,
)


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

DB_PATH = os.getenv("DB_PATH", "shop.sqlite3")
CARD_NUMBER = os.getenv("CARD_NUMBER", "4874070023914656")
CARD_OWNER = os.getenv("CARD_OWNER", "Данило Р.")
MANAGER_URL = os.getenv("MANAGER_URL", "https://t.me/ManagerFDSMARKETUA")
REVIEWS_URL = os.getenv("REVIEWS_URL", "https://t.me/FDSMARKETUA_reviews")

STAR_BUY_RATE = 0.8
STAR_SELL_RATE = 0.38
MIN_STARS_BUY = 50
MIN_STARS_SELL = 100

router = Router()
LOCK_SOCKET: socket.socket | None = None
LOCK_MUTEX_HANDLE: int | None = None


class StarBuy(StatesGroup):
    choosing_recipient = State()
    entering_friend_username = State()
    choosing_amount = State()
    entering_custom_amount = State()
    choosing_payment = State()
    waiting_receipt = State()


class BalanceTopUp(StatesGroup):
    entering_amount = State()
    choosing_payment = State()
    waiting_receipt = State()


class Calculator(StatesGroup):
    choosing_direction = State()
    entering_stars = State()
    entering_uah = State()


class VirtualNumbers(StatesGroup):
    choosing_country = State()
    choosing_payment = State()
    waiting_receipt = State()


class Checks(StatesGroup):
    waiting_receipt = State()


class AdminPanel(StatesGroup):
    waiting_broadcast = State()


@dataclass(frozen=True)
class Product:
    code: str
    title: str
    price: float
    description: str


STAR_PACKS = {
    "50": 40.0,
    "100": 80.0,
    "200": 160.0,
    "500": 400.0,
}

VIRTUAL_NUMBERS = [
    Product("ua", "🇺🇦 +380 Украина", 160.0, "Віртуальний номер для Telegram з швидкою видачею."),
    Product("us", "🇺🇸 +1 США", 55.0, "Доступний номер США для реєстрації Telegram."),
    Product("de", "🇩🇪 +49 Германия", 180.0, "Стабільний номер Німеччини під Telegram."),
    Product("gb", "🇬🇧 +44 Британия", 90.0, "Віртуальний номер Великої Британії."),
    Product("jp", "🇯🇵 +81 Япония", 140.0, "Японський номер для Telegram-активації."),
    Product("es", "🇪🇸 +34 Испания", 170.0, "Іспанський номер з акуратною видачею."),
    Product("ca", "🇨🇦 +1 Канада", 55.0, "Канадський номер для швидкого старту."),
    Product("kz", "🇰🇿 +7 Казахстан", 135.0, "Номер Казахстану для Telegram."),
    Product("ru", "🇷🇺 +7 Россия", 110.0, "Номер РФ для Telegram-реєстрації."),
    Product("bd", "🇧🇩 +880 Бангладеш", 55.0, "Номер Бангладеш за вигідною ціною."),
    Product("co", "🇨🇴 Колумбия", 85.0, "Колумбійський номер для Telegram."),
    Product("tr", "🇹🇷 Турция", 109.8, "Турецький номер з якісною видачею."),
    Product("mx", "🇲🇽 Мексика", 130.0, "Мексиканський номер для Telegram."),
    Product("vn", "🇻🇳 Вьетнам", 95.0, "В'єтнамський номер для активації."),
    Product("id", "🇮🇩 Индонезия", 70.0, "Індонезійський номер для Telegram."),
]

def money(value: float) -> str:
    if float(value).is_integer():
        return f"{int(value)} грн"
    return f"{value:.2f}".rstrip("0").rstrip(".") + " грн"


def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


async def mark_update_processed(update_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO processed_updates (update_id, created_at) VALUES (?, ?)",
                (update_id, now_iso()),
            )
            await db.commit()
            return True
        except sqlite3.IntegrityError:
            return False


async def mark_message_processed(chat_id: int, message_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO processed_messages (chat_id, message_id, created_at) VALUES (?, ?, ?)",
                (chat_id, message_id, now_iso()),
            )
            await db.commit()
            return True
        except sqlite3.IntegrityError:
            return False


class UpdateDeduplicateMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Update, dict[str, Any]], Awaitable[Any]],
        event: Update,
        data: dict[str, Any],
    ) -> Any:
        if not await mark_update_processed(event.update_id):
            return None
        return await handler(event, data)


class MessageDeduplicateMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Message, dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: dict[str, Any],
    ) -> Any:
        if not await mark_message_processed(event.chat.id, event.message_id):
            return None
        return await handler(event, data)


def clean_username(username: str | None) -> str:
    return f"@{username}" if username else "без username"


def safe(text: object) -> str:
    return html.escape(str(text))


def acquire_single_instance_lock() -> None:
    """Stops accidental double starts on the same Windows PC."""
    global LOCK_SOCKET, LOCK_MUTEX_HANDLE
    if os.name == "nt":
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateMutexW.restype = ctypes.c_void_p
        mutex = kernel32.CreateMutexW(None, False, "Global\\FDS_MARKET_UA_TELEGRAM_BOT_SINGLE_INSTANCE")
        last_error = ctypes.get_last_error()
        if not mutex:
            raise RuntimeError("Не удалось создать системный замок для защиты от второго запуска.")
        if last_error == 183:
            raise RuntimeError(
                "Бот уже запущен в другом окне. Закрой старое окно PowerShell/CMD или перезагрузи ПК, "
                "а потом запускай только start_bot.bat один раз."
            )
        LOCK_MUTEX_HANDLE = int(mutex)

    LOCK_SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    LOCK_SOCKET.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
    try:
        LOCK_SOCKET.bind(("127.0.0.1", 39876))
        LOCK_SOCKET.listen(1)
    except OSError as exc:
        raise RuntimeError(
            "Бот уже запущен в другом окне. Закрой старое окно PowerShell или перезагрузи ПК, "
            "а потом запускай только одну копию."
        ) from exc


def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="💎 Купити Stars"),
                KeyboardButton(text="🔄 Продати Stars"),
                KeyboardButton(text="🧮 Калькулятор"),
            ],
            [KeyboardButton(text="🌍 Віртуальні номери")],
            [KeyboardButton(text="👤 Мій Баланс")],
            [KeyboardButton(text="📞 Підтримка"), KeyboardButton(text="💬 Відгуки")],
        ],
        resize_keyboard=True,
    )


def ikb(rows: list[list[tuple[str, str]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=text, callback_data=data) for text, data in row]
            for row in rows
        ]
    )


def url_kb(text: str, url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=text, url=url)]])


def payment_kb(back_to: str) -> InlineKeyboardMarkup:
    return ikb(
        [
            [("💳 Монобанк", "pay:mono"), ("🏦 Термінал", "pay:terminal")],
            [("🪙 Оплатити з балансу", "pay:balance")],
            [("⬅️ Назад", back_to)],
        ]
    )


def topup_payment_kb(back_to: str) -> InlineKeyboardMarkup:
    return ikb(
        [
            [("💳 Монобанк", "pay:mono"), ("🏦 Термінал", "pay:terminal")],
            [("⬅️ Назад", back_to)],
        ]
    )


def receipt_kb() -> InlineKeyboardMarkup:
    return ikb([[("❌ Скасувати замовлення", "order:cancel")], [("⬅️ Назад", "back:payment")]])


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE NOT NULL,
                username TEXT,
                balance REAL NOT NULL DEFAULT 0,
                total_deposit REAL NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT UNIQUE NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT,
                product_type TEXT NOT NULL,
                product_name TEXT NOT NULL,
                price REAL NOT NULL,
                payment_method TEXT,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_updates (
                update_id INTEGER PRIMARY KEY,
                created_at TEXT NOT NULL
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_messages (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, message_id)
            )
            """
        )
        await db.commit()


async def ensure_user(message_or_query: Message | CallbackQuery) -> None:
    user = message_or_query.from_user
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO users (user_id, username, balance, total_deposit, created_at)
            VALUES (?, ?, 0, 0, ?)
            ON CONFLICT(user_id) DO UPDATE SET username = excluded.username
            """,
            (user.id, user.username, now_iso()),
        )
        await db.commit()


async def get_user(user_id: int) -> sqlite3.Row | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = sqlite3.Row
        cur = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return await cur.fetchone()


async def get_all_user_ids() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id FROM users")
        rows = await cur.fetchall()
        return [int(row[0]) for row in rows]


async def get_stats() -> dict[str, float | int]:
    async with aiosqlite.connect(DB_PATH) as db:
        users_cur = await db.execute("SELECT COUNT(*), COALESCE(SUM(balance), 0), COALESCE(SUM(total_deposit), 0) FROM users")
        orders_cur = await db.execute("SELECT COUNT(*), COALESCE(SUM(price), 0) FROM orders")
        users_count, balance_sum, deposit_sum = await users_cur.fetchone()
        orders_count, orders_sum = await orders_cur.fetchone()
        return {
            "users": int(users_count),
            "balance_sum": float(balance_sum),
            "deposit_sum": float(deposit_sum),
            "orders": int(orders_count),
            "orders_sum": float(orders_sum),
        }


async def add_balance(user_id: int, amount: float, include_total: bool = True) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        if include_total:
            await db.execute(
                "UPDATE users SET balance = balance + ?, total_deposit = total_deposit + ? WHERE user_id = ?",
                (amount, amount, user_id),
            )
        else:
            await db.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
        await db.commit()


async def charge_balance(user_id: int, amount: float) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        if not row or float(row[0]) < amount:
            return False
        await db.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (amount, user_id))
        await db.commit()
        return True


async def create_order(
    user_id: int,
    username: str | None,
    product_type: str,
    product_name: str,
    price: float,
    payment_method: str | None,
    status: str,
) -> str:
    order_id = f"#{random.randint(100000, 999999)}"
    async with aiosqlite.connect(DB_PATH) as db:
        while True:
            try:
                await db.execute(
                    """
                    INSERT INTO orders
                    (order_id, user_id, username, product_type, product_name, price, payment_method, status, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (order_id, user_id, username, product_type, product_name, price, payment_method, status, now_iso()),
                )
                await db.commit()
                return order_id
            except sqlite3.IntegrityError:
                order_id = f"#{random.randint(100000, 999999)}"


async def update_order_status(order_id: str, status: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE orders SET status = ? WHERE order_id = ?", (status, order_id))
        await db.commit()


async def get_order(order_id: str) -> sqlite3.Row | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = sqlite3.Row
        cur = await db.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,))
        return await cur.fetchone()


async def show_home(message: Message, state: FSMContext | None = None) -> None:
    if state:
        await state.clear()
    await ensure_user(message)
    await message.answer(
        "👑 <b>FDS MARKET UA</b>\n\n"
        "💎 Telegram Stars, акаунти, номери та баланс в одному преміальному магазині.\n"
        "🫶 Оберіть розділ у меню нижче — усе працює швидко, чисто й красиво.",
        reply_markup=main_menu(),
    )


async def edit_or_answer(target: Message | CallbackQuery, text: str, markup: InlineKeyboardMarkup | None = None) -> None:
    if isinstance(target, CallbackQuery):
        with suppress(TelegramBadRequest):
            await target.message.edit_text(text, reply_markup=markup)
            return
        await target.message.answer(text, reply_markup=markup)
    else:
        await target.answer(text, reply_markup=markup)


def star_amount_kb() -> InlineKeyboardMarkup:
    return ikb(
        [
            [("⭐50 — 40 грн", "stars:amount:50"), ("⭐100 — 80 грн", "stars:amount:100")],
            [("⭐200 — 160 грн", "stars:amount:200"), ("⭐500 — 400 грн", "stars:amount:500")],
            [("✏️ Своя кількість 50+", "stars:custom")],
            [("⬅️ Назад", "back:recipient")],
        ]
    )


async def show_star_amounts(target: Message | CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    recipient = data.get("recipient_username", clean_username(target.from_user.username))
    await state.set_state(StarBuy.choosing_amount)
    await edit_or_answer(
        target,
        f"💎 <b>Оберіть потрібну кількість зірок для {safe(recipient)}</b>\n\n"
        "🔥 Чим більше пакет — тим швидше оформлення.\n"
        "✅ Після оплати менеджер перевірить чек і замовлення піде в роботу.",
        star_amount_kb(),
    )


async def show_virtual_numbers_menu(target: Message | CallbackQuery, state: FSMContext) -> None:
    await state.set_state(VirtualNumbers.choosing_country)
    rows = [[(f"{p.title} — {money(p.price)}", f"num:{p.code}")] for p in VIRTUAL_NUMBERS]
    rows.append([("⬅️ Назад", "back:main")])
    await edit_or_answer(
        target,
        "🌍 <b>Оберіть країну:</b>\n\n"
        "📱 Номер підійде для Telegram-активації. Після оплати менеджер видасть доступні дані.",
        ikb(rows),
    )


async def show_payment_details(message: Message, buyer: User, state: FSMContext, payment_method: str) -> None:
    data = await state.get_data()
    order_id = await create_order(
        buyer.id,
        buyer.username,
        data["product_type"],
        data["product_name"],
        float(data["price"]),
        payment_method,
        "waiting_receipt",
    )
    await state.update_data(order_id=order_id, payment_method=payment_method)
    bank_name = "Monobank 🏦" if payment_method == "mono" else "Термінал 🏦"
    extra = ""
    if data["product_type"] == "stars":
        extra = (
            f"👤 Зірки на акаунт: <b>{safe(data['recipient_username'])}</b>\n"
            f"⭐️ {safe(data['recipient_username'])} отримає: <b>{int(data['stars_amount'])} ⭐️</b>\n"
        )
    await message.answer(
        f"💳 <b>Банк: {bank_name}</b>\n\n"
        f"💳 Картка: <code>{CARD_NUMBER}</code>\n"
        f"👤 Отримувач: <b>{safe(CARD_OWNER)}</b>\n\n"
        f"🪙 До оплати: <b>{money(float(data['price']))}</b>\n"
        f"{extra}\n"
        f"📞 Номер замовлення:\n<b>{order_id}</b>\n\n"
        "📸 Після оплати надішліть квитанцію сюди одним повідомленням.",
        reply_markup=receipt_kb(),
    )


async def notify_admin_new_paid_order(bot: Bot, order_id: str, user_id: int, username: str | None) -> None:
    order = await get_order(order_id)
    if not order or not ADMIN_ID:
        return
    kb = ikb([[("✅ Підтвердити", f"admin:approve:{order_id}"), ("❌ Відхилити", f"admin:reject:{order_id}")]])
    await bot.send_message(
        ADMIN_ID,
        "📥 <b>Нове замовлення з балансу</b>\n\n"
        f"📞 Order ID: <b>{safe(order_id)}</b>\n"
        f"👤 Клієнт: <b>{safe(clean_username(username))}</b>\n"
        f"🆔 User ID: <code>{user_id}</code>\n"
        f"🎁 Товар: <b>{safe(order['product_name'])}</b>\n"
        f"🪙 Сума: <b>{money(float(order['price']))}</b>\n"
        "💳 Оплата: <b>внутрішній баланс</b>",
        reply_markup=kb,
    )


async def handle_balance_payment(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    data = await state.get_data()
    price = float(data["price"])
    if data.get("product_type") == "balance":
        await call.message.answer(
            "⚠️ <b>Баланс не можна поповнити самим балансом.</b>\n\n"
            "💳 Оберіть Монобанк або Термінал.",
            reply_markup=topup_payment_kb("back:balance_amount"),
        )
        return
    ok = await charge_balance(call.from_user.id, price)
    if not ok:
        user = await get_user(call.from_user.id)
        balance = float(user["balance"]) if user else 0.0
        await call.message.answer(
            "⚠️ <b>Недостатньо коштів на балансі</b>\n\n"
            f"💰 Ваш баланс: <b>{money(balance)}</b>\n"
            f"🪙 Потрібно: <b>{money(price)}</b>\n\n"
            "💳 Поповніть баланс або оберіть інший спосіб оплати.",
            reply_markup=payment_kb("back:payment"),
        )
        return
    order_id = await create_order(
        call.from_user.id,
        call.from_user.username,
        data["product_type"],
        data["product_name"],
        price,
        "balance",
        "paid",
    )
    await notify_admin_new_paid_order(bot, order_id, call.from_user.id, call.from_user.username)
    await call.message.answer(
        "✅ <b>Оплата з балансу пройшла успішно!</b>\n\n"
        f"📞 Замовлення: <b>{order_id}</b>\n"
        f"🎁 Товар: <b>{safe(data['product_name'])}</b>\n"
        f"🪙 Списано: <b>{money(price)}</b>\n\n"
        "⏱ Очікуйте обробку менеджером.",
        reply_markup=main_menu(),
    )
    await state.clear()


async def forward_receipt_to_admin(message: Message, state: FSMContext, bot: Bot) -> None:
    data = await state.get_data()
    order_id = data.get("order_id")
    if not order_id:
        await message.answer("⚠️ Замовлення не знайдено. Поверніться у меню та створіть його ще раз.", reply_markup=main_menu())
        await state.clear()
        return
    order = await get_order(order_id)
    if not order:
        await message.answer("⚠️ Замовлення не знайдено в базі. Спробуйте ще раз.", reply_markup=main_menu())
        await state.clear()
        return
    await message.answer(
        "🤩 <b>Добре!</b>\n\n"
        f"Замовлення <b>{safe(order_id)}</b> отримано ✔️\n\n"
        "⏱ Очікуйте перевірку.",
        reply_markup=main_menu(),
    )
    await update_order_status(order_id, "checking")
    if ADMIN_ID:
        await bot.send_message(
            ADMIN_ID,
            "📥 <b>Новий чек на перевірку</b>\n\n"
            f"📞 Order ID: <b>{safe(order_id)}</b>\n"
            f"👤 Username: <b>{safe(clean_username(message.from_user.username))}</b>\n"
            f"🆔 User ID: <code>{message.from_user.id}</code>\n"
            f"🪙 Сума: <b>{money(float(order['price']))}</b>\n"
            f"🎁 Товар: <b>{safe(order['product_name'])}</b>\n"
            f"💳 Метод: <b>{safe(order['payment_method'])}</b>",
        )
        with suppress(Exception):
            await message.forward(ADMIN_ID)
        await bot.send_message(
            ADMIN_ID,
            "✅ Оберіть результат перевірки:",
            reply_markup=ikb([[("✅ Підтвердити", f"admin:approve:{order_id}"), ("❌ Відхилити", f"admin:reject:{order_id}")]]),
        )
    await state.clear()


@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext) -> None:
    await show_home(message, state)


@router.message(Command("setbalance"))
async def cmd_setbalance(message: Message, command: CommandObject) -> None:
    await ensure_user(message)
    if message.from_user.id != ADMIN_ID:
        await message.answer("🚫 Ця команда доступна тільки адміну.")
        return
    parts = (command.args or "").split()
    if len(parts) != 2:
        await message.answer("⚠️ Формат: <code>/setbalance user_id amount</code>")
        return
    try:
        user_id = int(parts[0])
        amount = float(parts[1].replace(",", "."))
    except ValueError:
        await message.answer("⚠️ user_id має бути числом, amount — сумою.")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO users (user_id, username, balance, total_deposit, created_at)
            VALUES (?, NULL, 0, 0, ?)
            ON CONFLICT(user_id) DO NOTHING
            """,
            (user_id, now_iso()),
        )
        await db.commit()
    await add_balance(user_id, amount, include_total=True)
    await message.answer(f"✅ Баланс користувача <code>{user_id}</code> поповнено на <b>{money(amount)}</b>.")


@router.message(Command("broadcast"))
async def cmd_broadcast(message: Message, command: CommandObject, bot: Bot) -> None:
    await ensure_user(message)
    if message.from_user.id != ADMIN_ID:
        await message.answer("🚫 Ця команда доступна тільки адміну.")
        return
    text = (command.args or "").strip()
    if not text:
        await message.answer("⚠️ Формат: <code>/broadcast текст</code>")
        return
    sent = failed = 0
    for user_id in await get_all_user_ids():
        with suppress(Exception):
            await bot.send_message(user_id, f"📣 <b>Оголошення</b>\n\n{text}")
            sent += 1
            await asyncio.sleep(0.035)
            continue
        failed += 1
    await message.answer(f"📊 Розсилка завершена.\n\n✅ Надіслано: <b>{sent}</b>\n🚫 Помилок: <b>{failed}</b>")


@router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext) -> None:
    await ensure_user(message)
    if message.from_user.id != ADMIN_ID:
        await message.answer("🚫 Адмін-панель доступна тільки власнику магазину.")
        return
    await state.clear()
    stats = await get_stats()
    await message.answer(
        "👑 <b>Адмін-панель FDS MARKET UA</b>\n\n"
        f"👤 Користувачів: <b>{stats['users']}</b>\n"
        f"📦 Замовлень: <b>{stats['orders']}</b>\n"
        f"💰 Баланс у системі: <b>{money(float(stats['balance_sum']))}</b>\n"
        f"🤑 Поповнень за весь час: <b>{money(float(stats['deposit_sum']))}</b>\n"
        f"📊 Сума замовлень: <b>{money(float(stats['orders_sum']))}</b>",
        reply_markup=ikb(
            [
                [("📊 Оновити статистику", "adminpanel:stats")],
                [("📣 Зробити розсилку", "adminpanel:broadcast")],
                [("⬅️ Назад", "back:main")],
            ]
        ),
    )


@router.callback_query(F.data == "adminpanel:stats")
async def admin_panel_stats(call: CallbackQuery, state: FSMContext) -> None:
    if call.from_user.id != ADMIN_ID:
        await call.answer("🚫 Немає доступу", show_alert=True)
        return
    await state.clear()
    stats = await get_stats()
    await call.message.edit_text(
        "👑 <b>Адмін-панель FDS MARKET UA</b>\n\n"
        f"👤 Користувачів: <b>{stats['users']}</b>\n"
        f"📦 Замовлень: <b>{stats['orders']}</b>\n"
        f"💰 Баланс у системі: <b>{money(float(stats['balance_sum']))}</b>\n"
        f"🤑 Поповнень за весь час: <b>{money(float(stats['deposit_sum']))}</b>\n"
        f"📊 Сума замовлень: <b>{money(float(stats['orders_sum']))}</b>",
        reply_markup=ikb(
            [
                [("📊 Оновити статистику", "adminpanel:stats")],
                [("📣 Зробити розсилку", "adminpanel:broadcast")],
                [("⬅️ Назад", "back:main")],
            ]
        ),
    )
    await call.answer("Оновлено")


@router.callback_query(F.data == "adminpanel:broadcast")
async def admin_panel_broadcast(call: CallbackQuery, state: FSMContext) -> None:
    if call.from_user.id != ADMIN_ID:
        await call.answer("🚫 Немає доступу", show_alert=True)
        return
    await state.set_state(AdminPanel.waiting_broadcast)
    await call.message.edit_text(
        "📣 <b>Введіть текст розсилки</b>\n\n"
        "Повідомлення буде надіслано всім користувачам магазину.",
        reply_markup=ikb([[("⬅️ Назад", "adminpanel:stats")]]),
    )
    await call.answer()


@router.message(AdminPanel.waiting_broadcast)
async def admin_panel_broadcast_text(message: Message, state: FSMContext, bot: Bot) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    text = (message.text or "").strip()
    if not text:
        await message.answer("⚠️ Надішліть текстове повідомлення для розсилки.")
        return
    sent = failed = 0
    for user_id in await get_all_user_ids():
        try:
            await bot.send_message(user_id, f"📣 <b>Оголошення</b>\n\n{text}")
            sent += 1
            await asyncio.sleep(0.035)
        except Exception:
            failed += 1
    await state.clear()
    await message.answer(f"📊 Розсилка завершена.\n\n✅ Надіслано: <b>{sent}</b>\n🚫 Помилок: <b>{failed}</b>")


@router.message(F.text == "💎 Купити Stars")
async def buy_stars(message: Message, state: FSMContext) -> None:
    await ensure_user(message)
    await state.set_state(StarBuy.choosing_recipient)
    await message.answer(
        "💎 <b>Кому хочете придбати Stars?</b>\n\n"
        "🎁 Можемо поповнити ваш акаунт або красиво відправити зірки другу.",
        reply_markup=ikb([[("👤 Собі", "stars:recipient:self"), ("👥 Другу", "stars:recipient:friend")], [("⬅️ Назад", "back:main")]]),
    )


@router.callback_query(F.data.startswith("stars:recipient:"))
async def stars_recipient(call: CallbackQuery, state: FSMContext) -> None:
    await ensure_user(call)
    target = call.data.split(":")[-1]
    if target == "self":
        if not call.from_user.username:
            await state.set_state(StarBuy.entering_friend_username)
            await call.message.edit_text(
                "⚠️ <b>У вас немає username у Telegram.</b>\n\n"
                "✏️ Укажите @username (тег), на который нужно отправить звезды.\n\n"
                "Например: <code>@ManagerFDSMARKETUA</code>\n\n"
                "⚠️ Обязательно проверьте, что вы указали правильный ник!",
                reply_markup=ikb([[("⬅️ Назад", "back:recipient")]]),
            )
        else:
            await state.update_data(recipient_username=clean_username(call.from_user.username))
            await show_star_amounts(call, state)
    else:
        await state.set_state(StarBuy.entering_friend_username)
        await call.message.edit_text(
            "✏️ <b>Укажите @username (тег), на который нужно отправить звезды.</b>\n\n"
            "Например: <code>@ManagerFDSMARKETUA</code>\n\n"
            "⚠️ Обязательно проверьте, что вы указали правильный ник!",
            reply_markup=ikb([[("⬅️ Назад", "back:recipient")]]),
        )
    await call.answer()


@router.message(StarBuy.entering_friend_username)
async def stars_friend_username(message: Message, state: FSMContext) -> None:
    username = (message.text or "").strip()
    if username == "⬅️ Назад":
        await state.set_state(StarBuy.choosing_recipient)
        await buy_stars(message, state)
        return
    if not username.startswith("@") or len(username) < 6 or " " in username:
        await message.answer("⚠️ Вкажіть коректний тег у форматі <code>@username</code>.", reply_markup=ikb([[("⬅️ Назад", "back:recipient")]]))
        return
    await state.update_data(recipient_username=username)
    await show_star_amounts(message, state)


@router.callback_query(F.data.startswith("stars:amount:"))
async def stars_amount(call: CallbackQuery, state: FSMContext) -> None:
    amount = int(call.data.split(":")[-1])
    price = STAR_PACKS[str(amount)]
    data = await state.get_data()
    await state.update_data(
        product_type="stars",
        product_name=f"Telegram Stars x{amount} для {data.get('recipient_username')}",
        stars_amount=amount,
        price=price,
    )
    await state.set_state(StarBuy.choosing_payment)
    await call.message.edit_text(
        "✅ <b>Добре!</b>\n\n"
        f"На акаунт буде відправлено <b>{amount}</b> зірок ⭐️\n\n"
        f"💸 До оплати: <b>{money(price)}</b>\n\n"
        "👇 Оберіть спосіб оплати",
        reply_markup=payment_kb("back:amounts"),
    )
    await call.answer()


@router.callback_query(F.data == "stars:custom")
async def stars_custom(call: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(StarBuy.entering_custom_amount)
    await call.message.edit_text(
        "⭐️ <b>Введіть кількість зірок (від 50 ⭐️)</b>\n\n"
        "🧮 Формула проста: <b>1⭐️ = 0.8 грн</b>",
        reply_markup=ikb([[("⬅️ Назад", "back:amounts")]]),
    )
    await call.answer()


@router.message(StarBuy.entering_custom_amount)
async def stars_custom_amount(message: Message, state: FSMContext) -> None:
    try:
        amount = int((message.text or "").strip())
    except ValueError:
        await message.answer("⚠️ Введіть кількість цілим числом, наприклад <b>141</b>.")
        return
    if amount < MIN_STARS_BUY:
        await message.answer("🚫 Мінімальна кількість — <b>50 ⭐️</b>.")
        return
    price = round(amount * STAR_BUY_RATE, 2)
    data = await state.get_data()
    await state.update_data(
        product_type="stars",
        product_name=f"Telegram Stars x{amount} для {data.get('recipient_username')}",
        stars_amount=amount,
        price=price,
    )
    await state.set_state(StarBuy.choosing_payment)
    await message.answer(
        "✅ <b>Добре!</b>\n\n"
        f"На акаунт буде відправлено <b>{amount}</b> зірок ⭐️\n\n"
        f"💸 До оплати: <b>{money(price)}</b>\n\n"
        "👇 Оберіть спосіб оплати",
        reply_markup=payment_kb("back:amounts"),
    )


@router.callback_query(F.data.startswith("pay:"))
async def choose_payment(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    method = call.data.split(":")[-1]
    current_state = await state.get_state()
    if method == "balance":
        await handle_balance_payment(call, state, bot)
    else:
        if current_state and current_state.startswith(StarBuy.__name__):
            await state.set_state(StarBuy.waiting_receipt)
        elif current_state and current_state.startswith(BalanceTopUp.__name__):
            await state.set_state(BalanceTopUp.waiting_receipt)
        elif current_state and current_state.startswith(VirtualNumbers.__name__):
            await state.set_state(VirtualNumbers.waiting_receipt)
        else:
            await state.set_state(Checks.waiting_receipt)
        await show_payment_details(call.message, call.from_user, state, method)
    await call.answer()


@router.message(StarBuy.waiting_receipt)
@router.message(BalanceTopUp.waiting_receipt)
@router.message(VirtualNumbers.waiting_receipt)
@router.message(Checks.waiting_receipt)
async def receive_receipt(message: Message, state: FSMContext, bot: Bot) -> None:
    if not (message.photo or message.document or message.text):
        await message.answer("📸 Надішліть квитанцію фото, файлом або текстом з реквізитами платежу.")
        return
    await forward_receipt_to_admin(message, state, bot)


@router.callback_query(F.data == "order:cancel")
async def cancel_order(call: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    if data.get("order_id"):
        await update_order_status(data["order_id"], "cancelled")
    await state.clear()
    await call.message.edit_text("❌ <b>Замовлення скасовано.</b>\n\n🫶 Ви можете створити нове замовлення у меню нижче.")
    await call.message.answer("👑 Головне меню активне.", reply_markup=main_menu())
    await call.answer()


@router.message(F.text == "🔄 Продати Stars")
async def sell_stars(message: Message, state: FSMContext) -> None:
    await state.clear()
    await ensure_user(message)
    await message.answer(
        "⭐️ <b>Хочеш продати свої зірки Telegram?</b>\n\n"
        f"💸 <b>1⭐️ = {STAR_SELL_RATE} грн</b>\n"
        f"📌 Мінімум — <b>{MIN_STARS_SELL}⭐️</b>\n\n"
        "📞 Менеджер підкаже курс, ліміти й оформить угоду без зайвих рухів.",
        reply_markup=url_kb("📞 Написати менеджеру", MANAGER_URL),
    )


@router.message(F.text == "📞 Підтримка")
async def support(message: Message, state: FSMContext) -> None:
    await state.clear()
    await ensure_user(message)
    await message.answer(
        "😊 <b>Є питання?</b>\n\n"
        "🫶 Напишіть менеджеру — допоможемо з оплатою, замовленням або підбором товару.",
        reply_markup=url_kb("📩 Написати", MANAGER_URL),
    )


@router.message(F.text == "💬 Відгуки")
async def reviews(message: Message, state: FSMContext) -> None:
    await state.clear()
    await ensure_user(message)
    await message.answer(
        "💬 <b>Відгуки наших клієнтів</b>\n\n"
        "🤩 Перегляньте реальні покупки, швидкі видачі й красивий сервіс FDS MARKET UA.",
        reply_markup=url_kb("🚀 Перейти до відгуків", REVIEWS_URL),
    )


@router.message(F.text == "👤 Мій Баланс")
async def my_balance(message: Message, state: FSMContext) -> None:
    await state.clear()
    await ensure_user(message)
    user = await get_user(message.from_user.id)
    await message.answer(
        "ℹ️ <b>Інформація про вас:</b>\n\n"
        f"🆔 ID: <code>{message.from_user.id}</code>\n"
        f"💰 Баланс: <b>{money(float(user['balance']))}</b>\n"
        f"🤑 Баланс за весь час: <b>{money(float(user['total_deposit']))}</b>",
        reply_markup=ikb([[("💳 Поповнити баланс", "balance:topup")]]),
    )


@router.callback_query(F.data == "balance:topup")
async def balance_topup(call: CallbackQuery, state: FSMContext) -> None:
    await ensure_user(call)
    await state.set_state(BalanceTopUp.entering_amount)
    await call.message.edit_text(
        "💸 <b>Введіть суму поповнення:</b>\n\n"
        "🪙 Мінімальна сума: <b>10 грн</b>\n"
        "✅ Після перевірки чека кошти автоматично зарахуються на баланс.",
        reply_markup=ikb([[("⬅️ Назад", "back:balance")]]),
    )
    await call.answer()


@router.message(BalanceTopUp.entering_amount)
async def balance_amount(message: Message, state: FSMContext) -> None:
    try:
        amount = round(float((message.text or "").replace(",", ".")), 2)
    except ValueError:
        await message.answer("⚠️ Введіть суму числом, наприклад <b>500</b>.")
        return
    if amount < 10:
        await message.answer("🚫 Мінімальна сума поповнення — <b>10 грн</b>.")
        return
    await state.update_data(product_type="balance", product_name="Поповнення внутрішнього балансу", price=amount)
    await state.set_state(BalanceTopUp.choosing_payment)
    await message.answer(
        "✅ <b>Суму зафіксовано</b>\n\n"
        f"🪙 До поповнення: <b>{money(amount)}</b>\n\n"
        "👇 Оберіть спосіб оплати",
        reply_markup=topup_payment_kb("back:balance_amount"),
    )


@router.message(F.text == "🧮 Калькулятор")
async def calculator(message: Message, state: FSMContext) -> None:
    await ensure_user(message)
    await state.set_state(Calculator.choosing_direction)
    await message.answer(
        "🧮 <b>Оберіть напрямок:</b>\n\n"
        "💎 Швидко порахуємо Stars або суму в гривнях.",
        reply_markup=ikb(
            [
                [("⭐️ Зірки ➡️ Гривні", "calc:stars_to_uah")],
                [("💸 Гривні ➡️ Зірки", "calc:uah_to_stars")],
                [("⬅️ Назад", "back:main")],
            ]
        ),
    )


@router.callback_query(F.data.startswith("calc:"))
async def calc_direction(call: CallbackQuery, state: FSMContext) -> None:
    direction = call.data.split(":")[-1]
    await state.update_data(calc_direction=direction)
    if direction == "stars_to_uah":
        await state.set_state(Calculator.entering_stars)
        text = "⭐️ <b>Введіть кількість Stars:</b>\n\n🧮 Рахуємо за курсом <b>1⭐️ = 0.8 грн</b>."
    else:
        await state.set_state(Calculator.entering_uah)
        text = "💸 <b>Введіть суму в гривнях:</b>\n\n🧮 Покажемо, скільки Stars можна купити."
    await call.message.edit_text(text, reply_markup=ikb([[("⬅️ Назад", "back:calculator")]]))
    await call.answer()


@router.message(Calculator.entering_stars)
async def calc_stars(message: Message, state: FSMContext) -> None:
    try:
        stars = int((message.text or "").strip())
    except ValueError:
        await message.answer("⚠️ Введіть кількість цілим числом.")
        return
    await message.answer(
        "🧮 <b>Результат</b>\n\n"
        f"⭐️ Stars: <b>{stars}</b>\n"
        f"💸 Гривні: <b>{money(round(stars * STAR_BUY_RATE, 2))}</b>",
        reply_markup=ikb([[("⬅️ Назад", "back:calculator")]]),
    )


@router.message(Calculator.entering_uah)
async def calc_uah(message: Message, state: FSMContext) -> None:
    try:
        uah = float((message.text or "").replace(",", "."))
    except ValueError:
        await message.answer("⚠️ Введіть суму числом.")
        return
    stars = int(uah / STAR_BUY_RATE)
    await message.answer(
        "🧮 <b>Результат</b>\n\n"
        f"💸 Сума: <b>{money(uah)}</b>\n"
        f"⭐️ Орієнтовно Stars: <b>{stars}</b>",
        reply_markup=ikb([[("⬅️ Назад", "back:calculator")]]),
    )


@router.message(F.text == "🌍 Віртуальні номери")
async def virtual_numbers(message: Message, state: FSMContext) -> None:
    await ensure_user(message)
    await show_virtual_numbers_menu(message, state)


@router.callback_query(F.data.startswith("num:"))
async def number_selected(call: CallbackQuery, state: FSMContext) -> None:
    code = call.data.split(":")[-1]
    product = next((p for p in VIRTUAL_NUMBERS if p.code == code), None)
    if not product:
        await call.answer("Товар не знайдено", show_alert=True)
        return
    await state.update_data(product_type="virtual_number", product_name=product.title, price=product.price)
    await state.set_state(VirtualNumbers.choosing_payment)
    await call.message.edit_text(
        f"🌍 <b>{safe(product.title)}</b>\n\n"
        f"{safe(product.description)}\n\n"
        f"💸 Ціна: <b>{money(product.price)}</b>\n\n"
        "👇 Оберіть спосіб оплати",
        reply_markup=payment_kb("back:numbers"),
    )
    await call.answer()


@router.callback_query(F.data.startswith("admin:approve:"))
async def admin_approve(call: CallbackQuery, bot: Bot, state: FSMContext) -> None:
    if call.from_user.id != ADMIN_ID:
        await call.answer("🚫 Немає доступу", show_alert=True)
        return
    order_id = call.data.split(":", 2)[-1]
    order = await get_order(order_id)
    if not order:
        await call.answer("Замовлення не знайдено", show_alert=True)
        return
    await update_order_status(order_id, "approved")
    if order["product_type"] == "balance":
        await add_balance(int(order["user_id"]), float(order["price"]), include_total=True)
        await update_order_status(order_id, "completed")
        await bot.send_message(
            int(order["user_id"]),
            "✅ <b>Поповнення підтверджено!</b>\n\n"
            f"🪙 На баланс зараховано: <b>{money(float(order['price']))}</b>\n"
            f"📞 Замовлення: <b>{safe(order_id)}</b>",
        )
        await call.message.edit_text(f"✅ Поповнення {safe(order_id)} підтверджено й зараховано.")
    else:
        await update_order_status(order_id, "completed")
        await bot.send_message(
            int(order["user_id"]),
            "✅ <b>Оплату підтверджено!</b>\n\n"
            f"📞 Замовлення: <b>{safe(order_id)}</b>\n"
            f"🎁 Товар: <b>{safe(order['product_name'])}</b>\n\n"
            "🚀 Замовлення прийнято в роботу. Менеджер скоро завершить видачу.",
        )
        await call.message.edit_text(f"✅ Замовлення {safe(order_id)} підтверджено.")
    await call.answer("Підтверджено")


@router.callback_query(F.data.startswith("admin:reject:"))
async def admin_reject(call: CallbackQuery, bot: Bot) -> None:
    if call.from_user.id != ADMIN_ID:
        await call.answer("🚫 Немає доступу", show_alert=True)
        return
    order_id = call.data.split(":", 2)[-1]
    order = await get_order(order_id)
    if not order:
        await call.answer("Замовлення не знайдено", show_alert=True)
        return
    await update_order_status(order_id, "rejected")
    await bot.send_message(
        int(order["user_id"]),
        "❌ <b>Платіж відхилено</b>\n\n"
        f"📞 Замовлення: <b>{safe(order_id)}</b>\n"
        "⚠️ Перевірте суму, реквізити або надішліть коректну квитанцію менеджеру.",
    )
    await call.message.edit_text(f"❌ Замовлення {safe(order_id)} відхилено.")
    await call.answer("Відхилено")


@router.callback_query(F.data.startswith("back:"))
async def go_back(call: CallbackQuery, state: FSMContext) -> None:
    where = call.data.split(":", 1)[-1]
    if where == "main":
        await state.clear()
        await call.message.edit_text("👑 <b>Повертаємось у головне меню.</b>")
        await call.message.answer("Оберіть потрібний розділ нижче 👇", reply_markup=main_menu())
    elif where == "recipient":
        await state.set_state(StarBuy.choosing_recipient)
        await call.message.edit_text(
            "💎 <b>Кому хочете придбати Stars?</b>",
            reply_markup=ikb([[("👤 Собі", "stars:recipient:self"), ("👥 Другу", "stars:recipient:friend")], [("⬅️ Назад", "back:main")]]),
        )
    elif where == "amounts":
        await show_star_amounts(call, state)
    elif where == "payment":
        data = await state.get_data()
        product_type = data.get("product_type")
        if product_type == "stars":
            await show_star_amounts(call, state)
        elif product_type == "virtual_number":
            await show_virtual_numbers_menu(call, state)
        elif product_type == "balance":
            await state.set_state(BalanceTopUp.entering_amount)
            await call.message.edit_text("💸 <b>Введіть суму поповнення:</b>", reply_markup=ikb([[("⬅️ Назад", "back:balance")]]))
    elif where == "balance":
        await state.clear()
        user = await get_user(call.from_user.id)
        await call.message.edit_text(
            "ℹ️ <b>Інформація про вас:</b>\n\n"
            f"🆔 ID: <code>{call.from_user.id}</code>\n"
            f"💰 Баланс: <b>{money(float(user['balance']))}</b>\n"
            f"🤑 Баланс за весь час: <b>{money(float(user['total_deposit']))}</b>",
            reply_markup=ikb([[("💳 Поповнити баланс", "balance:topup")]]),
        )
    elif where == "balance_amount":
        await state.set_state(BalanceTopUp.entering_amount)
        await call.message.edit_text("💸 <b>Введіть суму поповнення:</b>", reply_markup=ikb([[("⬅️ Назад", "back:balance")]]))
    elif where == "calculator":
        await state.set_state(Calculator.choosing_direction)
        await call.message.edit_text(
            "🧮 <b>Оберіть напрямок:</b>",
            reply_markup=ikb([[("⭐️ Зірки ➡️ Гривні", "calc:stars_to_uah")], [("💸 Гривні ➡️ Зірки", "calc:uah_to_stars")], [("⬅️ Назад", "back:main")]]),
        )
    elif where == "numbers":
        await show_virtual_numbers_menu(call, state)
    await call.answer()


@router.message(F.text == "⬅️ Назад")
async def reply_back(message: Message, state: FSMContext) -> None:
    await show_home(message, state)


@router.message()
async def fallback(message: Message, state: FSMContext) -> None:
    await ensure_user(message)
    current = await state.get_state()
    if current:
        await message.answer("⚠️ Не зовсім зрозумів. Скористайтесь кнопкою <b>⬅️ Назад</b> або завершіть поточний крок.")
        return
    await message.answer(
        "🫶 <b>Я поруч.</b>\n\n"
        "Оберіть потрібний розділ у меню нижче — магазин уже готовий прийняти замовлення.",
        reply_markup=main_menu(),
    )


async def main() -> None:
    acquire_single_instance_lock()
    if not BOT_TOKEN:
        raise RuntimeError("Set BOT_TOKEN environment variable before running the bot.")
    if not ADMIN_ID:
        raise RuntimeError("Set ADMIN_ID environment variable before running the bot.")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    await init_db()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.update.outer_middleware(UpdateDeduplicateMiddleware())
    dp.message.outer_middleware(MessageDeduplicateMiddleware())
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
