#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
TELEGRAM БОТ @Moscow_Seller — ОПРИХОДОВАНИЕ ТОВАРОВ
============================================================================

Назначение:
    Telegram-бот для создания документов оприходования на складе.
    Позволяет добавлять приходы прямо с телефона без доступа к веб-интерфейсу.

Возможности:
    - Создание документов оприходования через диалог
    - Выбор товаров из списка или поиск по названию/SKU
    - Добавление нескольких товаров в один документ
    - Указание имени приёмщика и даты прихода

Использование:
    1. Создать бота через @BotFather
    2. Добавить токен в .env: TELEGRAM_BOT_TOKEN=xxx
    3. Добавить секрет в .env: TELEGRAM_BOT_SECRET=xxx
    4. Запустить: python telegram_bot.py

Зависимости:
    - python-telegram-bot>=20.0
    - requests

@author OZON Tracker Team
@version 1.0.0
@lastUpdated 2026-02-07
"""

import os
import sys
import logging
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters
)

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

# Загружаем переменные окружения
load_dotenv()

# Токен бота (получить у @BotFather)
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')

# Секретный ключ для API (должен совпадать с TELEGRAM_BOT_SECRET в .env сервера)
TELEGRAM_BOT_SECRET = os.getenv('TELEGRAM_BOT_SECRET', '')

# URL API сервера
API_BASE_URL = os.getenv('API_BASE_URL', 'http://127.0.0.1:8000')

# Разрешённые chat_id (если пусто — разрешены все)
ALLOWED_CHAT_IDS = os.getenv('TELEGRAM_ALLOWED_CHATS', '').split(',')
ALLOWED_CHAT_IDS = [int(x.strip()) for x in ALLOWED_CHAT_IDS if x.strip().isdigit()]

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ============================================================================
# СОСТОЯНИЯ ДИАЛОГА
# ============================================================================

# Состояния для ConversationHandler
(
    STATE_RECEIVER_NAME,      # Ввод имени приёмщика
    STATE_RECEIPT_DATE,       # Выбор даты прихода
    STATE_SELECT_PRODUCT,     # Выбор товара
    STATE_ENTER_QUANTITY,     # Ввод количества
    STATE_MORE_PRODUCTS,      # Добавить ещё товар?
    STATE_COMMENT,            # Комментарий
    STATE_CONFIRM,            # Подтверждение
    STATE_WAITING_REPLY       # Ожидание ответа на сообщение
) = range(8)

# Состояния для отправки сообщения в контейнер (начинаем с 200, чтобы не конфликтовать)
STATE_MSG_CONTAINER_SELECT = 200   # Выбор контейнера из списка
STATE_MSG_RECIPIENTS = 201          # Выбор получателей (мультивыбор)
STATE_MSG_TEXT = 202                # Ввод текста сообщения / прикрепление файла
STATE_MSG_CONFIRM = 203             # Подтверждение перед отправкой

# Состояния для финансового модуля (300-399)
STATE_FIN_TYPE = 300               # Выбор типа: доход или расход
STATE_FIN_AMOUNT = 301             # Ввод суммы
STATE_FIN_ACCOUNT = 302            # Выбор счёта/источника
STATE_FIN_CATEGORY = 303           # Выбор категории
STATE_FIN_DESCRIPTION = 304        # Ввод описания (на что)
STATE_FIN_CONFIRM = 305            # Подтверждение перед сохранением
STATE_FIN_YUAN_AMOUNT = 306        # Ввод суммы в юанях (для категорий с requires_yuan)

# Состояния для создания отправки (400-402)
STATE_SHIPMENT_COMMENT = 400       # Ввод комментария к отправке (обязательно)
STATE_SHIPMENT_FILE = 401          # Прикрепление файла (опционально)
STATE_SHIPMENT_CONFIRM = 402       # Подтверждение создания отправки

# Количество контейнеров на странице в списке выбора
MSG_PAGE_SIZE = 6


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def escape_markdown(text: str) -> str:
    """
    Экранирует специальные символы Markdown.
    Telegram использует символы *_`[ для форматирования.
    """
    escape_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in escape_chars:
        text = text.replace(char, f'\\{char}')
    return text


def escape_md(text: str) -> str:
    """
    Экранирует символы Markdown v1 для безопасной вставки в сообщения с parse_mode='Markdown'.
    Только 4 символа: _ * ` [
    Используется для динамического пользовательского контента (имена, текст, имена файлов).
    """
    for char in ['_', '*', '`', '[']:
        text = text.replace(char, f'\\{char}')
    return text


def is_authorized(chat_id: int) -> bool:
    """
    Проверяет, авторизован ли пользователь.
    Если ALLOWED_CHAT_IDS пуст — разрешены все.
    """
    if not ALLOWED_CHAT_IDS:
        return True
    return chat_id in ALLOWED_CHAT_IDS


def get_products(search: str = '') -> list:
    """
    Получить список товаров с сервера.

    Аргументы:
        search: Строка поиска (SKU или часть названия)

    Возвращает:
        Список товаров: [{'sku': 123, 'name': 'Название', 'offer_id': 'ART123'}, ...]
    """
    try:
        params = {'token': TELEGRAM_BOT_SECRET}
        if search:
            params['search'] = search

        response = requests.get(
            f'{API_BASE_URL}/api/telegram/products',
            params=params,
            timeout=10
        )
        data = response.json()

        if data.get('success'):
            return data.get('products', [])
        else:
            logger.error(f"Ошибка API: {data.get('error')}")
            return []
    except Exception as e:
        logger.error(f"Ошибка получения товаров: {e}")
        return []


def create_receipt(receipt_data: dict) -> dict:
    """
    Создать документ оприходования на сервере.

    Аргументы:
        receipt_data: Данные документа

    Возвращает:
        {'success': True, 'doc_id': 123} или {'success': False, 'error': 'текст'}
    """
    try:
        receipt_data['token'] = TELEGRAM_BOT_SECRET

        response = requests.post(
            f'{API_BASE_URL}/api/telegram/create-receipt',
            json=receipt_data,
            timeout=10
        )
        return response.json()
    except Exception as e:
        logger.error(f"Ошибка создания документа: {e}")
        return {'success': False, 'error': str(e)}


def get_finance_accounts() -> list:
    """
    Получить список финансовых счетов с сервера.

    Возвращает:
        Список счетов: [{'id': 1, 'name': 'ООО'}, ...]
    """
    try:
        response = requests.get(
            f'{API_BASE_URL}/api/telegram/finance/accounts',
            params={'token': TELEGRAM_BOT_SECRET},
            timeout=10
        )
        data = response.json()
        if data.get('success'):
            return data.get('accounts', [])
        else:
            logger.error(f"Ошибка API (финансовые счета): {data.get('error')}")
            return []
    except Exception as e:
        logger.error(f"Ошибка получения финансовых счетов: {e}")
        return []


def get_finance_categories(record_type: str = '') -> list:
    """
    Получить список финансовых категорий с сервера.

    Аргументы:
        record_type (str): Тип записи ('income' или 'expense'). Если пусто — все.

    Возвращает:
        Список категорий: [{'id': 1, 'name': 'Упаковка'}, ...]
    """
    try:
        params = {'token': TELEGRAM_BOT_SECRET}
        if record_type in ('income', 'expense'):
            params['type'] = record_type
        response = requests.get(
            f'{API_BASE_URL}/api/telegram/finance/categories',
            params=params,
            timeout=10
        )
        data = response.json()
        if data.get('success'):
            return data.get('categories', [])
        else:
            logger.error(f"Ошибка API (финансовые категории): {data.get('error')}")
            return []
    except Exception as e:
        logger.error(f"Ошибка получения финансовых категорий: {e}")
        return []


def create_finance_record(record_data: dict) -> dict:
    """
    Создать финансовую запись на сервере.

    Аргументы:
        record_data: Данные записи (record_type, amount, account_id, description, telegram_chat_id, telegram_username)

    Возвращает:
        {'success': True, 'id': 123} или {'success': False, 'error': 'текст'}
    """
    try:
        record_data['token'] = TELEGRAM_BOT_SECRET
        response = requests.post(
            f'{API_BASE_URL}/api/telegram/finance/add',
            json=record_data,
            timeout=10
        )
        return response.json()
    except Exception as e:
        logger.error(f"Ошибка создания финансовой записи: {e}")
        return {'success': False, 'error': str(e)}


def create_finance_record_with_files(record_data: dict, files: list) -> dict:
    """
    Создать финансовую запись с файлами через multipart API.

    Аргументы:
        record_data: Данные записи (record_type, amount, account_id, description, и т.д.)
        files: Список файлов [{'data': bytes, 'filename': str}, ...]

    Возвращает:
        {'success': True, 'id': 123} или {'success': False, 'error': 'текст'}
    """
    try:
        form_data = {k: str(v) for k, v in record_data.items()}
        form_data['token'] = TELEGRAM_BOT_SECRET

        file_tuples = [('files', (f['filename'], f['data'])) for f in files]

        response = requests.post(
            f'{API_BASE_URL}/api/telegram/finance/add',
            data=form_data,
            files=file_tuples,
            timeout=30
        )
        return response.json()
    except Exception as e:
        logger.error(f"Ошибка создания финансовой записи с файлами: {e}")
        return {'success': False, 'error': str(e)}


def format_amount(amount: float) -> str:
    """
    Форматирует число с пробелами между разрядами.
    Пример: 15000.50 → '15 000.50', 5000 → '5 000'
    """
    if amount == int(amount):
        return f"{int(amount):,}".replace(',', ' ')
    return f"{amount:,.2f}".replace(',', ' ')


def create_shipment(chat_id: int, comment: str, sender_name: str,
                    file_data: bytes = None, filename: str = None) -> dict:
    """
    Создать новую отправку (контейнер) через API.

    Аргументы:
        chat_id: Telegram chat_id отправителя
        comment: Комментарий к отправке (обязательный)
        sender_name: Имя отправителя (@username или имя)
        file_data: Байты файла (опционально)
        filename: Имя файла (опционально)

    Возвращает:
        {'success': True, 'doc_id': N, 'message_id': N} или {'success': False, 'error': '...'}
    """
    try:
        if file_data and filename:
            # Multipart/form-data для файлов
            response = requests.post(
                f'{API_BASE_URL}/api/telegram/create-shipment',
                data={
                    'token': TELEGRAM_BOT_SECRET,
                    'chat_id': chat_id,
                    'comment': comment,
                    'sender_name': sender_name
                },
                files={
                    'files': (filename, file_data)
                },
                timeout=30
            )
        else:
            # JSON для текстовых запросов
            response = requests.post(
                f'{API_BASE_URL}/api/telegram/create-shipment',
                json={
                    'token': TELEGRAM_BOT_SECRET,
                    'chat_id': chat_id,
                    'comment': comment,
                    'sender_name': sender_name
                },
                timeout=15
            )
        return response.json()
    except Exception as e:
        logger.error(f"Ошибка создания отправки: {e}")
        return {'success': False, 'error': str(e)}


def send_reply_to_server(chat_id: int, message: str, reply_to_message_id: int, sender_name: str) -> dict:
    """
    Отправить ответ пользователя на сервер.

    Аргументы:
        chat_id: ID чата Telegram
        message: Текст сообщения
        reply_to_message_id: ID сообщения, на которое ответили
        sender_name: Имя отправителя (username или имя)

    Возвращает:
        {'success': True, 'doc_id': 123} или {'success': False, 'error': 'текст'}
    """
    try:
        response = requests.post(
            f'{API_BASE_URL}/api/document-messages/receive',
            json={
                'token': TELEGRAM_BOT_SECRET,
                'chat_id': chat_id,
                'message': message,
                'reply_to_message_id': reply_to_message_id,
                'sender_name': sender_name
            },
            timeout=10
        )
        return response.json()
    except Exception as e:
        logger.error(f"Ошибка отправки ответа на сервер: {e}")
        return {'success': False, 'error': str(e)}


def send_reply_to_document(chat_id: int, doc_type: str, doc_id: int, message: str, sender_name: str) -> dict:
    """
    Отправить ответ на документ напрямую (по doc_type и doc_id).

    Используется при нажатии на кнопку "Ответить" под сообщением.

    Аргументы:
        chat_id: ID чата Telegram
        doc_type: Тип документа (receipt, shipment)
        doc_id: ID документа
        message: Текст сообщения
        sender_name: Имя отправителя

    Возвращает:
        {'success': True} или {'success': False, 'error': 'текст'}
    """
    try:
        response = requests.post(
            f'{API_BASE_URL}/api/document-messages/receive-direct',
            json={
                'token': TELEGRAM_BOT_SECRET,
                'chat_id': chat_id,
                'doc_type': doc_type,
                'doc_id': doc_id,
                'message': message,
                'sender_name': sender_name
            },
            timeout=10
        )
        return response.json()
    except Exception as e:
        logger.error(f"Ошибка отправки ответа на сервер: {e}")
        return {'success': False, 'error': str(e)}


def format_product_list(items: list) -> str:
    """
    Форматирует список товаров для отображения.
    """
    if not items:
        return "Нет товаров"

    lines = []
    for i, item in enumerate(items, 1):
        lines.append(f"{i}. {escape_markdown(item['offer_id'])} × {item['quantity']} шт.")

    return "\n".join(lines)


# ============================================================================
# ГЛАВНОЕ МЕНЮ
# ============================================================================

def get_main_menu():
    """
    Возвращает главное меню бота с кнопками.
    """
    keyboard = [
        ["📦 Новый приход"],
        ["🚚 Отправка товара"],
        ["💰 Финансы"],
        ["✉️ Сообщение", "📊 Остатки"],
        ["❓ Помощь"]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


# ============================================================================
# ОБРАБОТЧИКИ КОМАНД
# ============================================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обработчик команды /start.
    Приветствие и список команд.
    """
    chat_id = update.effective_chat.id

    if not is_authorized(chat_id):
        await update.message.reply_text(
            "⛔ У вас нет доступа к этому боту.\n"
            f"Ваш chat_id: {chat_id}"
        )
        return

    await update.message.reply_text(
        "👋 Привет! Я бот *Moscow Seller*.\n\n"
        "Выберите действие из меню ниже 👇",
        parse_mode='Markdown',
        reply_markup=get_main_menu()
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обработчик команды /помощь или /help.
    """
    await update.message.reply_text(
        "📖 *Справка по боту Moscow Seller*\n\n"
        "*Создание прихода:*\n"
        "1. Нажмите «📦 Новый приход»\n"
        "2. Укажите имя приёмщика\n"
        "3. Выберите дату прихода\n"
        "4. Выберите товары и количество\n"
        "5. Добавьте комментарий (опционально)\n"
        "6. Подтвердите создание\n\n"
        "*Поиск товаров:*\n"
        "При выборе товара можно ввести:\n"
        "• SKU (числовой код)\n"
        "• Часть названия\n"
        "• Артикул\n\n"
        "Документ появится во вкладке Склад → Оприходование\n"
        "с пометкой 📱 TG и статусом 🔴 Новый",
        parse_mode='Markdown',
        reply_markup=get_main_menu()
    )


# ============================================================================
# ДИАЛОГ СОЗДАНИЯ ПРИХОДА
# ============================================================================

async def receipt_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Начало диалога создания прихода.
    Запрашивает имя приёмщика.
    """
    chat_id = update.effective_chat.id

    if not is_authorized(chat_id):
        await update.message.reply_text("⛔ У вас нет доступа к этому боту.")
        return ConversationHandler.END

    # Инициализируем данные документа
    context.user_data['receipt'] = {
        'items': [],
        'receiver_name': '',
        'receipt_date': '',
        'comment': '',
        'telegram_chat_id': chat_id,
        'telegram_username': update.effective_user.username or str(chat_id)
    }

    await update.message.reply_text(
        "📦 *НОВЫЙ ПРИХОД*\n\n"
        "👤 Введите имя приёмщика:",
        parse_mode='Markdown'
    )

    return STATE_RECEIVER_NAME


async def receiver_name_entered(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Получено имя приёмщика. Запрашиваем дату.
    """
    receiver_name = update.message.text.strip()

    if not receiver_name:
        await update.message.reply_text("❌ Введите имя приёмщика:")
        return STATE_RECEIVER_NAME

    context.user_data['receipt']['receiver_name'] = receiver_name

    # Кнопки выбора даты
    today = datetime.now()
    yesterday = today - timedelta(days=1)

    keyboard = [
        [
            InlineKeyboardButton(f"Сегодня ({today.strftime('%d.%m')})", callback_data=f"date:{today.strftime('%Y-%m-%d')}"),
            InlineKeyboardButton(f"Вчера ({yesterday.strftime('%d.%m')})", callback_data=f"date:{yesterday.strftime('%Y-%m-%d')}")
        ],
        [
            InlineKeyboardButton("Указать другую дату", callback_data="date:custom")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"✅ Приёмщик: *{receiver_name}*\n\n"
        "📅 Выберите дату прихода:",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

    return STATE_RECEIPT_DATE


async def date_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработка выбора даты.
    """
    query = update.callback_query
    await query.answer()

    data = query.data.split(':')[1]

    if data == 'custom':
        await query.edit_message_text(
            "📅 Введите дату в формате ДД.ММ.ГГГГ\n"
            "Например: 05.02.2026"
        )
        return STATE_RECEIPT_DATE

    context.user_data['receipt']['receipt_date'] = data

    # Переходим к выбору товара
    return await show_product_selection(query, context)


async def custom_date_entered(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработка ввода произвольной даты.
    Запрещены будущие даты — только сегодня или раньше.
    """
    date_str = update.message.text.strip()

    try:
        # Парсим дату в формате ДД.ММ.ГГГГ
        parsed_date = datetime.strptime(date_str, '%d.%m.%Y')

        # Проверяем что дата не в будущем
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        if parsed_date > today:
            await update.message.reply_text(
                "❌ Нельзя указывать будущую дату.\n"
                "Введите сегодняшнюю или прошедшую дату:"
            )
            return STATE_RECEIPT_DATE

        context.user_data['receipt']['receipt_date'] = parsed_date.strftime('%Y-%m-%d')
    except ValueError:
        await update.message.reply_text(
            "❌ Неверный формат даты.\n"
            "Введите в формате ДД.ММ.ГГГГ (например: 05.02.2026):"
        )
        return STATE_RECEIPT_DATE

    # Переходим к выбору товара
    return await show_product_selection(update, context, is_message=True)


async def show_product_selection(update_or_query, context: ContextTypes.DEFAULT_TYPE, is_message: bool = False, page: int = 0) -> int:
    """
    Показать список товаров для выбора с пагинацией.

    Аргументы:
        page: Номер страницы (0, 1, 2...)
    """
    PAGE_SIZE = 8  # Товаров на странице

    products = get_products()

    if not products:
        text = "❌ Не удалось загрузить список товаров.\nПопробуйте позже."
        if is_message:
            await update_or_query.message.reply_text(text)
        else:
            await update_or_query.edit_message_text(text)
        return ConversationHandler.END

    # Сохраняем текущую страницу
    context.user_data['product_page'] = page

    # Вычисляем срез для текущей страницы
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_products = products[start:end]

    # Создаём кнопки с товарами (показываем артикул)
    keyboard = []
    for product in page_products:
        offer_id = product['offer_id'][:40] + '...' if len(product['offer_id']) > 40 else product['offer_id']
        keyboard.append([
            InlineKeyboardButton(offer_id, callback_data=f"product:{product['sku']}")
        ])

    # Кнопки навигации
    nav_buttons = []

    # Кнопка "Назад" если не первая страница
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"page:{page - 1}"))

    # Кнопка "Ещё" если есть следующие товары
    if end < len(products):
        remaining = len(products) - end
        nav_buttons.append(InlineKeyboardButton(f"➡️ Ещё {remaining}", callback_data=f"page:{page + 1}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    # Кнопка поиска
    keyboard.append([
        InlineKeyboardButton("🔍 Поиск по названию/SKU", callback_data="product:search")
    ])

    reply_markup = InlineKeyboardMarkup(keyboard)

    receipt = context.user_data['receipt']
    items_text = ""
    if receipt['items']:
        items_text = f"\n\n📋 *В документе:*\n{format_product_list(receipt['items'])}"

    # Показываем номер страницы
    total_pages = (len(products) + PAGE_SIZE - 1) // PAGE_SIZE
    page_info = f" (стр. {page + 1}/{total_pages})" if total_pages > 1 else ""

    text = f"📦 *Выберите товар{page_info}:*{items_text}"

    if is_message:
        await update_or_query.message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    else:
        await update_or_query.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)

    return STATE_SELECT_PRODUCT


async def page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработка пагинации — переход на другую страницу списка товаров.
    """
    query = update.callback_query
    await query.answer()

    # Извлекаем номер страницы из callback_data (например, "page:2")
    page = int(query.data.split(':')[1])

    # Показываем товары на выбранной странице
    return await show_product_selection(query, context, is_message=False, page=page)


async def product_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработка выбора товара из кнопок.
    """
    query = update.callback_query
    await query.answer()

    data = query.data.split(':')[1]

    if data == 'search':
        await query.edit_message_text(
            "🔍 Введите SKU или часть названия товара:"
        )
        return STATE_SELECT_PRODUCT

    # Выбран конкретный товар
    sku = int(data)
    products = get_products(str(sku))

    if not products:
        await query.edit_message_text("❌ Товар не найден. Попробуйте ещё раз.")
        return await show_product_selection(query, context)

    product = products[0]
    context.user_data['current_product'] = product

    await query.edit_message_text(
        f"✅ *{escape_markdown(product['offer_id'])}*\n"
        f"SKU: `{product['sku']}`\n\n"
        "📊 Введите количество (шт.):",
        parse_mode='Markdown'
    )

    return STATE_ENTER_QUANTITY


async def product_search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Поиск товара по тексту.
    """
    search_text = update.message.text.strip()

    if not search_text:
        await update.message.reply_text("🔍 Введите SKU или часть названия:")
        return STATE_SELECT_PRODUCT

    products = get_products(search_text)

    if not products:
        await update.message.reply_text(
            f"❌ По запросу \"{search_text}\" ничего не найдено.\n"
            "Попробуйте другой запрос:"
        )
        return STATE_SELECT_PRODUCT

    if len(products) == 1:
        # Единственный результат — сразу выбираем
        product = products[0]
        context.user_data['current_product'] = product

        await update.message.reply_text(
            f"✅ *{escape_markdown(product['offer_id'])}*\n"
            f"SKU: `{product['sku']}`\n\n"
            "📊 Введите количество (шт.):",
            parse_mode='Markdown'
        )
        return STATE_ENTER_QUANTITY

    # Несколько результатов — показываем кнопки (артикулы)
    keyboard = []
    for product in products[:10]:
        offer_id = product['offer_id'][:40] + '...' if len(product['offer_id']) > 40 else product['offer_id']
        keyboard.append([
            InlineKeyboardButton(offer_id, callback_data=f"product:{product['sku']}")
        ])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"🔍 Найдено {len(products)} товаров:",
        reply_markup=reply_markup
    )

    return STATE_SELECT_PRODUCT


async def quantity_entered(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Получено количество товара.
    """
    try:
        quantity = int(update.message.text.strip())
        if quantity <= 0:
            raise ValueError("Количество должно быть положительным")
    except ValueError:
        await update.message.reply_text(
            "❌ Введите целое положительное число:"
        )
        return STATE_ENTER_QUANTITY

    product = context.user_data.get('current_product')
    if not product:
        await update.message.reply_text("❌ Ошибка. Начните заново: /приход")
        return ConversationHandler.END

    # Добавляем товар в список
    context.user_data['receipt']['items'].append({
        'sku': product['sku'],
        'offer_id': product['offer_id'],
        'quantity': quantity
    })

    # Спрашиваем, добавить ещё?
    keyboard = [
        [
            InlineKeyboardButton("➕ Добавить ещё товар", callback_data="more:yes"),
            InlineKeyboardButton("✅ Завершить", callback_data="more:no")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    items = context.user_data['receipt']['items']

    await update.message.reply_text(
        f"✅ Добавлено: *{escape_markdown(product['offer_id'])}* × {quantity} шт.\n\n"
        f"────────────────────────\n"
        f"📋 *В документе:*\n"
        f"{format_product_list(items)}\n"
        f"────────────────────────",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

    return STATE_MORE_PRODUCTS


async def more_products_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработка решения: добавить ещё товар или завершить.
    """
    query = update.callback_query
    await query.answer()

    data = query.data.split(':')[1]

    if data == 'yes':
        # Добавить ещё товар
        return await show_product_selection(query, context)

    # Завершить — запросить комментарий
    keyboard = [
        [InlineKeyboardButton("⏭ Пропустить", callback_data="comment:skip")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        "💬 Комментарий к приходу (или нажмите Пропустить):",
        reply_markup=reply_markup
    )

    return STATE_COMMENT


async def comment_entered(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Получен комментарий. Показываем подтверждение.
    """
    comment = update.message.text.strip()
    context.user_data['receipt']['comment'] = comment

    return await show_confirmation(update, context, is_message=True)


async def comment_skipped(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Комментарий пропущен.
    """
    query = update.callback_query
    await query.answer()

    context.user_data['receipt']['comment'] = ''

    return await show_confirmation(query, context)


async def show_confirmation(update_or_query, context: ContextTypes.DEFAULT_TYPE, is_message: bool = False) -> int:
    """
    Показать итоговое подтверждение.
    """
    receipt = context.user_data['receipt']

    # Форматируем дату
    try:
        date_obj = datetime.strptime(receipt['receipt_date'], '%Y-%m-%d')
        date_str = date_obj.strftime('%d.%m.%Y')
    except:
        date_str = receipt['receipt_date']

    text = (
        "────────────────────────\n"
        "📋 *ПРОВЕРЬТЕ ДОКУМЕНТ:*\n"
        "────────────────────────\n\n"
        f"👤 Приёмщик: *{receipt['receiver_name']}*\n"
        f"📅 Дата: *{date_str}*\n\n"
        f"📦 *Товары:*\n"
        f"{format_product_list(receipt['items'])}\n"
    )

    if receipt['comment']:
        text += f"\n💬 _{receipt['comment']}_\n"

    text += "\n────────────────────────"

    keyboard = [
        [
            InlineKeyboardButton("✅ Создать", callback_data="confirm:yes"),
            InlineKeyboardButton("❌ Отменить", callback_data="confirm:no")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if is_message:
        await update_or_query.message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    else:
        await update_or_query.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)

    return STATE_CONFIRM


async def confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработка подтверждения создания документа.
    """
    query = update.callback_query
    await query.answer()

    data = query.data.split(':')[1]

    if data == 'no':
        await query.edit_message_text("❌ Приход отменён.")
        return ConversationHandler.END

    # Создаём документ
    receipt = context.user_data['receipt']

    # Преобразуем items для API
    api_items = [{'sku': item['sku'], 'quantity': item['quantity']} for item in receipt['items']]

    result = create_receipt({
        'receipt_date': receipt['receipt_date'],
        'receiver_name': receipt['receiver_name'],
        'comment': receipt['comment'],
        'telegram_chat_id': receipt['telegram_chat_id'],
        'telegram_username': receipt['telegram_username'],
        'items': api_items
    })

    if result.get('success'):
        total_qty = sum(item['quantity'] for item in receipt['items'])

        await query.edit_message_text(
            "✅ *ПРИХОД ОФОРМЛЕН!*\n\n"
            f"📄 Документ #{result.get('doc_id')}\n"
            f"📦 Товаров: {len(receipt['items'])} поз. ({total_qty} шт.)\n\n"
            "⏳ Ожидает проверки в веб-интерфейсе",
            parse_mode='Markdown'
        )

        # Показываем меню для следующего действия
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Что делаем дальше? 👇",
            reply_markup=get_main_menu()
        )
    else:
        await query.edit_message_text(
            f"❌ Ошибка создания документа:\n{result.get('error', 'Неизвестная ошибка')}"
        )

        # Показываем меню даже при ошибке
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Попробуйте ещё раз 👇",
            reply_markup=get_main_menu()
        )

    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Отмена текущего диалога.
    """
    await update.message.reply_text(
        "❌ Операция отменена.\n\nВыберите действие 👇",
        reply_markup=get_main_menu()
    )
    return ConversationHandler.END


async def menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обработчик нажатий на кнопки главного меню.
    Кнопка "📦 Новый приход" обрабатывается в ConversationHandler.
    """
    text = update.message.text

    if text == "📊 Остатки":
        await update.message.reply_text(
            "🚧 Функция в разработке.\n\n"
            "Скоро здесь можно будет проверить остатки товаров.",
            reply_markup=get_main_menu()
        )

    elif text == "❓ Помощь":
        await help_command(update, context)


async def reply_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обработчик ответов (reply) на сообщения от бота.
    Когда пользователь отвечает на сообщение администратора — отправляем ответ на сервер.
    """
    message = update.message

    # Проверяем, что это ответ на сообщение
    if not message.reply_to_message:
        return

    # Проверяем, что отвечают на сообщение от бота (не от самого пользователя)
    if message.reply_to_message.from_user.id != context.bot.id:
        return

    chat_id = message.chat_id
    reply_text = message.text or ''
    reply_to_message_id = message.reply_to_message.message_id

    # Получаем имя отправителя
    user = message.from_user
    sender_name = user.username or user.first_name or str(chat_id)
    if user.username:
        sender_name = f"@{user.username}"

    # Отправляем ответ на сервер
    result = send_reply_to_server(chat_id, reply_text, reply_to_message_id, sender_name)

    if result.get('success'):
        await message.reply_text(
            "✅ Ваш ответ отправлен!",
            reply_markup=get_main_menu()
        )
    else:
        logger.error(f"Ошибка отправки ответа: {result.get('error')}")
        # Не показываем ошибку пользователю, чтобы не путать


# ============================================================================
# ОБРАБОТЧИК КНОПКИ "ОТВЕТИТЬ" ПОД СООБЩЕНИЯМИ
# ============================================================================

async def reply_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработчик нажатия на кнопку "💬 Ответить" под сообщением.
    Сохраняет информацию о документе и запрашивает текст ответа.
    """
    query = update.callback_query
    await query.answer()

    # Парсим callback_data: reply_msg:doc_type:doc_id
    parts = query.data.split(':')
    if len(parts) != 3:
        await query.message.reply_text("❌ Ошибка: неверный формат данных")
        return ConversationHandler.END

    doc_type = parts[1]
    doc_id = int(parts[2])

    # Сохраняем в context.user_data для последующей обработки
    context.user_data['pending_reply'] = {
        'doc_type': doc_type,
        'doc_id': doc_id,
        'original_message_id': query.message.message_id
    }

    # Создаём клавиатуру с кнопкой отмены
    keyboard = ReplyKeyboardMarkup(
        [['❌ Отменить ответ']],
        resize_keyboard=True,
        one_time_keyboard=True
    )

    await query.message.reply_text(
        f"💬 *Ответ на документ \\#{doc_id}*\n\n"
        "Напишите ваш ответ\\.\n"
        "Или нажмите «❌ Отменить ответ» для отмены\\.",
        parse_mode='MarkdownV2',
        reply_markup=keyboard
    )

    return STATE_WAITING_REPLY


async def receive_reply_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Получает текст ответа от пользователя и отправляет его на сервер.
    """
    message = update.message
    text = message.text.strip()

    # Проверка на отмену
    if text == '❌ Отменить ответ':
        context.user_data.pop('pending_reply', None)
        await message.reply_text(
            "↩️ Ответ отменён.",
            reply_markup=get_main_menu()
        )
        return ConversationHandler.END

    # Проверяем, есть ли данные о документе
    pending = context.user_data.get('pending_reply')
    if not pending:
        await message.reply_text(
            "❌ Ошибка: нет данных о документе. Попробуйте ещё раз нажать кнопку «Ответить».",
            reply_markup=get_main_menu()
        )
        return ConversationHandler.END

    doc_type = pending['doc_type']
    doc_id = pending['doc_id']

    # Получаем имя отправителя
    user = message.from_user
    sender_name = user.username or user.first_name or str(message.chat_id)
    if user.username:
        sender_name = f"@{user.username}"

    # Отправляем ответ на сервер
    result = send_reply_to_document(
        chat_id=message.chat_id,
        doc_type=doc_type,
        doc_id=doc_id,
        message=text,
        sender_name=sender_name
    )

    # Очищаем pending_reply
    context.user_data.pop('pending_reply', None)

    if result.get('success'):
        await message.reply_text(
            f"✅ Ваш ответ на документ #{doc_id} отправлен!",
            reply_markup=get_main_menu()
        )
    else:
        error = result.get('error', 'Неизвестная ошибка')
        logger.error(f"Ошибка отправки ответа: {error}")
        await message.reply_text(
            f"❌ Ошибка отправки ответа: {error}",
            reply_markup=get_main_menu()
        )

    return ConversationHandler.END


# ============================================================================
# ОТВЕТЫ НА СООБЩЕНИЯ КОНТЕЙНЕРОВ ВЭД
# ============================================================================

# Состояние для ответа на контейнер
STATE_CONTAINER_REPLY = 100

async def container_reply_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработчик нажатия на кнопку "💬 Ответить" под сообщением о контейнере.
    callback_data формат: reply_container:container_id:message_id
    """
    query = update.callback_query
    await query.answer()

    # Парсим callback_data
    parts = query.data.split(':')
    if len(parts) != 3:
        await query.message.reply_text("❌ Ошибка: неверный формат данных")
        return ConversationHandler.END

    container_id = int(parts[1])
    message_id = int(parts[2])

    # Сохраняем в context.user_data для последующей обработки
    context.user_data['pending_container_reply'] = {
        'container_id': container_id,
        'message_id': message_id,
        'original_message_id': query.message.message_id
    }

    # Создаём клавиатуру с кнопкой отмены
    keyboard = ReplyKeyboardMarkup(
        [['❌ Отменить ответ']],
        resize_keyboard=True,
        one_time_keyboard=True
    )

    await query.message.reply_text(
        f"📦 *Ответ на контейнер \\#{container_id}*\n\n"
        "Напишите ваш ответ, отправьте фото или файл\\.\n"
        "Или нажмите «❌ Отменить ответ» для отмены\\.",
        parse_mode='MarkdownV2',
        reply_markup=keyboard
    )

    return STATE_CONTAINER_REPLY


async def receive_container_reply_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Получает текст ответа на контейнер и отправляет его на сервер.
    """
    message = update.message
    text = message.text.strip()

    # Проверка на отмену
    if text == '❌ Отменить ответ':
        context.user_data.pop('pending_container_reply', None)
        await message.reply_text(
            "↩️ Ответ отменён.",
            reply_markup=get_main_menu()
        )
        return ConversationHandler.END

    # Проверяем, есть ли данные о контейнере
    pending = context.user_data.get('pending_container_reply')
    if not pending:
        await message.reply_text(
            "❌ Ошибка: нет данных о контейнере. Попробуйте ещё раз нажать кнопку «Ответить».",
            reply_markup=get_main_menu()
        )
        return ConversationHandler.END

    container_id = pending['container_id']

    # Получаем имя отправителя
    user = message.from_user
    sender_name = user.username or user.first_name or str(message.chat_id)
    if user.username:
        sender_name = f"@{user.username}"

    # Отправляем ответ на сервер
    result = send_container_reply(
        chat_id=message.chat_id,
        container_id=container_id,
        message=text,
        sender_name=sender_name
    )

    # Очищаем pending_container_reply
    context.user_data.pop('pending_container_reply', None)

    if result.get('success'):
        await message.reply_text(
            f"✅ Ваш ответ по контейнеру #{container_id} отправлен!",
            reply_markup=get_main_menu()
        )
    else:
        error = result.get('error', 'Неизвестная ошибка')
        logger.error(f"Ошибка отправки ответа на контейнер: {error}")
        await message.reply_text(
            f"❌ Ошибка отправки ответа: {error}",
            reply_markup=get_main_menu()
        )

    return ConversationHandler.END


def send_container_reply(chat_id: int, container_id: int, message: str, sender_name: str) -> dict:
    """
    Отправляет ответ на сообщение контейнера через API (только текст).
    """
    try:
        response = requests.post(
            f"{API_BASE_URL}/api/container-messages/receive",
            json={
                'token': TELEGRAM_BOT_SECRET,
                'container_id': container_id,
                'chat_id': chat_id,
                'message': message,
                'sender_name': sender_name
            },
            timeout=10
        )
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Ошибка API (container reply): {e}")
        return {'success': False, 'error': str(e)}


def send_container_reply_with_file(chat_id: int, container_id: int, message: str,
                                    sender_name: str, file_data: bytes, filename: str) -> dict:
    """
    Отправляет ответ на сообщение контейнера с прикрепленным файлом через multipart API.
    """
    try:
        response = requests.post(
            f"{API_BASE_URL}/api/container-messages/receive",
            data={
                'token': TELEGRAM_BOT_SECRET,
                'container_id': container_id,
                'chat_id': chat_id,
                'message': message,
                'sender_name': sender_name
            },
            files={
                'files': (filename, file_data)
            },
            timeout=30
        )
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Ошибка API (container reply with file): {e}")
        return {'success': False, 'error': str(e)}


# ============================================================================
# API-ХЕЛПЕРЫ ДЛЯ ОТПРАВКИ СООБЩЕНИЙ В КОНТЕЙНЕР
# ============================================================================

def get_containers(page: int = 0, page_size: int = MSG_PAGE_SIZE) -> dict:
    """
    Получить список контейнеров ВЭД с сервера (пагинация).

    Аргументы:
        page: Номер страницы (0, 1, 2...)
        page_size: Количество контейнеров на странице

    Возвращает:
        {'containers': [...], 'total': N, 'page': 0, 'page_size': 6} или пустой dict при ошибке
    """
    try:
        response = requests.get(
            f'{API_BASE_URL}/api/telegram/containers',
            params={
                'token': TELEGRAM_BOT_SECRET,
                'page': page,
                'page_size': page_size
            },
            timeout=10
        )
        data = response.json()
        if data.get('success'):
            return data
        else:
            logger.error(f"Ошибка API containers: {data.get('error')}")
            return {}
    except Exception as e:
        logger.error(f"Ошибка получения контейнеров: {e}")
        return {}


def get_users_list(exclude_chat_id: int = None) -> list:
    """
    Получить список пользователей для выбора получателей сообщения.

    Аргументы:
        exclude_chat_id: Исключить пользователя с этим chat_id (сам отправитель)

    Возвращает:
        Список: [{'id': 1, 'username': 'admin', 'display_name': 'Иванов', ...}, ...]
    """
    try:
        params = {'token': TELEGRAM_BOT_SECRET}
        if exclude_chat_id:
            params['exclude_chat_id'] = exclude_chat_id
        response = requests.get(
            f'{API_BASE_URL}/api/telegram/users',
            params=params,
            timeout=10
        )
        data = response.json()
        if data.get('success'):
            return data.get('users', [])
        return []
    except Exception as e:
        logger.error(f"Ошибка получения пользователей: {e}")
        return []


def send_container_message_api(chat_id: int, container_id: int, recipient_ids: list,
                                message: str, sender_name: str,
                                file_data: bytes = None, filename: str = None) -> dict:
    """
    Отправить сообщение в контейнер через API (с поддержкой файлов).

    Аргументы:
        chat_id: Telegram chat_id отправителя
        container_id: ID контейнера
        recipient_ids: Список ID пользователей-получателей
        message: Текст сообщения
        sender_name: Имя отправителя (@username или имя)
        file_data: Байты файла (опционально)
        filename: Имя файла (опционально)

    Возвращает:
        {'success': True, 'message_id': N} или {'success': False, 'error': '...'}
    """
    try:
        if file_data and filename:
            # Multipart/form-data для файлов
            response = requests.post(
                f'{API_BASE_URL}/api/telegram/send-container-message',
                data={
                    'token': TELEGRAM_BOT_SECRET,
                    'chat_id': chat_id,
                    'container_id': container_id,
                    'recipient_ids': ','.join(map(str, recipient_ids)),
                    'message': message,
                    'sender_name': sender_name
                },
                files={
                    'files': (filename, file_data)
                },
                timeout=30
            )
        else:
            # JSON для текстовых сообщений
            response = requests.post(
                f'{API_BASE_URL}/api/telegram/send-container-message',
                json={
                    'token': TELEGRAM_BOT_SECRET,
                    'chat_id': chat_id,
                    'container_id': container_id,
                    'recipient_ids': recipient_ids,
                    'message': message,
                    'sender_name': sender_name
                },
                timeout=15
            )
        return response.json()
    except Exception as e:
        logger.error(f"Ошибка отправки сообщения контейнера: {e}")
        return {'success': False, 'error': str(e)}


async def receive_container_reply_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработчик файлов/фото в ответе на контейнер.
    Скачивает файл из Telegram и отправляет на сервер.
    """
    message = update.message

    # Проверяем, есть ли данные о контейнере
    pending = context.user_data.get('pending_container_reply')
    if not pending:
        await message.reply_text(
            "❌ Ошибка: нет данных о контейнере. Попробуйте ещё раз нажать кнопку «Ответить».",
            reply_markup=get_main_menu()
        )
        return ConversationHandler.END

    container_id = pending['container_id']

    # Получаем имя отправителя
    user = message.from_user
    sender_name = user.username or user.first_name or str(message.chat_id)
    if user.username:
        sender_name = f"@{user.username}"

    # Текст подписи (caption) если есть
    caption = message.caption or ''

    # Скачиваем файл из Telegram
    try:
        if message.photo:
            # Фото — берём самое большое разрешение (последний элемент)
            file_obj = await message.photo[-1].get_file()
            filename = f"photo_{message.photo[-1].file_unique_id}.jpg"
        elif message.document:
            file_obj = await message.document.get_file()
            filename = message.document.file_name or f"file_{message.document.file_unique_id}"
        else:
            await message.reply_text(
                "❌ Неподдерживаемый тип файла.",
                reply_markup=get_main_menu()
            )
            context.user_data.pop('pending_container_reply', None)
            return ConversationHandler.END

        # Скачиваем содержимое файла в память
        file_bytes = await file_obj.download_as_bytearray()

        # Отправляем на сервер через multipart API
        result = send_container_reply_with_file(
            chat_id=message.chat_id,
            container_id=container_id,
            message=caption,
            sender_name=sender_name,
            file_data=bytes(file_bytes),
            filename=filename
        )

        # Очищаем pending
        context.user_data.pop('pending_container_reply', None)

        if result.get('success'):
            file_label = '📷 фото' if message.photo else f'📄 {filename}'
            await message.reply_text(
                f"✅ {file_label} по контейнеру #{container_id} отправлен!",
                reply_markup=get_main_menu()
            )
        else:
            error = result.get('error', 'Неизвестная ошибка')
            logger.error(f"Ошибка отправки файла на контейнер: {error}")
            await message.reply_text(
                f"❌ Ошибка отправки: {error}",
                reply_markup=get_main_menu()
            )

    except Exception as e:
        logger.error(f"Ошибка скачивания/отправки файла: {e}")
        context.user_data.pop('pending_container_reply', None)
        await message.reply_text(
            f"❌ Ошибка обработки файла: {e}",
            reply_markup=get_main_menu()
        )

    return ConversationHandler.END


# ============================================================================
# ОТПРАВКА СООБЩЕНИЙ В КОНТЕЙНЕР (НОВЫЙ ФЛОУ)
# ============================================================================

async def send_message_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Начало флоу отправки сообщения. Показывает список контейнеров.
    Точка входа: кнопка "✉️ Сообщение" в главном меню.
    """
    chat_id = update.effective_chat.id
    if not is_authorized(chat_id):
        await update.message.reply_text("⛔ У вас нет доступа к этому боту.")
        return ConversationHandler.END

    # Инициализируем данные для флоу отправки сообщения
    context.user_data['msg_flow'] = {
        'container_id': None,
        'container_info': '',
        'selected_recipients': [],
        'all_users': [],
        'message_text': '',
        'file_data': None,
        'filename': None
    }

    return await show_container_selection(update, context, page=0, is_message=True)


async def show_container_selection(update_or_query, context, page=0, is_message=False):
    """
    Показать пагинированный список контейнеров для выбора.

    Каждый контейнер отображается как кнопка с информацией:
    📦 #45 | 15.01.25 | ABC Trading | 12шт ¥15,000

    Аргументы:
        update_or_query: Update (сообщение) или CallbackQuery (пагинация)
        context: контекст бота
        page: номер страницы (0-индекс)
        is_message: True если вызвано из текстового сообщения, False из callback
    """
    data = get_containers(page=page, page_size=MSG_PAGE_SIZE)

    if not data or not data.get('containers'):
        text = "📭 Нет контейнеров.\nСписок контейнеров пуст."
        if is_message:
            await update_or_query.message.reply_text(text, reply_markup=get_main_menu())
        else:
            await update_or_query.edit_message_text(text)
        return ConversationHandler.END

    containers = data['containers']
    total = data.get('total', len(containers))

    context.user_data['msg_container_page'] = page

    keyboard = []
    for c in containers:
        # Форматируем дату (YYYY-MM-DD → DD.MM.YY)
        try:
            from datetime import datetime as dt_cls
            parsed_date = dt_cls.strptime(c['container_date'], '%Y-%m-%d')
            date_str = parsed_date.strftime('%d.%m.%y')
        except Exception:
            date_str = c['container_date']

        # Форматируем сумму в юанях
        sum_cny = c.get('total_sum_cny', 0)
        if sum_cny >= 1000:
            sum_str = f"¥{sum_cny:,.0f}"
        else:
            sum_str = f"¥{sum_cny:.0f}"

        total_qty = c.get('total_qty', 0)
        supplier = c.get('supplier', '')
        # Обрезаем поставщика, если длинный
        if len(supplier) > 15:
            supplier = supplier[:12] + '...'

        # Иконка статуса: ✅ завершён, 📦 активный
        status_icon = "✅" if c.get('is_completed') else "📦"

        label = f"{status_icon} #{c['id']} | {date_str} | {supplier} | {total_qty}шт {sum_str}"

        keyboard.append([
            InlineKeyboardButton(label, callback_data=f"msgc:{c['id']}")
        ])

    # Кнопки навигации (паттерн из show_product_selection)
    nav_buttons = []
    total_pages = (total + MSG_PAGE_SIZE - 1) // MSG_PAGE_SIZE

    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"msgcp:{page - 1}"))

    if page + 1 < total_pages:
        remaining = total - (page + 1) * MSG_PAGE_SIZE
        nav_buttons.append(InlineKeyboardButton(f"➡️ Ещё ({remaining})", callback_data=f"msgcp:{page + 1}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    # Кнопка отмены
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="msgcancel")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    page_info = f" (стр. {page + 1}/{total_pages})" if total_pages > 1 else ""
    text = f"📦 *Выберите контейнер{page_info}:*"

    if is_message:
        await update_or_query.message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    else:
        await update_or_query.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)

    return STATE_MSG_CONTAINER_SELECT


async def msg_container_select_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработка выбора контейнера из списка. Переходит к выбору получателей.
    Callback data: msgc:{container_id}
    """
    query = update.callback_query
    await query.answer()

    container_id = int(query.data.split(':')[1])
    context.user_data['msg_flow']['container_id'] = container_id

    # Загружаем список пользователей для выбора получателей
    chat_id = update.effective_chat.id
    users = get_users_list(exclude_chat_id=chat_id)

    if not users:
        await query.edit_message_text(
            "❌ Нет пользователей с привязанным Telegram.\n"
            "Невозможно отправить сообщение.",
        )
        context.user_data.pop('msg_flow', None)
        return ConversationHandler.END

    context.user_data['msg_flow']['all_users'] = users
    context.user_data['msg_flow']['selected_recipients'] = []

    return await show_recipient_selection(query, context)


async def msg_container_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработка пагинации контейнеров (кнопки ⬅️ Назад / ➡️ Ещё).
    Callback data: msgcp:{page}
    """
    query = update.callback_query
    await query.answer()
    page = int(query.data.split(':')[1])
    return await show_container_selection(query, context, page=page, is_message=False)


# ============================================================================
# ВЫБОР ПОЛУЧАТЕЛЕЙ (МУЛЬТИВЫБОР)
# ============================================================================

async def show_recipient_selection(query, context):
    """
    Показать UI выбора получателей с галочками.

    Отображает:
    - Кнопка "Все" для быстрого выбора/снятия всех
    - Список пользователей по 2 в ряд с галочками ✓
    - Кнопка "Готово" (появляется когда хотя бы один выбран)
    - Кнопка "Назад" для возврата к контейнерам
    """
    flow = context.user_data['msg_flow']
    users = flow['all_users']
    selected = flow['selected_recipients']
    all_selected = len(selected) == len(users) and len(users) > 0

    keyboard = []

    # Кнопка "Все" — переключает выбор всех
    all_label = "✅ Все" if all_selected else "☐ Все"
    keyboard.append([InlineKeyboardButton(all_label, callback_data="msgrall")])

    # Пользователи по 2 в ряд
    row = []
    for user in users:
        is_selected = user['id'] in selected
        check = "✓" if is_selected else "  "
        name = user.get('display_name') or user.get('username', '?')
        # Обрезаем длинные имена для кнопок
        if len(name) > 18:
            name = name[:15] + "..."
        label = f"{check} {name}"
        row.append(InlineKeyboardButton(label, callback_data=f"msgr:{user['id']}"))

        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    # Кнопка "Готово" (только если хотя бы один получатель выбран)
    bottom_row = []
    if selected:
        bottom_row.append(InlineKeyboardButton("✅ Готово", callback_data="msgrdone"))

    # Кнопка "Назад к контейнерам"
    back_page = context.user_data.get('msg_container_page', 0)
    bottom_row.append(InlineKeyboardButton("⬅️ Контейнеры", callback_data=f"msgcp:{back_page}"))
    keyboard.append(bottom_row)

    # Кнопка отмены
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="msgcancel")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    count_text = f" ({len(selected)} выбрано)" if selected else ""
    container_id = flow['container_id']
    text = f"📦 Контейнер *#{container_id}*\n\n👥 *Выберите получателей{count_text}:*\nНажмите на имя для выбора/отмены"

    await query.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    return STATE_MSG_RECIPIENTS


async def msg_recipient_toggle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Переключить выбор одного получателя (toggle).
    Callback data: msgr:{user_id}
    """
    query = update.callback_query
    await query.answer()

    user_id = int(query.data.split(':')[1])
    selected = context.user_data['msg_flow']['selected_recipients']

    if user_id in selected:
        selected.remove(user_id)
    else:
        selected.append(user_id)

    return await show_recipient_selection(query, context)


async def msg_recipient_all_toggle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Переключить выбор всех получателей (выбрать всех / снять все).
    Callback data: msgrall
    """
    query = update.callback_query
    await query.answer()

    flow = context.user_data['msg_flow']
    users = flow['all_users']
    selected = flow['selected_recipients']

    if len(selected) == len(users):
        # Снять все
        flow['selected_recipients'] = []
    else:
        # Выбрать все
        flow['selected_recipients'] = [u['id'] for u in users]

    return await show_recipient_selection(query, context)


async def msg_recipient_done_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Получатели выбраны, переходим к вводу текста сообщения.
    Callback data: msgrdone
    """
    query = update.callback_query

    selected = context.user_data['msg_flow']['selected_recipients']
    if not selected:
        await query.answer("Выберите хотя бы одного получателя", show_alert=True)
        return STATE_MSG_RECIPIENTS

    await query.answer()

    # Формируем список имён для отображения
    users = context.user_data['msg_flow']['all_users']
    names = [u.get('display_name') or u.get('username') for u in users if u['id'] in selected]
    names_str = ", ".join(names)

    container_id = context.user_data['msg_flow']['container_id']

    keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="msgcancel")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        f"📦 Контейнер *#{container_id}*\n"
        f"👥 Получатели: {escape_md(names_str)}\n\n"
        "💬 *Введите текст сообщения:*\n"
        "Можно также отправить фото или документ",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

    return STATE_MSG_TEXT


# ============================================================================
# ВВОД ТЕКСТА / ФАЙЛА
# ============================================================================

async def msg_text_entered(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработка текстового сообщения. Сохраняет текст и переходит к подтверждению.
    """
    text = update.message.text.strip()

    if not text:
        await update.message.reply_text("❌ Сообщение не может быть пустым. Введите текст:")
        return STATE_MSG_TEXT

    context.user_data['msg_flow']['message_text'] = text
    context.user_data['msg_flow']['file_data'] = None
    context.user_data['msg_flow']['filename'] = None

    return await show_send_confirmation(update, context, is_message=True)


async def msg_file_entered(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработка фото/документа. Скачивает файл и переходит к подтверждению.
    Паттерн из receive_container_reply_file.
    """
    message = update.message

    try:
        if message.photo:
            # Фото — берём самое большое разрешение (последний элемент)
            file_obj = await message.photo[-1].get_file()
            filename = f"photo_{message.photo[-1].file_unique_id}.jpg"
        elif message.document:
            file_obj = await message.document.get_file()
            filename = message.document.file_name or f"file_{message.document.file_unique_id}"
        else:
            await message.reply_text("❌ Неподдерживаемый тип файла. Отправьте фото или документ.")
            return STATE_MSG_TEXT

        # Скачиваем файл в память
        file_bytes = await file_obj.download_as_bytearray()

        # Текст подписи (caption) как текст сообщения
        caption = message.caption or ''

        context.user_data['msg_flow']['message_text'] = caption
        context.user_data['msg_flow']['file_data'] = bytes(file_bytes)
        context.user_data['msg_flow']['filename'] = filename

        return await show_send_confirmation(update, context, is_message=True)

    except Exception as e:
        logger.error(f"Ошибка скачивания файла: {e}")
        await message.reply_text(f"❌ Ошибка обработки файла: {e}\nПопробуйте ещё раз.")
        return STATE_MSG_TEXT


# ============================================================================
# ПОДТВЕРЖДЕНИЕ И ОТПРАВКА
# ============================================================================

async def show_send_confirmation(update_or_msg, context, is_message=False):
    """
    Показать превью сообщения перед отправкой.

    Отображает: контейнер, получателей, текст, наличие файла.
    Кнопки: ✅ Отправить / ❌ Отменить
    """
    flow = context.user_data['msg_flow']
    container_id = flow['container_id']
    users = flow['all_users']
    selected = flow['selected_recipients']
    message_text = flow['message_text']
    has_file = flow.get('file_data') is not None

    names = [u.get('display_name') or u.get('username') for u in users if u['id'] in selected]
    names_str = ", ".join(names)

    text = (
        "────────────────────────\n"
        "📋 *ПРОВЕРЬТЕ СООБЩЕНИЕ:*\n"
        "────────────────────────\n\n"
        f"📦 Контейнер: *#{container_id}*\n"
        f"👥 Получатели: {escape_md(names_str)}\n\n"
    )

    if message_text:
        text += f"💬 {escape_md(message_text)}\n"
    if has_file:
        text += f"📎 Файл: {escape_md(flow['filename'])}\n"
    if not message_text and not has_file:
        text += "⚠️ Пустое сообщение\n"

    text += "\n────────────────────────"

    keyboard = [
        [
            InlineKeyboardButton("✅ Отправить", callback_data="msgconfirm"),
            InlineKeyboardButton("❌ Отменить", callback_data="msgcancel")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if is_message:
        await update_or_msg.message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    else:
        await update_or_msg.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)

    return STATE_MSG_CONFIRM


async def msg_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Подтверждение и отправка сообщения через API.
    Callback data: msgconfirm
    """
    query = update.callback_query
    await query.answer("Отправляю...")

    flow = context.user_data.get('msg_flow', {})
    if not flow.get('container_id'):
        await query.edit_message_text("❌ Ошибка: данные сообщения потеряны.")
        return ConversationHandler.END

    chat_id = update.effective_chat.id
    user = update.effective_user
    sender_name = f"@{user.username}" if user.username else user.first_name or str(chat_id)

    # Отправляем через API
    result = send_container_message_api(
        chat_id=chat_id,
        container_id=flow['container_id'],
        recipient_ids=flow['selected_recipients'],
        message=flow.get('message_text', ''),
        sender_name=sender_name,
        file_data=flow.get('file_data'),
        filename=flow.get('filename')
    )

    # Очищаем данные флоу
    context.user_data.pop('msg_flow', None)

    if result.get('success'):
        await query.edit_message_text(
            f"✅ *Сообщение отправлено!*\n\n"
            f"📦 Контейнер #{flow['container_id']}\n"
            f"👥 Получателей: {len(flow['selected_recipients'])}",
            parse_mode='Markdown'
        )
        # Отправляем главное меню отдельным сообщением
        await context.bot.send_message(
            chat_id=chat_id,
            text="Что делаем дальше? 👇",
            reply_markup=get_main_menu()
        )
    else:
        error = result.get('error', 'Неизвестная ошибка')
        await query.edit_message_text(f"❌ Ошибка отправки: {error}")
        await context.bot.send_message(
            chat_id=chat_id,
            text="Попробуйте ещё раз 👇",
            reply_markup=get_main_menu()
        )

    return ConversationHandler.END


async def msg_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Отмена флоу отправки сообщения из любого шага (через inline-кнопку).
    Callback data: msgcancel
    """
    query = update.callback_query
    await query.answer()

    # Очищаем данные флоу
    context.user_data.pop('msg_flow', None)

    await query.edit_message_text("↩️ Отправка сообщения отменена.")

    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="Выберите действие 👇",
        reply_markup=get_main_menu()
    )
    return ConversationHandler.END


# ============================================================================
# ФИНАНСЫ — ДИАЛОГ ДОБАВЛЕНИЯ ДОХОДОВ/РАСХОДОВ
# ============================================================================
# Пошаговый диалог: Тип → Сумма → Счёт → Описание → Подтверждение.
# Данные сохраняются в context.user_data['finance'] и отправляются на API.
# ============================================================================


async def finance_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Начало диалога финансов.
    Показывает выбор типа записи: Расход или Доход.
    """
    chat_id = update.effective_chat.id
    if not is_authorized(chat_id):
        await update.message.reply_text("⛔ У вас нет доступа к этому боту.")
        return ConversationHandler.END

    # Инициализируем данные финансовой записи
    user = update.effective_user
    display_name = user.first_name or user.username or str(chat_id)
    context.user_data['finance'] = {
        'record_type': None,
        'amount': None,
        'account_id': None,
        'account_name': None,
        'category_id': None,
        'category_name': None,
        'description': None,
        'yuan_amount': None,
        'requires_yuan': 0,
        'requires_description': 0,
        'description_hint': '',
        'files': [],
        'telegram_chat_id': chat_id,
        'telegram_username': display_name
    }

    keyboard = [
        [
            InlineKeyboardButton("📉 Расход", callback_data="fin_type:expense"),
            InlineKeyboardButton("📈 Доход", callback_data="fin_type:income")
        ]
    ]
    await update.message.reply_text(
        "💰 *ФИНАНСЫ*\n\nВыберите тип записи:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return STATE_FIN_TYPE


async def finance_type_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработка выбора типа (расход/доход).
    Запрашивает ввод суммы.
    """
    query = update.callback_query
    await query.answer()

    record_type = query.data.split(':')[1]  # 'expense' или 'income'
    context.user_data['finance']['record_type'] = record_type

    type_label = "📉 РАСХОД" if record_type == 'expense' else "📈 ДОХОД"
    await query.edit_message_text(
        f"💰 *{type_label}*\n\n"
        "💵 Введите сумму (в рублях):",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Назад", callback_data="fin_back_type")]
        ])
    )
    return STATE_FIN_AMOUNT


async def finance_amount_entered(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработка введённой суммы.
    Валидирует число, загружает список счетов и показывает выбор.
    """
    text = update.message.text.strip().replace(',', '.').replace(' ', '')
    try:
        amount = float(text)
        if amount <= 0:
            raise ValueError("Сумма должна быть больше 0")
    except ValueError:
        await update.message.reply_text(
            "❌ Введите корректную сумму (число больше 0).\n"
            "Примеры: 5000, 15000.50, 1500"
        )
        return STATE_FIN_AMOUNT

    context.user_data['finance']['amount'] = amount

    # Загружаем список счетов с сервера
    accounts = get_finance_accounts()
    if not accounts:
        await update.message.reply_text(
            "❌ Не удалось загрузить список счетов. Попробуйте позже.",
            reply_markup=get_main_menu()
        )
        return ConversationHandler.END

    # Формируем inline-кнопки со счетами (по 2 в ряд)
    keyboard = []
    row = []
    for acc in accounts:
        # Ограничиваем длину callback_data: fin_acc:id:name (до 64 байт)
        acc_name = acc['name'][:30]
        row.append(InlineKeyboardButton(
            acc['name'],
            callback_data=f"fin_acc:{acc['id']}:{acc_name}"
        ))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="fin_back_amount")])

    formatted = format_amount(amount)
    fin = context.user_data['finance']
    type_label = "📉 Расход" if fin['record_type'] == 'expense' else "📈 Доход"

    await update.message.reply_text(
        f"💰 *{escape_md(type_label)}*\n"
        f"💵 Сумма: *{escape_md(formatted)} ₽*\n\n"
        "🏦 Выберите счёт / источник:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return STATE_FIN_ACCOUNT


async def finance_account_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработка выбора счёта.
    Загружает категории и показывает выбор.
    """
    query = update.callback_query
    await query.answer()

    # Парсим callback: fin_acc:id:name
    parts = query.data.split(':', 2)
    account_id = int(parts[1])
    account_name = parts[2]

    context.user_data['finance']['account_id'] = account_id
    context.user_data['finance']['account_name'] = account_name

    # Загружаем категории только для выбранного типа (расход/доход)
    fin_type = context.user_data['finance'].get('record_type', 'expense')
    categories = get_finance_categories(record_type=fin_type)
    if not categories:
        await query.edit_message_text(
            "❌ Нет доступных категорий.\n\n"
            "Сначала создайте категории в веб\\-интерфейсе "
            "\\(вкладка Финансы → 🏷 Управление категориями\\)\\.",
            parse_mode='MarkdownV2'
        )
        await query.message.reply_text(
            "Выберите действие 👇",
            reply_markup=get_main_menu()
        )
        return ConversationHandler.END

    # Сохраняем кеш категорий для получения description_hint при выборе
    context.user_data['finance']['categories_cache'] = categories

    # Формируем inline-кнопки с категориями (по 2 в ряд)
    keyboard = []
    row = []
    for cat in categories:
        cat_name = cat['name'][:25]
        linked = cat.get('is_container_linked', 0) or 0
        yuan = cat.get('requires_yuan', 0) or 0
        desc_req = cat.get('requires_description', 0) or 0
        row.append(InlineKeyboardButton(
            cat['name'],
            callback_data=f"fin_cat:{cat['id']}:{cat_name}:{linked}:{yuan}:{desc_req}"
        ))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="fin_back_account")])

    fin = context.user_data['finance']
    type_label = "📉 Расход" if fin['record_type'] == 'expense' else "📈 Доход"
    formatted = format_amount(fin['amount'])

    await query.edit_message_text(
        f"💰 *{escape_md(type_label)}*\n"
        f"💵 Сумма: *{escape_md(formatted)} ₽*\n"
        f"🏦 Счёт: *{escape_md(account_name)}*\n\n"
        "🏷 Выберите категорию:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return STATE_FIN_CATEGORY


async def finance_category_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработка выбора категории.
    Если категория требует юани — запрашивает сумму в юанях.
    Иначе — запрашивает описание (на что потрачено / за что получено).
    """
    query = update.callback_query
    await query.answer()

    # Парсим callback: fin_cat:id:name:is_container_linked:requires_yuan:requires_description
    parts = query.data.split(':', 5)
    category_id = int(parts[1])
    category_name = parts[2] if len(parts) > 2 else ''
    is_container_linked = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
    requires_yuan = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
    requires_description = int(parts[5]) if len(parts) > 5 and parts[5].isdigit() else 0

    context.user_data['finance']['category_id'] = category_id
    context.user_data['finance']['category_name'] = category_name
    context.user_data['finance']['is_container_linked'] = is_container_linked
    context.user_data['finance']['requires_yuan'] = requires_yuan
    context.user_data['finance']['requires_description'] = requires_description

    # Получаем description_hint из кеша категорий
    categories_cache = context.user_data['finance'].get('categories_cache', [])
    desc_hint = ''
    for cached_cat in categories_cache:
        if cached_cat.get('id') == category_id:
            desc_hint = cached_cat.get('description_hint', '')
            break
    context.user_data['finance']['description_hint'] = desc_hint

    fin = context.user_data['finance']
    type_label = "📉 Расход" if fin['record_type'] == 'expense' else "📈 Доход"
    formatted = format_amount(fin['amount'])

    # Если категория требует юани — сначала запрашиваем сумму в юанях
    if requires_yuan:
        await query.edit_message_text(
            f"💰 *{escape_md(type_label)}*\n"
            f"💵 Сумма: *{escape_md(formatted)} ₽*\n"
            f"🏦 Счёт: *{escape_md(fin['account_name'])}*\n"
            f"🏷 Категория: *{escape_md(category_name)}*\n\n"
            "💴 Введите сумму в юанях (¥):",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="fin_back_category")]
            ])
        )
        return STATE_FIN_YUAN_AMOUNT

    # Комментарий обязателен при requires_description или "Другое"
    is_other = category_name.lower() == 'другое'
    back_btn = [InlineKeyboardButton("⬅️ Назад", callback_data="fin_back_category")]
    if requires_description:
        hint_text = desc_hint if desc_hint else 'Описание обязательно для данной категории'
        comment_prompt = (
            f"📝 *Описание обязательно!*\n\n"
            f"{hint_text}\n\n"
            "Введите описание или отправьте фото/документ:"
        )
        reply_markup = InlineKeyboardMarkup([back_btn])
    elif is_other:
        comment_prompt = "📝 Введите комментарий (обязательно при категории «Другое»):"
        reply_markup = InlineKeyboardMarkup([back_btn])
    else:
        comment_prompt = "📝 Введите комментарий или нажмите «Пропустить»:"
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("⏩ Пропустить", callback_data="fin_skip_comment")],
            back_btn
        ])

    await query.edit_message_text(
        f"💰 *{escape_md(type_label)}*\n"
        f"💵 Сумма: *{escape_md(formatted)} ₽*\n"
        f"🏦 Счёт: *{escape_md(fin['account_name'])}*\n"
        f"🏷 Категория: *{escape_md(category_name)}*\n\n"
        f"{comment_prompt}",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )
    return STATE_FIN_DESCRIPTION


async def finance_yuan_amount_entered(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработка введённой суммы в юанях.
    Валидирует число, сохраняет и переходит к вводу комментария.
    """
    text = update.message.text.strip().replace(',', '.').replace(' ', '')
    try:
        yuan_amount = float(text)
        if yuan_amount <= 0:
            raise ValueError("Сумма должна быть больше 0")
    except ValueError:
        await update.message.reply_text(
            "❌ Введите корректную сумму в юанях (число больше 0).\n"
            "Примеры: 5000, 15000.50, 1500"
        )
        return STATE_FIN_YUAN_AMOUNT

    context.user_data['finance']['yuan_amount'] = yuan_amount

    fin = context.user_data['finance']
    type_label = "📉 Расход" if fin['record_type'] == 'expense' else "📈 Доход"
    formatted = format_amount(fin['amount'])
    yuan_formatted = format_amount(yuan_amount)
    category_name = fin.get('category_name', '')
    description_hint = fin.get('description_hint', '')
    requires_description = fin.get('requires_description', 0)
    is_other = category_name.lower() == 'другое'

    back_btn = [InlineKeyboardButton("⬅️ Назад", callback_data="fin_back_yuan")]
    if requires_description:
        hint_text = description_hint if description_hint else 'Описание обязательно для данной категории'
        comment_prompt = (
            f"📝 *Описание обязательно!*\n\n"
            f"{hint_text}\n\n"
            "Введите описание или отправьте фото/документ:"
        )
        reply_markup = InlineKeyboardMarkup([back_btn])
    elif is_other:
        comment_prompt = "📝 Введите комментарий (обязательно при категории «Другое»):"
        reply_markup = InlineKeyboardMarkup([back_btn])
    else:
        comment_prompt = "📝 Введите комментарий или нажмите «Пропустить»:"
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("⏩ Пропустить", callback_data="fin_skip_comment")],
            back_btn
        ])

    await update.message.reply_text(
        f"💰 *{escape_md(type_label)}*\n"
        f"💵 Сумма: *{escape_md(formatted)} ₽*\n"
        f"💴 Юани: *{escape_md(yuan_formatted)} ¥*\n"
        f"🏦 Счёт: *{escape_md(fin['account_name'])}*\n"
        f"🏷 Категория: *{escape_md(category_name)}*\n\n"
        f"{comment_prompt}",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )
    return STATE_FIN_DESCRIPTION


async def finance_back_to_yuan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Возврат к вводу суммы в юанях.
    Вызывается из шага DESCRIPTION при нажатии «⬅️ Назад» (если категория требует юани).
    """
    query = update.callback_query
    await query.answer()

    fin = context.user_data['finance']
    type_label = "📉 Расход" if fin['record_type'] == 'expense' else "📈 Доход"
    formatted = format_amount(fin['amount'])

    await query.edit_message_text(
        f"💰 *{escape_md(type_label)}*\n"
        f"💵 Сумма: *{escape_md(formatted)} ₽*\n"
        f"🏦 Счёт: *{escape_md(fin['account_name'])}*\n"
        f"🏷 Категория: *{escape_md(fin.get('category_name', ''))}*\n\n"
        "💴 Введите сумму в юанях (¥):",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Назад", callback_data="fin_back_category")]
        ])
    )
    return STATE_FIN_YUAN_AMOUNT


async def finance_skip_comment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Пропуск комментария (доступно только если категория НЕ "Другое" и НЕ requires_description).
    """
    query = update.callback_query

    # Блокируем пропуск для категорий с обязательным описанием
    fin = context.user_data.get('finance', {})
    if fin.get('requires_description'):
        await query.answer("Описание обязательно для данной категории", show_alert=True)
        return STATE_FIN_DESCRIPTION

    await query.answer()

    context.user_data['finance']['description'] = ''
    fin = context.user_data['finance']

    type_label = "📉 Расход" if fin['record_type'] == 'expense' else "📈 Доход"
    formatted = format_amount(fin['amount'])

    keyboard = [
        [
            InlineKeyboardButton("✅ Подтвердить", callback_data="fin_confirm:yes"),
            InlineKeyboardButton("❌ Отменить", callback_data="fin_confirm:no")
        ],
        [InlineKeyboardButton("⬅️ Назад", callback_data="fin_back_description")]
    ]

    category_line = ""
    if fin.get('category_name'):
        category_line = f"Категория: *{escape_md(fin['category_name'])}*\n"

    yuan_line = ""
    if fin.get('yuan_amount'):
        yuan_line = f"Юани: *{escape_md(format_amount(fin['yuan_amount']))} ¥*\n"

    files_count = len(fin.get('files', []))
    files_line = f"📎 Файлов: {files_count}\n" if files_count else ''

    await query.edit_message_text(
        f"📋 *ПОДТВЕРЖДЕНИЕ*\n\n"
        f"Тип: {escape_md(type_label)}\n"
        f"Сумма: *{escape_md(formatted)} ₽*\n"
        f"{yuan_line}"
        f"Счёт: *{escape_md(fin['account_name'])}*\n"
        f"{category_line}"
        f"{files_line}"
        "Всё верно?",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return STATE_FIN_CONFIRM


async def finance_description_entered(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработка введённого комментария.
    Показывает итоговую сводку для подтверждения.
    """
    description = update.message.text.strip()
    fin = context.user_data['finance']
    is_other = (fin.get('category_name') or '').lower() == 'другое'
    requires_description = fin.get('requires_description', 0)

    # Валидация обязательного описания
    if requires_description and not description:
        await update.message.reply_text("❌ Описание обязательно для этой категории.")
        return STATE_FIN_DESCRIPTION

    if is_other and not description:
        await update.message.reply_text(
            "❌ При категории «Другое» комментарий обязателен. Введите комментарий:"
        )
        return STATE_FIN_DESCRIPTION

    context.user_data['finance']['description'] = description
    fin = context.user_data['finance']

    type_label = "📉 Расход" if fin['record_type'] == 'expense' else "📈 Доход"
    formatted = format_amount(fin['amount'])

    keyboard = [
        [
            InlineKeyboardButton("✅ Подтвердить", callback_data="fin_confirm:yes"),
            InlineKeyboardButton("❌ Отменить", callback_data="fin_confirm:no")
        ],
        [InlineKeyboardButton("⬅️ Назад", callback_data="fin_back_description")]
    ]

    category_line = ""
    if fin.get('category_name'):
        category_line = f"Категория: *{escape_md(fin['category_name'])}*\n"

    yuan_line = ""
    if fin.get('yuan_amount'):
        yuan_line = f"Юани: *{escape_md(format_amount(fin['yuan_amount']))} ¥*\n"

    comment_line = ""
    if description:
        comment_line = f"Комментарий: {escape_md(description)}\n"

    files_count = len(fin.get('files', []))
    files_line = f"📎 Файлов: {files_count}\n" if files_count else ''

    await update.message.reply_text(
        f"📋 *ПОДТВЕРЖДЕНИЕ*\n\n"
        f"Тип: {escape_md(type_label)}\n"
        f"Сумма: *{escape_md(formatted)} ₽*\n"
        f"{yuan_line}"
        f"Счёт: *{escape_md(fin['account_name'])}*\n"
        f"{category_line}"
        f"{comment_line}"
        f"{files_line}\n"
        "Всё верно?",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return STATE_FIN_CONFIRM


# ============================================================================
# ОБРАБОТЧИКИ КНОПКИ «НАЗАД» В ФИНАНСОВОМ ПОТОКЕ
# ============================================================================


async def finance_back_to_type(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Возврат к выбору типа (расход/доход).
    Вызывается из шага AMOUNT при нажатии «⬅️ Назад».
    """
    query = update.callback_query
    await query.answer()

    keyboard = [
        [
            InlineKeyboardButton("📉 Расход", callback_data="fin_type:expense"),
            InlineKeyboardButton("📈 Доход", callback_data="fin_type:income")
        ]
    ]
    await query.edit_message_text(
        "💰 *ФИНАНСЫ*\n\nВыберите тип записи:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return STATE_FIN_TYPE


async def finance_back_to_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Возврат к вводу суммы.
    Вызывается из шага ACCOUNT при нажатии «⬅️ Назад».
    """
    query = update.callback_query
    await query.answer()

    fin = context.user_data['finance']
    type_label = "📉 РАСХОД" if fin['record_type'] == 'expense' else "📈 ДОХОД"
    await query.edit_message_text(
        f"💰 *{type_label}*\n\n"
        "💵 Введите сумму (в рублях):",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Назад", callback_data="fin_back_type")]
        ])
    )
    return STATE_FIN_AMOUNT


async def finance_back_to_account(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Возврат к выбору счёта.
    Вызывается из шага CATEGORY при нажатии «⬅️ Назад».
    """
    query = update.callback_query
    await query.answer()

    fin = context.user_data['finance']

    # Загружаем список счетов с сервера
    accounts = get_finance_accounts()
    if not accounts:
        await query.edit_message_text("❌ Не удалось загрузить список счетов.")
        return ConversationHandler.END

    # Формируем inline-кнопки со счетами (по 2 в ряд)
    keyboard = []
    row = []
    for acc in accounts:
        acc_name = acc['name'][:30]
        row.append(InlineKeyboardButton(
            acc['name'],
            callback_data=f"fin_acc:{acc['id']}:{acc_name}"
        ))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="fin_back_amount")])

    formatted = format_amount(fin['amount'])
    type_label = "📉 Расход" if fin['record_type'] == 'expense' else "📈 Доход"

    await query.edit_message_text(
        f"💰 *{escape_md(type_label)}*\n"
        f"💵 Сумма: *{escape_md(formatted)} ₽*\n\n"
        "🏦 Выберите счёт / источник:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return STATE_FIN_ACCOUNT


async def finance_back_to_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Возврат к выбору категории.
    Вызывается из шага DESCRIPTION при нажатии «⬅️ Назад».
    """
    query = update.callback_query
    await query.answer()

    fin = context.user_data['finance']

    # Загружаем категории для выбранного типа (расход/доход)
    fin_type = fin.get('record_type', 'expense')
    categories = get_finance_categories(record_type=fin_type)
    if not categories:
        await query.edit_message_text("❌ Нет доступных категорий.")
        return ConversationHandler.END

    # Сохраняем кеш категорий для получения description_hint при выборе
    context.user_data['finance']['categories_cache'] = categories

    # Формируем inline-кнопки с категориями (по 2 в ряд)
    keyboard = []
    row = []
    for cat in categories:
        cat_name = cat['name'][:25]
        linked = cat.get('is_container_linked', 0) or 0
        yuan = cat.get('requires_yuan', 0) or 0
        desc_req = cat.get('requires_description', 0) or 0
        row.append(InlineKeyboardButton(
            cat['name'],
            callback_data=f"fin_cat:{cat['id']}:{cat_name}:{linked}:{yuan}:{desc_req}"
        ))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="fin_back_account")])

    type_label = "📉 Расход" if fin['record_type'] == 'expense' else "📈 Доход"
    formatted = format_amount(fin['amount'])

    await query.edit_message_text(
        f"💰 *{escape_md(type_label)}*\n"
        f"💵 Сумма: *{escape_md(formatted)} ₽*\n"
        f"🏦 Счёт: *{escape_md(fin['account_name'])}*\n\n"
        "🏷 Выберите категорию:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return STATE_FIN_CATEGORY


async def finance_back_to_description(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Возврат к вводу комментария.
    Вызывается из шага CONFIRM при нажатии «⬅️ Назад».
    """
    query = update.callback_query
    await query.answer()

    fin = context.user_data['finance']
    type_label = "📉 Расход" if fin['record_type'] == 'expense' else "📈 Доход"
    formatted = format_amount(fin['amount'])

    # Определяем, обязателен ли комментарий
    is_other = (fin.get('category_name') or '').lower() == 'другое'
    requires_description = fin.get('requires_description', 0)
    description_hint = fin.get('description_hint', '')
    requires_yuan = fin.get('requires_yuan', 0)
    # Кнопка «Назад» ведёт на шаг юаней или категории
    back_callback = "fin_back_yuan" if requires_yuan else "fin_back_category"
    back_btn = [InlineKeyboardButton("⬅️ Назад", callback_data=back_callback)]

    if requires_description:
        hint_text = description_hint if description_hint else 'Описание обязательно для данной категории'
        comment_prompt = (
            f"📝 *Описание обязательно!*\n\n"
            f"{hint_text}\n\n"
            "Введите описание или отправьте фото/документ:"
        )
        reply_markup = InlineKeyboardMarkup([back_btn])
    elif is_other:
        comment_prompt = "📝 Введите комментарий (обязательно при категории «Другое»):"
        reply_markup = InlineKeyboardMarkup([back_btn])
    else:
        comment_prompt = "📝 Введите комментарий или нажмите «Пропустить»:"
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("⏩ Пропустить", callback_data="fin_skip_comment")],
            back_btn
        ])

    await query.edit_message_text(
        f"💰 *{escape_md(type_label)}*\n"
        f"💵 Сумма: *{escape_md(formatted)} ₽*\n"
        f"🏦 Счёт: *{escape_md(fin['account_name'])}*\n"
        f"🏷 Категория: *{escape_md(fin.get('category_name', ''))}*\n\n"
        f"{comment_prompt}",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )
    return STATE_FIN_DESCRIPTION


async def finance_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработка подтверждения или отмены.
    При подтверждении — отправляет данные на API.
    """
    query = update.callback_query
    await query.answer()

    action = query.data.split(':')[1]

    if action == 'no':
        await query.edit_message_text("❌ Операция отменена.")
        await query.message.reply_text(
            "Выберите действие 👇",
            reply_markup=get_main_menu()
        )
        return ConversationHandler.END

    fin = context.user_data['finance']
    record_data = {
        'record_type': fin['record_type'],
        'amount': fin['amount'],
        'account_id': fin['account_id'],
        'description': fin['description'],
        'telegram_chat_id': fin['telegram_chat_id'],
        'telegram_username': fin['telegram_username']
    }
    if fin.get('category_id'):
        record_data['category_id'] = fin['category_id']
    if fin.get('yuan_amount'):
        record_data['yuan_amount'] = fin['yuan_amount']

    # Если есть файлы — отправляем через multipart, иначе обычный JSON
    files = fin.get('files', [])
    if files:
        result = create_finance_record_with_files(record_data, files)
    else:
        result = create_finance_record(record_data)

    if result.get('success'):
        type_emoji = "📉" if fin['record_type'] == 'expense' else "📈"
        formatted = format_amount(fin['amount'])
        cat_line = ""
        if fin.get('category_name'):
            cat_line = f"\n🏷 {escape_markdown(fin['category_name'])}"
        yuan_line = ""
        if fin.get('yuan_amount'):
            yuan_line = f"\n💴 {escape_markdown(format_amount(fin['yuan_amount']))} ¥"
        files_count = len(files)
        files_line = f"\n📎 Файлов: {files_count}" if files_count else ""
        await query.edit_message_text(
            f"✅ *Запись сохранена\\!*\n\n"
            f"{type_emoji} {escape_markdown(formatted)} ₽ — {escape_markdown(fin['account_name'])}{cat_line}{yuan_line}{escape_markdown(files_line)}\n"
            f"📝 {escape_markdown(fin['description'])}",
            parse_mode='MarkdownV2'
        )
    else:
        error_msg = result.get('error', 'Неизвестная ошибка')
        await query.edit_message_text(
            f"❌ Ошибка сохранения: {error_msg}"
        )

    await query.message.reply_text(
        "Выберите действие 👇",
        reply_markup=get_main_menu()
    )
    return ConversationHandler.END


async def finance_file_entered(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработчик файлов/фото на шаге описания финансовой записи.
    Принимает фото или документ, сохраняет в контекст, при наличии caption использует как описание.
    Если описание обязательно и не заполнено — просит ввести текстом.
    """
    message = update.message
    fin = context.user_data.get('finance', {})

    caption = message.caption or ''

    try:
        if message.photo:
            file_obj = await message.photo[-1].get_file()
            filename = f"photo_{message.photo[-1].file_unique_id}.jpg"
        elif message.document:
            file_obj = await message.document.get_file()
            filename = message.document.file_name or f"file_{message.document.file_unique_id}"
        else:
            await message.reply_text("❌ Неподдерживаемый тип файла.")
            return STATE_FIN_DESCRIPTION

        file_bytes = await file_obj.download_as_bytearray()

        if 'files' not in fin:
            fin['files'] = []
        fin['files'].append({
            'data': bytes(file_bytes),
            'filename': filename
        })

        if caption:
            fin['description'] = caption

        context.user_data['finance'] = fin

        requires_description = fin.get('requires_description', 0)
        is_other = (fin.get('category_name') or '').lower() == 'другое'

        if (requires_description or is_other) and not fin.get('description'):
            await message.reply_text("✅ Файл принят! Но описание обязательно.\nВведите описание текстом:")
            return STATE_FIN_DESCRIPTION

        # Формируем сообщение подтверждения (аналогично finance_description_entered)
        description = fin.get('description', '')
        record_type = fin.get('record_type', '')
        amount = fin.get('amount', 0)
        account_name = fin.get('account_name', '')
        category_name = fin.get('category_name', '')

        type_emoji = '📈' if record_type == 'income' else '📉'
        type_label = 'Доход' if record_type == 'income' else 'Расход'

        yuan_line = ''
        yuan_amount = fin.get('yuan_amount')
        if yuan_amount:
            yuan_line = f"\n💴 Юани: {format_amount(yuan_amount)} ¥"

        files_count = len(fin.get('files', []))
        files_line = f"\n📎 Файлов: {files_count}" if files_count else ''

        formatted = format_amount(amount)
        confirm_text = (
            f"📋 *Проверьте данные:*\n\n"
            f"{type_emoji} Тип: {escape_md(type_label)}\n"
            f"💰 Сумма: {escape_md(formatted)} ₽\n"
            f"🏦 Счёт: {escape_md(account_name)}\n"
            f"📂 Категория: {escape_md(category_name)}\n"
            f"📝 Описание: {escape_md(description) if description else '—'}"
            f"{yuan_line}{files_line}\n\n"
            f"Всё верно?"
        )

        keyboard = [
            [InlineKeyboardButton("✅ Подтвердить", callback_data="fin_confirm:yes")],
            [InlineKeyboardButton("❌ Отменить", callback_data="fin_confirm:no")]
        ]

        await message.reply_text(confirm_text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
        return STATE_FIN_CONFIRM
    except Exception as e:
        logger.error(f"Ошибка обработки файла: {e}")
        await message.reply_text(f"❌ Ошибка обработки файла: {e}")
        return STATE_FIN_DESCRIPTION


async def finance_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Отмена диалога финансов через /cancel.
    """
    await update.message.reply_text(
        "❌ Финансовая операция отменена.",
        reply_markup=get_main_menu()
    )
    return ConversationHandler.END


# ============================================================================
# СОЗДАНИЕ ОТПРАВКИ (КОНТЕЙНЕРА) ЧЕРЕЗ TELEGRAM
# ============================================================================

async def shipment_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Начало создания отправки.
    Запрашивает комментарий (обязательный).
    """
    chat_id = update.effective_chat.id

    if not is_authorized(chat_id):
        await update.message.reply_text("⛔ У вас нет доступа к этому боту.")
        return ConversationHandler.END

    # Очищаем предыдущие данные
    context.user_data['shipment'] = {}

    await update.message.reply_text(
        "🚚 *Создание отправки*\n\n"
        "Введите комментарий к отправке (обязательно):\n\n"
        "📎 На следующем шаге можно будет прикрепить файл\n"
        "Для отмены нажмите /cancel",
        parse_mode='Markdown',
        reply_markup=ReplyKeyboardRemove()
    )
    return STATE_SHIPMENT_COMMENT


async def shipment_comment_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Получен комментарий к отправке.
    Предлагает прикрепить файл или пропустить.
    """
    comment = update.message.text.strip()

    if not comment:
        await update.message.reply_text(
            "❌ Комментарий не может быть пустым. Введите комментарий:"
        )
        return STATE_SHIPMENT_COMMENT

    context.user_data['shipment']['comment'] = comment

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏩ Пропустить файл", callback_data="ship_skip_file")]
    ])

    await update.message.reply_text(
        "📎 Можете прикрепить файл (фото или документ) к отправке.\n\n"
        "Отправьте файл или нажмите кнопку ниже, чтобы пропустить:",
        reply_markup=keyboard
    )
    return STATE_SHIPMENT_FILE


async def shipment_file_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Получен файл (фото или документ) для отправки.
    Показывает подтверждение.
    """
    message = update.message

    try:
        if message.photo:
            # Фото — берём самое большое разрешение
            file_obj = await message.photo[-1].get_file()
            filename = f"photo_{message.photo[-1].file_unique_id}.jpg"
        elif message.document:
            file_obj = await message.document.get_file()
            filename = message.document.file_name or f"file_{message.document.file_unique_id}"
        else:
            await message.reply_text("❌ Неподдерживаемый тип файла. Отправьте фото или документ.")
            return STATE_SHIPMENT_FILE

        # Скачиваем файл в память
        file_bytes = await file_obj.download_as_bytearray()

        context.user_data['shipment']['file_data'] = bytes(file_bytes)
        context.user_data['shipment']['filename'] = filename

        # Текст подписи (caption) если есть — добавляем к комментарию
        if message.caption:
            existing_comment = context.user_data['shipment'].get('comment', '')
            context.user_data['shipment']['comment'] = f"{existing_comment}\n{message.caption}".strip()

    except Exception as e:
        logger.error(f"Ошибка скачивания файла для отправки: {e}")
        await message.reply_text(
            "❌ Ошибка при загрузке файла. Попробуйте ещё раз или пропустите.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏩ Пропустить файл", callback_data="ship_skip_file")]
            ])
        )
        return STATE_SHIPMENT_FILE

    return await _show_shipment_confirm(update, context)


async def shipment_skip_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Пользователь пропустил прикрепление файла.
    Показывает подтверждение.
    """
    query = update.callback_query
    await query.answer()

    return await _show_shipment_confirm(update, context, is_callback=True)


async def _show_shipment_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, is_callback: bool = False) -> int:
    """
    Показывает итоговое подтверждение перед созданием отправки.
    """
    shipment = context.user_data.get('shipment', {})
    comment = shipment.get('comment', '')
    filename = shipment.get('filename', '')

    text = "🚚 *Подтверждение отправки*\n\n"
    text += f"💬 *Комментарий:*\n{escape_md(comment)}\n\n"

    if filename:
        text += f"📎 *Файл:* {escape_md(filename)}\n\n"
    else:
        text += "📎 *Файл:* нет\n\n"

    text += "Создать отправку?"

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Создать", callback_data="ship_confirm:yes"),
            InlineKeyboardButton("❌ Отменить", callback_data="ship_confirm:no")
        ]
    ])

    if is_callback:
        await update.callback_query.edit_message_text(
            text, parse_mode='Markdown', reply_markup=keyboard
        )
    else:
        await update.message.reply_text(
            text, parse_mode='Markdown', reply_markup=keyboard
        )

    return STATE_SHIPMENT_CONFIRM


async def shipment_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработка подтверждения создания отправки.
    """
    query = update.callback_query
    await query.answer()

    action = query.data.replace('ship_confirm:', '')

    if action != 'yes':
        await query.edit_message_text(
            "❌ Создание отправки отменено.",
        )
        # Возвращаем главное меню
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Выберите действие:",
            reply_markup=get_main_menu()
        )
        context.user_data.pop('shipment', None)
        return ConversationHandler.END

    # Получаем данные для создания
    shipment = context.user_data.get('shipment', {})
    comment = shipment.get('comment', '')
    file_data = shipment.get('file_data')
    filename = shipment.get('filename')

    chat_id = update.effective_chat.id
    user = query.from_user
    sender_name = f"@{user.username}" if user.username else user.first_name or str(chat_id)

    # Показываем "в процессе"
    await query.edit_message_text("⏳ Создаю отправку...")

    # Вызываем API
    result = create_shipment(
        chat_id=chat_id,
        comment=comment,
        sender_name=sender_name,
        file_data=file_data,
        filename=filename
    )

    if result.get('success'):
        doc_id = result.get('doc_id', '?')
        await query.edit_message_text(
            f"✅ Отправка *#{doc_id}* успешно создана\\!\n\n"
            f"💬 Комментарий сохранён в сообщениях контейнера\\.\n"
            f"📢 Уведомления отправлены администраторам\\.",
            parse_mode='MarkdownV2'
        )
    else:
        error = result.get('error', 'Неизвестная ошибка')
        await query.edit_message_text(
            f"❌ Ошибка создания отправки: {error}"
        )

    # Возвращаем главное меню
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="Выберите действие:",
        reply_markup=get_main_menu()
    )

    context.user_data.pop('shipment', None)
    return ConversationHandler.END


async def shipment_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Отмена создания отправки через /cancel.
    """
    context.user_data.pop('shipment', None)
    await update.message.reply_text(
        "❌ Создание отправки отменено.",
        reply_markup=get_main_menu()
    )
    return ConversationHandler.END


# ============================================================================
# ЗАПУСК БОТА
# ============================================================================

def main():
    """
    Точка входа — запуск бота.
    """
    if not TELEGRAM_BOT_TOKEN:
        print("❌ Ошибка: TELEGRAM_BOT_TOKEN не задан в .env")
        sys.exit(1)

    if not TELEGRAM_BOT_SECRET:
        print("❌ Ошибка: TELEGRAM_BOT_SECRET не задан в .env")
        sys.exit(1)

    print("🤖 Запуск бота Moscow Seller...")
    print(f"📡 API URL: {API_BASE_URL}")

    if ALLOWED_CHAT_IDS:
        print(f"🔒 Разрешённые chat_id: {ALLOWED_CHAT_IDS}")
    else:
        print("⚠️ Внимание: бот доступен всем пользователям!")

    # Создаём приложение
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Обработчик диалога создания прихода
    receipt_handler = ConversationHandler(
        entry_points=[
            CommandHandler('prihod', receipt_start),
            CommandHandler('receipt', receipt_start),
            CommandHandler('new', receipt_start),
            MessageHandler(filters.Regex(r'^📦 Новый приход$'), receipt_start)
        ],
        states={
            STATE_RECEIVER_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receiver_name_entered)
            ],
            STATE_RECEIPT_DATE: [
                CallbackQueryHandler(date_selected, pattern=r'^date:'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, custom_date_entered)
            ],
            STATE_SELECT_PRODUCT: [
                CallbackQueryHandler(page_callback, pattern=r'^page:'),
                CallbackQueryHandler(product_callback, pattern=r'^product:'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, product_search)
            ],
            STATE_ENTER_QUANTITY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, quantity_entered)
            ],
            STATE_MORE_PRODUCTS: [
                CallbackQueryHandler(more_products_callback, pattern=r'^more:')
            ],
            STATE_COMMENT: [
                CallbackQueryHandler(comment_skipped, pattern=r'^comment:skip'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, comment_entered)
            ],
            STATE_CONFIRM: [
                CallbackQueryHandler(confirm_callback, pattern=r'^confirm:')
            ]
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            CommandHandler('stop', cancel),
            # Позволяем начать новый приход из любого состояния
            MessageHandler(filters.Regex(r'^📦 Новый приход$'), receipt_start)
        ]
    )

    # Обработчик диалога ответа на сообщение (через кнопку "Ответить")
    reply_conversation_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(reply_button_callback, pattern=r'^reply_msg:')
        ],
        states={
            STATE_WAITING_REPLY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_reply_text)
            ]
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            MessageHandler(filters.Regex(r'^❌ Отменить ответ$'), receive_reply_text)
        ]
    )

    # Обработчик диалога ответа на сообщение о контейнере ВЭД
    # PHOTO и Document хендлеры стоят ПЕРЕД TEXT, потому что фото с caption содержат и текст
    container_reply_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(container_reply_button_callback, pattern=r'^reply_container:')
        ],
        states={
            STATE_CONTAINER_REPLY: [
                MessageHandler(filters.PHOTO | filters.Document.ALL, receive_container_reply_file),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_container_reply_text),
            ]
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            MessageHandler(filters.Regex(r'^❌ Отменить ответ$'), receive_container_reply_text)
        ]
    )

    # Обработчик отправки сообщения в контейнер (новый флоу)
    send_message_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex(r'^✉️ Сообщение$'), send_message_start)
        ],
        states={
            STATE_MSG_CONTAINER_SELECT: [
                CallbackQueryHandler(msg_container_select_callback, pattern=r'^msgc:\d+$'),
                CallbackQueryHandler(msg_container_page_callback, pattern=r'^msgcp:\d+$'),
                CallbackQueryHandler(msg_cancel_callback, pattern=r'^msgcancel$'),
            ],
            STATE_MSG_RECIPIENTS: [
                CallbackQueryHandler(msg_recipient_toggle_callback, pattern=r'^msgr:\d+$'),
                CallbackQueryHandler(msg_recipient_all_toggle_callback, pattern=r'^msgrall$'),
                CallbackQueryHandler(msg_recipient_done_callback, pattern=r'^msgrdone$'),
                # Назад к контейнерам (кнопка "⬅️ Контейнеры")
                CallbackQueryHandler(msg_container_page_callback, pattern=r'^msgcp:\d+$'),
                CallbackQueryHandler(msg_cancel_callback, pattern=r'^msgcancel$'),
            ],
            STATE_MSG_TEXT: [
                # PHOTO/Document ПЕРЕД TEXT (фото с caption содержат и текст)
                MessageHandler(filters.PHOTO | filters.Document.ALL, msg_file_entered),
                MessageHandler(filters.TEXT & ~filters.COMMAND, msg_text_entered),
                CallbackQueryHandler(msg_cancel_callback, pattern=r'^msgcancel$'),
            ],
            STATE_MSG_CONFIRM: [
                CallbackQueryHandler(msg_confirm_callback, pattern=r'^msgconfirm$'),
                CallbackQueryHandler(msg_cancel_callback, pattern=r'^msgcancel$'),
            ],
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            CommandHandler('stop', cancel),
            # Позволяем перезапустить флоу
            MessageHandler(filters.Regex(r'^✉️ Сообщение$'), send_message_start),
        ]
    )

    # Обработчик диалога финансов (доход/расход)
    finance_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex(r'^💰 Финансы$'), finance_start)
        ],
        states={
            STATE_FIN_TYPE: [
                CallbackQueryHandler(finance_type_selected, pattern=r'^fin_type:')
            ],
            STATE_FIN_AMOUNT: [
                CallbackQueryHandler(finance_back_to_type, pattern=r'^fin_back_type$'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, finance_amount_entered)
            ],
            STATE_FIN_ACCOUNT: [
                CallbackQueryHandler(finance_back_to_amount, pattern=r'^fin_back_amount$'),
                CallbackQueryHandler(finance_account_selected, pattern=r'^fin_acc:')
            ],
            STATE_FIN_CATEGORY: [
                CallbackQueryHandler(finance_back_to_account, pattern=r'^fin_back_account$'),
                CallbackQueryHandler(finance_category_selected, pattern=r'^fin_cat:')
            ],
            STATE_FIN_YUAN_AMOUNT: [
                CallbackQueryHandler(finance_back_to_category, pattern=r'^fin_back_category$'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, finance_yuan_amount_entered)
            ],
            STATE_FIN_DESCRIPTION: [
                CallbackQueryHandler(finance_back_to_category, pattern=r'^fin_back_category$'),
                CallbackQueryHandler(finance_back_to_yuan, pattern=r'^fin_back_yuan$'),
                CallbackQueryHandler(finance_skip_comment, pattern=r'^fin_skip_comment$'),
                MessageHandler(filters.PHOTO | filters.Document.ALL, finance_file_entered),
                MessageHandler(filters.TEXT & ~filters.COMMAND, finance_description_entered)
            ],
            STATE_FIN_CONFIRM: [
                CallbackQueryHandler(finance_back_to_description, pattern=r'^fin_back_description$'),
                CallbackQueryHandler(finance_confirm, pattern=r'^fin_confirm:')
            ]
        },
        fallbacks=[
            CommandHandler('cancel', finance_cancel),
            CommandHandler('stop', finance_cancel),
            # Позволяем перезапустить флоу
            MessageHandler(filters.Regex(r'^💰 Финансы$'), finance_start),
        ]
    )

    # Обработчик создания отправки (контейнера)
    shipment_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex(r'^🚚 Отправка товара$'), shipment_start)
        ],
        states={
            STATE_SHIPMENT_COMMENT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, shipment_comment_received)
            ],
            STATE_SHIPMENT_FILE: [
                # PHOTO/Document ПЕРЕД callback (фото с caption)
                MessageHandler(filters.PHOTO | filters.Document.ALL, shipment_file_received),
                CallbackQueryHandler(shipment_skip_file, pattern=r'^ship_skip_file$'),
            ],
            STATE_SHIPMENT_CONFIRM: [
                CallbackQueryHandler(shipment_confirm, pattern=r'^ship_confirm:'),
            ],
        },
        fallbacks=[
            CommandHandler('cancel', shipment_cancel),
            CommandHandler('stop', shipment_cancel),
            # Позволяем перезапустить флоу
            MessageHandler(filters.Regex(r'^🚚 Отправка товара$'), shipment_start),
        ]
    )

    # Регистрируем обработчики
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('help', help_command))
    application.add_handler(reply_conversation_handler)  # Должен быть до receipt_handler
    application.add_handler(container_reply_handler)  # Обработчик ответов на контейнеры
    application.add_handler(send_message_handler)  # Отправка сообщений в контейнер
    application.add_handler(shipment_handler)  # Создание отправки (контейнера)
    application.add_handler(finance_handler)  # Финансы: доход/расход
    application.add_handler(receipt_handler)

    # Обработчик кнопок главного меню (должен быть после receipt_handler)
    # "📦 Новый приход" обрабатывается в ConversationHandler
    application.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(r'^(📊 Остатки|❓ Помощь)$'),
        menu_handler
    ))

    # Обработчик ответов на сообщения от администратора
    # Должен быть с низким приоритетом, чтобы не перехватывать другие сообщения
    application.add_handler(MessageHandler(
        filters.REPLY & filters.TEXT,
        reply_handler
    ), group=1)

    # ========================================================================
    # ПЕРИОДИЧЕСКАЯ ПРОВЕРКА НЕОТВЕЧЕННЫХ СООБЩЕНИЙ (НАПОМИНАНИЯ ЧЕРЕЗ 24Ч)
    # ========================================================================
    # Запускаем проверку каждый час. Если сообщение не отвечено >24ч —
    # отправляем напоминание получателю. В выходные (сб, вс) не отправляем.
    job_queue = application.job_queue
    job_queue.run_repeating(
        check_unanswered_messages_job,
        interval=3600,   # Каждый час
        first=60,        # Первая проверка через 60 сек после старта
        name='unanswered_messages_reminder'
    )
    logger.info("📬 Периодическая проверка неотвеченных сообщений запущена (интервал: 1 час)")

    # Запускаем бота
    print("✅ Бот запущен. Нажмите Ctrl+C для остановки.")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


# ============================================================================
# ПЕРИОДИЧЕСКАЯ ПРОВЕРКА НЕОТВЕЧЕННЫХ СООБЩЕНИЙ
# ============================================================================

async def check_unanswered_messages_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Периодическая задача: проверяет наличие неотвеченных сообщений контейнеров
    старше 24 часов и отправляет напоминания получателям в Telegram.

    Правила:
    - Не отправляем напоминания в субботу и воскресенье (по московскому времени)
    - Каждое сообщение получает напоминание только один раз (reminder_sent = 1)
    - Вызываем API /api/container-messages/pending-reminders из ozon_app.py
    """
    from zoneinfo import ZoneInfo

    # Проверяем день недели по московскому времени
    moscow_tz = ZoneInfo('Europe/Moscow')
    now_moscow = datetime.now(moscow_tz)
    weekday = now_moscow.weekday()  # 0=пн, 1=вт, ..., 5=сб, 6=вс

    if weekday >= 5:
        logger.info("📬 Сегодня выходной — напоминания не отправляем")
        return

    logger.info("📬 Проверяю неотвеченные сообщения контейнеров...")

    try:
        response = requests.post(
            f"{API_BASE_URL}/api/container-messages/pending-reminders",
            json={'token': TELEGRAM_BOT_SECRET},
            timeout=15
        )

        if response.status_code != 200:
            logger.error(f"📬 API вернул статус {response.status_code}: {response.text}")
            return

        data = response.json()
        if not data.get('success'):
            logger.error(f"📬 API ошибка: {data.get('error', 'unknown')}")
            return

        reminders = data.get('reminders', [])
        if not reminders:
            logger.info("📬 Нет неотвеченных сообщений для напоминания")
            return

        logger.info(f"📬 Найдено {len(reminders)} пользователей с неотвеченными сообщениями")

        site_url = os.getenv('SITE_URL', 'https://moscowseller.ru')

        for reminder in reminders:
            chat_id = reminder['chat_id']
            display_name = reminder['display_name']
            messages = reminder['messages']

            # Формируем текст напоминания
            text = f"⏰ *Напоминание о неотвеченных сообщениях*\n\n"
            text += f"Привет, {display_name}! У тебя есть неотвеченные сообщения:\n\n"

            for msg in messages[:5]:  # Показываем максимум 5 сообщений
                container_id = msg['container_id']
                container_info = f"#{container_id}"
                if msg['container_date']:
                    container_info += f" ({msg['container_date']}"
                    if msg['supplier']:
                        container_info += f", {msg['supplier']}"
                    container_info += ")"

                text += f"📦 Контейнер {container_info}\n"
                text += f"   От: {msg['sender_name']}\n"
                # URL с / перед # для корректного парсинга в Telegram
                container_url = f"{site_url}/#ved:ved-containers:{container_id}"
                text += f"   🔗 [Открыть]({container_url})\n\n"

            if len(messages) > 5:
                text += f"_...и ещё {len(messages) - 5} сообщений_\n\n"

            text += "💬 Пожалуйста, ответь на сообщения, когда будет возможность."

            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
                logger.info(f"📬 Напоминание отправлено пользователю {display_name} (chat_id={chat_id}), сообщений: {len(messages)}")
            except Exception as e:
                logger.error(f"📬 Ошибка отправки напоминания chat_id={chat_id}: {e}")

    except Exception as e:
        logger.error(f"📬 Ошибка проверки неотвеченных сообщений: {e}")


if __name__ == '__main__':
    main()
