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
    STATE_CONFIRM             # Подтверждение
) = range(7)


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

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


def format_product_list(items: list) -> str:
    """
    Форматирует список товаров для отображения.
    """
    if not items:
        return "Нет товаров"

    lines = []
    for i, item in enumerate(items, 1):
        lines.append(f"{i}. {item['name']} × {item['quantity']} шт.")

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
        ["📊 Остатки", "❓ Помощь"]
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
    """
    date_str = update.message.text.strip()

    try:
        # Парсим дату в формате ДД.ММ.ГГГГ
        parsed_date = datetime.strptime(date_str, '%d.%m.%Y')
        context.user_data['receipt']['receipt_date'] = parsed_date.strftime('%Y-%m-%d')
    except ValueError:
        await update.message.reply_text(
            "❌ Неверный формат даты.\n"
            "Введите в формате ДД.ММ.ГГГГ (например: 05.02.2026):"
        )
        return STATE_RECEIPT_DATE

    # Переходим к выбору товара
    return await show_product_selection(update, context, is_message=True)


async def show_product_selection(update_or_query, context: ContextTypes.DEFAULT_TYPE, is_message: bool = False) -> int:
    """
    Показать список товаров для выбора.
    """
    products = get_products()

    if not products:
        text = "❌ Не удалось загрузить список товаров.\nПопробуйте позже."
        if is_message:
            await update_or_query.message.reply_text(text)
        else:
            await update_or_query.edit_message_text(text)
        return ConversationHandler.END

    # Создаём кнопки с товарами (показываем первые 10)
    keyboard = []
    for product in products[:10]:
        name = product['name'][:40] + '...' if len(product['name']) > 40 else product['name']
        keyboard.append([
            InlineKeyboardButton(name, callback_data=f"product:{product['sku']}")
        ])

    if len(products) > 10:
        keyboard.append([
            InlineKeyboardButton(f"📋 Ещё {len(products) - 10} товаров...", callback_data="product:more")
        ])

    keyboard.append([
        InlineKeyboardButton("🔍 Поиск по названию/SKU", callback_data="product:search")
    ])

    reply_markup = InlineKeyboardMarkup(keyboard)

    receipt = context.user_data['receipt']
    items_text = ""
    if receipt['items']:
        items_text = f"\n\n📋 *В документе:*\n{format_product_list(receipt['items'])}"

    text = f"📦 *Выберите товар:*{items_text}"

    if is_message:
        await update_or_query.message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    else:
        await update_or_query.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)

    return STATE_SELECT_PRODUCT


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

    if data == 'more':
        # Показать все товары (в реальности нужна пагинация)
        await query.edit_message_text(
            "🔍 Введите SKU или часть названия товара для поиска:"
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
        f"✅ *{product['name']}*\n"
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
            f"✅ *{product['name']}*\n"
            f"SKU: `{product['sku']}`\n\n"
            "📊 Введите количество (шт.):",
            parse_mode='Markdown'
        )
        return STATE_ENTER_QUANTITY

    # Несколько результатов — показываем кнопки
    keyboard = []
    for product in products[:10]:
        name = product['name'][:40] + '...' if len(product['name']) > 40 else product['name']
        keyboard.append([
            InlineKeyboardButton(name, callback_data=f"product:{product['sku']}")
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
        'name': product['name'],
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
        f"✅ Добавлено: *{product['name']}* × {quantity} шт.\n\n"
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
            CommandHandler('stop', cancel)
        ]
    )

    # Регистрируем обработчики
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('help', help_command))
    application.add_handler(receipt_handler)

    # Обработчик кнопок главного меню (должен быть после receipt_handler)
    # "📦 Новый приход" обрабатывается в ConversationHandler
    application.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(r'^(📊 Остатки|❓ Помощь)$'),
        menu_handler
    ))

    # Запускаем бота
    print("✅ Бот запущен. Нажмите Ctrl+C для остановки.")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
