#!/usr/bin/env python3
"""
🌐 OZON ТОВАРЫ - Веб интерфейс для просмотра товаров FBO
Правильная реализация: чистый запрос, правильный SQL, debug для проверки
"""

import sqlite3
import requests
import json
import os
import sys
import re
import time
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template_string, jsonify, request
from bs4 import BeautifulSoup

# ✅ TIMEZONE FIX - Белград (Serbia/Balkans)
try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo("Europe/Belgrade")
except Exception:
    TZ = None

def get_snapshot_date():
    """Получить дату снимка по локальному времени Белграда"""
    if TZ:
        return datetime.now(TZ).date().isoformat()
    return datetime.now().date().isoformat()

def get_snapshot_time():
    """Получить время снимка по локальному времени Белграда"""
    if TZ:
        return datetime.now(TZ).isoformat()
    return datetime.now().isoformat()

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

# ✅ Загружаем переменные окружения
def load_env_variables():
    """Загружает .env файл с поддержкой разных кодировок и BOM"""
    # Сначала пробуем dotenv
    try:
        from dotenv import load_dotenv
        if load_dotenv():
            return
    except ImportError:
        pass
    
    # Если не сработало, читаем вручную
    env_path = ".env"
    if os.path.exists(env_path):
        try:
            # Пробуем разные кодировки
            for encoding in ["utf-8-sig", "utf-8", "cp1252", "latin-1"]:
                try:
                    with open(env_path, "r", encoding=encoding) as f:
                        content = f.read()
                    
                    # Парсим построчно
                    for line in content.split('\n'):
                        # Убираем пробелы и BOM символы
                        line = line.strip().lstrip('\ufeff')
                        
                        # Пропускаем пустые строки и комментарии
                        if not line or line.startswith("#"):
                            continue
                        
                        # Ищем знак =
                        if "=" not in line:
                            continue
                        
                        # Парсим ключ и значение
                        parts = line.split("=", 1)
                        if len(parts) != 2:
                            continue
                        
                        key = parts[0].strip()
                        value = parts[1].strip()
                        
                        # Убираем кавычки
                        for quote in ['"', "'"]:
                            if value.startswith(quote) and value.endswith(quote):
                                value = value[1:-1]
                                break
                        
                        # Сохраняем в os.environ
                        if key and value:
                            os.environ[key] = value
                    
                    break
                except (UnicodeDecodeError, UnicodeError) as e:
                    continue
                except Exception as e:
                    print(f"⚠️  Ошибка при парсинге .env ({encoding}): {e}")
        except Exception as e:
            print(f"⚠️  Ошибка при чтении .env: {e}")

load_env_variables()

OZON_CLIENT_ID = os.environ.get("OZON_CLIENT_ID")
OZON_API_KEY = os.environ.get("OZON_API_KEY")

# ✅ Ключи для Ozon Performance API (реклама)
OZON_PERFORMANCE_CLIENT_ID = os.environ.get("OZON_PERFORMANCE_CLIENT_ID")
OZON_PERFORMANCE_API_KEY = os.environ.get("OZON_PERFORMANCE_API_KEY")

# ✅ Проверяем что ключи установлены
if not OZON_CLIENT_ID or not OZON_API_KEY:
    import os.path
    print("\n❌ ОШИБКА: Не установлены переменные окружения!")
    print(f"   📂 Текущая папка: {os.getcwd()}")
    print(f"   📋 .env существует: {os.path.exists('.env')}")
    print(f"   📋 OZON_CLIENT_ID: {OZON_CLIENT_ID}")
    print(f"   📋 OZON_API_KEY: {OZON_API_KEY}")
    print("\n🔧 Способ 1 - PowerShell (в одной команде):")
    print("   $env:OZON_CLIENT_ID='138926'; $env:OZON_API_KEY='***REDACTED***'; python ozon_app.py")
    print("\n🔧 Способ 2 - Создать .env файл в папке (рекомендуется):")
    print("   Содержимое .env:")
    print("   OZON_CLIENT_ID=138926")
    print("   OZON_API_KEY=***REDACTED***")
    print("\n📢 Дополнительно для Performance API (реклама):")
    print("   OZON_PERFORMANCE_CLIENT_ID=твой_performance_client_id")
    print("   OZON_PERFORMANCE_API_KEY=твой_performance_api_key")
    sys.exit(1)

OZON_HOST = "https://api-seller.ozon.ru"
DB_PATH = "ozon_data.db"

# ✅ Выбор поля для считывания остатков
# Варианты: "free_to_sell_amount" | "available" | "present"
STOCK_FIELD = os.environ.get("OZON_STOCK_FIELD", "free_to_sell_amount")
print(f"\n📊 Используется поле остатка: {STOCK_FIELD}\n")

# ============================================================================
# СИНХРОНИЗАЦИЯ ДАННЫХ
# ============================================================================

def ensure_column(cursor, table, column, ddl):
    """Проверяет наличие столбца и добавляет если его нет"""
    cols = {r[1] for r in cursor.execute(f"PRAGMA table_info({table})")}
    if column not in cols:
        cursor.execute(ddl)
        return True
    return False


def init_database():
    """Инициализация базы данных"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # ✅ Текущие остатки (самый свежий снимок)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            sku INTEGER PRIMARY KEY,
            name TEXT,
            fbo_stock INTEGER DEFAULT 0,
            orders_qty INTEGER DEFAULT 0,
            updated_at TIMESTAMP
        )
    ''')
    
    # ✅ ТАБЛИЦА ИСТОРИИ - для всех снимков по датам
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku INTEGER NOT NULL,
            name TEXT,
            fbo_stock INTEGER DEFAULT 0,
            orders_qty INTEGER DEFAULT 0,
            avg_position REAL DEFAULT 0,
            impressions INTEGER DEFAULT 0,
            snapshot_date DATE NOT NULL,
            snapshot_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notes TEXT DEFAULT '',
            UNIQUE(sku, snapshot_date)
        )
    ''')
    
    # Индекс для быстрого поиска по SKU и дате
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_history_sku_date 
        ON products_history(sku, snapshot_date)
    ''')
    
    # ✅ Добавляем столбец impressions если его нет (миграция)
    if ensure_column(cursor, "products_history", "impressions",
                     "ALTER TABLE products_history ADD COLUMN impressions INTEGER DEFAULT 0"):
        print("✅ Столбец impressions добавлен в products_history")
    
    # ✅ Добавляем столбец ctr если его нет (миграция)
    if ensure_column(cursor, "products_history", "ctr",
                     "ALTER TABLE products_history ADD COLUMN ctr REAL DEFAULT 0"):
        print("✅ Столбец ctr добавлен в products_history")
    
    # ✅ Добавляем столбцы для показов и конверсии
    if ensure_column(cursor, "products_history", "hits_view_search",
                     "ALTER TABLE products_history ADD COLUMN hits_view_search INTEGER DEFAULT 0"):
        print("✅ Столбец hits_view_search добавлен в products_history")
    
    if ensure_column(cursor, "products_history", "hits_view_search_pdp",
                     "ALTER TABLE products_history ADD COLUMN hits_view_search_pdp INTEGER DEFAULT 0"):
        print("✅ Столбец hits_view_search_pdp добавлен в products_history")
    
    if ensure_column(cursor, "products_history", "search_ctr",
                     "ALTER TABLE products_history ADD COLUMN search_ctr REAL DEFAULT 0"):
        print("✅ Столбец search_ctr добавлен в products_history")
    
    # ✅ Добавляем колонки в products таблицу
    if ensure_column(cursor, "products", "hits_view_search",
                     "ALTER TABLE products ADD COLUMN hits_view_search INTEGER DEFAULT 0"):
        print("✅ Столбец hits_view_search добавлен в products")
    
    if ensure_column(cursor, "products", "hits_view_search_pdp",
                     "ALTER TABLE products ADD COLUMN hits_view_search_pdp INTEGER DEFAULT 0"):
        print("✅ Столбец hits_view_search_pdp добавлен в products")
    
    if ensure_column(cursor, "products", "search_ctr",
                     "ALTER TABLE products ADD COLUMN search_ctr REAL DEFAULT 0"):
        print("✅ Столбец search_ctr добавлен в products")
    
    # ✅ Добавляем колонки для "В корзину" и CR1
    if ensure_column(cursor, "products_history", "hits_add_to_cart",
                     "ALTER TABLE products_history ADD COLUMN hits_add_to_cart INTEGER DEFAULT 0"):
        print("✅ Столбец hits_add_to_cart добавлен в products_history")
    
    if ensure_column(cursor, "products_history", "cr1",
                     "ALTER TABLE products_history ADD COLUMN cr1 REAL DEFAULT 0"):
        print("✅ Столбец cr1 добавлен в products_history")
    
    if ensure_column(cursor, "products", "hits_add_to_cart",
                     "ALTER TABLE products ADD COLUMN hits_add_to_cart INTEGER DEFAULT 0"):
        print("✅ Столбец hits_add_to_cart добавлен в products")
    
    if ensure_column(cursor, "products", "cr1",
                     "ALTER TABLE products ADD COLUMN cr1 REAL DEFAULT 0"):
        print("✅ Столбец cr1 добавлен в products")
    
    # ✅ Добавляем колонку для CR2
    if ensure_column(cursor, "products_history", "cr2",
                     "ALTER TABLE products_history ADD COLUMN cr2 REAL DEFAULT 0"):
        print("✅ Столбец cr2 добавлен в products_history")
    
    if ensure_column(cursor, "products", "cr2",
                     "ALTER TABLE products ADD COLUMN cr2 REAL DEFAULT 0"):
        print("✅ Столбец cr2 добавлен в products")
    
    # ✅ Добавляем колонку для расходов на рекламу
    if ensure_column(cursor, "products_history", "adv_spend",
                     "ALTER TABLE products_history ADD COLUMN adv_spend REAL DEFAULT 0"):
        print("✅ Столбец adv_spend добавлен в products_history")
    
    if ensure_column(cursor, "products", "adv_spend",
                     "ALTER TABLE products ADD COLUMN adv_spend REAL DEFAULT 0"):
        print("✅ Столбец adv_spend добавлен в products")

    # ✅ Добавляем колонки для цен товаров
    if ensure_column(cursor, "products_history", "price",
                     "ALTER TABLE products_history ADD COLUMN price REAL DEFAULT 0"):
        print("✅ Столбец price добавлен в products_history")

    if ensure_column(cursor, "products", "price",
                     "ALTER TABLE products ADD COLUMN price REAL DEFAULT 0"):
        print("✅ Столбец price добавлен в products")

    # ✅ Добавляем колонку для артикула товара (offer_id)
    if ensure_column(cursor, "products", "offer_id",
                     "ALTER TABLE products ADD COLUMN offer_id TEXT DEFAULT NULL"):
        print("✅ Столбец offer_id добавлен в products")

    if ensure_column(cursor, "products_history", "offer_id",
                     "ALTER TABLE products_history ADD COLUMN offer_id TEXT DEFAULT NULL"):
        print("✅ Столбец offer_id добавлен в products_history")

    # ✅ Добавляем колонку для плановых заказов (orders_plan)
    if ensure_column(cursor, "products_history", "orders_plan",
                     "ALTER TABLE products_history ADD COLUMN orders_plan INTEGER DEFAULT NULL"):
        print("✅ Столбец orders_plan добавлен в products_history")

    # ✅ Добавляем колонку для планового CPO (cpo_plan)
    if ensure_column(cursor, "products_history", "cpo_plan",
                     "ALTER TABLE products_history ADD COLUMN cpo_plan INTEGER DEFAULT NULL"):
        print("✅ Столбец cpo_plan добавлен в products_history")

    # ✅ Добавляем колонку для плановой цены (price_plan)
    if ensure_column(cursor, "products_history", "price_plan",
                     "ALTER TABLE products_history ADD COLUMN price_plan INTEGER DEFAULT NULL"):
        print("✅ Столбец price_plan добавлен в products_history")

    # ✅ Добавляем колонку для рейтинга товара
    if ensure_column(cursor, "products_history", "rating",
                     "ALTER TABLE products_history ADD COLUMN rating REAL DEFAULT NULL"):
        print("✅ Столбец rating добавлен в products_history")

    # ✅ Добавляем колонку для количества отзывов
    if ensure_column(cursor, "products_history", "review_count",
                     "ALTER TABLE products_history ADD COLUMN review_count INTEGER DEFAULT NULL"):
        print("✅ Столбец review_count добавлен в products_history")

    if ensure_column(cursor, "products_history", "marketing_price",
                     "ALTER TABLE products_history ADD COLUMN marketing_price REAL DEFAULT 0"):
        print("✅ Столбец marketing_price добавлен в products_history")

    if ensure_column(cursor, "products", "marketing_price",
                     "ALTER TABLE products ADD COLUMN marketing_price REAL DEFAULT 0"):
        print("✅ Столбец marketing_price добавлен в products")

    # ✅ Добавляем колонки для поставок FBO
    if ensure_column(cursor, "products_history", "in_transit",
                     "ALTER TABLE products_history ADD COLUMN in_transit INTEGER DEFAULT 0"):
        print("✅ Столбец in_transit добавлен в products_history")

    if ensure_column(cursor, "products", "in_transit",
                     "ALTER TABLE products ADD COLUMN in_transit INTEGER DEFAULT 0"):
        print("✅ Столбец in_transit добавлен в products")

    if ensure_column(cursor, "products_history", "in_draft",
                     "ALTER TABLE products_history ADD COLUMN in_draft INTEGER DEFAULT 0"):
        print("✅ Столбец in_draft добавлен в products_history")

    if ensure_column(cursor, "products", "in_draft",
                     "ALTER TABLE products ADD COLUMN in_draft INTEGER DEFAULT 0"):
        print("✅ Столбец in_draft добавлен в products")

    # ✅ Добавляем колонку для индекса цены (price_index)
    if ensure_column(cursor, "products_history", "price_index",
                     "ALTER TABLE products_history ADD COLUMN price_index TEXT DEFAULT NULL"):
        print("✅ Столбец price_index добавлен в products_history")

    if ensure_column(cursor, "products", "price_index",
                     "ALTER TABLE products ADD COLUMN price_index TEXT DEFAULT NULL"):
        print("✅ Столбец price_index добавлен в products")

    # ✅ Таблица fbo_warehouse_stock — остатки по складам/кластерам
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fbo_warehouse_stock (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku INTEGER NOT NULL,
            warehouse_name TEXT,
            stock INTEGER DEFAULT 0,
            snapshot_date DATE NOT NULL
        )
    ''')

    # ✅ Таблица fbo_analytics — аналитика по кластерам (ADS, IDC)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fbo_analytics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku INTEGER NOT NULL,
            cluster_name TEXT,
            ads REAL DEFAULT 0,
            idc REAL DEFAULT 0,
            days_without_sales INTEGER DEFAULT 0,
            liquidity_status TEXT DEFAULT '',
            stock INTEGER DEFAULT 0,
            snapshot_date DATE NOT NULL
        )
    ''')

    # ============================================================================
    # ТАБЛИЦА ПОСТАВОК — для вкладки "ПОСТАВКИ"
    # ============================================================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS supplies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku INTEGER NOT NULL,
            product_name TEXT,
            exit_plan_date TEXT,
            order_qty_plan INTEGER DEFAULT 0,
            exit_factory_date TEXT,
            exit_factory_qty INTEGER DEFAULT 0,
            arrival_warehouse_date TEXT,
            arrival_warehouse_qty INTEGER DEFAULT 0,
            logistics_cost_per_unit REAL DEFAULT 0,
            price_cny REAL DEFAULT 0,
            cost_plus_6 REAL DEFAULT 0,
            add_to_marketing INTEGER DEFAULT 0,
            add_to_debts INTEGER DEFAULT 0,
            plan_fbo INTEGER DEFAULT 0,
            is_locked INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # ============================================================================
    # ТАБЛИЦА КУРСОВ ВАЛЮТ — кэш курсов ЦБ РФ
    # ============================================================================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS currency_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            currency_code TEXT NOT NULL,
            rate REAL NOT NULL,
            fetch_date DATE NOT NULL,
            UNIQUE(currency_code, fetch_date)
        )
    ''')

    conn.commit()
    conn.close()


# ============================================================================
# КУРСЫ ВАЛЮТ ЦБ РФ
# ============================================================================

# Кэш курсов валют (обновляется раз в день)
_currency_cache = {
    'rates': {},
    'date': None
}

def fetch_cbr_rates():
    """
    Получить курсы валют с сайта ЦБ РФ (XML API).

    Возвращает словарь с курсами: {'CNY': 12.34, 'USD': 89.56, 'EUR': 97.12}
    Кэширует результат на весь день.
    """
    today = get_snapshot_date()

    # Проверяем кэш в памяти
    if _currency_cache['date'] == today and _currency_cache['rates']:
        return _currency_cache['rates']

    # Проверяем кэш в базе данных
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT currency_code, rate FROM currency_rates
            WHERE fetch_date = ?
        ''', (today,))
        rows = cursor.fetchall()
        conn.close()

        if rows:
            rates = {row[0]: row[1] for row in rows}
            if 'CNY' in rates and 'USD' in rates and 'EUR' in rates:
                _currency_cache['rates'] = rates
                _currency_cache['date'] = today
                return rates
    except Exception as e:
        print(f"⚠️ Ошибка чтения кэша курсов: {e}")

    # Запрашиваем с ЦБ РФ
    try:
        url = "https://www.cbr.ru/scripts/XML_daily.asp"
        response = requests.get(url, timeout=10)
        response.encoding = 'windows-1251'

        soup = BeautifulSoup(response.text, 'html.parser')

        # Коды валют которые нам нужны
        target_codes = {'CNY': None, 'USD': None, 'EUR': None}

        for valute in soup.find_all('valute'):
            char_code = valute.find('charcode').text
            if char_code in target_codes:
                # ЦБ РФ возвращает курс через запятую и с номиналом
                nominal = int(valute.find('nominal').text)
                value_str = valute.find('value').text.replace(',', '.')
                rate = float(value_str) / nominal
                target_codes[char_code] = round(rate, 4)

        rates = {k: v for k, v in target_codes.items() if v is not None}

        if rates:
            # Сохраняем в базу
            try:
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                for code, rate in rates.items():
                    cursor.execute('''
                        INSERT OR REPLACE INTO currency_rates (currency_code, rate, fetch_date)
                        VALUES (?, ?, ?)
                    ''', (code, rate, today))
                conn.commit()
                conn.close()
            except Exception as e:
                print(f"⚠️ Ошибка сохранения курсов: {e}")

            _currency_cache['rates'] = rates
            _currency_cache['date'] = today
            print(f"✅ Курсы ЦБ РФ загружены: CNY={rates.get('CNY')}, USD={rates.get('USD')}, EUR={rates.get('EUR')}")
            return rates

    except Exception as e:
        print(f"❌ Ошибка загрузки курсов ЦБ РФ: {e}")

    # Фоллбэк — последние известные курсы из базы
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT currency_code, rate FROM currency_rates
            WHERE fetch_date = (SELECT MAX(fetch_date) FROM currency_rates)
        ''')
        rows = cursor.fetchall()
        conn.close()
        if rows:
            rates = {row[0]: row[1] for row in rows}
            _currency_cache['rates'] = rates
            _currency_cache['date'] = today
            return rates
    except Exception:
        pass

    return {'CNY': 0, 'USD': 0, 'EUR': 0}


def get_ozon_headers():
    """Заголовки для запросов к Ozon Seller API"""
    return {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json"
    }


# Кэш для Performance API токена
_performance_token_cache = {
    "access_token": None,
    "expires_at": 0
}

def get_performance_access_token():
    """Получить access_token для Ozon Performance API (с кэшированием)"""
    import time

    # Проверяем кэш (оставляем 60 сек запаса до истечения)
    if _performance_token_cache["access_token"] and time.time() < (_performance_token_cache["expires_at"] - 60):
        return _performance_token_cache["access_token"]

    # Получаем новый токен
    try:
        token_url = "https://api-performance.ozon.ru/api/client/token"
        payload = {
            "client_id": OZON_PERFORMANCE_CLIENT_ID,
            "client_secret": OZON_PERFORMANCE_API_KEY,
            "grant_type": "client_credentials"
        }

        response = requests.post(token_url, json=payload, timeout=15)

        if response.status_code != 200:
            print(f"  ⚠️  Ошибка получения токена (status={response.status_code}): {response.text[:200]}")
            return None

        data = response.json()
        access_token = data.get("access_token")
        expires_in = data.get("expires_in", 1800)  # default 30 минут

        # Сохраняем в кэш
        _performance_token_cache["access_token"] = access_token
        _performance_token_cache["expires_at"] = time.time() + expires_in

        print(f"  ✅ Получен новый access_token (действует {expires_in} сек)")
        return access_token

    except Exception as e:
        print(f"  ❌ Ошибка при получении access_token: {e}")
        return None

def get_ozon_performance_headers():
    """Заголовки для запросов к Ozon Performance API (реклама)"""
    access_token = get_performance_access_token()
    if not access_token:
        return None

    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }


def get_async_report(uuid, headers, max_attempts=30, sleep_seconds=2):
    """
    Получение асинхронного отчёта Performance API по UUID.

    Процесс:
    1. Проверяем статус формирования отчёта (polling)
    2. Ждём пока state != OK
    3. Скачиваем готовый отчёт

    Параметры:
        uuid: UUID запроса от асинхронного эндпоинта
        headers: HTTP заголовки с авторизацией
        max_attempts: максимум попыток проверки статуса
        sleep_seconds: секунд между попытками

    Возвращает: CSV содержимое отчёта или None в случае ошибки
    """
    import time

    # Шаг 1: Проверяем статус формирования отчёта (polling)
    for attempt in range(max_attempts):
        status_r = requests.get(
            f"https://api-performance.ozon.ru/api/client/statistics/{uuid}",
            headers=headers,
            timeout=15
        )

        if status_r.status_code != 200:
            print(f"     ⚠️  Ошибка проверки статуса UUID (status={status_r.status_code})")
            return None

        status_data = status_r.json()
        state = status_data.get("state")

        if state == "OK":
            # Отчёт готов!
            break
        elif state == "ERROR":
            error_msg = status_data.get("error", "Неизвестная ошибка")
            print(f"     ❌ Ошибка формирования отчёта: {error_msg}")
            return None
        elif state in ["NOT_STARTED", "IN_PROGRESS"]:
            # Ждём и повторяем
            if attempt < max_attempts - 1:  # Не спим на последней попытке
                time.sleep(sleep_seconds)
        else:
            print(f"     ⚠️  Неизвестный статус: {state}")
            return None

    if state != "OK":
        print(f"     ⏱️  Превышено время ожидания (state={state})")
        return None

    # Шаг 2: Скачиваем готовый отчёт
    report_r = requests.get(
        f"https://api-performance.ozon.ru/api/client/statistics/report?UUID={uuid}",
        headers=headers,
        timeout=30
    )

    if report_r.status_code != 200:
        print(f"     ⚠️  Ошибка скачивания отчёта (status={report_r.status_code})")
        return None

    return report_r.text


def load_search_promo_products_async(date_from, date_to, headers):
    """
    Загрузка товаров с расходами для кампаний SEARCH_PROMO через асинхронный API.

    Используется когда стандартный эндпоинт не возвращает товары
    (например, для неактивных кампаний или кампаний типа "все товары").

    API: POST /api/client/statistic/orders/generate (асинхронный)

    ⚠️ ВАЖНО: Для SEARCH_PROMO используем отчёт по ЗАКАЗАМ, а не по товарам!
    Расходы в "Оплата за заказ" привязаны к заказам, а не к списку товаров в кампании.

    Параметры:
        date_from: начало периода (ГГГГ-ММ-ДД)
        date_to: конец периода (ГГГГ-ММ-ДД)
        headers: HTTP заголовки с авторизацией

    Возвращает: {date: {sku: spend}} - словарь с расходами по датам и SKU
    """
    import csv
    import io
    from datetime import datetime

    print(f"     🔄 Загружаем отчёт по заказам (асинхронный API)...")

    # Конвертируем даты в RFC 3339 формат для API
    try:
        dt_from = datetime.strptime(date_from, '%Y-%m-%d')
        dt_to = datetime.strptime(date_to, '%Y-%m-%d')
        rfc_from = dt_from.strftime('%Y-%m-%dT00:00:00Z')
        rfc_to = dt_to.strftime('%Y-%m-%dT23:59:59Z')
    except Exception as e:
        print(f"     ⚠️  Ошибка конвертации дат: {e}")
        return {}

    # Шаг 1: Отправляем запрос на формирование отчёта по ЗАКАЗАМ
    # ⚠️ ВАЖНО: В пути /statistic/ без "s" (опечатка в API!)
    r = requests.post(
        "https://api-performance.ozon.ru/api/client/statistic/orders/generate",
        headers=headers,
        json={
            "from": rfc_from,
            "to": rfc_to
        },
        timeout=15
    )

    if r.status_code != 200:
        print(f"     ⚠️  Ошибка запроса отчёта (status={r.status_code})")
        return {}

    response_data = r.json()
    uuid = response_data.get("UUID")

    if not uuid:
        print(f"     ⚠️  UUID не получен в ответе")
        return {}

    print(f"     📋 UUID: {uuid}, ожидание формирования отчёта...")

    # Шаг 2-3: Получаем готовый отчёт (polling + download)
    csv_content = get_async_report(uuid, headers)

    if not csv_content:
        return {}

    print(f"     ✅ Отчёт получен ({len(csv_content)} байт)")

    # Шаг 4: Парсим CSV отчёта по заказам
    # Формат CSV:
    #   Строка 0: Заголовок отчёта (пропускаем)
    #   Строка 1: Дата;ID заказа;Номер заказа;SKU;...;Расход, ₽
    #   Строка 2+: Данные заказов
    spend_by_date_sku = {}  # {date: {sku: spend}}

    try:
        # ⚠️ ВАЖНО: Пропускаем первую строку (заголовок отчёта)
        csv_lines = csv_content.split('\n')
        if len(csv_lines) < 2:
            print(f"     ℹ️  CSV пустой или слишком короткий")
            return {}

        # Удаляем первую строку и собираем обратно
        csv_without_header = '\n'.join(csv_lines[1:])

        csv_reader = csv.DictReader(io.StringIO(csv_without_header), delimiter=';')

        for row in csv_reader:
            # Дата заказа (формат: ДД.ММ.ГГГГ)
            date_str = row.get('Дата', '').strip()
            if not date_str:
                continue

            # Конвертируем дату из ДД.ММ.ГГГГ в ГГГГ-ММ-ДД
            try:
                dt = datetime.strptime(date_str, '%d.%m.%Y')
                date = dt.strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                continue

            # SKU товара
            sku_str = row.get('SKU', '').strip()
            if not sku_str:
                continue

            try:
                sku = int(sku_str)
            except (ValueError, TypeError):
                continue

            # Расход в рублях (колонка "Расход, ₽")
            spend_str = row.get('Расход, ₽', '0').strip().replace(',', '.')
            try:
                spend = float(spend_str)
            except (ValueError, TypeError):
                spend = 0.0

            if spend <= 0:
                continue

            # Аккумулируем расходы по датам и SKU
            if date not in spend_by_date_sku:
                spend_by_date_sku[date] = {}

            spend_by_date_sku[date][sku] = spend_by_date_sku[date].get(sku, 0) + spend

        if spend_by_date_sku:
            total_skus = sum(len(skus) for skus in spend_by_date_sku.values())
            total_spend = sum(sum(skus.values()) for skus in spend_by_date_sku.values())
            print(f"     ✅ Извлечено: {len(spend_by_date_sku)} дат, {total_skus} уникальных SKU")
            print(f"     💰 Общий расход: {total_spend:.2f}₽")
        else:
            print(f"     ℹ️  Нет заказов с расходами за период")

    except Exception as e:
        print(f"     ⚠️  Ошибка парсинга CSV: {e}")
        return {}

    return spend_by_date_sku


def load_adv_spend_by_sku(date_from, date_to):
    """
    Загрузка расходов на рекламу по SKU через Performance API.

    Логика работы:
    1. Получаем список ВСЕХ активных кампаний (SKU, SEARCH_PROMO, BANNER)
    2. Для каждой кампании получаем расход через GET /api/client/statistics/expense за период
    3. Собираем данные за ВСЕ доступные даты в CSV (автоматическое обновление истории!)
    4. Получаем товары в кампании через GET /api/client/campaign/{id}/v2/products
    5. Распределяем расход каждого дня между товарами

    Типы кампаний:
    - SKU: Оплата за клик (Performance)
    - SEARCH_PROMO: Оплата за заказ
    - BANNER: Баннерная реклама

    ⚠️ API СТАТУС:
    - Текущий эндпоинт: GET /api/client/statistics/expense (синхронный, ✅ работает)
    - Статус: Поддерживается с обратной совместимостью после изменений от 20.01.2026
    - Альтернатива: POST /api/client/statistics (асинхронный, больше возможностей)

    📚 Документация миграции: .claude/ozon-api-docs/performance-api-changes-2026-01-20.md

    TODO (будущая миграция):
    Для расширенной функциональности можно мигрировать на асинхронный API:
    1. POST /api/client/statistics → получить UUID
    2. GET /api/client/statistics/{UUID} → проверить статус (polling)
    3. GET /api/client/statistics/report?UUID={UUID} → скачать CSV/ZIP
    Преимущества: больше метрик, группировка данных, единый формат для всех типов кампаний

    Параметры:
        date_from: начало периода запроса
        date_to: конец периода запроса

    Возвращает: {date: {sku: adv_spend}} - словарь с расходами по датам и SKU
    """
    print(f"\n📊 Загрузка расходов на рекламу ({date_from} - {date_to})...")

    if not OZON_PERFORMANCE_API_KEY or not OZON_PERFORMANCE_CLIENT_ID:
        print("  ⚠️  Performance API ключи не установлены - пропускаю рекламные расходы")
        return {}

    try:
        import csv
        import io

        headers = get_ozon_performance_headers()
        if not headers:
            print("  ⚠️  Не удалось получить access_token для Performance API")
            return {}

        # Шаг 1: Получаем список ВСЕХ кампаний (SKU, SEARCH_PROMO, BANNER)
        # ⚠️ ВАЖНО: Загружаем ВСЕ кампании, не только активные!
        # Причина: Если кампания была активна вчера, но сегодня остановлена,
        # у неё всё равно есть расходы за вчера, которые нужно загрузить.
        print("  📋 Получение списка всех кампаний (включая остановленные)...")

        campaigns_url = "https://api-performance.ozon.ru/api/client/campaign"
        # НЕ фильтруем по state - загружаем ВСЕ кампании
        params = {}

        r = requests.get(campaigns_url, headers=headers, params=params, timeout=15)

        if r.status_code != 200:
            print(f"  ⚠️  Ошибка получения кампаний (status={r.status_code})")
            return {}

        campaigns_data = r.json()
        campaigns = campaigns_data.get("list", [])

        if not campaigns:
            print("  ⚠️  Нет рекламных кампаний в аккаунте")
            return {}

        # Группируем кампании по типам и статусам для статистики
        by_type = {}
        by_state = {}
        for camp in campaigns:
            camp_type = camp.get("advObjectType", "Unknown")
            camp_state = camp.get("state", "Unknown")
            by_type[camp_type] = by_type.get(camp_type, 0) + 1
            by_state[camp_state] = by_state.get(camp_state, 0) + 1

        print(f"  ✅ Найдено кампаний: {len(campaigns)}")
        for camp_type, count in by_type.items():
            type_name = {
                "SKU": "Оплата за клик",
                "SEARCH_PROMO": "Оплата за заказ",
                "BANNER": "Баннерная реклама"
            }.get(camp_type, camp_type)
            print(f"     • {type_name}: {count}")

        # Показываем статусы
        if len(by_state) > 1 or "CAMPAIGN_STATE_RUNNING" not in by_state:
            print(f"  📊 Статусы кампаний:")
            for state, count in by_state.items():
                state_name = {
                    "CAMPAIGN_STATE_RUNNING": "Активные",
                    "CAMPAIGN_STATE_PAUSED": "На паузе",
                    "CAMPAIGN_STATE_STOPPED": "Остановлены",
                    "CAMPAIGN_STATE_FINISHED": "Завершены"
                }.get(state, state)
                print(f"     • {state_name}: {count}")

        # Шаг 2: Получаем расходы по каждой кампании
        spend_by_date = {}  # {date: {sku: spend}}

        for campaign in campaigns:
            campaign_id = campaign.get("id")
            campaign_title = campaign.get("title", "Без названия")
            campaign_type = campaign.get("advObjectType", "Unknown")
            campaign_state = campaign.get("state", "Unknown")

            # Эмодзи для статуса
            state_emoji = {
                "CAMPAIGN_STATE_RUNNING": "🟢",
                "CAMPAIGN_STATE_PAUSED": "⏸️",
                "CAMPAIGN_STATE_STOPPED": "🔴",
                "CAMPAIGN_STATE_FINISHED": "✅"
            }.get(campaign_state, "⚪")

            print(f"\n  📊 {state_emoji} Кампания: {campaign_title} (ID: {campaign_id}, Тип: {campaign_type})")

            # 2.1. Получаем расход по кампании
            # ✅ СИНХРОННЫЙ API: GET /api/client/statistics/expense
            # Всё ещё работает после изменений от 20.01.2026 (обратная совместимость)
            # Для новых проектов рекомендуется асинхронный POST /api/client/statistics
            expense_url = "https://api-performance.ozon.ru/api/client/statistics/expense"
            params = {
                "campaignIds": campaign_id,
                "dateFrom": date_from,
                "dateTo": date_to
            }

            r = requests.get(expense_url, headers=headers, params=params, timeout=15)

            if r.status_code != 200:
                print(f"     ⚠️  Ошибка получения расходов (status={r.status_code})")
                continue

            # Парсим CSV с расходами
            # 🔄 НОВАЯ ЛОГИКА: Собираем расходы за ВСЕ даты из CSV, не фильтруем по date_to
            csv_content = r.text
            csv_reader = csv.DictReader(io.StringIO(csv_content), delimiter=';')

            # Словарь для аккумуляции расходов кампании по датам
            campaign_spend_by_date = {}  # {date: total_spend}

            for row in csv_reader:
                # Колонка "Дата" содержит дату в формате ГГГГ-ММ-ДД
                row_date = row.get('Дата', '').strip()

                if not row_date:
                    continue

                # Парсим расход за эту дату
                spend_str = row.get('Расход', '0').strip().replace(',', '.')
                try:
                    day_spend = float(spend_str)
                    campaign_spend_by_date[row_date] = campaign_spend_by_date.get(row_date, 0.0) + day_spend
                except (ValueError, TypeError):
                    pass

            if not campaign_spend_by_date:
                print(f"     ℹ️  Нет данных о расходах в CSV")
                continue

            print(f"     💰 Найдено дат с расходами: {len(campaign_spend_by_date)}")
            for date, spend in sorted(campaign_spend_by_date.items()):
                print(f"        {date}: {spend:.2f}₽")

            # 2.2. Получаем товары в кампании
            # ⚠️ ВАЖНО: Разные эндпоинты для разных типов кампаний!
            # - SKU, BANNER: GET /api/client/campaign/{id}/v2/products
            # - SEARCH_PROMO: POST /api/client/campaign/search_promo/v2/products

            products = []
            search_promo_spend_by_date_sku = {}  # {date: {sku: spend}}

            if campaign_type == "SEARCH_PROMO":
                # Для "Оплата за заказ" загружаем товары из отчёта по ЗАКАЗАМ
                # ⚠️ ВАЖНО: Расходы SEARCH_PROMO привязаны к заказам, а не к списку товаров кампании
                search_promo_spend_by_date_sku = load_search_promo_products_async(date_from, date_to, headers)

                # Если получили данные из отчёта по заказам, извлекаем список SKU
                if search_promo_spend_by_date_sku:
                    # Собираем уникальные SKU из всех дат
                    all_skus = set()
                    for date_skus in search_promo_spend_by_date_sku.values():
                        all_skus.update(date_skus.keys())

                    # Создаём products список для совместимости со старым кодом
                    products = [{"sku": sku} for sku in all_skus]
                    print(f"     ✅ Загружено {len(products)} товаров из отчёта по заказам")

            else:
                # Для SKU и BANNER используем стандартный эндпоинт
                products_url = f"https://api-performance.ozon.ru/api/client/campaign/{campaign_id}/v2/products"

                r = requests.get(products_url, headers=headers, timeout=15)

                if r.status_code != 200:
                    print(f"     ⚠️  Ошибка получения товаров (status={r.status_code})")
                    continue

                products_data = r.json()
                products = products_data.get("products", [])

            if not products:
                # Если у кампании нет товаров, но есть расходы
                if campaign_spend_by_date:
                    print(f"     ⚠️  В кампании нет товаров, но есть расходы")
                    print(f"     💡 Распределяем расходы между всеми товарами магазина")

                    # Загружаем ВСЕ товары магазина для распределения
                    # Это товары из текущей БД (уже загружены sync_products)
                    # Для простоты - отложим распределение, сохраним как общий расход кампании
                    # TODO: Реализовать распределение между товарами из БД
                    print(f"     ℹ️  Функция распределения в разработке - расходы будут учтены позже")
                else:
                    print(f"     ⚠️  В кампании нет товаров и нет расходов → пропускаем")

                continue

            print(f"     📦 Товаров в кампании: {len(products)}")

            # 2.3. Распределяем расход между товарами для КАЖДОЙ даты

            if campaign_type == "SEARCH_PROMO" and search_promo_spend_by_date_sku:
                # Для SEARCH_PROMO используем ТОЧНЫЕ данные из отчёта по заказам
                # У нас есть реальные расходы по каждому SKU, не нужно распределять поровну
                print(f"     💡 Используем точные данные из отчёта по заказам")

                for date, sku_spends in search_promo_spend_by_date_sku.items():
                    # Инициализируем словарь для этой даты, если его ещё нет
                    if date not in spend_by_date:
                        spend_by_date[date] = {}

                    for sku, spend in sku_spends.items():
                        spend_by_date[date][sku] = spend_by_date[date].get(sku, 0) + spend

                print(f"     ✅ Загружено точных расходов: {len(search_promo_spend_by_date_sku)} дат")

            else:
                # Для SKU и BANNER распределяем поровну между товарами
                # (можно улучшить пропорционально кликам)
                for date, total_spend in campaign_spend_by_date.items():
                    spend_per_product = total_spend / len(products)

                    # Инициализируем словарь для этой даты, если его ещё нет
                    if date not in spend_by_date:
                        spend_by_date[date] = {}

                    for product in products:
                        sku_str = product.get("sku", "")
                        try:
                            sku = int(sku_str)
                            spend_by_date[date][sku] = spend_by_date[date].get(sku, 0) + spend_per_product
                        except (ValueError, TypeError):
                            continue

                print(f"     ✅ Расход распределен по {len(campaign_spend_by_date)} датам")

        if spend_by_date:
            total_dates = len(spend_by_date)
            total_skus = sum(len(skus) for skus in spend_by_date.values())
            print(f"\n  ✅ Загружено расходов: {total_dates} дат, {total_skus} товаров (уникальных SKU)")

            # Примеры данных
            for date in sorted(spend_by_date.keys())[:3]:
                skus = spend_by_date[date]
                examples = list(skus.items())[:2]
                print(f"     {date}: {len(skus)} товаров, примеры: {[(sku, f'{spend:.2f}₽') for sku, spend in examples]}")
        else:
            print(f"\n  ⚠️  Нет данных о расходах")

        return spend_by_date

    except Exception as e:
        print(f"  ❌ Ошибка при загрузке расходов рекламы: {e}")
        import traceback
        traceback.print_exc()
        return {}



def load_avg_positions():
    """Загрузка средней позиции товаров в категории через /v1/analytics/data"""
    print("\n📊 Загрузка позиций товаров в категории...")
    
    try:
        snapshot_date = get_snapshot_date()
        d0 = datetime.fromisoformat(snapshot_date).date()
        d1 = d0 + timedelta(days=1)
        
        data = {
            "date_from": d0.isoformat(),
            "date_to": d1.isoformat(),
            "metrics": ["position_category"],  # ✅ Правильная метрика для позиций
            "dimension": ["sku"],
            "limit": 1000,
            "offset": 0
        }
        
        print(f"  📅 Диапазон: {d0.isoformat()} → {d1.isoformat()}")
        
        r = requests.post(
            f"{OZON_HOST}/v1/analytics/data",
            json=data,
            headers=get_ozon_headers(),
            timeout=15
        )
        
        print(f"  📥 /v1/analytics/data position_category status={r.status_code}")
        
        if r.status_code != 200:
            j = r.json()
            msg = j.get("message") or j.get("error") or str(j)
            print(f"  ⚠️ Ошибка: {msg}")
            return {}
        
        j = r.json()
        result = j.get("result") or {}
        rows = result.get("data") or []
        
        # DEBUG
        totals = result.get("totals")
        print(f"  🔎 totals={totals}, data_len={len(rows)}")
        
        if not rows:
            print(f"  ⚠️ Нет данных о позициях")
            return {}
        
        if rows:
            print(f"  🔍 DEBUG первая строка: {json.dumps(rows[0], ensure_ascii=False)[:800]}")
        
        avg_positions = {}
        for row in rows:
            dims = row.get("dimensions") or []
            mets = row.get("metrics") or []
            
            if not mets:
                continue
            
            # Ищем SKU (число)
            sku = None
            for d in (dims or []):
                _id = (d or {}).get("id")
                if _id is None:
                    continue
                try:
                    sku = int(_id)
                    break
                except (TypeError, ValueError):
                    continue
            
            if sku is None:
                continue
            
            try:
                position = float(mets[0] or 0)
                avg_positions[sku] = position
            except (TypeError, ValueError):
                continue
        
        if avg_positions:
            print(f"  ✅ Загружено позиций: {len(avg_positions)} sku")
            examples = list(avg_positions.items())[:3]
            print(f"     Примеры: {examples}")
        else:
            print(f"  ⚠️ Позиции не найдены в ответе")
        
        return avg_positions
        
    except Exception as e:
        print(f"  ❌ Ошибка при загрузке позиций: {e}")
        import traceback
        traceback.print_exc()
        return {}


def load_conversion():
    """Загрузка конверсии (CTR) - запрашиваем обе метрики и считаем CTR вручную"""
    print("\n📊 Загрузка конверсии CTR (поиск→карточка)...")
    
    try:
        snapshot_date = get_snapshot_date()
        d0 = datetime.fromisoformat(snapshot_date).date()
        d1 = d0 + timedelta(days=1)
        
        # Запрашиваем ОБЕ метрики одновременно
        data = {
            "date_from": d0.isoformat(),
            "date_to": d1.isoformat(),
            "metrics": ["hits_view_search", "hits_view_search_pdp"],  # Показы и переходы в карточку
            "dimension": ["sku"],
            "limit": 1000,
            "offset": 0
        }
        
        print(f"  📅 Период: {d0.isoformat()} → {d1.isoformat()}")
        
        r = requests.post(
            f"{OZON_HOST}/v1/analytics/data",
            json=data,
            headers=get_ozon_headers(),
            timeout=15
        )
        
        print(f"  📥 /v1/analytics/data CTR status={r.status_code}")
        
        if r.status_code != 200:
            j = r.json()
            msg = j.get("message") or j.get("error") or str(j)
            print(f"  ⚠️ Ошибка: {msg}")
            return {}
        
        j = r.json()
        result = j.get("result") or {}
        rows = result.get("data") or []
        
        # DEBUG
        totals = result.get("totals")
        print(f"  🔎 totals={totals} (2 метрики), data_len={len(rows)}")
        if rows:
            print(f"  🔍 DEBUG: metrics в первой строке = {rows[0].get('metrics', [])}")
        
        if not rows:
            print(f"  ⚠️ Нет данных о конверсии")
            return {}
        
        ctr_by_sku = {}
        
        for row in rows:
            dims = row.get("dimensions") or []
            mets = row.get("metrics") or []
            
            # Нужны ОБА значения: hits_view_search и hits_view_search_pdp
            if len(mets) < 2:
                continue
            
            # Ищем SKU (число)
            sku = None
            for d in (dims or []):
                _id = (d or {}).get("id")
                if _id is None:
                    continue
                try:
                    sku = int(_id)
                    break
                except (TypeError, ValueError):
                    continue
            
            if sku is None:
                continue
            
            try:
                views = float(mets[0] or 0)        # hits_view_search (показы в поиске)
                clicks = float(mets[1] or 0)       # hits_view_search_pdp (переходы в карточку)
                
                # CTR = (Переходы / Показы) * 100
                if views > 0:
                    ctr = round((clicks / views) * 100, 2)
                else:
                    ctr = 0.0
                
                ctr_by_sku[sku] = ctr
            except (TypeError, ValueError):
                continue
        
        if ctr_by_sku:
            print(f"  ✅ Загружено CTR: {len(ctr_by_sku)} sku")
            examples = list(ctr_by_sku.items())[:3]
            print(f"     Примеры: {examples}")
        else:
            print(f"  ⚠️ CTR не найден в ответе")
        
        return ctr_by_sku
        
    except Exception as e:
        print(f"  ❌ Ошибка при загрузке CTR: {e}")
        import traceback
        traceback.print_exc()
        return {}


def load_hits_view_search():
    """Загрузка показов в поиске и каталоге"""
    print("\n📊 Загрузка показов (поиск+категория)...")
    
    try:
        snapshot_date = get_snapshot_date()
        d0 = datetime.fromisoformat(snapshot_date).date()
        d1 = d0 + timedelta(days=1)
        
        impressions_by_sku = {}
        offset = 0
        total_loaded = 0

        while True:
            payload = {
                "date_from": d0.isoformat(),
                "date_to": d1.isoformat(),
                "metrics": ["hits_view_search"],
                "dimension": ["sku"],
                "limit": 1000,
                "offset": offset
            }

            r = requests.post(
                f"{OZON_HOST}/v1/analytics/data",
                json=payload,
                headers=get_ozon_headers(),
                timeout=25
            )

            print(f"  📥 /v1/analytics/data {d0.isoformat()}→{d1.isoformat()} offset={offset} status={r.status_code}")

            if r.status_code != 200:
                j = r.json()
                if j.get("message"):
                    print(f"  📋 {j.get('message')}")
                return {}

            j = r.json()
            result = j.get("result") or {}
            rows = result.get("data") or []
            
            if offset == 0:
                totals = result.get("totals")
                print(f"  🔎 totals={totals}, data_len={len(rows)}")

            if not rows:
                break

            for row in rows:
                dims = row.get("dimensions") or []
                mets = row.get("metrics") or []
                if not mets:
                    continue

                sku = None
                for d in (dims or []):
                    _id = (d or {}).get("id")
                    if _id is None:
                        continue
                    try:
                        sku = int(_id)
                        break
                    except (TypeError, ValueError):
                        continue

                if sku is None:
                    continue

                try:
                    impressions_by_sku[sku] = impressions_by_sku.get(sku, 0) + int(mets[0] or 0)
                except (TypeError, ValueError):
                    pass

            total_loaded += len(rows)
            offset += 1000

        print(f"  ✓ Загружено {total_loaded} строк")
        if impressions_by_sku:
            print(f"  ✅ Показы: {len(impressions_by_sku)} sku")
        return impressions_by_sku
        
    except Exception as e:
        print(f"  ❌ Ошибка при загрузке показов: {e}")
        return {}


def load_hits_view_search_pdp():
    """Загрузка переходов в карточку (посещения PDP)"""
    print("\n📊 Загрузка посещений карточки товара (PDP)...")
    
    try:
        snapshot_date = get_snapshot_date()
        d0 = datetime.fromisoformat(snapshot_date).date()
        d1 = d0 + timedelta(days=1)
        
        pdp_by_sku = {}
        offset = 0

        # ✅ Правильная метрика для посещений карточки
        payload = {
            "date_from": d0.isoformat(),
            "date_to": d1.isoformat(),
            "metrics": ["session_view_pdp"],  # ✅ session_view_pdp - посещения карточки
            "dimension": ["sku"],
            "limit": 1000,
            "offset": offset
        }
        
        print(f"  📝 Метрика: {payload.get('metrics')}")

        while True:
            payload["offset"] = offset  # Обновляем offset в payload перед каждым запросом
            r = requests.post(
                f"{OZON_HOST}/v1/analytics/data",
                json=payload,
                headers=get_ozon_headers(),
                timeout=25
            )

            print(f"  📥 /v1/analytics/data session_view_pdp offset={offset} status={r.status_code}")

            if r.status_code != 200:
                j = r.json()
                if j.get("message"):
                    print(f"  ⚠️ {j.get('message')}")
                # Если ошибка - возвращаем что успели загрузить
                if pdp_by_sku:
                    break
                return {}

            j = r.json()
            result = j.get("result") or {}
            rows = result.get("data") or []

            # ✅ Правильное условие остановки
            if not rows:
                print(f"  ✓ Конец данных при offset={offset}")
                break
            
            if len(rows) < 1000:
                print(f"  ✓ Последняя страница ({len(rows)} строк)")


            for row in rows:
                dims = row.get("dimensions") or []
                mets = row.get("metrics") or []
                if not mets:
                    continue

                sku = None
                for d in (dims or []):
                    _id = (d or {}).get("id")
                    try:
                        sku = int(_id)
                        break
                    except (TypeError, ValueError):
                        continue

                if sku is None:
                    continue

                try:
                    pdp_by_sku[sku] = pdp_by_sku.get(sku, 0) + int(mets[0] or 0)
                except (TypeError, ValueError):
                    pass

            offset += 1000

        if pdp_by_sku:
            print(f"  ✅ Переходы в карточку: {len(pdp_by_sku)} sku")
        else:
            print(f"  ⚠️ Нет данных по переходам в карточку")
        return pdp_by_sku
        
    except Exception as e:
        print(f"  ❌ Ошибка при загрузке PDP: {e}")
        import traceback
        traceback.print_exc()
        return {}


def load_hits_add_to_cart():
    """Загрузка добавлений в корзину (hits_tocart_pdp)"""
    print("\n📊 Загрузка добавлений в корзину (hits_tocart_pdp)...")
    
    try:
        snapshot_date = get_snapshot_date()
        d0 = datetime.fromisoformat(snapshot_date).date()
        d1 = d0 + timedelta(days=1)
        
        # 🧪 ТЕСТОВЫЙ ЗАПРОС - без циклов, просто проверяем метрику
        payload = {
            "date_from": d0.isoformat(),
            "date_to": d1.isoformat(),
            "metrics": ["hits_tocart_pdp"],  # ✅ Новая метрика
            "dimension": ["sku"],
            "limit": 1000,
            "offset": 0
        }

        print(f"  🧾 TEST payload: {json.dumps(payload, ensure_ascii=False)}")
        print(f"  🧾 metrics: {payload['metrics']}")

        r = requests.post(
            f"{OZON_HOST}/v1/analytics/data",
            json=payload,
            headers=get_ozon_headers(),
            timeout=25
        )

        print(f"  📥 /v1/analytics/data hits_tocart_pdp status={r.status_code}")

        if r.status_code != 200:
            j = r.json()
            print(f"  ❌ API Error: {json.dumps(j, ensure_ascii=False)[:800]}")
            return {}

        j = r.json()
        result = j.get("result") or {}
        rows = result.get("data") or []
        
        print(f"  📊 Получено {len(rows)} строк")
        if rows:
            print(f"  🔍 Первая строка: {json.dumps(rows[0], ensure_ascii=False)[:300]}")
            mets = rows[0].get("metrics", [])
            print(f"  🔍 metrics в первой строке: {mets}, type: {type(mets)}")
        
        cart_by_sku = {}
        
        for row in rows:
            dims = row.get("dimensions") or []
            mets = row.get("metrics") or []
            if not mets:
                continue

            sku = None
            for d in (dims or []):
                _id = (d or {}).get("id")
                try:
                    sku = int(_id)
                    break
                except (TypeError, ValueError):
                    continue

            if sku is None:
                continue

            try:
                cart_by_sku[sku] = cart_by_sku.get(sku, 0) + int(mets[0] or 0)
            except (TypeError, ValueError):
                pass

        if cart_by_sku:
            print(f"  ✅ Добавлений в корзину: {len(cart_by_sku)} sku")
            examples = list(cart_by_sku.items())[:3]
            print(f"     Примеры: {examples}")
        else:
            print(f"  ⚠️ Нет данных по добавлениям в корзину")
        return cart_by_sku
        
    except Exception as e:
        print(f"  ❌ Ошибка при загрузке hits_tocart_pdp: {e}")
        import traceback
        traceback.print_exc()
        return {}
    """Показы из /v1/analytics/data - требует Premium Plus подписку"""
    print("\n📊 Загрузка показов (поиск+категория)...")
    
    try:
        from zoneinfo import ZoneInfo
        MSK = ZoneInfo("Europe/Moscow")
        today_msk = datetime.now(MSK).date()
    except Exception:
        today_msk = datetime.now().date()

    def fetch_range(date_from: str, date_to: str):
        impressions_by_sku = {}
        offset = 0
        total_loaded = 0

        while True:
            payload = {
                "date_from": date_from,
                "date_to": date_to,
                "metrics": ["hits_view_search"],  # ✅ Показы в поиске + каталоге
                "dimension": ["sku"],
                "limit": 1000,
                "offset": offset
            }

            r = requests.post(
                f"{OZON_HOST}/v1/analytics/data",
                json=payload,
                headers=get_ozon_headers(),
                timeout=25
            )

            print(f"  📥 /v1/analytics/data {date_from}→{date_to} offset={offset} status={r.status_code}")

            if r.status_code != 200:
                j = r.json()
                if j.get("message"):
                    print(f"  📋 {j.get('message')}")
                return {}, False

            j = r.json()
            result = j.get("result") or {}
            rows = result.get("data") or []
            
            # DEBUG на первой странице
            if offset == 0:
                totals = result.get("totals")
                print(f"  🔎 totals={totals} (сумма по всем SKU за период)")
                print(f"  🔎 data_len={len(rows)} (количество SKU с данными)")
                if rows:
                    first_metrics = rows[0].get("metrics", [])
                    print(f"  🔍 DEBUG: первая строка метрика={first_metrics[0] if first_metrics else 'нет'}")
                    print(f"  🔍 DEBUG: полная первая строка: {json.dumps(rows[0], ensure_ascii=False)[:1200]}")

            if not rows:
                break

            for row in rows:
                dims = row.get("dimensions") or []
                mets = row.get("metrics") or []
                if not mets:
                    continue

                # Ищем SKU (число)
                sku = None
                for d in (dims or []):
                    _id = (d or {}).get("id")
                    if _id is None:
                        continue
                    try:
                        sku = int(_id)
                        break
                    except (TypeError, ValueError):
                        continue

                if sku is None:
                    continue

                try:
                    impressions_by_sku[sku] = impressions_by_sku.get(sku, 0) + int(mets[0] or 0)
                except (TypeError, ValueError):
                    pass

            total_loaded += len(rows)
            offset += 1000

        print(f"  ✓ Загружено {total_loaded} строк, {len(impressions_by_sku)} уникальных SKU")
        if impressions_by_sku:
            sum_impressions = sum(impressions_by_sku.values())
            print(f"  📊 СУММА показов по всем SKU: {sum_impressions}")
        return impressions_by_sku, True

    # Загружаем за СЕГОДНЯ (текущий день)
    d0 = today_msk  # Сегодня (начало дня)
    d1 = today_msk + timedelta(days=1)  # Завтра (начало)
    print(f"  📅 Период: {d0.isoformat()} (включая) → {d1.isoformat()} (не включая)")
    imp, ok = fetch_range(d0.isoformat(), d1.isoformat())
    if ok and imp:
        print(f"  ✅ Показы (сегодня): {len(imp)} sku")
        examples = list(imp.items())[:3]
        print(f"     Примеры: {examples}")
        return imp
    
    # Fallback: если за сегодня нет, пробуем вчера
    print(f"  ⚠️ За сегодня нет данных, пробую вчера...")
    y0 = today_msk - timedelta(days=1)
    y1 = today_msk
    print(f"  📅 Период: {y0.isoformat()} (включая) → {y1.isoformat()} (не включая)")
    imp, ok = fetch_range(y0.isoformat(), y1.isoformat())
    if ok and imp:
        print(f"  ✅ Показы (вчера): {len(imp)} sku")
        examples = list(imp.items())[:3]
        print(f"     Примеры: {examples}")
        return imp

    print("  ⚠️ Нет данных показов")
    return {}


def load_fbo_orders():
    """Загрузка активных FBO заказов за ТЕКУЩИЙ ДЕНЬ: SKU -> qty"""
    print("\n📦 Загрузка активных заказов FBO за текущий день...")

    orders_by_sku = {}

    # Статусы для FBO (только активные заказы):
    # awaiting_packaging - ожидают сборки
    # awaiting_deliver - ожидают отгрузки (это реальный статус в API, не awaiting_approve!)
    # delivering - доставляются
    statuses = ["awaiting_packaging", "awaiting_deliver", "delivering"]

    from datetime import datetime, timedelta, timezone
    
    # ✅ ТЕКУЩИЙ ДЕНЬ (по МСК)
    try:
        from zoneinfo import ZoneInfo
        MSK = ZoneInfo("Europe/Moscow")
        today_msk = datetime.now(MSK).date()
    except Exception:
        today_msk = datetime.now().date()
    
    # Начало текущего дня в UTC
    day_start_msk = datetime.combine(today_msk, datetime.min.time())
    day_start_utc = day_start_msk.astimezone(timezone.utc)
    
    # Конец текущего дня в UTC
    day_end_msk = datetime.combine(today_msk + timedelta(days=1), datetime.min.time())
    day_end_utc = day_end_msk.astimezone(timezone.utc)
    
    from_dt = day_start_utc
    to_dt = day_end_utc
    since_str = from_dt.isoformat().replace("+00:00", "Z")
    to_str = to_dt.isoformat().replace("+00:00", "Z")
    
    print(f"  📅 Период: {today_msk.isoformat()} (текущий день по МСК)")
    print(f"  ⏰ UTC: {since_str} → {to_str}")

    def post_json(path, payload, timeout=20):
        r = requests.post(
            f"{OZON_HOST}{path}",
            json=payload,
            headers=get_ozon_headers(),
            timeout=timeout
        )
        return r

    # ✅ ШАГ 1: Собираем список отправлений (posting_number)
    posting_numbers = []
    status_counter = {}  # ✅ Логирование статусов
    
    for status in statuses:
        print(f"  🔍 Статус: {status}")
        offset = 0

        while True:
            data = {
                "filter": {
                    "status": status,
                    "since": since_str,
                    "to": to_str
                },
                "limit": 50,
                "offset": offset
            }

            r = post_json("/v2/posting/fbo/list", data)

            if offset == 0:
                print(f"    📥 /v2/posting/fbo/list статус={r.status_code}")
                if r.status_code != 200:
                    print(f"    📋 Ошибка: {r.text[:500]}")
                    break

            if r.status_code != 200:
                break

            j = r.json()
            
            # ✅ DEBUG: показываем структуру первого ответа
            if offset == 0:
                print(f"    🔍 Структура ответа:")
                print(f"       Тип result: {type(j.get('result'))}")
                print(f"       Первые 300 символов: {str(j)[:300]}")
            
            # Обработка в зависимости от структуры
            result = j.get("result", {})
            if isinstance(result, list):
                postings = result
            else:
                postings = result.get("postings", [])

            
            if not postings:
                if offset == 0:
                    print(f"    ℹ️  Нет отправлений со статусом {status}")
                break

            print(f"    ✓ Получено {len(postings)} отправлений")

            # Вытаскиваем posting_number и логируем статусы
            for p in postings:
                pn = p.get("posting_number")
                if pn:
                    posting_numbers.append(pn)
                
                # ✅ Логируем реальный статус
                st = p.get("status")
                if st:
                    status_counter[st] = status_counter.get(st, 0) + 1

            offset += 50

    # ✅ Выводим найденные статусы
    if status_counter:
        print(f"\n  📌 Найденные статусы в ответах: {status_counter}\n")

    # Уникализируем
    posting_numbers = list(dict.fromkeys(posting_numbers))
    print(f"  ✅ Всего уникальных отправлений: {len(posting_numbers)}")

    if not posting_numbers:
        print(f"  ⚠️  Нет активных отправлений FBO")
        return orders_by_sku

    # ✅ ШАГ 2: Детализируем каждое отправление через /v2/posting/fbo/get
    print(f"  📊 Загрузка деталей отправлений...")
    successful = 0
    for i, posting_number in enumerate(posting_numbers, 1):
        if i % 10 == 0:
            print(f"    ✓ Обработано {i}/{len(posting_numbers)}")
        
        data = {"posting_number": posting_number}
        r = post_json("/v2/posting/fbo/get", data, timeout=25)

        if r.status_code != 200:
            continue

        j = r.json()
        
        # ✅ DEBUG первого posting
        if i == 1:
            print(f"\n  🔍 DEBUG fbo/get для первого posting:")
            print(f"     result keys: {list(j.get('result', {}).keys())}")
            print(f"     sample: {json.dumps(j.get('result', {}), ensure_ascii=False)[:1500]}\n")
        
        posting = j.get("result", {})
        products = posting.get("products", []) or []

        # Суммируем товары
        for pr in products:
            sku = pr.get("sku")
            qty = pr.get("quantity", 0)
            
            if sku:
                try:
                    qty = int(qty)
                except (TypeError, ValueError):
                    qty = 0
                
                orders_by_sku[sku] = orders_by_sku.get(sku, 0) + qty
                successful += 1

    print(f"  ✅ Обработано отправлений: {i}")
    print(f"  ✅ Всего SKU с активными заказами: {len(orders_by_sku)}")
    if orders_by_sku:
        examples = list(orders_by_sku.items())[:5]
        print(f"     Примеры: {examples}")
    
    return orders_by_sku


def load_fbo_supply_orders():
    """
    Загрузка заявок на поставку FBO из Ozon API.

    Возвращает два словаря:
    - in_transit: {sku: qty} - товары "в пути" (статусы ACCEPTED, IN_PROCESS)
    - in_draft: {sku: qty} - товары "в заявках" (статусы NEW, FILLING_DELIVERY_DETAILS)
    """
    print("\n📦 Загрузка заявок на поставку FBO...")

    in_transit = {}  # Товары в пути
    in_draft = {}    # Товары в заявках/черновиках

    try:
        # Запрос списка заявок на поставку
        # API: /v3/supply-order/list
        data = {
            "limit": 100,
            "offset": 0,
            "sort_by": 1,  # 1 = сортировка по дате создания
            "filter": {
                "states": [
                    "NEW",
                    "FILLING_DELIVERY_DETAILS",
                    "READY_TO_SUPPLY",
                    "ACCEPTED",
                    "IN_PROCESS",
                    "COURIER_ASSIGNED",
                    "COURIER_PICKED_UP",
                    "IN_TRANSIT_TO_STORAGE_WAREHOUSE",
                    "ACCEPTANCE_AT_STORAGE_WAREHOUSE"
                ]
            }
        }

        all_orders = []
        offset = 0
        max_pages = 10  # Ограничение для безопасности

        while offset < max_pages * 100:
            data["offset"] = offset

            response = requests.post(
                f"{OZON_HOST}/v3/supply-order/list",
                json=data,
                headers=get_ozon_headers(),
                timeout=30
            )

            if response.status_code != 200:
                if offset == 0:
                    print(f"  ⚠️  Ошибка API /v3/supply-order/list: {response.status_code}")
                    print(f"     {response.text[:300]}")
                break

            result = response.json()
            orders = result.get("result", [])

            if not orders:
                break

            all_orders.extend(orders)

            if len(orders) < 100:
                break

            offset += 100

        if not all_orders:
            print("  ℹ️  Нет заявок на поставку")
            return in_transit, in_draft

        print(f"  📊 Найдено заявок: {len(all_orders)}")

        # Обрабатываем каждую заявку
        for order in all_orders:
            supply_order_id = order.get("supply_order_id")
            state = order.get("state", "")

            if not supply_order_id:
                continue

            # Получаем детали заявки через /v3/supply-order/get
            detail_data = {"supply_order_id": supply_order_id}

            detail_response = requests.post(
                f"{OZON_HOST}/v3/supply-order/get",
                json=detail_data,
                headers=get_ozon_headers(),
                timeout=30
            )

            if detail_response.status_code != 200:
                continue

            detail_result = detail_response.json()
            order_detail = detail_result.get("result", {})
            products = order_detail.get("products", [])

            # Определяем, куда записать товары в зависимости от статуса
            # Статусы "в пути": ACCEPTED, IN_PROCESS
            # Статусы "в заявках": NEW, FILLING_DELIVERY_DETAILS, COURIER_ASSIGNED

            target_dict = None
            if state in ["ACCEPTED", "IN_PROCESS", "COURIER_PICKED_UP", "IN_TRANSIT_TO_STORAGE_WAREHOUSE"]:
                target_dict = in_transit
            elif state in ["NEW", "FILLING_DELIVERY_DETAILS", "COURIER_ASSIGNED", "READY_TO_SUPPLY"]:
                target_dict = in_draft

            if target_dict is not None:
                for product in products:
                    sku = product.get("sku")
                    quantity = product.get("quantity", 0)

                    if sku:
                        try:
                            quantity = int(quantity)
                        except (TypeError, ValueError):
                            quantity = 0

                        target_dict[sku] = target_dict.get(sku, 0) + quantity

        print(f"  ✅ Товаров 'в пути': {len(in_transit)} SKU")
        print(f"  ✅ Товаров 'в заявках': {len(in_draft)} SKU")

        if in_transit:
            examples = list(in_transit.items())[:3]
            print(f"     Примеры (в пути): {examples}")

        if in_draft:
            examples = list(in_draft.items())[:3]
            print(f"     Примеры (в заявках): {examples}")

    except Exception as e:
        print(f"  ⚠️  Ошибка загрузки заявок на поставку: {e}")
        import traceback
        traceback.print_exc()

    return in_transit, in_draft


def load_product_prices(products_data=None):
    """
    Загрузка цен товаров через Seller API.

    API: POST /v4/product/info

    Параметры:
        products_data - словарь {sku: {...}} с данными товаров (опционально, если не передан - загружает из БД)

    Возвращает: {sku: {"price": цена_в_лк, "marketing_price": цена_на_сайте}}

    price - цена которую ставите в личном кабинете (до скидки)
    marketing_price - цена которую видит клиент на сайте (с учётом скидки)
    """
    print("\n💰 Загрузка цен товаров...")

    prices_by_sku = {}  # {sku: {"price": X, "marketing_price": Y}}

    try:
        # Получаем список всех SKU
        if products_data:
            # Используем переданный словарь товаров
            all_skus = list(products_data.keys())
        else:
            # Получаем из базы данных (для случая когда функция вызывается отдельно)
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute('SELECT DISTINCT sku FROM products WHERE sku IS NOT NULL')
            all_skus = [row[0] for row in cursor.fetchall()]
            conn.close()

        if not all_skus:
            print("  ⚠️  Нет товаров для загрузки цен")
            return prices_by_sku

        print(f"  📊 Загрузка цен для {len(all_skus)} товаров...")

        # ШАГ 1: Получаем mapping SKU → offer_id через /v3/product/info/list
        sku_to_offer_id = {}
        batch_size = 1000

        for i in range(0, len(all_skus), batch_size):
            batch_skus = all_skus[i:i + batch_size]

            response = requests.post(
                f"{OZON_HOST}/v3/product/info/list",
                json={"sku": batch_skus},
                headers=get_ozon_headers(),
                timeout=30
            )

            if response.status_code == 200:
                items = response.json().get("items", [])

                # DEBUG: выводим структуру первого товара для проверки полей
                if i == 0 and items and len(items) > 0:
                    print(f"  🔍 DEBUG структура ответа /v3/product/info/list:")
                    print(f"     Доступные поля: {items[0].keys()}")
                    # Проверяем есть ли рейтинг и отзывы
                    if 'rating' in items[0]:
                        print(f"     ✅ Найдено поле rating: {items[0].get('rating')}")
                    if 'rating_count' in items[0]:
                        print(f"     ✅ Найдено поле rating_count: {items[0].get('rating_count')}")

                for item in items:
                    sku = item.get("sku")
                    offer_id = item.get("offer_id")

                    # Извлекаем рейтинг и количество отзывов если есть
                    rating = item.get("rating", None)
                    review_count = item.get("rating_count", None)

                    if sku and offer_id:
                        sku_to_offer_id[sku] = {
                            "offer_id": offer_id,
                            "rating": rating,
                            "review_count": review_count
                        }

        print(f"  ✓ Получено {len(sku_to_offer_id)} offer_id (с рейтингом и отзывами)")

        # ШАГ 2: Получаем точные цены через /v5/product/info/prices
        all_offer_ids = [info["offer_id"] for info in sku_to_offer_id.values()]

        for i in range(0, len(all_offer_ids), batch_size):
            batch_offer_ids = all_offer_ids[i:i + batch_size]

            # /v5/product/info/prices содержит marketing_seller_price - точную "Вашу цену"
            data = {
                "filter": {
                    "offer_id": batch_offer_ids,
                    "product_id": [],
                    "visibility": "ALL"
                },
                "limit": 1000
            }

            response = requests.post(
                f"{OZON_HOST}/v5/product/info/prices",
                json=data,
                headers=get_ozon_headers(),
                timeout=30
            )

            if response.status_code != 200:
                print(f"  ⚠️  Ошибка API /v5/product/info/prices (batch {i // batch_size + 1}): {response.status_code}")
                print(f"     {response.text[:200]}")
                continue

            result = response.json()
            items = result.get("items", [])

            for item in items:
                offer_id = item.get("offer_id")
                if not offer_id:
                    continue

                # Находим SKU по offer_id
                sku = None
                sku_info = None
                for s, info in sku_to_offer_id.items():
                    if info["offer_id"] == offer_id:
                        sku = s
                        sku_info = info
                        break

                if not sku:
                    continue

                # Извлекаем цены из price объекта
                price_obj = item.get("price", {})
                price_indexes = item.get("price_indexes", {})

                # "Ваша цена" в ЛК (с учетом акций/бустинга) = marketing_seller_price
                marketing_seller_price = price_obj.get("marketing_seller_price", 0)

                # Цена на сайте (с Ozon картой) = минимальная цена из индекса
                external_index = price_indexes.get("external_index_data", {})
                website_price = external_index.get("min_price", 0)

                # Индекс цены (price_index) — строка типа "WITHOUT_INDEX", "PROFIT", "AVG_PROFIT" и т.д.
                # Находится в price_indexes.price_index
                price_index_value = price_indexes.get("price_index", None)

                # Конвертируем в float
                try:
                    seller_price = float(marketing_seller_price) if marketing_seller_price else 0
                    site_price = float(website_price) if website_price else 0
                except (ValueError, TypeError):
                    seller_price = 0
                    site_price = 0

                prices_by_sku[sku] = {
                    "price": seller_price,  # Цена в ЛК (Ваша цена с бустингом) - 19,492₽
                    "marketing_price": site_price,  # Цена на сайте (с Ozon картой) - 11,658₽
                    "rating": sku_info["rating"],  # Рейтинг товара
                    "review_count": sku_info["review_count"],  # Количество отзывов
                    "offer_id": offer_id,  # Артикул товара (текстовый, например "ABC-123")
                    "price_index": price_index_value  # Индекс цены (WITHOUT_INDEX, PROFIT, AVG_PROFIT и т.д.)
                }

            print(f"  ✓ Обработано {len(items)} товаров (batch {i // batch_size + 1})")

        print(f"  ✅ Загружено цен для {len(prices_by_sku)} товаров")

    except Exception as e:
        print(f"  ❌ Ошибка при загрузке цен: {e}")
        import traceback
        traceback.print_exc()

    return prices_by_sku


def parse_product_card(sku):
    """
    ============================================================================
    ПАРСИНГ КАРТОЧКИ ТОВАРА OZON
    ============================================================================

    Извлекает рейтинг и количество отзывов с карточки товара на сайте Ozon

    Args:
        sku (int): SKU товара

    Returns:
        dict: {'rating': float, 'review_count': int} или None при ошибке
    """
    try:
        url = f"https://www.ozon.ru/product/-{sku}/"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        }

        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code != 200:
            print(f"  ⚠️  Не удалось загрузить карточку SKU {sku}: статус {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, 'html.parser')

        # Ищем JSON данные в script тегах
        rating = None
        review_count = None

        # Вариант 1: Поиск в JSON данных внутри script тегов
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and 'aggregateRating' in data:
                    rating = float(data['aggregateRating'].get('ratingValue', 0))
                    review_count = int(data['aggregateRating'].get('reviewCount', 0))
                    break
            except:
                continue

        # Вариант 2: Поиск через регулярные выражения в HTML
        if rating is None or review_count is None:
            # Поиск рейтинга: обычно в формате "4.5" или "4,5"
            rating_match = re.search(r'"ratingValue["\s:]+([0-9]+[.,][0-9]+)', response.text)
            if rating_match:
                rating = float(rating_match.group(1).replace(',', '.'))

            # Поиск количества отзывов
            review_match = re.search(r'"reviewCount["\s:]+(\d+)', response.text)
            if review_match:
                review_count = int(review_match.group(1))

        if rating is not None and review_count is not None:
            print(f"  ✅ SKU {sku}: рейтинг={rating}, отзывов={review_count}")
            return {'rating': rating, 'review_count': review_count}
        else:
            print(f"  ⚠️  SKU {sku}: не удалось извлечь рейтинг или отзывы")
            return None

    except Exception as e:
        print(f"  ❌ Ошибка при парсинге карточки SKU {sku}: {e}")
        return None


def load_all_account_skus():
    """
    ============================================================================
    ПОЛУЧЕНИЕ ВСЕХ SKU АККАУНТА
    ============================================================================

    Запрашивает /v3/product/list (все активные товары), затем через
    /v3/product/info/list получает FBO SKU каждого товара.

    Возвращает:
        list[int]: Список всех SKU (включая товары без FBO остатков)
    """
    try:
        # Шаг 1: получаем все product_id из аккаунта
        resp = requests.post(
            f"{OZON_HOST}/v3/product/list",
            json={"filter": {"visibility": "ALL"}, "limit": 1000},
            headers=get_ozon_headers(),
            timeout=15
        )
        if resp.status_code != 200:
            print(f"  ⚠️  Ошибка /v3/product/list: {resp.status_code}")
            return [], {}

        items = resp.json().get("result", {}).get("items", [])
        if not items:
            return [], {}

        # Шаг 2: получаем SKU и имена через /v3/product/info/list по product_id
        product_ids = [it["product_id"] for it in items]
        all_skus = []
        sku_names = {}  # {sku: name} — имена товаров для заполнения products_data

        for i in range(0, len(product_ids), 100):
            batch = product_ids[i:i + 100]
            resp2 = requests.post(
                f"{OZON_HOST}/v3/product/info/list",
                json={"product_id": batch},
                headers=get_ozon_headers(),
                timeout=30
            )
            if resp2.status_code == 200:
                info_items = resp2.json().get("items", [])
                for it in info_items:
                    sku = it.get("sku")
                    if sku:
                        all_skus.append(sku)
                        sku_names[sku] = it.get("name", "")

        print(f"  📦 Всего товаров в аккаунте: {len(all_skus)} SKU")
        return all_skus, sku_names

    except Exception as e:
        print(f"  ❌ Ошибка load_all_account_skus: {e}")
        return [], {}


def load_fbo_analytics(cursor, conn, snapshot_date, sku_list=None):
    """
    ============================================================================
    ЗАГРУЗКА АНАЛИТИКИ FBO ПО КЛАСТЕРАМ
    ============================================================================

    Вызывает /v1/analytics/stocks для получения:
    - ADS (среднесуточные продажи) — общий по товару и по кластеру
    - IDC (дней до конца остатка по кластеру)
    - Дней без продаж (по кластеру)
    - Статус оборачиваемости (turnover_grade_cluster)
    - Остатки по кластерам (available_stock_count)

    API требует список SKU. Если sku_list передан — используем его,
    иначе берём из таблицы products.
    Каждая строка в ответе — один кластер для одного SKU.
    Данные сохраняются в таблицу fbo_analytics.
    """
    print("\n📊 Загрузка аналитики FBO по кластерам...")

    try:
        # Используем переданный список SKU или берём из БД
        if sku_list:
            all_skus = list(sku_list)
        else:
            cursor.execute('SELECT sku FROM products')
            all_skus = [row[0] for row in cursor.fetchall()]

        if not all_skus:
            print("  ⚠️  Нет товаров в БД — пропускаем загрузку аналитики")
            return

        print(f"  📦 SKU для запроса: {len(all_skus)}")

        # Очищаем старые данные за сегодня
        cursor.execute('DELETE FROM fbo_analytics WHERE snapshot_date = ?', (snapshot_date,))

        # Словарь для агрегации по (sku, cluster_name)
        # API возвращает по строке на КАЖДЫЙ СКЛАД внутри кластера.
        # Нужно объединить: суммировать stock, а метрики (ADS, IDC и т.д.)
        # одинаковые для всех складов в кластере — берём один раз.
        cluster_agg = {}  # ключ: (sku, cluster_name) -> {ads, idc, days, liq, stock}

        # API принимает до 100 SKU за раз
        for batch_start in range(0, len(all_skus), 100):
            batch_skus = all_skus[batch_start:batch_start + 100]
            offset = 0

            while True:
                response = requests.post(
                    f"{OZON_HOST}/v1/analytics/stocks",
                    json={
                        "limit": 100,
                        "offset": offset,
                        "warehouse_type": "FBO",
                        "skus": batch_skus
                    },
                    headers=get_ozon_headers(),
                    timeout=30
                )

                if response.status_code != 200:
                    print(f"  ⚠️  Ошибка API /v1/analytics/stocks: {response.status_code}")
                    if offset == 0:
                        print(f"     {response.text[:300]}")
                    break

                result = response.json()
                # Ответ: {"items": [...]} — каждый элемент = один СКЛАД для одного SKU
                items = result.get("items", [])

                if not items:
                    break

                for item in items:
                    sku = item.get("sku")
                    if not sku:
                        continue

                    cluster_name = item.get("cluster_name", "")
                    key = (sku, cluster_name)

                    # available_stock_count — остаток на КОНКРЕТНОМ складе
                    stock = int(item.get("available_stock_count", 0) or 0)

                    if key not in cluster_agg:
                        # Первый склад в этом кластере — записываем метрики
                        # ads_cluster, idc_cluster, days_without_sales_cluster,
                        # turnover_grade_cluster — одинаковые для всех складов кластера
                        cluster_agg[key] = {
                            'ads': float(item.get("ads_cluster", 0) or 0),
                            'idc': float(item.get("idc_cluster", 0) or 0),
                            'days_no_sales': int(item.get("days_without_sales_cluster", 0) or 0),
                            'liquidity': item.get("turnover_grade_cluster", ""),
                            'stock': stock
                        }
                    else:
                        # Ещё один склад в том же кластере — суммируем только stock
                        cluster_agg[key]['stock'] += stock

                if len(items) < 100:
                    break

                offset += 100

        # Записываем агрегированные данные в БД — одна строка на кластер
        total_rows = 0
        for (sku, cluster_name), data in cluster_agg.items():
            cursor.execute('''
                INSERT INTO fbo_analytics
                (sku, cluster_name, ads, idc, days_without_sales, liquidity_status, stock, snapshot_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (sku, cluster_name, data['ads'], data['idc'],
                  data['days_no_sales'], data['liquidity'], data['stock'], snapshot_date))
            total_rows += 1

        conn.commit()
        print(f"  ✅ Загружено {total_rows} кластеров (агрегировано из {sum(1 for _ in cluster_agg)} уник. пар)")

    except Exception as e:
        print(f"  ⚠️  Ошибка загрузки аналитики FBO: {e}")
        import traceback
        traceback.print_exc()


def sync_products():
    """
    ============================================================================
    ЦЕНТРАЛЬНАЯ ФУНКЦИЯ СИНХРОНИЗАЦИИ ВСЕХ ДАННЫХ
    ============================================================================

    ⚠️  ВАЖНО: Эта функция вызывается автоматически каждые 6 часов через cron!

    При добавлении НОВЫХ типов данных (новые API эндпоинты, новые метрики):
    1. Создай функцию load_новые_данные()
    2. Вызови её ЗДЕСЬ в sync_products()
    3. Добавь данные в INSERT запросы для products и products_history
    4. Данные автоматически будут обновляться каждые 6 часов

    Текущие источники данных:
    - Остатки FBO (/v2/analytics/stock_on_warehouses)
    - Заказы (load_fbo_orders)
    - Средние позиции (load_avg_positions)
    - Показы в поиске (load_hits_view_search)
    - Переходы в карточку (load_hits_view_search_pdp)
    - Добавления в корзину (load_hits_add_to_cart)
    - Расходы на рекламу (load_adv_spend_by_sku) с автообновлением истории
    - Расчетные метрики: CTR, CR1, CR2

    ============================================================================
    """
    print("\n📥 Загрузка остатков FBO из Ozon...")
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # ⚠️ НЕ удаляем products здесь — удалим перед записью, когда данные уже получены
        # Это защищает от ситуации когда API временно возвращает пустой ответ

        print("\n📊 Загрузка остатков...")
        
        products_data = {}  # sku -> {name, fbo_stock}
        warehouse_rows = []  # Для сохранения остатков по складам

        offset = 0
        while True:
            # ✅ ПРАВИЛЬНЫЙ запрос - БЕЗ filter, только warehouse_type!
            stocks_data = {
                "warehouse_type": "FBO",
                "limit": 1000,
                "offset": offset
            }
            
            print(f"  📤 Запрос: offset={offset}")
            
            stocks_response = requests.post(
                f"{OZON_HOST}/v2/analytics/stock_on_warehouses",
                json=stocks_data,
                headers=get_ozon_headers(),
                timeout=15
            )
            
            if stocks_response.status_code != 200:
                print(f"  ❌ Ошибка API: {stocks_response.status_code}")
                print(f"  📋 Ответ: {stocks_response.text}")
                conn.close()
                return False  # ✅ Не пишем частичные данные
            
            stocks_result = stocks_response.json()
            rows = stocks_result.get("result", {}).get("rows", [])

            # ✅ DEBUG: показываем структуру первой строки
            if offset == 0 and rows:
                print(f"\n  🔍 Структура первой строки:")
                print(f"     {json.dumps(rows[0], ensure_ascii=False, indent=6)}\n")

            if not rows:
                print(f"  ✓ Конец данных при offset={offset}")
                break
            
            print(f"  ✓ Получено {len(rows)} строк")
            
            # Обрабатываем строки - суммируем по SKU
            for row in rows:
                sku = row.get("sku")
                item_name = row.get("item_name", "")
                
                # ✅ ПРАВИЛЬНО: используем STOCK_FIELD из .env, с fallback
                # Иначе 0 считается False и мы берём значение из другого поля
                free_amount = row.get(STOCK_FIELD)  # ← используем то, что выбрал пользователь
                
                # fallback если STOCK_FIELD отсутствует/None
                if free_amount is None:
                    free_amount = row.get("free_to_sell_amount")
                if free_amount is None:
                    free_amount = row.get("available")
                if free_amount is None:
                    free_amount = row.get("present")
                if free_amount is None:
                    free_amount = 0
                
                # ✅ Конвертируем в int (иногда API отдаёт числа как строки)
                try:
                    free_amount = int(free_amount)
                except (TypeError, ValueError):
                    free_amount = 0
                
                if not sku:
                    continue
                
                # Суммируем остатки по всем FBO складам для одного SKU
                if sku not in products_data:
                    products_data[sku] = {
                        "name": item_name,
                        "fbo_stock": 0
                    }
                
                products_data[sku]["fbo_stock"] += free_amount

                # Сохраняем строку для таблицы fbo_warehouse_stock
                wh_name = row.get("warehouse_name", "Неизвестный склад")
                warehouse_rows.append((sku, wh_name, free_amount))
            
            offset += 1000
        
        print(f"\n  ✅ Всего уникальных товаров: {len(products_data)}")

        # Сохраняем остатки по складам в отдельную таблицу
        snapshot_date = get_snapshot_date()
        cursor.execute('DELETE FROM fbo_warehouse_stock WHERE snapshot_date = ?', (snapshot_date,))
        for wh_sku, wh_name, wh_stock in warehouse_rows:
            cursor.execute('''
                INSERT INTO fbo_warehouse_stock (sku, warehouse_name, stock, snapshot_date)
                VALUES (?, ?, ?, ?)
            ''', (wh_sku, wh_name, wh_stock, snapshot_date))
        conn.commit()
        print(f"  ✅ Сохранено {len(warehouse_rows)} строк по складам")

        # ============================================================================
        # ЗАГРУЗКА ДОПОЛНИТЕЛЬНЫХ ДАННЫХ
        # ============================================================================
        # ⚠️  ПРИ ДОБАВЛЕНИИ НОВЫХ ДАННЫХ: добавляй вызовы load_новые_данные() СЮДА

        # ✅ Загружаем заказы
        orders_by_sku = load_fbo_orders()

        # ✅ Загружаем заявки на поставку (В ПУТИ и В ЗАЯВКАХ)
        in_transit_by_sku, in_draft_by_sku = load_fbo_supply_orders()

        # ✅ Загружаем аналитику FBO (ADS, IDC, ликвидность по кластерам)
        # Получаем ВСЕ SKU аккаунта (не только те что на складах FBO),
        # чтобы на вкладке "Аналитика FBO" отображались все товары
        all_account_skus, sku_names = load_all_account_skus()

        # ✅ Если stock_on_warehouses вернул пустой результат, заполняем products_data
        # из all_account_skus, чтобы товары всё равно записались в БД с fbo_stock=0
        if not products_data and all_account_skus:
            print(f"\n  ⚠️  Остатки FBO пустые, но найдено {len(all_account_skus)} SKU в аккаунте")
            print(f"  📦 Создаём записи товаров с fbo_stock=0")
            for sku in all_account_skus:
                products_data[sku] = {
                    "name": sku_names.get(sku, ""),
                    "fbo_stock": 0
                }
        elif all_account_skus:
            # Добавляем SKU, которые есть в аккаунте, но отсутствуют в stock_on_warehouses
            missing_count = 0
            for sku in all_account_skus:
                if sku not in products_data:
                    products_data[sku] = {
                        "name": sku_names.get(sku, ""),
                        "fbo_stock": 0
                    }
                    missing_count += 1
            if missing_count > 0:
                print(f"  📦 Добавлено {missing_count} SKU без FBO остатков")

        # Объединяем с SKU из stock_on_warehouses (на случай если какой-то SKU не в product/list)
        combined_skus = list(set(list(products_data.keys()) + all_account_skus))
        load_fbo_analytics(cursor, conn, snapshot_date, sku_list=combined_skus)

        # ✅ Загружаем цены товаров
        prices_by_sku = load_product_prices(products_data)

        # ✅ Загружаем средние позиции
        avg_positions = load_avg_positions()
        
        # ✅ Загружаем показы в поиске и каталоге
        hits_view_search_data = load_hits_view_search()
        
        # ✅ Загружаем переходы в карточку
        hits_view_search_pdp_data = load_hits_view_search_pdp()
        
        # ✅ Загружаем добавления в корзину
        hits_tocart_pdp_data = load_hits_add_to_cart()



        # ✅ Определяем дату снимка по Белграду (YYYY-MM-DD) - ПЕРЕД использованием!
        snapshot_date = get_snapshot_date()
        snapshot_time = get_snapshot_time()

        # ✅ Загружаем расходы на рекламу за последние 7 дней (данные могут появляться с задержкой)
        date_to = snapshot_date
        date_from = (datetime.fromisoformat(snapshot_date) - timedelta(days=7)).date().isoformat()
        adv_spend_data = load_adv_spend_by_sku(date_from, date_to)

        # ✅ АВТОМАТИЧЕСКОЕ ОБНОВЛЕНИЕ: Обновляем расходы на рекламу для всех исторических дат
        # Формат adv_spend_data: {date: {sku: spend}}
        if adv_spend_data:
            print(f"\n📊 Обновление исторических расходов на рекламу...")
            updated_count = 0
            for date, skus_spend in adv_spend_data.items():
                for sku, spend in skus_spend.items():
                    # Обновляем ТОЛЬКО колонку adv_spend в products_history
                    cursor.execute('''
                        UPDATE products_history
                        SET adv_spend = ?
                        WHERE sku = ? AND snapshot_date = ?
                    ''', (spend, sku, date))
                    if cursor.rowcount > 0:
                        updated_count += 1

            conn.commit()
            print(f"  ✅ Обновлено записей: {updated_count}")

        # ✅ Очищаем products ТОЛЬКО когда есть данные для записи
        if products_data:
            cursor.execute('DELETE FROM products')
            conn.commit()
        else:
            print("  ⚠️  Нет данных для записи — оставляем старые products без изменений")

        # ✅ Пишем в обе таблицы
        for sku, data in products_data.items():
            orders_qty = orders_by_sku.get(sku, 0)
            avg_pos = avg_positions.get(sku, 0)

            # Показы и метрики
            views = int(hits_view_search_data.get(sku, 0) or 0)
            pdp = int(hits_view_search_pdp_data.get(sku, 0) or 0)
            cart = int(hits_tocart_pdp_data.get(sku, 0) or 0)
            adv_spend = float(adv_spend_data.get(snapshot_date, {}).get(sku, 0) or 0)

            # Поставки FBO
            in_transit = int(in_transit_by_sku.get(sku, 0))
            in_draft = int(in_draft_by_sku.get(sku, 0))

            # CTR = (посещения карточки / показы) * 100
            search_ctr = round((pdp / views * 100), 2) if views > 0 else 0.0
            
            # CR1 = (в корзину / посещения карточки) * 100
            cr1 = round((cart / pdp * 100), 2) if pdp > 0 else 0.0
            
            # CR2 = (заказы / в корзину) * 100
            cr2 = round((orders_qty / cart * 100), 2) if cart > 0 else 0.0

            # Цены и артикул товара
            price_data = prices_by_sku.get(sku, {})
            price = price_data.get("price", 0)
            marketing_price = price_data.get("marketing_price", 0)
            offer_id = price_data.get("offer_id", None)
            price_index = price_data.get("price_index", None)

            # Рейтинг и отзывы - пока оставляем пустыми
            # (парсинг не работает с сервера из-за блокировки Ozon)
            # Используйте API endpoint /api/update-rating для ручного обновления
            rating = None
            review_count = None

            # 1️⃣ Обновляем текущие остатки
            cursor.execute('''
                INSERT INTO products (sku, name, offer_id, fbo_stock, orders_qty, price, marketing_price, price_index, hits_view_search, hits_view_search_pdp, search_ctr, hits_add_to_cart, cr1, cr2, adv_spend, in_transit, in_draft, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sku) DO UPDATE SET
                    name=excluded.name,
                    offer_id=COALESCE(excluded.offer_id, products.offer_id),
                    fbo_stock=excluded.fbo_stock,
                    orders_qty=excluded.orders_qty,
                    price=excluded.price,
                    marketing_price=excluded.marketing_price,
                    price_index=COALESCE(excluded.price_index, products.price_index),
                    hits_view_search=excluded.hits_view_search,
                    hits_view_search_pdp=excluded.hits_view_search_pdp,
                    search_ctr=excluded.search_ctr,
                    hits_add_to_cart=excluded.hits_add_to_cart,
                    cr1=excluded.cr1,
                    cr2=excluded.cr2,
                    adv_spend=excluded.adv_spend,
                    in_transit=excluded.in_transit,
                    in_draft=excluded.in_draft,
                    updated_at=excluded.updated_at
            ''', (
                sku,
                data.get("name", ""),
                offer_id,
                data.get("fbo_stock", 0),
                orders_qty,
                price,
                marketing_price,
                price_index,
                views,
                pdp,
                search_ctr,
                cart,
                cr1,
                cr2,
                adv_spend,
                in_transit,
                in_draft,
                get_snapshot_time()
            ))
            
            # 2️⃣ Сохраняем в историю (один раз в день на SKU)
            cursor.execute('''
                INSERT INTO products_history (sku, name, offer_id, fbo_stock, orders_qty, rating, review_count, price, marketing_price, price_index, avg_position, hits_view_search, hits_view_search_pdp, search_ctr, hits_add_to_cart, cr1, cr2, adv_spend, in_transit, in_draft, snapshot_date, snapshot_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sku, snapshot_date) DO UPDATE SET
                    name=excluded.name,
                    offer_id=COALESCE(excluded.offer_id, products_history.offer_id),
                    fbo_stock=excluded.fbo_stock,
                    orders_qty=excluded.orders_qty,
                    rating=COALESCE(excluded.rating, products_history.rating),
                    review_count=COALESCE(excluded.review_count, products_history.review_count),
                    price=excluded.price,
                    marketing_price=excluded.marketing_price,
                    price_index=COALESCE(excluded.price_index, products_history.price_index),
                    avg_position=excluded.avg_position,
                    hits_view_search=excluded.hits_view_search,
                    hits_view_search_pdp=excluded.hits_view_search_pdp,
                    search_ctr=excluded.search_ctr,
                    hits_add_to_cart=excluded.hits_add_to_cart,
                    cr1=excluded.cr1,
                    cr2=excluded.cr2,
                    adv_spend=excluded.adv_spend,
                    in_transit=excluded.in_transit,
                    in_draft=excluded.in_draft,
                    snapshot_time=excluded.snapshot_time
            ''', (
                sku,
                data.get("name", ""),
                offer_id,
                data.get("fbo_stock", 0),
                orders_qty,
                rating,
                review_count,
                price,
                marketing_price,
                price_index,
                avg_pos,
                views,
                pdp,
                search_ctr,
                cart,
                cr1,
                cr2,
                adv_spend,
                in_transit,
                in_draft,
                snapshot_date,
                snapshot_time
            ))
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ Синхронизация завершена!")
        print(f"   📦 Товаров загружено: {len(products_data)}")
        print(f"   📅 История сохранена на дату: {snapshot_date}")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка подключения: {e}")
        try:
            conn.close()
        except:
            pass
        return False
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        try:
            conn.close()
        except:
            pass
        return False


# ============================================================================
# FLASK ПРИЛОЖЕНИЕ
# ============================================================================

app = Flask(__name__)

# ✅ Инициализация БД при старте (нужно для gunicorn, который не запускает __main__)
init_database()

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Ozon Товары FBO</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #ffffff;
            min-height: 100vh;
            padding: 20px;
            margin: 0;
        }

        .container {
            width: 100%;
            margin: 0 auto;
        }

        .header {
            background: white;
            padding: 4px 50px;
            border-radius: 12px;
            margin-bottom: 8px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            margin-left: -20px;
            margin-right: -20px;
        }

        .header h1 {
            color: #333;
            margin: 0;
            font-size: 24px;
        }

        .header p {
            color: #666;
            font-size: 14px;
            margin: 0;
        }

        .table-container {
            background: white;
            border-radius: 12px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            overflow: hidden;
            margin-left: -20px;
            margin-right: -20px;
        }

        .table-header {
            background: #f8f9fa;
            padding: 8px 30px;
            border-bottom: 2px solid #e9ecef;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .table-header h2 {
            color: #333;
            font-size: 18px;
        }

        .search-box input {
            padding: 10px 15px;
            border: 1px solid #ddd;
            border-radius: 6px;
            width: 250px;
            font-size: 14px;
        }

        .search-box input:focus {
            outline: none;
            border-color: #667eea;
        }

        table {
            width: 100%;
            border-collapse: collapse;
        }

        th {
            padding: 15px 30px;
            text-align: left;
            font-weight: 600;
            color: #333;
            background: #f8f9fa;
            border-bottom: 2px solid #e9ecef;
        }

        td {
            padding: 15px 30px;
            border-bottom: 1px solid #e9ecef;
            color: #333;
        }

        tbody tr:hover {
            background-color: #f8f9fa;
        }

        .sku {
            background: #f0f0f0;
            padding: 4px 8px;
            border-radius: 4px;
            font-family: monospace;
            font-size: 13px;
        }

        .stock {
            font-weight: 600;
            color: #667eea;
            font-size: 16px;
        }

        .stock.low {
            color: #ff6b6b;
        }

        .position {
            font-weight: 600;
            color: #ff6f00;
            font-size: 14px;
            background: #fff3e0;
            padding: 3px 6px;
            border-radius: 3px;
        }

        .loading {
            text-align: center;
            padding: 40px;
            color: #999;
        }

        .error {
            background: #fee;
            color: #c33;
            padding: 20px 30px;
            border-radius: 8px;
            margin: 20px 0;
        }

        .empty-state {
            text-align: center;
            padding: 60px 30px;
            color: #999;
        }

        .tabs {
            display: flex;
            gap: 0;
            border-bottom: 2px solid #e9ecef;
            margin: 0;
        }

        .tab-button {
            padding: 8px 30px;
            background: none;
            border: none;
            cursor: pointer;
            font-size: 15px;
            font-weight: 500;
            color: #666;
            border-bottom: 3px solid transparent;
            transition: all 0.3s;
        }

        .tab-button.active {
            color: #667eea;
            border-bottom-color: #667eea;
        }

        .tab-button:hover {
            color: #667eea;
        }

        .tab-content {
            display: none;
            padding: 20px 30px;
        }

        .tab-content.active {
            display: block;
        }

        .history-select {
            padding: 10px 15px;
            border: 1px solid #ddd;
            border-radius: 6px;
            font-size: 14px;
            width: 100%;
            max-width: 400px;
        }

        .history-select:focus {
            outline: none;
            border-color: #667eea;
        }

        .note-input {
            width: 100%;
            padding: 8px 12px;
            border: 1px solid #e9ecef;
            border-radius: 4px;
            font-size: 13px;
            font-family: inherit;
            transition: border-color 0.2s;
            display: none;
            min-height: 60px;
            resize: vertical;
        }

        .note-input.editing {
            display: block;
            border-color: #667eea;
            box-shadow: 0 0 0 2px rgba(102, 126, 234, 0.1);
        }

        .note-display {
            padding: 8px 12px;
            background: #f8f9fa;
            border-radius: 4px;
            min-height: 60px;
            word-wrap: break-word;
            overflow-wrap: break-word;
            word-break: break-word;
            white-space: pre-wrap;
            font-size: 13px;
            line-height: 1.5;
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: flex-start;
            text-align: left;
        }

        .note-display:hover {
            background: #e9ecef;
        }

        .note-content {
            flex: 1;
            word-wrap: break-word;
            white-space: pre-wrap;
        }

        .note-empty {
            color: #bbb;
            font-style: italic;
        }

        .edit-icon {
            width: 20px;
            height: 20px;
            cursor: pointer;
            margin-left: 10px;
            flex-shrink: 0;
            opacity: 0.6;
            transition: opacity 0.2s;
        }

        .edit-icon:hover {
            opacity: 1;
        }

        .note-cell {
            width: 200px;
            min-width: 200px;
            max-width: 200px;
            position: relative;
            word-wrap: break-word;
            overflow-wrap: break-word;
            text-align: left;
        }

        .note-display {
            padding: 12px;
            background: #f8f9fa;
            border-radius: 4px;
            min-height: 80px;
            word-wrap: break-word;
            overflow-wrap: break-word;
            word-break: break-word;
            white-space: pre-wrap;
            font-size: 13px;
            line-height: 1.5;
            cursor: pointer;
            display: flex;
            align-items: flex-start;
            transition: background 0.2s;
            border: 1px solid #e9ecef;
            text-align: left;
        }

        .note-display:hover {
            background: #e9ecef;
        }

        .note-edit-btn {
            position: absolute;
            top: 8px;
            right: 8px;
            background: none;
            border: none;
            font-size: 16px;
            cursor: pointer;
            padding: 4px 6px;
            opacity: 0.6;
            transition: opacity 0.2s;
            border-radius: 3px;
        }

        .note-edit-btn:hover {
            opacity: 1;
            background: rgba(102, 126, 234, 0.1);
        }

        .note-textarea {
            width: 100%;
            min-height: 100px;
            padding: 10px 12px;
            border: 2px solid #667eea;
            border-radius: 4px;
            font-size: 13px;
            font-family: inherit;
            resize: vertical;
            box-sizing: border-box;
        }

        .note-textarea:focus {
            outline: none;
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }

        .note-save-btn, .note-cancel-btn {
            padding: 6px 12px;
            font-size: 12px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-weight: 500;
            transition: all 0.2s;
        }

        .note-save-btn {
            background: #667eea;
            color: white;
        }

        .note-save-btn:hover {
            background: #5568d3;
        }

        .note-cancel-btn {
            background: #e9ecef;
            color: #333;
        }

        .note-cancel-btn:hover {
            background: #dde1e6;
        }

        .note-cell {
            display: flex;
            gap: 8px;
            align-items: flex-start;
            width: 200px;
            min-width: 200px;
            max-width: 200px;
            word-wrap: break-word;
            overflow-wrap: break-word;
            text-align: left;
        }

        .note-display {
            flex: 1;
            padding: 10px 12px;
            background: #f8f9fa;
            border: 1px solid #e9ecef;
            border-radius: 4px;
            font-size: 13px;
            line-height: 1.5;
            word-wrap: break-word;
            overflow-wrap: break-word;
            word-break: break-word;
            white-space: pre-wrap;
            min-height: 60px;
            cursor: pointer;
            transition: background-color 0.2s;
            text-align: left;
        }

        .note-display:hover {
            background: #f0f1f3;
        }

        .note-textarea {
            flex: 1;
            padding: 10px 12px;
            border: 2px solid #667eea;
            border-radius: 4px;
            font-size: 13px;
            font-family: inherit;
            line-height: 1.5;
            min-height: 60px;
            resize: none;
            word-wrap: break-word;
            overflow-wrap: break-word;
            white-space: pre-wrap;
            overflow: hidden;
        }

        .note-textarea:focus {
            outline: none;
            box-shadow: 0 0 0 2px rgba(102, 126, 234, 0.1);
        }

        .note-edit-btn {
            padding: 6px 8px;
            background: none;
            border: none;
            cursor: pointer;
            font-size: 16px;
            color: #999;
            transition: color 0.2s;
            margin-top: 2px;
        }

        .note-edit-btn:hover {
            color: #667eea;
        }

        .note-save-btn {
            padding: 6px 8px;
            background: #667eea;
            color: white;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 12px;
            transition: background 0.2s;
            margin-top: 2px;
        }

        .note-save-btn:hover {
            background: #5568d3;
        }

        .note-cancel-btn {
            padding: 6px 8px;
            background: #ddd;
            color: #333;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 12px;
            margin-top: 2px;
        }

        .note-cancel-btn:hover {
            background: #ccc;
        }

        .refresh-btn {
            background: rgba(102, 126, 234, 0.2);
            color: #667eea;
            border: none;
            padding: 8px 16px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 500;
        }

        .refresh-btn:hover {
            background: rgba(102, 126, 234, 0.3);
        }

        @media (max-width: 768px) {
            .table-header {
                flex-direction: column;
                gap: 15px;
            }

            .search-box input {
                width: 100%;
            }

            table {
                font-size: 12px;
            }

            th, td {
                padding: 10px 15px;
            }
        }

        /* Стили для таблицы со скроллом */
        #history-content {
            position: relative;
        }

        .table-wrapper {
            overflow-x: auto;
            max-height: 600px;
            overflow-y: auto;
            border: 1px solid #ddd;
            border-radius: 4px;
        }

        /* Стили для фильтров по дате (в хедере) */
        .date-filters-inline {
            display: flex;
            gap: 8px;
            align-items: center;
        }

        .date-filter-input {
            padding: 8px 12px;
            border: 1px solid #ddd;
            border-radius: 6px;
            font-size: 14px;
            background: #fff;
            cursor: pointer;
            transition: border-color 0.2s, box-shadow 0.2s;
        }

        .date-filter-input:hover {
            border-color: #0066ff;
        }

        .date-filter-input:focus {
            outline: none;
            border-color: #0066ff;
            box-shadow: 0 0 0 3px rgba(0, 102, 255, 0.1);
        }

        .date-separator {
            color: #999;
            font-size: 16px;
            padding: 0 4px;
        }

        .date-filter-reset {
            padding: 8px 16px;
            margin-left: 4px;
            background: #fff;
            border: 1px solid #ddd;
            border-radius: 6px;
            font-size: 14px;
            color: #666;
            cursor: pointer;
            transition: all 0.2s;
        }

        .date-filter-reset:hover {
            background: #f5f5f5;
            border-color: #ccc;
            color: #333;
        }

        .date-filter-reset.active {
            color: #000;
            border-color: #333;
            font-weight: 500;
        }

        .table-controls {
            margin-bottom: 12px;
            padding: 16px 0;
            display: flex;
            gap: 8px;
            align-items: center;
            flex-wrap: wrap;
        }

        .toggle-col-btn {
            padding: 6px 12px;
            font-size: 12px;
            background: #f0f2f5;
            border: 1px solid #ccc;
            border-radius: 4px;
            cursor: pointer;
            transition: all 0.2s;
        }

        .toggle-col-btn:hover {
            background: #e4e6eb;
        }

        .toggle-col-btn.hidden {
            opacity: 0.6;
            background: #fff3cd;
        }

        table {
            border-collapse: collapse;
            width: 100%;
            min-width: 1200px;
            user-select: none;
        }

        th {
            position: sticky;
            top: 0;
            background: #f8f9fa;
            border-bottom: 2px solid #ddd;
            font-weight: 600;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            min-width: 30px;
            max-width: 300px;
            text-align: center;
            padding: 10px 8px;
        }

        th.resizable {
            position: relative;
            cursor: col-resize;
        }

        .resize-handle {
            position: absolute;
            right: 0;
            top: 0;
            width: 4px;
            height: 100%;
            background: transparent;
            cursor: col-resize;
            user-select: none;
        }

        .resize-handle:hover {
            background: #667eea;
        }

        td {
            border-bottom: 1px solid #eee;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            min-width: 30px;
            max-width: 300px;
            text-align: center;
            padding: 10px 8px;
        }

        td.col-hidden {
            display: none;
        }

        th.col-hidden {
            display: none;
        }

        /* ============================================================ */
        /* АНАЛИТИКА FBO — АККОРДЕОН                                    */
        /* ============================================================ */

        .fbo-table {
            width: 100%;
            border-collapse: collapse;
        }

        .fbo-header {
            background: #f8f9fa;
            border-bottom: 2px solid #e0e0e0;
        }

        .fbo-header th {
            padding: 12px 10px;
            text-align: left;
            font-size: 13px;
            font-weight: 600;
            color: #555;
            white-space: nowrap;
        }

        .fbo-row {
            cursor: pointer;
            transition: background 0.15s;
            border-bottom: 1px solid #eee;
        }

        .fbo-row:hover {
            background: #f0f4ff;
        }

        .fbo-row td {
            padding: 12px 10px;
            font-size: 14px;
            white-space: nowrap;
        }

        .fbo-row .fbo-arrow {
            display: inline-block;
            transition: transform 0.2s;
            margin-right: 6px;
            font-size: 12px;
            color: #999;
        }

        .fbo-row.expanded .fbo-arrow {
            transform: rotate(90deg);
        }

        .fbo-clusters {
            display: none;
        }

        .fbo-clusters.visible {
            display: table-row-group;
        }

        .cluster-row td {
            padding: 8px 10px 8px 38px;
            font-size: 13px;
            color: #555;
            background: #fafbfc;
            border-bottom: 1px solid #f0f0f0;
        }

        .cluster-row td:first-child {
            padding-left: 38px;
        }

        /* Бейджи статуса ликвидности */
        .liq-badge {
            display: inline-block;
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 500;
        }

        .liq-DEFICIT { background: #fee2e2; color: #dc2626; }
        .liq-NO_SALES { background: #f3f4f6; color: #6b7280; }
        .liq-ACTUAL { background: #dbeafe; color: #2563eb; }
        .liq-POPULAR { background: #dcfce7; color: #16a34a; }
        .liq-SURPLUS { background: #fef9c3; color: #ca8a04; }

        .fbo-loading {
            text-align: center;
            padding: 40px;
            color: #888;
        }

        .fbo-stock-val {
            font-weight: 600;
        }

        .fbo-stock-zero {
            color: #ccc;
        }

        /* ============================================================ */
        /* ВКЛАДКА ПОСТАВКИ                                             */
        /* ============================================================ */

        .currency-rates-panel {
            background: #f8f9fb;
            border-radius: 12px;
            padding: 16px 24px;
            margin-bottom: 20px;
        }

        .currency-rates-title {
            font-size: 13px;
            font-weight: 600;
            color: #888;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 12px;
        }

        .currency-rates-row {
            display: flex;
            gap: 16px;
        }

        .currency-rate-card {
            background: #fff;
            border: 1px solid #e9ecef;
            border-radius: 8px;
            padding: 12px 20px;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .currency-label {
            font-size: 13px;
            color: #555;
            font-weight: 500;
        }

        .currency-value {
            font-size: 18px;
            font-weight: 700;
            color: #333;
        }

        .currency-rub {
            font-size: 14px;
            color: #999;
        }

        .supplies-table-wrapper {
            position: relative;
        }

        .supplies-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }

        .supplies-table thead th {
            background: #f1f3f5;
            padding: 10px 8px;
            text-align: center;
            font-weight: 600;
            font-size: 12px;
            color: #444;
            border: 1px solid #dee2e6;
            white-space: nowrap;
            position: sticky;
            top: 0;
            z-index: 2;
        }

        .supplies-table tbody td {
            padding: 6px 8px;
            border: 1px solid #dee2e6;
            text-align: center;
            vertical-align: middle;
        }

        .supplies-table tbody tr:hover {
            background: #f8f9fa;
        }

        .supplies-table tbody tr.locked-row td {
            background: #fafafa;
            color: #888;
        }

        .supply-input {
            width: 100%;
            border: 1px solid transparent;
            background: transparent;
            padding: 4px 6px;
            font-size: 13px;
            text-align: center;
            border-radius: 4px;
            outline: none;
            transition: border-color 0.2s;
        }

        .supply-input:hover:not([disabled]) {
            border-color: #dee2e6;
        }

        .supply-input:focus:not([disabled]) {
            border-color: #667eea;
            background: #fff;
        }

        .supply-input[disabled] {
            color: #666;
            cursor: not-allowed;
        }

        .supply-select {
            width: 100%;
            min-width: 180px;
            border: 1px solid transparent;
            background: transparent;
            padding: 4px 6px;
            font-size: 13px;
            border-radius: 4px;
            outline: none;
            cursor: pointer;
            transition: border-color 0.2s;
        }

        .supply-select:hover:not([disabled]) {
            border-color: #dee2e6;
        }

        .supply-select:focus:not([disabled]) {
            border-color: #667eea;
            background: #fff;
        }

        .supply-select[disabled] {
            color: #666;
            cursor: not-allowed;
        }

        .supply-checkbox {
            width: 18px;
            height: 18px;
            cursor: pointer;
            accent-color: #667eea;
        }

        .supply-checkbox[disabled] {
            cursor: not-allowed;
            opacity: 0.5;
        }

        .supply-cost-auto {
            font-weight: 600;
            color: #333;
            background: #f0f7ff;
            padding: 4px 6px;
            border-radius: 4px;
        }

        .supplies-add-btn {
            display: flex;
            align-items: center;
            justify-content: center;
            width: 100%;
            padding: 10px;
            margin-top: 8px;
            border: 2px dashed #dee2e6;
            background: transparent;
            color: #667eea;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            border-radius: 8px;
            transition: all 0.2s;
        }

        .supplies-add-btn:hover {
            border-color: #667eea;
            background: #f8f9ff;
        }

        /* Модальное окно подтверждения редактирования */
        .supplies-filter-bar {
            display: flex;
            align-items: center;
            gap: 10px;
            margin-bottom: 12px;
        }

        .sortable-date {
            cursor: pointer;
            user-select: none;
        }

        .sortable-date:hover {
            background: #e2e6ea;
        }

        .sort-arrow {
            font-size: 10px;
            color: #999;
        }

        .supply-cell-empty {
            background: #fff5f5 !important;
        }

        /* Скрываем год в date-input */
        .supply-date-input {
            font-family: inherit;
        }
        .supply-date-input::-webkit-datetime-edit-year-field {
            display: none;
        }
        .supply-date-input::-webkit-datetime-edit-text:first-of-type {
            /* Убираем первый разделитель (точка/тире перед годом или после) */
        }

        .supply-delete-btn {
            background: none;
            border: none;
            cursor: pointer;
            font-size: 14px;
            padding: 4px;
            color: #ccc;
            transition: color 0.2s;
        }

        .supply-delete-btn:hover {
            color: #ef4444;
        }

        .supplies-totals-row td {
            padding: 8px;
            font-weight: 700;
            font-size: 13px;
            background: #f1f3f5;
            border: 1px solid #dee2e6;
            text-align: center;
            color: #333;
        }

        .supplies-totals-row td {
            position: sticky;
            top: 36px;
            z-index: 2;
        }

        .supply-edit-confirm {
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0,0,0,0.4);
            display: flex;
            align-items: center;
            justify-content: center;
            z-index: 9999;
        }

        .supply-edit-confirm-box {
            background: #fff;
            border-radius: 12px;
            padding: 24px 32px;
            box-shadow: 0 8px 32px rgba(0,0,0,0.15);
            max-width: 400px;
            text-align: center;
        }

        .supply-edit-confirm-box h3 {
            margin-bottom: 12px;
            color: #333;
        }

        .supply-edit-confirm-box p {
            margin-bottom: 20px;
            color: #666;
            font-size: 14px;
        }

        .supply-edit-confirm-box button {
            padding: 8px 24px;
            border-radius: 6px;
            border: none;
            font-size: 14px;
            font-weight: 500;
            cursor: pointer;
            margin: 0 6px;
        }

        .supply-confirm-yes {
            background: #667eea;
            color: #fff;
        }

        .supply-confirm-no {
            background: #f1f3f5;
            color: #333;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div style="display: flex; justify-content: flex-end; align-items: center;">
                <div style="display: flex; gap: 8px;">
                    <button class="refresh-btn" onclick="syncData()" id="sync-btn">Обновить данные</button>
                </div>
            </div>
        </div>

        <div class="table-container">
            <div class="tabs">
                <button class="tab-button active" onclick="switchTab(event, 'history')">OZON</button>
                <button class="tab-button" onclick="switchTab(event, 'fbo')">Аналитика FBO</button>
                <button class="tab-button" onclick="switchTab(event, 'supplies')">ПОСТАВКИ</button>
            </div>

            <!-- ТАБ: История товара -->
            <div id="history" class="tab-content active">
                <div class="table-header">
                    <div class="date-filters-inline">
                        <input type="date" id="date-from" class="date-filter-input" onclick="this.showPicker()" onchange="applyDateFilter()">
                        <span class="date-separator">—</span>
                        <input type="date" id="date-to" class="date-filter-input" onclick="this.showPicker()" onchange="applyDateFilter()">
                        <button id="date-filter-reset-btn" class="date-filter-reset" onclick="resetDateFilter()">Сбросить</button>
                    </div>
                    <div>
                        <label for="product-select" style="margin-right: 10px; font-weight: 500;">Выберите товар:</label>
                        <select
                            id="product-select"
                            class="history-select"
                            onchange="loadHistoryForProduct()"
                        >
                        </select>
                    </div>
                </div>
                <div id="history-content">
                    <div class="loading">Выберите товар из списка</div>
                </div>
            </div>

            <!-- ТАБ: Аналитика FBO -->
            <div id="fbo" class="tab-content">
                <div id="fbo-content">
                    <div class="fbo-loading">Загрузка данных...</div>
                </div>
            </div>

            <!-- ТАБ: Поставки -->
            <div id="supplies" class="tab-content">
                <!-- Курсы валют ЦБ РФ -->
                <div class="currency-rates-panel">
                    <div class="currency-rates-title">Курсы ЦБ РФ</div>
                    <div class="currency-rates-row">
                        <div class="currency-rate-card">
                            <span class="currency-label">¥ Юань (CNY)</span>
                            <span class="currency-value" id="rate-cny">—</span>
                            <span class="currency-rub">₽</span>
                        </div>
                        <div class="currency-rate-card">
                            <span class="currency-label">$ Доллар (USD)</span>
                            <span class="currency-value" id="rate-usd">—</span>
                            <span class="currency-rub">₽</span>
                        </div>
                        <div class="currency-rate-card">
                            <span class="currency-label">€ Евро (EUR)</span>
                            <span class="currency-value" id="rate-eur">—</span>
                            <span class="currency-rub">₽</span>
                        </div>
                    </div>
                    <!-- Стоимость товара в пути -->
                    <div class="currency-rates-row" style="margin-top: 12px; flex-wrap: wrap;">
                        <div class="currency-rate-card" style="background:#fffbeb; border-color:#f59e0b;">
                            <span class="currency-label">Товар в пути</span>
                            <span class="currency-value" id="goods-in-transit-qty" style="color:#d97706;">—</span>
                            <span class="currency-rub" style="color:#92400e;">шт.</span>
                        </div>
                        <div class="currency-rate-card" style="background:#fffbeb; border-color:#f59e0b;">
                            <span class="currency-label">Вся себестоимость в пути</span>
                            <span class="currency-value" id="goods-in-transit-cost" style="color:#d97706;">—</span>
                            <span class="currency-rub" style="color:#92400e;">₽</span>
                            <span style="display:block;font-size:12px;color:#92400e;margin-top:6px;border-top:1px solid #f59e0b;padding-top:4px;" id="goods-in-transit-cost-no6">без наценки +6%: —</span>
                        </div>
                        <div class="currency-rate-card" style="background:#fefce8; border-color:#ca8a04;">
                            <span class="currency-label">Себестоимость в пути без логистики</span>
                            <span class="currency-value" id="goods-in-transit-cost-no-log" style="color:#ca8a04;">—</span>
                            <span class="currency-rub" style="color:#713f12;">₽</span>
                            <span style="display:block;font-size:12px;color:#713f12;margin-top:6px;border-top:1px solid #ca8a04;padding-top:4px;" id="goods-in-transit-cost-no-log-no6">без наценки +6%: —</span>
                        </div>
                        <div class="currency-rate-card" style="background:#fef2f2; border-color:#ef4444;">
                            <span class="currency-label">Логистика в пути</span>
                            <span class="currency-value" id="logistics-in-transit" style="color:#dc2626;">—</span>
                            <span class="currency-rub" style="color:#7f1d1d;">₽</span>
                            <span style="display:block;font-size:12px;color:#7f1d1d;margin-top:6px;border-top:1px solid #ef4444;padding-top:4px;" id="logistics-in-transit-no6">без наценки +6%: —</span>
                        </div>
                    </div>
                    <div class="currency-rates-row" style="margin-top: 8px; flex-wrap: wrap;">
                        <div class="currency-rate-card" style="background:#eff6ff; border-color:#3b82f6;">
                            <span class="currency-label">План не доставлен</span>
                            <span class="currency-value" id="plan-not-delivered-qty" style="color:#2563eb;">—</span>
                            <span class="currency-rub" style="color:#1e40af;">шт.</span>
                        </div>
                        <div class="currency-rate-card" style="background:#eff6ff; border-color:#3b82f6;">
                            <span class="currency-label">Вся себестоимость плана</span>
                            <span class="currency-value" id="plan-not-delivered-cost" style="color:#2563eb;">—</span>
                            <span class="currency-rub" style="color:#1e40af;">₽</span>
                            <span style="display:block;font-size:12px;color:#1e40af;margin-top:6px;border-top:1px solid #3b82f6;padding-top:4px;" id="plan-cost-no6">без наценки +6%: —</span>
                        </div>
                        <div class="currency-rate-card" style="background:#eef2ff; border-color:#6366f1;">
                            <span class="currency-label">Себестоимость плана без логистики</span>
                            <span class="currency-value" id="plan-not-delivered-cost-no-log" style="color:#4f46e5;">—</span>
                            <span class="currency-rub" style="color:#312e81;">₽</span>
                            <span style="display:block;font-size:12px;color:#312e81;margin-top:6px;border-top:1px solid #6366f1;padding-top:4px;" id="plan-cost-no-log-no6">без наценки +6%: —</span>
                        </div>
                        <div class="currency-rate-card" style="background:#fef2f2; border-color:#ef4444;">
                            <span class="currency-label">Логистика план</span>
                            <span class="currency-value" id="logistics-plan" style="color:#dc2626;">—</span>
                            <span class="currency-rub" style="color:#7f1d1d;">₽</span>
                            <span style="display:block;font-size:12px;color:#7f1d1d;margin-top:6px;border-top:1px solid #ef4444;padding-top:4px;" id="logistics-plan-no6">без наценки +6%: —</span>
                        </div>
                    </div>
                </div>

                <!-- Фильтр по товару -->
                <div class="supplies-filter-bar">
                    <label style="font-weight:500; font-size:13px; color:#555;">Фильтр по товару:</label>
                    <select id="supplies-product-filter" class="supply-select" style="min-width:220px; border:1px solid #dee2e6;" onchange="filterSuppliesTable()">
                        <option value="">Все товары</option>
                    </select>
                </div>

                <!-- Таблица поставок -->
                <div class="supplies-table-wrapper">
                    <div style="overflow-x: auto;">
                        <table class="supplies-table" id="supplies-table">
                            <thead>
                                <tr>
                                    <th>Товар</th>
                                    <th class="sortable-date" data-col="1" onclick="sortSuppliesByDate(1)">Выход с фабрики<br>ПЛАН <span class="sort-arrow"></span></th>
                                    <th style="min-width: 130px;">Заказ кол-во<br>ПЛАН</th>
                                    <th class="sortable-date" data-col="3" onclick="sortSuppliesByDate(3)">Дата выхода<br>с фабрики <span class="sort-arrow"></span></th>
                                    <th>Кол-во выхода<br>с фабрики</th>
                                    <th class="sortable-date" data-col="5" onclick="sortSuppliesByDate(5)">Дата прихода<br>на склад <span class="sort-arrow"></span></th>
                                    <th>Кол-во прихода<br>на склад</th>
                                    <th>Стоимость логистики<br>за единицу, ₽</th>
                                    <th>Цена товара<br>единица, ¥</th>
                                    <th>Себестоимость<br>товара +6%, ₽</th>
                                    <th>Внести<br>в долги</th>
                                    <th>План<br>на FBO</th>
                                    <th style="width: 40px;">🔒</th>
                                    <th style="width: 40px;"></th>
                                </tr>
                                <tr class="supplies-totals-row" id="supplies-tfoot-row"></tr>
                            </thead>
                            <tbody id="supplies-tbody">
                            </tbody>
                        </table>
                    </div>
                    <button class="supplies-add-btn" onclick="addSupplyRow()" title="Добавить строку">
                        <span style="font-size: 20px; line-height: 1;">+</span>
                    </button>
                </div>
            </div>
        </div>
    </div>

    <script>
        let allProducts = [];
        let currentHistoryData = null;  // Хранит загруженные данные истории для фильтрации

        document.addEventListener('DOMContentLoaded', function() {
            // Восстанавливаем активный таб из URL hash при обновлении страницы
            const savedTab = location.hash.replace('#', '');
            const validTabs = ['history', 'fbo', 'supplies'];

            if (savedTab && validTabs.includes(savedTab)) {
                // Активируем сохранённый таб
                document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
                document.querySelectorAll('.tab-button').forEach(el => el.classList.remove('active'));

                document.getElementById(savedTab).classList.add('active');
                // Находим кнопку таба по onclick атрибуту
                document.querySelectorAll('.tab-button').forEach(btn => {
                    if (btn.getAttribute('onclick') && btn.getAttribute('onclick').includes("'" + savedTab + "'")) {
                        btn.classList.add('active');
                    }
                });

                // Загружаем данные для восстановленного таба
                if (savedTab === 'history') {
                    loadProductsList();
                } else if (savedTab === 'fbo') {
                    loadProductsList(); // Список товаров нужен всегда
                    loadFboAnalytics();
                } else if (savedTab === 'supplies') {
                    loadProductsList();
                    loadSupplies();
                }
            } else {
                // По умолчанию — первый таб
                loadProductsList();
            }
        });

        // ✅ СИНХРОНИЗАЦИЯ ДАННЫХ С OZON

        async function syncData() {
            const btn = document.getElementById('sync-btn');
            const originalText = btn.innerHTML;

            try {
                // Показываем индикатор загрузки
                btn.disabled = true;
                btn.innerHTML = '⏳ Обновление...';
                btn.style.opacity = '0.7';

                const response = await fetch('/api/sync', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    }
                });

                const data = await response.json();

                if (data.success) {
                    btn.innerHTML = '✅ Готово!';
                    btn.style.backgroundColor = '#4CAF50';

                    // Перезагрузим страницу через 1 секунду
                    setTimeout(() => {
                        location.reload();
                    }, 1000);
                } else {
                    btn.innerHTML = '❌ Ошибка';
                    btn.style.backgroundColor = '#f44336';
                    alert('Ошибка: ' + data.message);

                    // Вернем кнопку через 2 секунды
                    setTimeout(() => {
                        btn.innerHTML = originalText;
                        btn.style.backgroundColor = '';
                        btn.style.opacity = '1';
                        btn.disabled = false;
                    }, 2000);
                }
            } catch (error) {
                console.error('Ошибка при синхронизации:', error);
                btn.innerHTML = '❌ Ошибка';
                btn.style.backgroundColor = '#f44336';
                alert('Ошибка подключения к серверу');

                // Вернем кнопку через 2 секунды
                setTimeout(() => {
                    btn.innerHTML = originalText;
                    btn.style.backgroundColor = '';
                    btn.style.opacity = '1';
                    btn.disabled = false;
                }, 2000);
            }
        }

        // ✅ ФУНКЦИИ ДЛЯ ТАБОВ И ИСТОРИИ

        function switchTab(e, tab) {
            // Скрываем все табы
            document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
            document.querySelectorAll('.tab-button').forEach(el => el.classList.remove('active'));

            // Показываем нужный таб
            document.getElementById(tab).classList.add('active');
            e.target.classList.add('active');

            // Сохраняем активный таб в URL hash, чтобы при обновлении страницы оставаться на месте
            location.hash = tab;

            // Если открыли историю - загружаем список товаров
            if (tab === 'history') {
                loadProductsList();
            }
            // Если открыли FBO аналитику - загружаем данные
            if (tab === 'fbo') {
                loadFboAnalytics();
            }
            // Если открыли поставки - загружаем данные
            if (tab === 'supplies') {
                loadSupplies();
            }
        }

        // ============================================================
        // АНАЛИТИКА FBO — АККОРДЕОН
        // ============================================================

        let fboDataLoaded = false;

        function loadFboAnalytics() {
            const container = document.getElementById('fbo-content');
            if (fboDataLoaded) return; // Не перезагружаем если уже загружено

            container.innerHTML = '<div class="fbo-loading">Загрузка аналитики FBO...</div>';

            fetch('/api/fbo-analytics')
                .then(r => r.json())
                .then(data => {
                    if (!data.success) {
                        container.innerHTML = '<div class="fbo-loading">Ошибка: ' + (data.error || 'неизвестная') + '</div>';
                        return;
                    }
                    if (!data.products || data.products.length === 0) {
                        container.innerHTML = '<div class="fbo-loading">Нет данных. Выполните синхронизацию.</div>';
                        return;
                    }
                    fboDataLoaded = true;
                    renderFboTable(data.products);
                })
                .catch(err => {
                    container.innerHTML = '<div class="fbo-loading">Ошибка загрузки: ' + err.message + '</div>';
                });
        }

        function getLiqBadge(status) {
            // Статусы оборачиваемости из API /v1/analytics/stocks
            const labels = {
                'DEFICIT': 'Дефицит',
                'WAS_DEFICIT': 'Был дефицит',
                'NO_SALES': 'Нет продаж',
                'WAS_NO_SALES': 'Были продажи',
                'ACTUAL': 'Актуальный',
                'WAS_ACTUAL': 'Был актуален',
                'POPULAR': 'Популярный',
                'WAS_POPULAR': 'Был популярен',
                'SURPLUS': 'Излишек',
                'WAS_SURPLUS': 'Был излишек',
                'WAITING_FOR_SUPPLY': 'Ожидает поставку',
                'RESTRICTED_NO_SALES': 'Ограничен'
            };
            // Группировка цветов: WAS_X использует цвет X
            var colorMap = {
                'DEFICIT': 'DEFICIT', 'WAS_DEFICIT': 'DEFICIT',
                'NO_SALES': 'NO_SALES', 'WAS_NO_SALES': 'NO_SALES', 'RESTRICTED_NO_SALES': 'NO_SALES',
                'ACTUAL': 'ACTUAL', 'WAS_ACTUAL': 'ACTUAL', 'WAITING_FOR_SUPPLY': 'ACTUAL',
                'POPULAR': 'POPULAR', 'WAS_POPULAR': 'POPULAR',
                'SURPLUS': 'SURPLUS', 'WAS_SURPLUS': 'SURPLUS'
            };
            var label = labels[status] || status || '\\u2014';
            var color = colorMap[status] || '';
            var cls = color ? 'liq-' + color : '';
            return '<span class="liq-badge ' + cls + '">' + label + '</span>';
        }

        function renderFboTable(products) {
            const container = document.getElementById('fbo-content');

            let html = '<table class="fbo-table">';
            html += '<thead class="fbo-header"><tr>';
            html += '<th>Товар</th>';
            html += '<th>Остаток FBO</th>';
            html += '<th>Продаж/день</th>';
            html += '<th>В пути</th>';
            html += '<th>В заявках</th>';
            html += '<th>Статус</th>';
            html += '</tr></thead>';
            html += '<tbody>';

            products.forEach(function(p) {
                const sku = p.sku;
                const stockClass = p.fbo_stock > 0 ? 'fbo-stock-val' : 'fbo-stock-val fbo-stock-zero';

                // Основная строка товара
                html += '<tr class="fbo-row" id="fbo-row-' + sku + '" onclick="toggleFboRow(' + sku + ')">';
                html += '<td><span class="fbo-arrow">&#9654;</span>' + (p.offer_id || p.name || 'SKU ' + sku) + '</td>';
                html += '<td class="' + stockClass + '">' + p.fbo_stock + ' шт</td>';
                html += '<td>' + p.total_ads + '</td>';
                html += '<td>' + (p.in_transit || 0) + '</td>';
                html += '<td>' + (p.in_draft || 0) + '</td>';
                html += '<td>' + getLiqBadge(p.worst_liquidity) + '</td>';
                html += '</tr>';

                // Блок кластеров (скрыт по умолчанию)
                html += '<tbody class="fbo-clusters" id="fbo-clusters-' + sku + '">';

                if (p.clusters && p.clusters.length > 0) {
                    // Заголовок кластеров
                    html += '<tr class="cluster-row" style="background:#f0f2f5;">';
                    html += '<td style="font-weight:600;color:#888;">Кластер</td>';
                    html += '<td style="font-weight:600;color:#888;">Остаток</td>';
                    html += '<td style="font-weight:600;color:#888;">Продаж/день</td>';
                    html += '<td style="font-weight:600;color:#888;">Дней до конца</td>';
                    html += '<td style="font-weight:600;color:#888;">Без продаж</td>';
                    html += '<td style="font-weight:600;color:#888;">Статус</td>';
                    html += '</tr>';

                    p.clusters.forEach(function(c) {
                        const cStockClass = c.stock > 0 ? '' : 'fbo-stock-zero';
                        html += '<tr class="cluster-row">';
                        html += '<td>' + c.cluster_name + '</td>';
                        html += '<td class="' + cStockClass + '">' + c.stock + ' шт</td>';
                        html += '<td>' + c.ads + '</td>';
                        html += '<td>' + c.idc + '</td>';
                        html += '<td>' + c.days_without_sales + ' дн</td>';
                        html += '<td>' + getLiqBadge(c.liquidity_status) + '</td>';
                        html += '</tr>';
                    });
                } else {
                    html += '<tr class="cluster-row"><td colspan="6" style="color:#aaa;">Нет данных по кластерам</td></tr>';
                }

                html += '</tbody>';
            });

            html += '</tbody></table>';
            container.innerHTML = html;
        }

        function toggleFboRow(sku) {
            const row = document.getElementById('fbo-row-' + sku);
            const clusters = document.getElementById('fbo-clusters-' + sku);

            if (!row || !clusters) return;

            const isExpanded = row.classList.contains('expanded');

            if (isExpanded) {
                row.classList.remove('expanded');
                clusters.classList.remove('visible');
            } else {
                row.classList.add('expanded');
                clusters.classList.add('visible');
            }
        }

        function loadProductsList() {
            fetch('/api/products/list')
                .then(response => response.json())
                .then(data => {
                    if (data.success && data.products.length > 0) {
                        const select = document.getElementById('product-select');
                        select.innerHTML = '';  // Не добавляю "Выберите товар"
                        
                        data.products.forEach(p => {
                            const option = document.createElement('option');
                            option.value = p.sku;
                            option.textContent = p.offer_id ? `${p.offer_id}` : `SKU: ${p.sku}`;
                            select.appendChild(option);
                        });
                        
                        // Автоматически выбираю первый товар и загружаю историю
                        select.value = data.products[0].sku;
                        loadHistoryForProduct();
                    }
                })
                .catch(error => console.error('Ошибка:', error));
        }

        function loadHistoryForProduct() {
            const sku = document.getElementById('product-select').value;
            
            if (!sku) {
                document.getElementById('history-content').innerHTML = 
                    '<div class="empty-state">Выберите товар из списка</div>';
                return;
            }
            
            document.getElementById('history-content').innerHTML = 
                '<div class="loading">Загрузка истории...</div>';
            
            fetch(`/api/history/${sku}`)
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        currentHistoryData = data;  // Сохраняем данные для фильтрации
                        renderHistory(data);
                    } else {
                        document.getElementById('history-content').innerHTML =
                            '<div class="error">' + data.error + '</div>';
                    }
                })
                .catch(error => {
                    document.getElementById('history-content').innerHTML = 
                        '<div class="error">❌ Ошибка при загрузке: ' + error + '</div>';
                });
        }

        function renderHistory(data) {
            const historyContent = document.getElementById('history-content');

            if (!data.history || data.history.length === 0) {
                historyContent.innerHTML = '<div class="empty-state">История не найдена</div>';
                return;
            }

            // ✅ Функция для форматирования чисел с пробелами (3 245 вместо 3245)
            function formatNumber(num) {
                if (num === null || num === undefined || num === 0) return '0';
                return String(Math.round(num)).replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
            }

            // ✅ Функция для сравнения значений и добавления стрелок
            function getTrendArrow(current, previous, reverseDirection = false) {
                // Если нет предыдущего значения или оба null/undefined - без стрелки
                if (previous === null || previous === undefined ||
                    current === null || current === undefined) {
                    return '';
                }

                const diff = current - previous;

                if (diff === 0) return ''; // Без изменений

                // Для средней позиции: меньше = лучше, поэтому инвертируем логику
                const isGood = reverseDirection ? (diff < 0) : (diff > 0);

                if (isGood) {
                    return ' <span style="color: #22c55e; font-size: 14px;">▲</span>';
                } else {
                    return ' <span style="color: #ef4444; font-size: 14px;">▼</span>';
                }
            }
            
            let html = '<table><thead><tr>';
            html += '<th>Заметки</th>';
            html += '<th>Дата</th>';
            html += '<th>Название</th>';
            html += '<th>SKU</th>';
            html += '<th>Рейтинг</th>';
            html += '<th>Отзывы</th>';
            html += '<th>Индекс цен</th>';
            html += '<th>FBO остаток</th>';
            html += '<th>Заказы</th>';
            html += '<th>Заказы план</th>';
            html += '<th>Цена в ЛК</th>';
            html += '<th>Цена план</th>';
            html += '<th>Соинвест</th>';
            html += '<th>Цена на сайте</th>';
            html += '<th>Ср. позиция</th>';
            html += '<th>Показы (поиск+кат.)</th>';
            html += '<th>Посещения</th>';
            html += '<th>CTR (%)</th>';
            html += '<th>Корзина</th>';
            html += '<th>CR1 (%)</th>';
            html += '<th>CR2 (%)</th>';
            html += '<th>Расходы</th>';
            html += '<th>CPO план</th>';
            html += '<th>CPO</th>';
            html += '<th>В пути</th>';
            html += '<th>В заявках</th>';
            html += '</tr></thead><tbody>';

            data.history.forEach((item, index) => {
                // Получаем данные за предыдущий день для сравнения
                const prevItem = data.history[index + 1] || null;

                const date = new Date(item.snapshot_date);
                // Формат: 01.01.26
                const day = String(date.getDate()).padStart(2, '0');
                const month = String(date.getMonth() + 1).padStart(2, '0');
                const year = String(date.getFullYear()).slice(-2);
                const dateStr = `${day}.${month}.${year}`;

                const stockClass = item.fbo_stock < 5 ? 'stock low' : 'stock';
                const uniqueId = `note_${data.product_sku}_${item.snapshot_date}`;
                const notes = item.notes || '';
                
                html += `<tr>`;
                html += `<td style="width: 220px; min-width: 220px; max-width: 220px; word-wrap: break-word; overflow-wrap: break-word; text-align: left;">
                    <div class="note-cell">
                        <div id="${uniqueId}_display" class="note-display" onclick="startEditNote('${uniqueId}', '${data.product_sku}', '${item.snapshot_date}')">
                            ${notes || '<span style="color: #bbb;">Нажмите чтобы добавить...</span>'}
                        </div>
                    </div>
                    <div id="${uniqueId}_editor" style="display: none;">
                        <textarea 
                            id="${uniqueId}_textarea"
                            class="note-textarea"
                            placeholder="Напишите заметку..."
                        >${notes}</textarea>
                        <div style="margin-top: 6px; display: flex; gap: 4px;">
                            <button class="note-save-btn" onclick="saveNote('${uniqueId}', ${data.product_sku}, '${item.snapshot_date}')">Сохранить</button>
                            <button class="note-cancel-btn" onclick="cancelEditNote('${uniqueId}')">Отмена</button>
                        </div>
                    </div>
                </td>`;
                html += `<td><strong>${dateStr}</strong></td>`;
                html += `<td><span onclick="openProductOnOzon('${item.sku}')" style="cursor: pointer; color: #0066cc; text-decoration: underline;" title="Открыть товар на Ozon">${item.name}</span></td>`;
                html += `<td><span class="sku" onclick="copySKU(this, '${item.sku}')" style="cursor: pointer;" title="Нажмите чтобы скопировать">${item.sku}</span></td>`;

                // Рейтинг товара
                const rating = item.rating !== null && item.rating !== undefined ? item.rating.toFixed(1) : '—';
                html += `<td><strong>${rating}</strong></td>`;

                // Количество отзывов
                const reviewCount = item.review_count !== null && item.review_count !== undefined ? formatNumber(item.review_count) : '—';
                html += `<td><strong>${reviewCount}</strong></td>`;

                // Индекс цены (price_index)
                // Возможные значения: WITHOUT_INDEX, PROFIT, AVG_PROFIT, NON_PROFIT
                const priceIndexMap = {
                    'WITHOUT_INDEX': { text: 'Без индекса', color: '#6b7280' },
                    'PROFIT': { text: 'Прибыльный', color: '#22c55e' },
                    'AVG_PROFIT': { text: 'Средний', color: '#f59e0b' },
                    'NON_PROFIT': { text: 'Неприбыльный', color: '#ef4444' }
                };
                const priceIndexValue = item.price_index || null;
                const priceIndexDisplay = priceIndexValue && priceIndexMap[priceIndexValue]
                    ? `<span style="color: ${priceIndexMap[priceIndexValue].color}; font-weight: 500;">${priceIndexMap[priceIndexValue].text}</span>`
                    : '—';
                html += `<td>${priceIndexDisplay}</td>`;

                html += `<td><span class="${stockClass}">${formatNumber(item.fbo_stock)}</span></td>`;

                // Заказы (с стрелкой)
                html += `<td><span class="stock">${formatNumber(item.orders_qty || 0)}${getTrendArrow(item.orders_qty, prevItem?.orders_qty)}</span></td>`;

                // Заказы план (редактируемое поле)
                // Если у текущей даты нет плана — ищем последнее установленное значение
                // в более старых записях (каскадная пропагация назад по истории)
                let ordersPlanValue = '';
                if (item.orders_plan !== null && item.orders_plan !== undefined) {
                    ordersPlanValue = item.orders_plan;
                } else {
                    // Ищем ближайшую старую запись с непустым orders_plan
                    for (let k = index + 1; k < data.history.length; k++) {
                        const olderItem = data.history[k];
                        if (olderItem.orders_plan !== null && olderItem.orders_plan !== undefined) {
                            ordersPlanValue = olderItem.orders_plan;
                            break;
                        }
                    }
                }
                // Сравниваем даты напрямую (без времени)
                const itemDate = new Date(item.snapshot_date);
                const today = new Date();
                today.setHours(0, 0, 0, 0);
                itemDate.setHours(0, 0, 0, 0);
                const isPast = itemDate < today;
                const planInputId = `orders_plan_${data.product_sku}_${item.snapshot_date}`;

                // Определяем цвет ячейки на основе сравнения плана и факта
                let cellBgColor = '#f5f5f5'; // По умолчанию бледно-серый
                const actualOrders = item.orders_qty || 0;
                const planOrders = parseInt(ordersPlanValue) || 0;

                if (ordersPlanValue !== '' && planOrders > 0) {
                    if (planOrders > actualOrders) {
                        cellBgColor = '#ffe5e5'; // Бледно-красный (план не выполнен)
                    } else if (planOrders < actualOrders) {
                        cellBgColor = '#e5ffe5'; // Бледно-зеленый (план перевыполнен)
                    }
                }

                html += `<td style="background-color: ${cellBgColor};">
                    <input
                        type="text"
                        id="${planInputId}"
                        value="${ordersPlanValue}"
                        style="width: 60px; padding: 4px; text-align: center; font-size: 14px; border: 1px solid #ddd; border-radius: 4px; background-color: ${isPast ? '#e5e5e5' : '#fff'};"
                        ${isPast ? 'readonly' : ''}
                        oninput="this.value = this.value.replace(/[^0-9]/g, '')"
                        onblur="saveOrdersPlan('${data.product_sku}', '${item.snapshot_date}', this.value)"
                    />
                </td>`;

                // Цена в ЛК (с стрелкой, инвертированная логика: меньше = лучше)
                html += `<td><strong>${(item.price !== null && item.price !== undefined && item.price > 0) ? formatNumber(Math.round(item.price)) + ' ₽' : '—'}${(item.price !== null && item.price !== undefined && item.price > 0) ? getTrendArrow(item.price, prevItem?.price, true) : ''}</strong></td>`;

                // Цена план (редактируемое поле, аналогично Заказы план)
                let pricePlanValue = '';
                if (item.price_plan !== null && item.price_plan !== undefined) {
                    pricePlanValue = item.price_plan;
                } else {
                    // Ищем ближайшую старую запись с непустым price_plan
                    for (let k = index + 1; k < data.history.length; k++) {
                        const olderItem = data.history[k];
                        if (olderItem.price_plan !== null && olderItem.price_plan !== undefined) {
                            pricePlanValue = olderItem.price_plan;
                            break;
                        }
                    }
                }
                const pricePlanInputId = `price_plan_${data.product_sku}_${item.snapshot_date}`;

                // Определяем цвет ячейки Цена план на основе сравнения плана и факта цены
                // Для цены: выше = лучше, если факт > план — зелёный (хорошо)
                let pricePlanBgColor = '#f5f5f5';
                const planPrice = parseInt(pricePlanValue) || 0;
                const actualPrice = (item.price !== null && item.price !== undefined && item.price > 0) ? Math.round(item.price) : 0;

                if (pricePlanValue !== '' && planPrice > 0 && actualPrice > 0) {
                    if (actualPrice < planPrice) {
                        pricePlanBgColor = '#ffe5e5'; // Бледно-красный (цена ниже плана — плохо)
                    } else if (actualPrice > planPrice) {
                        pricePlanBgColor = '#e5ffe5'; // Бледно-зеленый (цена выше плана — хорошо)
                    }
                }

                // Форматируем значение с пробелами между тысячами для отображения
                const pricePlanDisplay = pricePlanValue !== '' ? formatNumber(parseInt(pricePlanValue)) : '';

                html += `<td style="background-color: ${pricePlanBgColor};">
                    <input
                        type="text"
                        id="${pricePlanInputId}"
                        value="${pricePlanDisplay}"
                        style="width: 80px; padding: 4px; text-align: center; font-size: 14px; border: 1px solid #ddd; border-radius: 4px; background-color: ${isPast ? '#e5e5e5' : '#fff'};"
                        ${isPast ? 'readonly' : ''}
                        oninput="this.value = this.value.replace(/[^0-9\\s]/g, '').replace(/\\s/g, ''); this.value = this.value.replace(/\\B(?=(\\d{3})+(?!\\d))/g, ' ');"
                        onblur="savePricePlan('${data.product_sku}', '${item.snapshot_date}', this.value.replace(/\\s/g, ''))"
                    />
                </td>`;

                // Соинвест (процент скидки от Цены в ЛК до Цены на сайте)
                let coinvest = '—';
                let coinvestValue = null;
                let prevCoinvestValue = null;

                // Вычисляем соинвест для текущего дня
                if (item.price !== null && item.price !== undefined && item.price > 0 &&
                    item.marketing_price !== null && item.marketing_price !== undefined && item.marketing_price > 0) {
                    coinvestValue = ((item.price - item.marketing_price) / item.price) * 100;
                    coinvest = coinvestValue.toFixed(1) + '%';
                }

                // Вычисляем соинвест для предыдущего дня (для стрелки)
                if (prevItem && prevItem.price !== null && prevItem.price !== undefined && prevItem.price > 0 &&
                    prevItem.marketing_price !== null && prevItem.marketing_price !== undefined && prevItem.marketing_price > 0) {
                    prevCoinvestValue = ((prevItem.price - prevItem.marketing_price) / prevItem.price) * 100;
                }

                // Добавляем ячейку со стрелкой
                html += `<td><strong>${coinvest}${coinvestValue !== null && prevCoinvestValue !== null ? getTrendArrow(coinvestValue, prevCoinvestValue) : ''}</strong></td>`;

                // Цена на сайте (с стрелкой, инвертированная логика: меньше = лучше)
                html += `<td><strong>${(item.marketing_price !== null && item.marketing_price !== undefined && item.marketing_price > 0) ? formatNumber(Math.round(item.marketing_price)) + ' ₽' : '—'}${(item.marketing_price !== null && item.marketing_price !== undefined && item.marketing_price > 0) ? getTrendArrow(item.marketing_price, prevItem?.marketing_price, true) : ''}</strong></td>`;

                // Ср. позиция (с стрелкой, инвертированная логика: меньше = лучше)
                html += `<td><span class="position">${(item.avg_position !== null && item.avg_position !== undefined) ? item.avg_position.toFixed(1) : '—'}${(item.avg_position !== null && item.avg_position !== undefined) ? getTrendArrow(item.avg_position, prevItem?.avg_position, true) : ''}</span></td>`;

                // Показы (поиск+кат.) - с стрелкой и разницей от прошлого дня
                const curViews = item.hits_view_search || 0;
                const prevViews = prevItem?.hits_view_search || 0;
                const viewsDiff = (prevItem && prevItem.hits_view_search !== null && prevItem.hits_view_search !== undefined) ? curViews - prevViews : null;
                let viewsDiffHtml = '';
                if (viewsDiff !== null && viewsDiff !== 0) {
                    const diffColor = viewsDiff > 0 ? '#22c55e' : '#ef4444';
                    const diffSign = viewsDiff > 0 ? '+' : '';
                    viewsDiffHtml = `<br><span style="font-size: 11px; color: ${diffColor}; font-weight: 400;">${diffSign}${formatNumber(viewsDiff)}</span>`;
                }
                html += `<td><strong>${formatNumber(curViews)}${getTrendArrow(item.hits_view_search, prevItem?.hits_view_search)}</strong>${viewsDiffHtml}</td>`;

                // Посещения - с стрелкой
                html += `<td><strong>${formatNumber(item.hits_view_search_pdp || 0)}${getTrendArrow(item.hits_view_search_pdp, prevItem?.hits_view_search_pdp)}</strong></td>`;

                // CTR (%) - с стрелкой
                html += `<td><strong>${(item.search_ctr !== null && item.search_ctr !== undefined) ? item.search_ctr.toFixed(2) + '%' : '—'}${(item.search_ctr !== null && item.search_ctr !== undefined) ? getTrendArrow(item.search_ctr, prevItem?.search_ctr) : ''}</strong></td>`;

                // Корзина - с стрелкой
                html += `<td><strong>${formatNumber(item.hits_add_to_cart || 0)}${getTrendArrow(item.hits_add_to_cart, prevItem?.hits_add_to_cart)}</strong></td>`;

                // CR1 (%) - с стрелкой
                html += `<td><strong>${(item.cr1 !== null && item.cr1 !== undefined) ? item.cr1.toFixed(2) + '%' : '—'}${(item.cr1 !== null && item.cr1 !== undefined) ? getTrendArrow(item.cr1, prevItem?.cr1) : ''}</strong></td>`;

                // CR2 (%) - с стрелкой
                html += `<td><strong>${(item.cr2 !== null && item.cr2 !== undefined) ? item.cr2.toFixed(2) + '%' : '—'}${(item.cr2 !== null && item.cr2 !== undefined) ? getTrendArrow(item.cr2, prevItem?.cr2) : ''}</strong></td>`;

                // Расходы - с стрелкой
                html += `<td><strong>${(item.adv_spend !== null && item.adv_spend !== undefined) ? formatNumber(Math.round(item.adv_spend)) + ' ₽' : '—'}${(item.adv_spend !== null && item.adv_spend !== undefined) ? getTrendArrow(item.adv_spend, prevItem?.adv_spend) : ''}</strong></td>`;

                // CPO план (редактируемое поле, аналогично Заказы план)
                // Если у текущей даты нет плана — ищем последнее установленное значение
                let cpoPlanValue = '';
                if (item.cpo_plan !== null && item.cpo_plan !== undefined) {
                    cpoPlanValue = item.cpo_plan;
                } else {
                    // Ищем ближайшую старую запись с непустым cpo_plan
                    for (let k = index + 1; k < data.history.length; k++) {
                        const olderItem = data.history[k];
                        if (olderItem.cpo_plan !== null && olderItem.cpo_plan !== undefined) {
                            cpoPlanValue = olderItem.cpo_plan;
                            break;
                        }
                    }
                }
                const cpoPlanInputId = `cpo_plan_${data.product_sku}_${item.snapshot_date}`;

                // CPO (Cost Per Order) - расходы/заказы
                const cpo = (item.adv_spend !== null && item.adv_spend !== undefined && item.orders_qty > 0)
                    ? Math.round(item.adv_spend / item.orders_qty)
                    : null;

                // Определяем цвет ячейки CPO план на основе сравнения плана и факта CPO
                // Для CPO: меньше = лучше, поэтому если факт < план — зелёный (хорошо)
                let cpoPlanBgColor = '#f5f5f5'; // По умолчанию бледно-серый
                const planCpo = parseInt(cpoPlanValue) || 0;
                const actualCpo = cpo || 0;

                if (cpoPlanValue !== '' && planCpo > 0 && cpo !== null) {
                    if (actualCpo > planCpo) {
                        cpoPlanBgColor = '#ffe5e5'; // Бледно-красный (CPO выше плана — плохо)
                    } else if (actualCpo < planCpo) {
                        cpoPlanBgColor = '#e5ffe5'; // Бледно-зеленый (CPO ниже плана — хорошо)
                    }
                }

                html += `<td style="background-color: ${cpoPlanBgColor};">
                    <input
                        type="text"
                        id="${cpoPlanInputId}"
                        value="${cpoPlanValue}"
                        style="width: 60px; padding: 4px; text-align: center; font-size: 14px; border: 1px solid #ddd; border-radius: 4px; background-color: ${isPast ? '#e5e5e5' : '#fff'};"
                        ${isPast ? 'readonly' : ''}
                        oninput="this.value = this.value.replace(/[^0-9]/g, '')"
                        onblur="saveCpoPlan('${data.product_sku}', '${item.snapshot_date}', this.value)"
                    />
                </td>`;

                // CPO (Cost Per Order) - с стрелкой (меньше = лучше)
                const prevCpo = (prevItem?.adv_spend !== null && prevItem?.adv_spend !== undefined && prevItem?.orders_qty > 0)
                    ? Math.round(prevItem.adv_spend / prevItem.orders_qty)
                    : null;
                html += `<td><strong>${cpo !== null ? cpo + ' ₽' : '—'}${cpo !== null ? getTrendArrow(cpo, prevCpo, true) : ''}</strong></td>`;

                // В ПУТИ - товары из заявок со статусом "в пути"
                html += `<td><span class="stock">${formatNumber(item.in_transit || 0)}</span></td>`;

                // В ЗАЯВКАХ - товары из черновиков/новых заявок
                html += `<td><span class="stock">${formatNumber(item.in_draft || 0)}</span></td>`;



                html += `</tr>`;
            });
            
            html += '</tbody></table>';
            
            // Обворачиваю таблицу в контейнер для скролла
            const fullHtml = `
                <div class="table-controls">
                    <span style="font-weight: 600; margin-right: 8px;">Видимые столбцы:</span>
                    <button class="toggle-col-btn" onclick="toggleColumn(1)">Дата</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(2)">Название</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(3)">SKU</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(4)">Рейтинг</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(5)">Отзывы</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(6)">Индекс цен</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(7)">FBO</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(8)">Заказы</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(9)">Заказы план</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(10)">Цена в ЛК</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(11)">Цена план</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(12)">Соинвест</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(13)">Цена на сайте</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(14)">Ср. позиция</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(15)">Показы</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(16)">Посещения</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(17)">CTR</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(18)">Корзина</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(19)">CR1</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(20)">CR2</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(21)">Расходы</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(22)">CPO план</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(23)">CPO</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(24)">В пути</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(25)">В заявках</button>
                </div>
                <div class="table-wrapper">
                    ${html}
                </div>
            `;
            
            historyContent.innerHTML = fullHtml;

            // Инициализирую изменение ширины столбцов
            initColumnResize();
        }

        // ============================================================================
        // ФИЛЬТРАЦИЯ ПО ДАТЕ
        // ============================================================================

        /**
         * Применяет фильтр по дате к данным истории.
         * Фильтрует записи по диапазону дат и перерисовывает таблицу.
         */
        function applyDateFilter() {
            if (!currentHistoryData) return;

            const dateFrom = document.getElementById('date-from')?.value;
            const dateTo = document.getElementById('date-to')?.value;
            const resetBtn = document.getElementById('date-filter-reset-btn');

            // Обновляем состояние кнопки сброса
            if (resetBtn) {
                if (dateFrom || dateTo) {
                    resetBtn.classList.add('active');
                } else {
                    resetBtn.classList.remove('active');
                }
            }

            // Создаём копию данных с отфильтрованной историей
            const filteredData = {
                ...currentHistoryData,
                history: currentHistoryData.history.filter(item => {
                    const itemDate = item.snapshot_date;
                    if (dateFrom && itemDate < dateFrom) return false;
                    if (dateTo && itemDate > dateTo) return false;
                    return true;
                })
            };

            renderHistory(filteredData);
        }

        /**
         * Сбрасывает фильтры по дате и показывает все записи.
         */
        function resetDateFilter() {
            if (!currentHistoryData) return;

            // Очищаем поля ввода
            const dateFromEl = document.getElementById('date-from');
            const dateToEl = document.getElementById('date-to');
            const resetBtn = document.getElementById('date-filter-reset-btn');

            if (dateFromEl) dateFromEl.value = '';
            if (dateToEl) dateToEl.value = '';
            if (resetBtn) resetBtn.classList.remove('active');

            // Перерисовываем с полными данными
            renderHistory(currentHistoryData);
        }

        function startEditNote(uniqueId, sku, date) {
            document.getElementById(uniqueId + '_display').style.display = 'none';
            document.getElementById(uniqueId + '_editor').style.display = 'block';
            document.getElementById(uniqueId + '_textarea').focus();
        }

        function cancelEditNote(uniqueId) {
            document.getElementById(uniqueId + '_display').style.display = 'flex';
            document.getElementById(uniqueId + '_editor').style.display = 'none';
        }

        function saveNote(uniqueId, sku, date) {
            const textarea = document.getElementById(uniqueId + '_textarea');
            const text = textarea.value;

            const payload = {
                sku: sku,
                date: date,
                notes: text
            };

            fetch('/api/history/save-note', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    // Обновляем отображение
                    const displayEl = document.getElementById(uniqueId + '_display');
                    displayEl.innerHTML = text || '<span style="color: #bbb;">Нажмите чтобы добавить...</span>';

                    // Скрываем редактор
                    document.getElementById(uniqueId + '_editor').style.display = 'none';
                    displayEl.style.display = 'flex';

                    console.log('✅ Заметка сохранена');
                } else {
                    alert('❌ Ошибка при сохранении: ' + data.error);
                }
            })
            .catch(error => {
                alert('❌ Ошибка: ' + error);
                console.error('Ошибка:', error);
            });
        }

        // ✅ Функция для сохранения плановых заказов
        function saveOrdersPlan(sku, date, value) {
            const payload = {
                sku: parseInt(sku),
                date: date,
                orders_plan: value
            };

            fetch('/api/history/save-orders-plan', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    console.log('✅ План заказов сохранен');
                } else {
                    alert('❌ Ошибка при сохранении: ' + data.error);
                }
            })
            .catch(error => {
                alert('❌ Ошибка: ' + error);
                console.error('Ошибка:', error);
            });
        }

        // ✅ Функция для сохранения планового CPO
        function saveCpoPlan(sku, date, value) {
            const payload = {
                sku: parseInt(sku),
                date: date,
                cpo_plan: value
            };

            fetch('/api/history/save-cpo-plan', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    console.log('✅ План CPO сохранен');
                } else {
                    alert('❌ Ошибка при сохранении: ' + data.error);
                }
            })
            .catch(error => {
                alert('❌ Ошибка: ' + error);
                console.error('Ошибка:', error);
            });
        }

        // ✅ Функция для сохранения плановой цены
        function savePricePlan(sku, date, value) {
            const payload = {
                sku: parseInt(sku),
                date: date,
                price_plan: value
            };

            fetch('/api/history/save-price-plan', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    console.log('✅ План цены сохранен');
                } else {
                    alert('❌ Ошибка при сохранении: ' + data.error);
                }
            })
            .catch(error => {
                alert('❌ Ошибка: ' + error);
                console.error('Ошибка:', error);
            });
        }

        // ✅ Функция для копирования SKU в буфер обмена
        function copySKU(element, sku) {
            // Функция для визуальной обратной связи
            const showSuccess = () => {
                const originalColor = element.style.color;
                element.style.color = '#10b981'; // Зеленый
                element.style.fontWeight = 'bold';

                setTimeout(() => {
                    element.style.color = originalColor;
                    element.style.fontWeight = '';
                }, 1000);
            };

            // Пробуем современный API (работает на HTTPS)
            if (navigator.clipboard && navigator.clipboard.writeText) {
                navigator.clipboard.writeText(sku).then(() => {
                    showSuccess();
                    console.log('✅ SKU скопирован (clipboard API):', sku);
                }).catch(err => {
                    console.warn('Clipboard API не сработал, пробуем fallback:', err);
                    fallbackCopy(sku);
                });
            } else {
                // Fallback для HTTP или старых браузеров
                fallbackCopy(sku);
            }

            // Альтернативный метод копирования (работает на HTTP)
            function fallbackCopy(text) {
                const textarea = document.createElement('textarea');
                textarea.value = text;
                textarea.style.position = 'fixed';
                textarea.style.opacity = '0';
                document.body.appendChild(textarea);
                textarea.select();

                try {
                    const successful = document.execCommand('copy');
                    document.body.removeChild(textarea);

                    if (successful) {
                        showSuccess();
                        console.log('✅ SKU скопирован (fallback):', text);
                    } else {
                        alert('Не удалось скопировать SKU');
                    }
                } catch (err) {
                    document.body.removeChild(textarea);
                    console.error('❌ Ошибка при копировании:', err);
                    alert('Ошибка при копировании SKU: ' + err);
                }
            }
        }

        // ✅ Функция для открытия товара на Ozon
        function openProductOnOzon(sku) {
            // Открываем поиск по SKU на Ozon в новой вкладке
            const url = `https://www.ozon.ru/search/?text=${sku}`;
            window.open(url, '_blank');
            console.log('🔗 Открываю товар на Ozon, SKU:', sku);
        }

        // ✅ Функция для скрывания/показа столбцов
        function toggleColumn(colIndex) {
            const table = document.querySelector('table');
            if (!table) return;
            
            const rows = table.querySelectorAll('tr');
            rows.forEach(row => {
                const cells = row.querySelectorAll('th, td');
                if (cells[colIndex]) {
                    cells[colIndex].classList.toggle('col-hidden');
                }
            });
            
            // Обновляю кнопку
            const buttons = document.querySelectorAll('.toggle-col-btn');
            if (buttons[colIndex - 1]) {
                buttons[colIndex - 1].classList.toggle('hidden');
            }
        }

        // ✅ Функция для изменения ширины столбца
        function initColumnResize() {
            const table = document.querySelector('table');
            if (!table) return;
            
            const headers = table.querySelectorAll('th');
            
            headers.forEach((header, index) => {
                // Добавляю handle для изменения ширины
                const handle = document.createElement('div');
                handle.className = 'resize-handle';
                header.appendChild(handle);
                header.classList.add('resizable');
                
                // По умолчанию берем автоматическую ширину (не фиксируем)
                // Минимум 50px (CSS min-width)
                header.style.width = 'auto';
                
                // Инициализирую перетаскивание
                let isResizing = false;
                let startX = 0;
                let startWidth = 0;
                
                handle.addEventListener('mousedown', (e) => {
                    isResizing = true;
                    startX = e.clientX;
                    startWidth = header.offsetWidth;
                    e.preventDefault();
                });
                
                document.addEventListener('mousemove', (e) => {
                    if (!isResizing) return;
                    
                    const delta = e.clientX - startX;
                    const newWidth = Math.max(30, startWidth + delta);  // ✅ Минимум 30px вместо 50px
                    
                    header.style.width = newWidth + 'px';
                    header.style.minWidth = newWidth + 'px';
                    
                    // Обновляю все td в этом столбце
                    const cells = table.querySelectorAll(`tbody tr td:nth-child(${index + 1})`);
                    cells.forEach(cell => {
                        cell.style.width = newWidth + 'px';
                        cell.style.minWidth = newWidth + 'px';
                    });
                });
                
                document.addEventListener('mouseup', () => {
                    isResizing = false;
                });
            });
        }
        // ============================================================
        // ПОСТАВКИ — ЛОГИКА ВКЛАДКИ
        // ============================================================

        let suppliesLoaded = false;
        let suppliesProducts = [];  // Все товары для выпадающего списка
        let currentCnyRate = 0;     // Текущий курс юаня

        /**
         * Загрузка данных вкладки "Поставки":
         * 1. Курсы валют ЦБ РФ
         * 2. Список товаров для выпадающего списка
         * 3. Существующие строки поставок из базы
         */
        function loadSupplies() {
            // Загружаем курсы валют (независимый запрос)
            fetch('/api/currency-rates')
                .then(r => r.json())
                .then(data => {
                    if (data.success) {
                        const rates = data.rates;
                        currentCnyRate = rates.CNY || 0;
                        document.getElementById('rate-cny').textContent = formatCurrencyRate(rates.CNY);
                        document.getElementById('rate-usd').textContent = formatCurrencyRate(rates.USD);
                        document.getElementById('rate-eur').textContent = formatCurrencyRate(rates.EUR);

                        // Пересчитываем себестоимости при обновлении курса
                        recalcAllCosts();
                    }
                });

            // Загружаем товары и поставки параллельно, но рендерим только когда ОБА готовы
            // Иначе suppliesProducts может быть пустым при отрисовке таблицы
            const productsPromise = fetch('/api/products/list')
                .then(r => r.json())
                .then(data => {
                    if (data.success) {
                        suppliesProducts = data.products;
                    }
                });

            const suppliesPromise = fetch('/api/supplies')
                .then(r => r.json());

            Promise.all([productsPromise, suppliesPromise]).then(([_, suppliesData]) => {
                if (suppliesData.success) {
                    renderSuppliesTable(suppliesData.supplies);
                }
            });

            suppliesLoaded = true;
        }

        /**
         * Форматирование курса валюты для отображения
         */
        function formatCurrencyRate(rate) {
            if (!rate) return '—';
            return rate.toFixed(2).replace(/\\B(?=(\\d{3})+(?!\\d))/g, ' ');
        }

        /**
         * Форматирование числа с пробелами между тысячными
         */
        function formatNumberWithSpaces(num) {
            if (num === null || num === undefined || num === '') return '';
            const n = parseInt(num);
            if (isNaN(n)) return '';
            return n.toString().replace(/\\B(?=(\\d{3})+(?!\\d))/g, ' ');
        }

        /**
         * Парсинг числа из строки с пробелами
         */
        function parseNumberFromSpaces(str) {
            if (!str) return 0;
            return parseInt(str.replace(/\\s/g, '')) || 0;
        }

        /**
         * Форматирование даты без года (ДД.ММ)
         */
        function formatDateNoYear(dateStr) {
            if (!dateStr) return '';
            const parts = dateStr.split('-');
            if (parts.length === 3) return parts[2] + '.' + parts[1];
            if (parts.length === 2) return parts[1] + '.' + parts[0];
            return dateStr;
        }

        /**
         * Отрисовка таблицы поставок из данных базы
         */
        function renderSuppliesTable(supplies) {
            const tbody = document.getElementById('supplies-tbody');
            tbody.innerHTML = '';

            supplies.forEach(s => {
                const row = createSupplyRowElement(s);
                tbody.appendChild(row);
            });

            // После отрисовки: подсветка пустых, фильтр, итоги
            highlightAllEmptyCells();
            populateSuppliesFilter();
            updateSupplyTotals();
        }

        /**
         * Создание HTML-элемента строки таблицы поставок
         *
         * Параметры:
         *   data — объект с данными поставки (из базы) или null для новой строки
         */
        function createSupplyRowElement(data) {
            const row = document.createElement('tr');
            // По умолчанию все существующие строки заблокированы
            // Проверяем: если строка была разблокирована менее 30 минут назад — оставляем открытой
            let isLocked = data ? true : false;
            if (isLocked && data && data.id) {
                const unlocks = JSON.parse(localStorage.getItem('supply_unlocks') || '{}');
                const unlockTime = unlocks[data.id];
                if (unlockTime && (Date.now() - unlockTime) < 30 * 60 * 1000) {
                    isLocked = false; // разблокирована менее 30 мин назад
                }
            }
            const rowId = data ? data.id : 'new_' + Date.now();
            row.dataset.supplyId = rowId;
            if (isLocked) row.classList.add('locked-row');

            // 1. Товар (выпадающий список)
            const tdProduct = document.createElement('td');
            const selectProduct = document.createElement('select');
            selectProduct.className = 'supply-select';
            selectProduct.disabled = isLocked;
            selectProduct.innerHTML = '<option value="">— Выберите товар —</option>';
            suppliesProducts.forEach(p => {
                const opt = document.createElement('option');
                opt.value = p.sku;
                opt.textContent = p.offer_id || p.sku;
                if (data && data.sku == p.sku) opt.selected = true;
                selectProduct.appendChild(opt);
            });
            selectProduct.onchange = () => onSupplyFieldChange(row);
            tdProduct.appendChild(selectProduct);
            row.appendChild(tdProduct);

            // 2. Выход с фабрики ПЛАН (дата без года)
            row.appendChild(createDateCell(data ? data.exit_plan_date : '', isLocked, row, 0));

            // 3. Заказ кол-во ПЛАН (число)
            row.appendChild(createNumberCell(data ? data.order_qty_plan : '', isLocked, row, 'order_qty_plan'));

            // 4. Дата выхода с фабрики (дата)
            row.appendChild(createDateCell(data ? data.exit_factory_date : '', isLocked, row, 1));

            // 5. Кол-во выхода с фабрики (число) — с логикой перераспределения
            row.appendChild(createNumberCell(data ? data.exit_factory_qty : '', isLocked, row, 'exit_factory_qty'));

            // 6. Дата прихода на склад (дата)
            row.appendChild(createDateCell(data ? data.arrival_warehouse_date : '', isLocked, row, 2));

            // 7. Кол-во прихода на склад (число)
            row.appendChild(createNumberCell(data ? data.arrival_warehouse_qty : '', isLocked, row, 'arrival_warehouse_qty'));

            // 8. Стоимость логистики за единицу (руб)
            row.appendChild(createNumberCell(data ? data.logistics_cost_per_unit : '', isLocked, row, 'logistics_cost'));

            // 9. Цена товара единица (юани)
            row.appendChild(createNumberCell(data ? data.price_cny : '', isLocked, row, 'price_cny'));

            // 10. Себестоимость товара +6% (рассчитывается автоматически)
            const tdCost = document.createElement('td');
            const costSpan = document.createElement('span');
            costSpan.className = 'supply-cost-auto';
            if (data && data.cost_plus_6) {
                costSpan.textContent = formatNumberWithSpaces(Math.round(data.cost_plus_6));
            } else {
                costSpan.textContent = '—';
            }
            tdCost.appendChild(costSpan);
            row.appendChild(tdCost);

            // 11. Внести в долги (чекбокс)
            row.appendChild(createCheckboxCell(data ? data.add_to_debts : false, isLocked, row));

            // 13. План на FBO (чекбокс)
            row.appendChild(createCheckboxCell(data ? data.plan_fbo : false, isLocked, row));

            // 14. Кнопка блокировки/разблокировки
            const tdLock = document.createElement('td');
            const lockBtn = document.createElement('button');
            lockBtn.className = 'supply-lock-btn';
            lockBtn.style.cssText = 'background:none; border:none; cursor:pointer; font-size:16px; padding:4px;';
            lockBtn.textContent = isLocked ? '🔒' : '🔓';
            lockBtn.title = isLocked ? 'Дважды кликните строку для разблокировки' : 'Нажмите чтобы заблокировать';
            lockBtn.onclick = function(e) {
                e.stopPropagation();
                if (row.classList.contains('locked-row')) {
                    showEditConfirm(row);
                } else {
                    lockSupplyRow(row);
                }
            };
            tdLock.appendChild(lockBtn);
            row.appendChild(tdLock);

            // 15. Кнопка удаления строки
            const tdDel = document.createElement('td');
            const delBtn = document.createElement('button');
            delBtn.className = 'supply-delete-btn';
            delBtn.textContent = '✕';
            delBtn.title = 'Удалить строку';
            delBtn.onclick = function(e) {
                e.stopPropagation();
                deleteSupplyRow(row);
            };
            tdDel.appendChild(delBtn);
            row.appendChild(tdDel);

            // Если строка заблокирована — ставим обработчик двойного клика
            if (isLocked) {
                row.ondblclick = function() {
                    showEditConfirm(row);
                };
            }

            return row;
        }

        /**
         * Создание ячейки с полем даты (без года — отображается ДД.ММ)
         */
        function createDateCell(value, isLocked, row, dateIndex) {
            const td = document.createElement('td');
            const input = document.createElement('input');
            input.type = 'date';
            input.className = 'supply-input supply-date-input';
            input.style.minWidth = '110px';
            if (value) input.value = value;
            input.disabled = isLocked;
            input.onchange = () => {
                // Валидация порядка дат внутри строки
                const dateInputs = row.querySelectorAll('input[type="date"]');
                const planDate = dateInputs[0] ? dateInputs[0].value : '';
                const factoryDate = dateInputs[1] ? dateInputs[1].value : '';
                const arrivalDate = dateInputs[2] ? dateInputs[2].value : '';

                // dateIndex: 0=план, 1=выход с фабрики, 2=приход на склад
                if (dateIndex === 1 && planDate && factoryDate && factoryDate < planDate) {
                    alert('⚠️ Дата выхода с фабрики не может быть раньше даты плана');
                    input.value = '';
                    return;
                }
                if (dateIndex === 2 && factoryDate && arrivalDate && arrivalDate < factoryDate) {
                    alert('⚠️ Дата прихода на склад не может быть раньше даты выхода с фабрики');
                    input.value = '';
                    return;
                }

                onSupplyFieldChange(row);
                highlightEmptyCells(row);
                updateSupplyTotals();
            };
            td.appendChild(input);
            return td;
        }

        /**
         * Создание ячейки с числовым полем (пробелы в тысячных)
         */
        /**
         * Проверка: можно ли заполнять exit_factory_qty или arrival_warehouse_qty.
         *
         * Для exit_factory_qty: нельзя, если предыдущая строка того же товара
         * не имеет заполненного "Кол-во выхода с фабрики".
         *
         * Для arrival_warehouse_qty: нельзя, если предыдущая строка того же товара
         * не имеет заполненного "Кол-во прихода на склад".
         *
         * Возвращает true если можно заполнять, false если нет.
         */
        function canFillQtyField(row, fieldName) {
            const data = getRowData(row);
            if (!data.sku) return true;

            const currentDate = data.exit_plan_date || data.exit_factory_date || '';
            const allRows = Array.from(document.querySelectorAll('#supplies-tbody tr'));

            // Ищем строки с тем же SKU и более ранней датой
            const prevRows = allRows.filter(r => {
                if (r === row) return false;
                const sel = r.querySelector('select');
                const sku = sel ? (parseInt(sel.value) || 0) : 0;
                if (sku !== data.sku) return false;

                const dateInputs = r.querySelectorAll('input[type="date"]');
                const rDate = dateInputs[0] ? dateInputs[0].value : '';
                if (currentDate && rDate) return rDate < currentDate;
                if (!currentDate && rDate) return true;
                return allRows.indexOf(r) < allRows.indexOf(row);
            });

            if (prevRows.length === 0) return true;

            for (const prevRow of prevRows) {
                const textInputs = prevRow.querySelectorAll('input[type="text"]');

                // Для выхода с фабрики — проверяем exit_factory_qty (textInputs[1])
                if (fieldName === 'exit_factory_qty') {
                    const val = textInputs[1] ? textInputs[1].value.trim() : '';
                    if (val === '') {
                        alert('⚠️ Сначала заполните "Кол-во выхода с фабрики" в предыдущей поставке этого товара');
                        return false;
                    }
                }

                // Для прихода на склад — проверяем arrival_warehouse_qty (textInputs[2])
                if (fieldName === 'arrival_warehouse_qty') {
                    const val = textInputs[2] ? textInputs[2].value.trim() : '';
                    if (val === '') {
                        alert('⚠️ Сначала заполните "Кол-во прихода на склад" в предыдущей поставке этого товара');
                        return false;
                    }
                }
            }
            return true;
        }

        function createNumberCell(value, isLocked, row, fieldName) {
            const td = document.createElement('td');
            const input = document.createElement('input');
            input.type = 'text';
            input.className = 'supply-input';
            input.dataset.field = fieldName || '';
            input.style.minWidth = '90px';
            if (value !== null && value !== undefined && value !== '') {
                input.value = formatNumberWithSpaces(Math.round(parseFloat(value)));
            }
            input.disabled = isLocked;

            // Кнопка-карандаш для редактирования плана (только для order_qty_plan)
            let pencilBtn = null;
            if (fieldName === 'order_qty_plan') {
                pencilBtn = document.createElement('button');
                pencilBtn.type = 'button';
                pencilBtn.className = 'supply-plan-edit-btn';
                pencilBtn.textContent = 'Ред.';
                pencilBtn.title = 'Редактировать план';
                pencilBtn.style.cssText = 'position:absolute;right:2px;top:50%;transform:translateY(-50%);border:1px solid #f59e0b;background:#fff8e1;border-radius:4px;cursor:pointer;padding:2px 6px;display:none;font-size:11px;color:#d97706;font-weight:600;line-height:1.4;z-index:1;';
                // Ячейка должна быть position:relative для позиционирования кнопки
                td.style.position = 'relative';
                td.style.overflow = 'visible';

                // Показываем карандаш если план заполнен (независимо от блокировки строки)
                if (input.value.trim() !== '') {
                    pencilBtn.style.display = 'inline-block';
                }

                // Клик по карандашу — разблокирует строку и разрешает редактирование поля план
                pencilBtn.onclick = function() {
                    // Если строка заблокирована — сначала разблокируем её
                    if (row.classList.contains('locked-row')) {
                        unlockSupplyRow(row);
                    }
                    input.disabled = false;
                    pencilBtn.style.display = 'none';
                    input.focus();
                };
            }

            // Валидация: нельзя заполнять количество без соответствующей даты
            input.onfocus = function() {
                const dateInputs = row.querySelectorAll('input[type="date"]');

                // Проверка: дата должна быть заполнена для соответствующего количества
                if (fieldName === 'order_qty_plan' && (!dateInputs[0] || !dateInputs[0].value)) {
                    this.blur();
                    alert('⚠️ Сначала заполните дату плана');
                    return;
                }
                if (fieldName === 'exit_factory_qty' && (!dateInputs[1] || !dateInputs[1].value)) {
                    this.blur();
                    alert('⚠️ Сначала заполните дату выхода с фабрики');
                    return;
                }
                if (fieldName === 'arrival_warehouse_qty' && (!dateInputs[2] || !dateInputs[2].value)) {
                    this.blur();
                    alert('⚠️ Сначала заполните дату прихода на склад');
                    return;
                }

                // Проверка: предыдущая поставка должна быть заполнена (выход с фабрики и приход)
                if (fieldName === 'exit_factory_qty' || fieldName === 'arrival_warehouse_qty') {
                    if (!canFillQtyField(row, fieldName)) {
                        this.blur();
                        return;
                    }
                }
            };

            // Форматирование при вводе — только цифры и пробелы
            input.oninput = function() {
                const raw = this.value.replace(/[^\\d]/g, '');
                this.value = raw ? formatNumberWithSpaces(parseInt(raw)) : '';
            };

            input.onblur = () => {
                // После ввода плана — блокируем поле и показываем карандаш
                if (fieldName === 'order_qty_plan' && input.value.trim() !== '' && pencilBtn) {
                    input.disabled = true;
                    pencilBtn.style.display = 'inline-block';
                }

                // Валидации для прихода на склад
                if (fieldName === 'arrival_warehouse_qty' && input.value.trim() !== '') {
                    const allRows = Array.from(document.querySelectorAll('#supplies-tbody tr'));
                    let totalPlan = 0;
                    let totalFactory = 0;
                    let totalArrival = 0;
                    allRows.forEach(r => {
                        const ti = r.querySelectorAll('input[type="text"]');
                        totalPlan += ti[0] ? (parseNumberFromSpaces(ti[0].value) || 0) : 0;
                        totalFactory += ti[1] ? (parseNumberFromSpaces(ti[1].value) || 0) : 0;
                        totalArrival += ti[2] ? (parseNumberFromSpaces(ti[2].value) || 0) : 0;
                    });
                    // Итого прихода не может быть больше итого плана
                    if (totalArrival > totalPlan) {
                        alert('⚠️ Итого прихода на склад (' + formatNumberWithSpaces(totalArrival) + ') не может быть больше итого плана (' + formatNumberWithSpaces(totalPlan) + ')');
                        input.value = '';
                    }
                    // Итого прихода не может быть больше итого выхода с фабрики
                    else if (totalArrival > totalFactory) {
                        alert('⚠️ Итого прихода на склад (' + formatNumberWithSpaces(totalArrival) + ') не может быть больше итого выхода с фабрики (' + formatNumberWithSpaces(totalFactory) + ')');
                        input.value = '';
                    }
                }

                // Валидации для выхода с фабрики
                if (fieldName === 'exit_factory_qty' && input.value.trim() !== '') {
                    const allRows = Array.from(document.querySelectorAll('#supplies-tbody tr'));
                    let totalPlan = 0;
                    let totalFactory = 0;
                    let totalArrival = 0;
                    allRows.forEach(r => {
                        const ti = r.querySelectorAll('input[type="text"]');
                        totalPlan += ti[0] ? (parseNumberFromSpaces(ti[0].value) || 0) : 0;
                        totalFactory += ti[1] ? (parseNumberFromSpaces(ti[1].value) || 0) : 0;
                        totalArrival += ti[2] ? (parseNumberFromSpaces(ti[2].value) || 0) : 0;
                    });
                    // Итого выхода с фабрики не может быть больше итого плана
                    if (totalFactory > totalPlan) {
                        alert('⚠️ Итого выхода с фабрики (' + formatNumberWithSpaces(totalFactory) + ') не может быть больше итого плана (' + formatNumberWithSpaces(totalPlan) + ')');
                        input.value = '';
                    }
                    // Итого выхода с фабрики не может быть меньше итого прихода на склад
                    else if (totalFactory < totalArrival) {
                        alert('⚠️ Итого выхода с фабрики (' + formatNumberWithSpaces(totalFactory) + ') не может быть меньше итого прихода на склад (' + formatNumberWithSpaces(totalArrival) + ')');
                        input.value = '';
                    }
                }

                onSupplyFieldChange(row);
            };

            td.appendChild(input);
            // Добавляем кнопку-карандаш рядом с полем плана
            if (pencilBtn) {
                td.appendChild(pencilBtn);
            }
            return td;
        }

        /**
         * Создание ячейки с чекбоксом
         */
        function createCheckboxCell(checked, isLocked, row) {
            const td = document.createElement('td');
            const cb = document.createElement('input');
            cb.type = 'checkbox';
            cb.className = 'supply-checkbox';
            cb.checked = !!checked;
            cb.disabled = isLocked;
            cb.onchange = () => {
                onSupplyFieldChange(row);
                highlightEmptyCells(row);
            };
            td.appendChild(cb);
            return td;
        }

        /**
         * Добавить новую строку в таблицу поставок
         */
        function addSupplyRow() {
            const overlay = document.createElement('div');
            overlay.className = 'supply-edit-confirm';
            overlay.innerHTML = `
                <div class="supply-edit-confirm-box">
                    <h3>Новая строка</h3>
                    <p>Создать новую строку поставки?</p>
                    <button class="supply-confirm-yes">Да, создать</button>
                    <button class="supply-confirm-no">Отмена</button>
                </div>
            `;
            overlay.querySelector('.supply-confirm-yes').onclick = () => {
                overlay.remove();
                const tbody = document.getElementById('supplies-tbody');
                const row = createSupplyRowElement(null);
                tbody.appendChild(row);
                highlightEmptyCells(row);
                updateSupplyTotals();
            };
            overlay.querySelector('.supply-confirm-no').onclick = () => {
                overlay.remove();
            };
            document.body.appendChild(overlay);
        }

        /**
         * Извлечь данные из строки таблицы в объект для отправки на сервер
         */
        function getRowData(row) {
            const cells = row.querySelectorAll('td');
            const select = cells[0].querySelector('select');
            const inputs = row.querySelectorAll('input');

            // Все input-ы по порядку:
            // 0: exit_plan_date, 1: order_qty_plan, 2: exit_factory_date,
            // 3: exit_factory_qty, 4: arrival_warehouse_date, 5: arrival_warehouse_qty,
            // 6: logistics_cost, 7: price_cny
            // Чекбоксы: 8: marketing, 9: debts, 10: fbo

            const dateInputs = row.querySelectorAll('input[type="date"]');
            const textInputs = row.querySelectorAll('input[type="text"]');
            const checkboxes = row.querySelectorAll('input[type="checkbox"]');

            // Вспомогательная функция: пустое поле → null, иначе число
            function numOrNull(input) {
                if (!input || input.value.trim() === '') return null;
                return parseNumberFromSpaces(input.value);
            }

            return {
                id: row.dataset.supplyId,
                sku: select ? parseInt(select.value) || 0 : 0,
                product_name: select ? select.options[select.selectedIndex]?.text || '' : '',
                exit_plan_date: dateInputs[0] ? dateInputs[0].value : '',
                order_qty_plan: numOrNull(textInputs[0]),
                exit_factory_date: dateInputs[1] ? dateInputs[1].value : '',
                exit_factory_qty: numOrNull(textInputs[1]),
                arrival_warehouse_date: dateInputs[2] ? dateInputs[2].value : '',
                arrival_warehouse_qty: numOrNull(textInputs[2]),
                logistics_cost_per_unit: numOrNull(textInputs[3]),
                price_cny: numOrNull(textInputs[4]),
                add_to_marketing: false,
                add_to_debts: checkboxes[0] ? checkboxes[0].checked : false,
                plan_fbo: checkboxes[1] ? checkboxes[1].checked : false
            };
        }

        /**
         * Пересчёт себестоимости товара +6%
         * Формула: (логистика_за_единицу + цена_юань * курс_юаня) * 1.06
         */
        function recalcCost(row) {
            const data = getRowData(row);
            const costSpan = row.querySelector('.supply-cost-auto');
            if (!costSpan) return;

            const logistics = data.logistics_cost_per_unit || 0;
            const priceCny = data.price_cny || 0;

            if (logistics > 0 || priceCny > 0) {
                const cost = (logistics + priceCny * currentCnyRate) * 1.06;
                costSpan.textContent = formatNumberWithSpaces(Math.round(cost));
            } else {
                costSpan.textContent = '—';
            }
        }

        /**
         * Пересчёт всех строк (при обновлении курса)
         */
        function recalcAllCosts() {
            const rows = document.querySelectorAll('#supplies-tbody tr');
            rows.forEach(row => recalcCost(row));
            // Пересчитываем стоимость товара в пути (зависит от себестоимости)
            updateGoodsInTransit();
        }

        /**
         * Обработчик изменения любого поля в строке поставки.
         * Пересчитывает себестоимость и автосохраняет строку.
         */
        function onSupplyFieldChange(row) {
            recalcCost(row);
            highlightEmptyCells(row);
            updateSupplyTotals();
            autoSaveSupplyRow(row);
        }

        /**
         * Автосохранение строки поставки на сервер
         */
        function autoSaveSupplyRow(row) {
            const data = getRowData(row);
            if (!data.sku) return; // Не сохраняем пустые строки

            // Если строка в режиме редактирования — не блокируем
            const isEditing = row.dataset.editing === 'true';

            // Рассчитываем себестоимость
            const logistics = data.logistics_cost_per_unit || 0;
            const priceCny = data.price_cny || 0;
            data.cost_plus_6 = (logistics + priceCny * currentCnyRate) * 1.06;

            fetch('/api/supplies/save', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data)
            })
            .then(r => r.json())
            .then(result => {
                if (result.success && result.id) {
                    row.dataset.supplyId = result.id;
                }
            });
        }

        // Перераспределение между столбцами ПЛАН / ВЫХОД / ПРИХОД удалено.
        // Столбцы независимы друг от друга.

        /**
         * Найти строку таблицы по supply ID
         */
        function findRowById(id) {
            if (!id) return null;
            return document.querySelector('#supplies-tbody tr[data-supply-id="' + id + '"]');
        }

        /**
         * Получить ID строки (dataset.supplyId)
         */
        function getRowId(row) {
            return row.dataset.supplyId || '';
        }

        // findNextSameSkuRow, modifyPlanQty, createRedistributionRow — удалены вместе с перераспределением.

        /**
         * Показать диалог подтверждения для редактирования заблокированного поля.
         * При нажатии "Да" — разблокирует строку, при "Отмена" — ничего не делает.
         */
        function showEditConfirm(row) {
            const overlay = document.createElement('div');
            overlay.className = 'supply-edit-confirm';
            overlay.innerHTML = `
                <div class="supply-edit-confirm-box">
                    <h3>Подтверждение</h3>
                    <p>Эта строка заблокирована. Разрешить редактирование?</p>
                    <button class="supply-confirm-yes">Да, редактировать</button>
                    <button class="supply-confirm-no">Отмена</button>
                </div>
            `;

            overlay.querySelector('.supply-confirm-yes').onclick = () => {
                overlay.remove();
                unlockSupplyRow(row);
                // Разблокируем на сервере
                fetch('/api/supplies/unlock', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ id: row.dataset.supplyId })
                });
            };
            overlay.querySelector('.supply-confirm-no').onclick = () => {
                overlay.remove();
            };

            document.body.appendChild(overlay);
        }

        /**
         * Блокировка строки (защита от случайного редактирования).
         * Вызывается по нажатию кнопки-замка в строке.
         */
        function lockSupplyRow(row) {
            const inputs = row.querySelectorAll('.supply-input, .supply-select, .supply-checkbox');
            inputs.forEach(el => el.disabled = true);
            row.classList.add('locked-row');

            // Скрываем кнопку-карандаш при блокировке
            const pencilBtn = row.querySelector('.supply-plan-edit-btn');
            if (pencilBtn) {
                pencilBtn.style.display = 'none';
            }

            // Обновляем иконку замка
            const lockBtn = row.querySelector('.supply-lock-btn');
            if (lockBtn) {
                lockBtn.textContent = '🔒';
                lockBtn.title = 'Дважды кликните для разблокировки';
            }

            // Двойной клик — разблокировка с подтверждением
            row.ondblclick = function(e) {
                // Не срабатывает на кнопке замка (у неё свой обработчик)
                showEditConfirm(row);
            };

            // Убираем из списка разблокированных
            const supplyId = row.dataset.supplyId;
            if (supplyId) {
                const unlocks = JSON.parse(localStorage.getItem('supply_unlocks') || '{}');
                delete unlocks[supplyId];
                localStorage.setItem('supply_unlocks', JSON.stringify(unlocks));
            }

            // Блокируем на сервере
            if (supplyId && !String(supplyId).startsWith('new_')) {
                fetch('/api/supplies/lock', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ id: supplyId })
                });
            }
        }

        /**
         * Разблокировка строки для редактирования
         */
        function unlockSupplyRow(row) {
            const inputs = row.querySelectorAll('.supply-input, .supply-select, .supply-checkbox');
            inputs.forEach(el => el.disabled = false);
            row.classList.remove('locked-row');
            row.ondblclick = null;

            // Обновляем иконку замка
            const lockBtn = row.querySelector('.supply-lock-btn');
            if (lockBtn) {
                lockBtn.textContent = '🔓';
                lockBtn.title = 'Нажмите чтобы заблокировать';
            }

            // Если план заполнен — оставляем поле заблокированным, показываем карандаш
            const planInput = row.querySelector('input[data-field="order_qty_plan"]');
            const pencilBtn = row.querySelector('.supply-plan-edit-btn');
            if (planInput && planInput.value.trim() !== '' && pencilBtn) {
                planInput.disabled = true;
                pencilBtn.style.display = 'inline-block';
            }

            // Запоминаем время разблокировки (сохраняется 30 минут)
            const supplyId = row.dataset.supplyId;
            if (supplyId) {
                const unlocks = JSON.parse(localStorage.getItem('supply_unlocks') || '{}');
                unlocks[supplyId] = Date.now();
                localStorage.setItem('supply_unlocks', JSON.stringify(unlocks));
            }
        }

        // ============================================================
        // УДАЛЕНИЕ СТРОКИ С ПОДТВЕРЖДЕНИЕМ
        // ============================================================

        function deleteSupplyRow(row) {
            const overlay = document.createElement('div');
            overlay.className = 'supply-edit-confirm';
            overlay.innerHTML = `
                <div class="supply-edit-confirm-box">
                    <h3>Удаление строки</h3>
                    <p>Вы уверены, что хотите удалить эту строку? Действие нельзя отменить.</p>
                    <button class="supply-confirm-yes" style="background:#ef4444;">Да, удалить</button>
                    <button class="supply-confirm-no">Отмена</button>
                </div>
            `;

            overlay.querySelector('.supply-confirm-yes').onclick = () => {
                overlay.remove();
                const supplyId = row.dataset.supplyId;

                // Удаляем из DOM
                row.remove();
                updateSupplyTotals();

                // Удаляем с сервера
                if (supplyId && !String(supplyId).startsWith('new_')) {
                    fetch('/api/supplies/delete', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ id: supplyId })
                    });
                }
            };
            overlay.querySelector('.supply-confirm-no').onclick = () => {
                overlay.remove();
            };

            document.body.appendChild(overlay);
        }

        // ============================================================
        // СОРТИРОВКА ПО СТОЛБЦАМ С ДАТАМИ
        // ============================================================

        let suppliesSortCol = -1;
        let suppliesSortAsc = true;

        /**
         * Сортировка таблицы поставок по столбцу с датой.
         * colIndex — индекс столбца (1 = Выход план, 3 = Дата выхода, 5 = Дата прихода)
         */
        function sortSuppliesByDate(colIndex) {
            const tbody = document.getElementById('supplies-tbody');
            const rows = Array.from(tbody.querySelectorAll('tr'));

            // Переключаем направление
            if (suppliesSortCol === colIndex) {
                suppliesSortAsc = !suppliesSortAsc;
            } else {
                suppliesSortCol = colIndex;
                suppliesSortAsc = true;
            }

            rows.sort((a, b) => {
                const dateA = a.querySelectorAll('input[type="date"]');
                const dateB = b.querySelectorAll('input[type="date"]');
                // colIndex 1→dateInputs[0], 3→dateInputs[1], 5→dateInputs[2]
                const dateIdx = colIndex === 1 ? 0 : colIndex === 3 ? 1 : 2;
                const valA = dateA[dateIdx] ? dateA[dateIdx].value : '';
                const valB = dateB[dateIdx] ? dateB[dateIdx].value : '';

                // Пустые даты — в конец
                if (!valA && !valB) return 0;
                if (!valA) return 1;
                if (!valB) return -1;

                const cmp = valA.localeCompare(valB);
                return suppliesSortAsc ? cmp : -cmp;
            });

            rows.forEach(r => tbody.appendChild(r));

            // Обновляем стрелки
            document.querySelectorAll('.sortable-date .sort-arrow').forEach(el => el.textContent = '');
            const th = document.querySelector('.sortable-date[data-col="' + colIndex + '"] .sort-arrow');
            if (th) th.textContent = suppliesSortAsc ? ' ▲' : ' ▼';
        }

        // ============================================================
        // ФИЛЬТР ПО ТОВАРУ
        // ============================================================

        /**
         * Заполняет выпадающий список фильтра уникальными товарами
         */
        function populateSuppliesFilter() {
            const filter = document.getElementById('supplies-product-filter');
            if (!filter) return;

            const currentVal = filter.value;
            // Очищаем, кроме первого пункта "Все товары"
            while (filter.options.length > 1) filter.remove(1);

            // Собираем уникальные товары из строк
            const seen = new Set();
            document.querySelectorAll('#supplies-tbody tr').forEach(row => {
                const sel = row.querySelector('select');
                if (sel && sel.value) {
                    const sku = sel.value;
                    if (!seen.has(sku)) {
                        seen.add(sku);
                        const opt = document.createElement('option');
                        opt.value = sku;
                        opt.textContent = sel.options[sel.selectedIndex]?.text || sku;
                        filter.appendChild(opt);
                    }
                }
            });

            filter.value = currentVal;
        }

        /**
         * Фильтрация строк таблицы по выбранному товару
         */
        function filterSuppliesTable() {
            const filter = document.getElementById('supplies-product-filter');
            const selectedSku = filter ? filter.value : '';

            document.querySelectorAll('#supplies-tbody tr').forEach(row => {
                if (!selectedSku) {
                    row.style.display = '';
                } else {
                    const sel = row.querySelector('select');
                    const rowSku = sel ? sel.value : '';
                    row.style.display = (rowSku === selectedSku) ? '' : 'none';
                }
            });

            updateSupplyTotals();
        }

        // ============================================================
        // СУММЫ И СРЕДНИЕ В ПОДВАЛЕ ТАБЛИЦЫ
        // ============================================================

        /**
         * Обновить итоги в tfoot.
         * Числовые столбцы (кол-во) — сумма.
         * Валютные столбцы (логистика, цена ¥, себестоимость) — среднее.
         */
        function updateSupplyTotals() {
            const tfoot = document.getElementById('supplies-tfoot-row');
            if (!tfoot) return;

            // Видимые строки
            const rows = Array.from(document.querySelectorAll('#supplies-tbody tr'))
                .filter(r => r.style.display !== 'none');

            // Индексы столбцов (0-based):
            // 0:товар, 1:дата план, 2:заказ план, 3:дата выхода, 4:кол выхода,
            // 5:дата прихода, 6:кол прихода, 7:логистика₽, 8:цена¥, 9:себестоимость,
            // 10:долги, 11:FBO, 12:замок, 13:удалить

            // Столбцы с суммами (числа, не валюты)
            const sumCols = [2, 4, 6];
            // Столбцы со средним (валюты)
            const avgCols = [7, 8, 9];

            // Собираем данные
            const sums = {};
            const avgs = {};
            const counts = {};

            sumCols.forEach(i => { sums[i] = 0; });
            avgCols.forEach(i => { avgs[i] = 0; counts[i] = 0; });

            rows.forEach(row => {
                const textInputs = row.querySelectorAll('input[type="text"]');
                // textInputs порядок: 0=order_qty_plan, 1=exit_factory_qty, 2=arrival_warehouse_qty,
                //                     3=logistics_cost, 4=price_cny
                const vals = [];
                textInputs.forEach(inp => vals.push(parseNumberFromSpaces(inp.value)));

                // Сумма: заказ план (idx 0→col 2), выход (idx 1→col 4), приход (idx 2→col 6)
                if (vals[0]) sums[2] += vals[0];
                if (vals[1]) sums[4] += vals[1];
                if (vals[2]) sums[6] += vals[2];

                // Среднее: логистика (idx 3→col 7), цена¥ (idx 4→col 8)
                if (vals[3]) { avgs[7] += vals[3]; counts[7]++; }
                if (vals[4]) { avgs[8] += vals[4]; counts[8]++; }

                // Себестоимость из span
                const costSpan = row.querySelector('.supply-cost-auto');
                if (costSpan && costSpan.textContent !== '—') {
                    const costVal = parseNumberFromSpaces(costSpan.textContent);
                    if (costVal) { avgs[9] += costVal; counts[9]++; }
                }
            });

            // Строим строку итогов
            let html = '<td style="font-weight:600; text-align:right;">Итого:</td>'; // товар
            html += '<td></td>'; // дата план

            // Заказ план (сумма)
            html += '<td>' + (sums[2] ? formatNumberWithSpaces(sums[2]) : '') + '</td>';
            html += '<td></td>'; // дата выхода

            // Кол-во выхода (сумма)
            html += '<td>' + (sums[4] ? formatNumberWithSpaces(sums[4]) : '') + '</td>';
            html += '<td></td>'; // дата прихода

            // Кол-во прихода (сумма)
            html += '<td>' + (sums[6] ? formatNumberWithSpaces(sums[6]) : '') + '</td>';

            // Логистика (среднее)
            html += '<td>' + (counts[7] ? formatNumberWithSpaces(Math.round(avgs[7] / counts[7])) : '') + '</td>';

            // Цена ¥ (среднее)
            html += '<td>' + (counts[8] ? formatNumberWithSpaces(Math.round(avgs[8] / counts[8])) : '') + '</td>';

            // Себестоимость (среднее)
            html += '<td>' + (counts[9] ? formatNumberWithSpaces(Math.round(avgs[9] / counts[9])) : '') + '</td>';

            html += '<td></td><td></td>'; // чекбоксы (долги, FBO)
            html += '<td></td><td></td>'; // замок, удалить

            tfoot.innerHTML = html;

            // Пересчитываем стоимость товара в пути
            updateGoodsInTransit();
        }

        // ============================================================
        // СТОИМОСТЬ ТОВАРА В ПУТИ
        // ============================================================

        /**
         * Расчёт стоимости товара в пути.
         *
         * Логика: группируем строки по товару (SKU), для каждого товара считаем
         * средние (себестоимость, цена ¥) и количества (план, фабрика, приход),
         * потом умножаем и складываем итоги по всем товарам.
         * Это даёт корректный результат когда показаны все товары сразу.
         */
        function updateGoodsInTransit() {
            const qtyEl = document.getElementById('goods-in-transit-qty');
            const costEl = document.getElementById('goods-in-transit-cost');
            const costNoLogEl = document.getElementById('goods-in-transit-cost-no-log');
            const logInTransitEl = document.getElementById('logistics-in-transit');
            const planQtyEl = document.getElementById('plan-not-delivered-qty');
            const planCostEl = document.getElementById('plan-not-delivered-cost');
            const planCostNoLogEl = document.getElementById('plan-not-delivered-cost-no-log');
            const logPlanEl = document.getElementById('logistics-plan');

            // Берём только видимые строки (с учётом фильтра по товару)
            const rows = Array.from(document.querySelectorAll('#supplies-tbody tr'))
                .filter(r => r.style.display !== 'none');

            // Группируем строки по SKU товара
            const byProduct = {};
            rows.forEach(row => {
                const sel = row.querySelector('select');
                const sku = sel ? sel.value : 'unknown';
                if (!byProduct[sku]) byProduct[sku] = [];
                byProduct[sku].push(row);
            });

            // Итоговые суммы по всем товарам (с наценкой +6%)
            let totalInTransitQty = 0;
            let totalInTransitCostFull = 0;
            let totalInTransitCostNoLog = 0;
            let totalPlanNotDeliveredQty = 0;
            let totalPlanCostFull = 0;
            let totalPlanCostNoLog = 0;

            // Итоговые суммы БЕЗ наценки +6%
            let totalInTransitCostFullNo6 = 0;      // себестоимость = (логистика + цена¥×курс) без ×1.06
            let totalInTransitCostNoLogNo6 = 0;     // только цена¥×курс
            let totalInTransitLogistics = 0;        // только логистика
            let totalPlanCostFullNo6 = 0;
            let totalPlanCostNoLogNo6 = 0;
            let totalPlanLogistics = 0;

            // Считаем по каждому товару отдельно
            Object.keys(byProduct).forEach(sku => {
                const productRows = byProduct[sku];
                let plan = 0, factory = 0, arrival = 0;
                let costSum = 0, costCount = 0;
                let cnySum = 0, cnyCount = 0;
                let logSum = 0, logCount = 0;

                productRows.forEach(row => {
                    const ti = row.querySelectorAll('input[type="text"]');
                    plan += ti[0] ? (parseNumberFromSpaces(ti[0].value) || 0) : 0;
                    factory += ti[1] ? (parseNumberFromSpaces(ti[1].value) || 0) : 0;
                    arrival += ti[2] ? (parseNumberFromSpaces(ti[2].value) || 0) : 0;
                    const logVal = ti[3] ? (parseNumberFromSpaces(ti[3].value) || 0) : 0;
                    const priceCny = ti[4] ? (parseNumberFromSpaces(ti[4].value) || 0) : 0;
                    if (priceCny) { cnySum += priceCny; cnyCount++; }
                    if (logVal) { logSum += logVal; logCount++; }

                    const costSpan = row.querySelector('.supply-cost-auto');
                    if (costSpan && costSpan.textContent !== '—') {
                        const cv = parseNumberFromSpaces(costSpan.textContent);
                        if (cv) { costSum += cv; costCount++; }
                    }
                });

                // Средние по этому товару
                const avgCost = costCount > 0 ? costSum / costCount : 0;           // себестоимость +6%
                const avgCny = cnyCount > 0 ? cnySum / cnyCount : 0;               // цена ¥
                const avgLog = logCount > 0 ? logSum / logCount : 0;               // логистика за ед.
                const avgCostNoLog = avgCny * currentCnyRate * 1.06;               // цена¥×курс×1.06

                // Без наценки +6%
                const avgCostNo6 = avgLog + avgCny * currentCnyRate;               // логистика + цена¥×курс
                const avgCostNoLogNo6 = avgCny * currentCnyRate;                   // только цена¥×курс

                // В пути по этому товару
                const inTransit = factory - arrival;
                if (inTransit > 0) {
                    totalInTransitQty += inTransit;
                    totalInTransitCostFull += inTransit * avgCost;
                    totalInTransitCostNoLog += inTransit * avgCostNoLog;
                    // Без наценки
                    totalInTransitCostFullNo6 += inTransit * avgCostNo6;
                    totalInTransitCostNoLogNo6 += inTransit * avgCostNoLogNo6;
                    totalInTransitLogistics += inTransit * avgLog;
                }

                // План не доставлен по этому товару
                const planNotDel = plan - arrival;
                if (planNotDel > 0) {
                    totalPlanNotDeliveredQty += planNotDel;
                    totalPlanCostFull += planNotDel * avgCost;
                    totalPlanCostNoLog += planNotDel * avgCostNoLog;
                    // Без наценки
                    totalPlanCostFullNo6 += planNotDel * avgCostNo6;
                    totalPlanCostNoLogNo6 += planNotDel * avgCostNoLogNo6;
                    totalPlanLogistics += planNotDel * avgLog;
                }
            });

            // Вспомогательная функция для заполнения значения карточки
            function fillVal(el, val) {
                if (!el) return;
                if (val > 0) {
                    el.textContent = formatNumberWithSpaces(Math.round(val));
                } else {
                    el.textContent = val === 0 ? '0' : '—';
                }
            }

            // Вспомогательная функция для подписи "без наценки +6%"
            function fillNo6(el, val) {
                if (!el) return;
                if (val > 0) {
                    el.textContent = 'без наценки +6%: ' + formatNumberWithSpaces(Math.round(val)) + ' ₽';
                } else {
                    el.textContent = 'без наценки +6%: —';
                }
            }

            // Товар в пути
            fillVal(qtyEl, totalInTransitQty);
            fillVal(costEl, totalInTransitCostFull);
            fillVal(costNoLogEl, totalInTransitCostNoLog);
            fillVal(logInTransitEl, totalInTransitCostFull - totalInTransitCostNoLog);
            // Без наценки +6%
            fillNo6(document.getElementById('goods-in-transit-cost-no6'), totalInTransitCostFullNo6);
            fillNo6(document.getElementById('goods-in-transit-cost-no-log-no6'), totalInTransitCostNoLogNo6);
            fillNo6(document.getElementById('logistics-in-transit-no6'), totalInTransitLogistics);

            // План не доставлен
            fillVal(planQtyEl, totalPlanNotDeliveredQty);
            fillVal(planCostEl, totalPlanCostFull);
            fillVal(planCostNoLogEl, totalPlanCostNoLog);
            fillVal(logPlanEl, totalPlanCostFull - totalPlanCostNoLog);
            // Без наценки +6%
            fillNo6(document.getElementById('plan-cost-no6'), totalPlanCostFullNo6);
            fillNo6(document.getElementById('plan-cost-no-log-no6'), totalPlanCostNoLogNo6);
            fillNo6(document.getElementById('logistics-plan-no6'), totalPlanLogistics);
        }

        // ============================================================
        // ПОДСВЕТКА ПУСТЫХ ЯЧЕЕК
        // ============================================================

        /**
         * Подсветить незаполненные ячейки в строке бледно-красным
         */
        function highlightEmptyCells(row) {
            // Все input и select в строке
            const inputs = row.querySelectorAll('.supply-input, .supply-select');
            inputs.forEach(el => {
                const td = el.closest('td');
                if (!td) return;

                let isEmpty = false;
                if (el.tagName === 'SELECT') {
                    isEmpty = !el.value;
                } else if (el.type === 'date') {
                    isEmpty = !el.value;
                } else if (el.type === 'text') {
                    isEmpty = !el.value.trim();
                }

                if (isEmpty) {
                    td.classList.add('supply-cell-empty');
                } else {
                    td.classList.remove('supply-cell-empty');
                }
            });

            // Чекбоксы — незаполненные (unchecked) помечаем бледно-красным
            const checkboxes = row.querySelectorAll('.supply-checkbox');
            checkboxes.forEach(cb => {
                const td = cb.closest('td');
                if (!td) return;
                if (!cb.checked) {
                    td.classList.add('supply-cell-empty');
                } else {
                    td.classList.remove('supply-cell-empty');
                }
            });
        }

        /**
         * Подсветить пустые ячейки во всех строках
         */
        function highlightAllEmptyCells() {
            document.querySelectorAll('#supplies-tbody tr').forEach(row => {
                highlightEmptyCells(row);
            });
        }

    </script>
</body>
</html>
'''


@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route('/api/products')
def get_products():
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        date_filter = request.args.get('date')
        print(f"\n🔍 DEBUG /api/products - date_filter: {date_filter}")

        if date_filter:
            # ✅ Берём снимок за выбранную дату ИЗ ИСТОРИИ
            cursor.execute('''
                SELECT
                    sku,
                    name,
                    fbo_stock,
                    orders_qty,
                    snapshot_date AS updated_at
                FROM products_history
                WHERE snapshot_date = ?
                ORDER BY fbo_stock DESC, name
            ''', (date_filter,))
            print(f"   📅 Читаю из products_history для даты {date_filter}")
        else:
            # ✅ Текущие данные — из products (сегодняшний снимок)
            cursor.execute('''
                SELECT sku, name, fbo_stock, orders_qty, updated_at
                FROM products
                ORDER BY fbo_stock DESC, name
            ''')
            print(f"   📊 Читаю из products (текущий снимок)")

        rows = cursor.fetchall()
        products = [dict(row) for row in rows]
        total_stock = sum(int(p.get('fbo_stock') or 0) for p in products)

        print(f"   ✅ Найдено {len(products)} товаров, всего остатков: {total_stock}")

        conn.close()

        return jsonify({
            'success': True,
            'count': len(products),
            'total_stock': total_stock,
            'products': products
        })
    except Exception as e:
        print(f"   ❌ Ошибка: {e}")
        return jsonify({'success': False, 'error': str(e), 'products': []})


@app.route('/api/dates')
def get_dates():
    """Получить все доступные даты в истории для выпадающего списка"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT DISTINCT snapshot_date
            FROM products_history
            ORDER BY snapshot_date DESC
        ''')
        
        dates = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        return jsonify({
            'success': True,
            'dates': dates
        })
    except Exception as e:
        print(f"❌ ERROR /api/dates: {e}")
        return jsonify({'success': False, 'error': str(e), 'dates': []})


@app.route('/api/products/current')
def get_products_current():
    """Получить текущие товары с показами и CTR"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                sku, name, fbo_stock, orders_qty, 
                hits_view_search, hits_view_search_pdp, search_ctr, updated_at
            FROM products
            ORDER BY sku DESC
        ''')
        
        products = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return jsonify({
            'success': True,
            'products': products
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'products': []})


@app.route('/api/products/list')
def get_products_list():
    """Получить список уникальных товаров для выпадающего списка"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Берём товары с последним известным названием и артикулом (по дате)
        # Если offer_id нет в products_history — подтягиваем из products
        # SKU 1235819146 (ПЖД) первым, потом остальные по имени
        cursor.execute('''
            SELECT ph.sku, ph.name,
                   COALESCE(ph.offer_id, p.offer_id) AS offer_id
            FROM products_history ph
            JOIN (
              SELECT sku, MAX(snapshot_date) AS max_date
              FROM products_history
              GROUP BY sku
            ) last
            ON last.sku = ph.sku AND last.max_date = ph.snapshot_date
            LEFT JOIN products p ON p.sku = ph.sku
            ORDER BY
                CASE WHEN ph.sku = 1235819146 THEN 0 ELSE 1 END,
                ph.name
        ''')

        products = [{'sku': row['sku'], 'name': row['name'], 'offer_id': row['offer_id']} for row in cursor.fetchall()]
        conn.close()
        
        return jsonify({
            'success': True,
            'products': products
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'products': []})


@app.route('/api/history/<int:sku>')
def get_product_history(sku):
    """Получить историю товара по SKU"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Получаем всю историю товара, отсортированную по датам (новые первыми)
        cursor.execute('''
            SELECT
                snapshot_date,
                name,
                sku,
                fbo_stock,
                orders_qty,
                orders_plan,
                cpo_plan,
                price_plan,
                rating,
                review_count,
                price_index,
                price,
                marketing_price,
                avg_position,
                hits_view_search,
                hits_view_search_pdp,
                search_ctr,
                hits_add_to_cart,
                cr1,
                cr2,
                adv_spend,
                in_transit,
                in_draft,
                snapshot_time,
                notes
            FROM products_history
            WHERE sku = ?
            ORDER BY snapshot_date DESC
        ''', (sku,))
        
        history = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        if not history:
            return jsonify({
                'success': False,
                'error': 'История не найдена для этого товара',
                'history': []
            })
        
        return jsonify({
            'success': True,
            'product_name': history[0]['name'] if history else '',
            'product_sku': sku,
            'history': history
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'history': []})


@app.route('/api/history/save-note', methods=['POST'])
def save_note():
    """Сохранить заметку для товара и даты"""
    try:
        data = request.json
        sku = data.get('sku')
        snapshot_date = data.get('date')
        notes = data.get('notes', '')

        if not sku or not snapshot_date:
            return jsonify({'success': False, 'error': 'Отсутствуют sku или date'})

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE products_history
            SET notes = ?
            WHERE sku = ? AND snapshot_date = ?
        ''', (notes, sku, snapshot_date))

        conn.commit()
        conn.close()

        return jsonify({'success': True, 'message': 'Заметка сохранена'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/history/save-orders-plan', methods=['POST'])
def save_orders_plan():
    """Сохранить плановое количество заказов для товара и даты"""
    try:
        data = request.json
        sku = data.get('sku')
        snapshot_date = data.get('date')
        orders_plan = data.get('orders_plan')

        if not sku or not snapshot_date:
            return jsonify({'success': False, 'error': 'Отсутствуют sku или date'})

        # Проверяем, что редактируем только сегодняшние или будущие данные
        from datetime import datetime
        today = datetime.now().date()
        target_date = datetime.strptime(snapshot_date, '%Y-%m-%d').date()

        if target_date < today:
            return jsonify({'success': False, 'error': 'Нельзя редактировать прошлые данные'})

        # Преобразуем пустую строку в NULL
        if orders_plan == '':
            orders_plan = None
        else:
            orders_plan = int(orders_plan)

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE products_history
            SET orders_plan = ?
            WHERE sku = ? AND snapshot_date = ?
        ''', (orders_plan, sku, snapshot_date))

        conn.commit()
        conn.close()

        return jsonify({'success': True, 'message': 'План заказов сохранен'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/history/save-cpo-plan', methods=['POST'])
def save_cpo_plan():
    """Сохранить плановый CPO для товара и даты"""
    try:
        data = request.json
        sku = data.get('sku')
        snapshot_date = data.get('date')
        cpo_plan = data.get('cpo_plan')

        if not sku or not snapshot_date:
            return jsonify({'success': False, 'error': 'Отсутствуют sku или date'})

        # Проверяем, что редактируем только сегодняшние или будущие данные
        from datetime import datetime
        today = datetime.now().date()
        target_date = datetime.strptime(snapshot_date, '%Y-%m-%d').date()

        if target_date < today:
            return jsonify({'success': False, 'error': 'Нельзя редактировать прошлые данные'})

        # Преобразуем пустую строку в NULL
        if cpo_plan == '':
            cpo_plan = None
        else:
            cpo_plan = int(cpo_plan)

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE products_history
            SET cpo_plan = ?
            WHERE sku = ? AND snapshot_date = ?
        ''', (cpo_plan, sku, snapshot_date))

        conn.commit()
        conn.close()

        return jsonify({'success': True, 'message': 'План CPO сохранен'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/history/save-price-plan', methods=['POST'])
def save_price_plan():
    """Сохранить плановую цену для товара и даты"""
    try:
        data = request.json
        sku = data.get('sku')
        snapshot_date = data.get('date')
        price_plan = data.get('price_plan')

        if not sku or not snapshot_date:
            return jsonify({'success': False, 'error': 'Отсутствуют sku или date'})

        # Проверяем, что редактируем только сегодняшние или будущие данные
        from datetime import datetime
        today = datetime.now().date()
        target_date = datetime.strptime(snapshot_date, '%Y-%m-%d').date()

        if target_date < today:
            return jsonify({'success': False, 'error': 'Нельзя редактировать прошлые данные'})

        # Преобразуем пустую строку в NULL
        if price_plan == '':
            price_plan = None
        else:
            price_plan = int(price_plan)

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE products_history
            SET price_plan = ?
            WHERE sku = ? AND snapshot_date = ?
        ''', (price_plan, sku, snapshot_date))

        conn.commit()
        conn.close()

        return jsonify({'success': True, 'message': 'План цены сохранен'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/update-rating/<int:sku>', methods=['POST'])
def update_rating(sku):
    """Обновить рейтинг и количество отзывов для товара"""
    try:
        data = request.json
        rating = data.get('rating')
        review_count = data.get('review_count')

        if rating is None or review_count is None:
            return jsonify({'success': False, 'error': 'Отсутствуют rating или review_count'})

        snapshot_date = get_snapshot_date()

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Обновляем в истории для сегодняшнего дня
        cursor.execute('''
            UPDATE products_history
            SET rating = ?, review_count = ?
            WHERE sku = ? AND snapshot_date = ?
        ''', (float(rating), int(review_count), sku, snapshot_date))

        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'message': f'Рейтинг обновлен: {rating} ({review_count} отзывов)'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/download/<filename>')
def download_file(filename):
    """Скачать файл обновления"""
    try:
        import os
        
        # Разрешаем только конкретные файлы
        allowed_files = ['ozon_app.py', 'run.py', 'auto_commit.ps1', 'auto_update.py', 'add_history_data.py']
        
        if filename not in allowed_files:
            return jsonify({'error': 'Файл не найден'}), 404
        
        # Отправляем файл текущей папки
        file_path = os.path.join(os.path.dirname(__file__), filename)
        
        if not os.path.exists(file_path):
            return jsonify({'error': 'Файл не найден на сервере'}), 404
        
        from flask import send_file
        return send_file(file_path, as_attachment=True, download_name=filename)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================================================
# ПАРСИНГ РЕЙТИНГОВ (флаг для локального парсера)
# ============================================================================
#
# Как работает:
# 1. Пользователь нажимает кнопку "Парсить рейтинги" на сайте
# 2. Сервер сохраняет запрос в файл /tmp/ozon-parse-request.json
# 3. Локальный скрипт (update_ratings_local.py --watch) опрашивает сервер
# 4. Когда видит запрос — парсит рейтинги через Chrome на ПК пользователя
# 5. Отправляет результаты обратно на сервер
# 6. Сайт показывает результат
# ============================================================================

PARSE_REQUEST_FILE = '/tmp/ozon-parse-request.json'


def _read_parse_state():
    """Читает текущее состояние запроса на парсинг из файла"""
    try:
        if os.path.exists(PARSE_REQUEST_FILE):
            with open(PARSE_REQUEST_FILE, 'r') as f:
                return json.load(f)
    except Exception:
        pass
    return {'status': 'idle'}


def _write_parse_state(state):
    """Записывает состояние запроса на парсинг в файл"""
    with open(PARSE_REQUEST_FILE, 'w') as f:
        json.dump(state, f)


@app.route('/api/parse-status')
def api_parse_status():
    """
    Возвращает текущий статус парсинга.
    Используется кнопкой на сайте (поллинг) и локальным скриптом.
    """
    return jsonify(_read_parse_state())


@app.route('/api/parse-complete', methods=['POST'])
def api_parse_complete():
    """
    Вызывается локальным парсером когда работа завершена.
    Сохраняет результаты и обновляет статус.
    """
    try:
        from datetime import datetime
        data = request.json or {}

        _write_parse_state({
            'status': 'completed',
            'completed_at': datetime.now().isoformat(),
            'results': {
                'success': data.get('success', 0),
                'failed': data.get('failed', 0)
            },
            'message': data.get('message', 'Парсинг завершен')
        })

        print(f"⭐ Парсинг завершен: {data.get('success', 0)} успешно, {data.get('failed', 0)} не удалось")
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/parse-start', methods=['POST'])
def api_parse_start():
    """
    Вызывается локальным парсером когда он начинает работу.
    Обновляет статус на 'running'.
    """
    try:
        from datetime import datetime
        _write_parse_state({
            'status': 'running',
            'started_at': datetime.now().isoformat(),
            'message': 'Парсер работает на ПК...'
        })
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/sync', methods=['POST'])
def api_sync():
    """Обновить данные из Ozon API"""
    try:
        print("\n🔄 Запуск синхронизации по запросу пользователя...")
        success = sync_products()

        if success:
            return jsonify({
                'success': True,
                'message': 'Данные успешно обновлены',
                'date': get_snapshot_date()
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Ошибка при обновлении данных'
            }), 500

    except Exception as e:
        print(f"❌ Ошибка при синхронизации: {e}")
        return jsonify({
            'success': False,
            'message': f'Ошибка: {str(e)}'
        }), 500

# ============================================================================
# ЭНДПОИНТ: АНАЛИТИКА FBO ПО КЛАСТЕРАМ
# ============================================================================

@app.route('/api/fbo-analytics')
def get_fbo_analytics():
    """
    Получить аналитику FBO с разбивкой по кластерам.

    Возвращает список товаров с общими показателями и вложенным массивом кластеров.
    Каждый кластер содержит: остатки, ADS, IDC, дни без продаж, статус ликвидности.
    Также включает данные о поставках (в пути и в заявках) из products_history.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Получаем последнюю дату снапшота для аналитики
        cursor.execute('SELECT MAX(snapshot_date) as max_date FROM fbo_analytics')
        row = cursor.fetchone()
        analytics_date = row['max_date'] if row else None

        if not analytics_date:
            conn.close()
            return jsonify({'success': True, 'products': [], 'message': 'Нет данных аналитики. Выполните синхронизацию.'})

        # Получаем кластерные данные за последнюю дату
        cursor.execute('''
            SELECT sku, cluster_name, ads, idc, days_without_sales, liquidity_status, stock
            FROM fbo_analytics
            WHERE snapshot_date = ?
            ORDER BY sku, cluster_name
        ''', (analytics_date,))
        analytics_rows = cursor.fetchall()

        # Получаем per-warehouse stock за последнюю дату
        cursor.execute('SELECT MAX(snapshot_date) as max_date FROM fbo_warehouse_stock')
        wh_row = cursor.fetchone()
        wh_date = wh_row['max_date'] if wh_row else None

        warehouse_stocks = {}
        if wh_date:
            cursor.execute('''
                SELECT sku, warehouse_name, stock
                FROM fbo_warehouse_stock
                WHERE snapshot_date = ?
                ORDER BY sku, warehouse_name
            ''', (wh_date,))
            for r in cursor.fetchall():
                sku = r['sku']
                if sku not in warehouse_stocks:
                    warehouse_stocks[sku] = []
                warehouse_stocks[sku].append({
                    'warehouse_name': r['warehouse_name'],
                    'stock': r['stock']
                })

        # Получаем информацию о товарах (название, артикул) и поставки
        cursor.execute('''
            SELECT ph.sku, ph.name, ph.offer_id, ph.fbo_stock, ph.in_transit, ph.in_draft
            FROM products_history ph
            JOIN (
                SELECT sku, MAX(snapshot_date) AS max_date
                FROM products_history
                GROUP BY sku
            ) last ON last.sku = ph.sku AND last.max_date = ph.snapshot_date
        ''')
        product_info = {}
        for r in cursor.fetchall():
            product_info[r['sku']] = {
                'name': r['name'],
                'offer_id': r['offer_id'],
                'fbo_stock': r['fbo_stock'] or 0,
                'in_transit': r['in_transit'] or 0,
                'in_draft': r['in_draft'] or 0
            }

        conn.close()

        # Для SKU которых нет в products_history или у которых нет offer_id — подтягиваем из API
        analytics_skus = set(r['sku'] for r in analytics_rows)
        missing_skus = [s for s in analytics_skus if s not in product_info or not product_info[s].get('offer_id')]
        if missing_skus:
            try:
                for i in range(0, len(missing_skus), 100):
                    batch = missing_skus[i:i + 100]
                    resp = requests.post(
                        f"{OZON_HOST}/v3/product/info/list",
                        json={"sku": batch},
                        headers=get_ozon_headers(),
                        timeout=15
                    )
                    if resp.status_code == 200:
                        for it in resp.json().get("items", []):
                            s = it.get("sku")
                            if s:
                                if s not in product_info:
                                    product_info[s] = {
                                        'name': it.get('name', ''),
                                        'offer_id': it.get('offer_id', ''),
                                        'fbo_stock': 0,
                                        'in_transit': 0,
                                        'in_draft': 0
                                    }
                                else:
                                    # Дополняем недостающие поля
                                    if not product_info[s].get('offer_id'):
                                        product_info[s]['offer_id'] = it.get('offer_id', '')
                                    if not product_info[s].get('name'):
                                        product_info[s]['name'] = it.get('name', '')
            except Exception:
                pass  # Если не получилось — покажем SKU без названия

        # Группируем аналитику по SKU
        products_map = {}
        for r in analytics_rows:
            sku = r['sku']
            if sku not in products_map:
                info = product_info.get(sku, {})
                products_map[sku] = {
                    'sku': sku,
                    'offer_id': info.get('offer_id', ''),
                    'name': info.get('name', ''),
                    'fbo_stock': info.get('fbo_stock', 0),
                    'in_transit': info.get('in_transit', 0),
                    'in_draft': info.get('in_draft', 0),
                    'total_ads': 0,
                    'total_stock_analytics': 0,
                    'worst_liquidity': '',
                    'clusters': []
                }

            cluster = {
                'cluster_name': r['cluster_name'],
                'stock': r['stock'] or 0,
                'ads': round(r['ads'] or 0, 2),
                'idc': round(r['idc'] or 0, 1),
                'days_without_sales': r['days_without_sales'] or 0,
                'liquidity_status': r['liquidity_status'] or ''
            }
            products_map[sku]['clusters'].append(cluster)
            products_map[sku]['total_ads'] += (r['ads'] or 0)
            products_map[sku]['total_stock_analytics'] += (r['stock'] or 0)

            # Определяем худший статус ликвидности
            liq = r['liquidity_status'] or ''
            current_worst = products_map[sku]['worst_liquidity']
            liq_priority = {
                'NO_SALES': 10, 'WAS_NO_SALES': 9, 'RESTRICTED_NO_SALES': 9,
                'DEFICIT': 8, 'WAS_DEFICIT': 7,
                'SURPLUS': 6, 'WAS_SURPLUS': 5,
                'ACTUAL': 4, 'WAS_ACTUAL': 3, 'WAITING_FOR_SUPPLY': 3,
                'POPULAR': 2, 'WAS_POPULAR': 1
            }
            if liq_priority.get(liq, 0) > liq_priority.get(current_worst, 0):
                products_map[sku]['worst_liquidity'] = liq

        # Финализируем данные
        products = []
        for sku, prod in products_map.items():
            prod['total_ads'] = round(prod['total_ads'], 2)
            products.append(prod)

        # Сортировка: по реальному остатку FBO (fbo_stock) от большего к меньшему
        # fbo_stock — точные данные из /v2/analytics/stock_on_warehouses
        # total_stock_analytics — из /v1/analytics/stocks (может быть с задержкой)
        products.sort(key=lambda p: (-p['fbo_stock'], p.get('offer_id') or ''))

        return jsonify({
            'success': True,
            'products': products,
            'analytics_date': analytics_date
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e), 'products': []})


# ============================================================================
# API ПОСТАВОК
# ============================================================================

@app.route('/api/currency-rates')
def get_currency_rates():
    """
    Получить текущие курсы валют ЦБ РФ (CNY, USD, EUR).

    Курсы кэшируются на весь день. При первом запросе за день
    загружаются с сайта ЦБ РФ.
    """
    try:
        rates = fetch_cbr_rates()
        return jsonify({
            'success': True,
            'rates': rates,
            'date': get_snapshot_date()
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'rates': {}})


@app.route('/api/supplies')
def get_supplies():
    """
    Получить все строки поставок из базы данных.

    Возвращает список поставок, отсортированных по дате создания.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM supplies
            ORDER BY exit_plan_date ASC, created_at ASC
        ''')

        supplies = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return jsonify({
            'success': True,
            'supplies': supplies
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e), 'supplies': []})


@app.route('/api/supplies/save', methods=['POST'])
def save_supply():
    """
    Сохранить или обновить строку поставки.

    Если id начинается с 'new_' — создаёт новую запись.
    Иначе — обновляет существующую.
    """
    try:
        data = request.json
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        supply_id = data.get('id', '')
        is_new = str(supply_id).startswith('new_') or not supply_id

        if is_new:
            cursor.execute('''
                INSERT INTO supplies (
                    sku, product_name, exit_plan_date, order_qty_plan,
                    exit_factory_date, exit_factory_qty,
                    arrival_warehouse_date, arrival_warehouse_qty,
                    logistics_cost_per_unit, price_cny, cost_plus_6,
                    add_to_marketing, add_to_debts, plan_fbo,
                    is_locked, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP)
            ''', (
                data.get('sku', 0),
                data.get('product_name', ''),
                data.get('exit_plan_date', ''),
                data.get('order_qty_plan'),
                data.get('exit_factory_date', ''),
                data.get('exit_factory_qty'),
                data.get('arrival_warehouse_date', ''),
                data.get('arrival_warehouse_qty'),
                data.get('logistics_cost_per_unit'),
                data.get('price_cny'),
                data.get('cost_plus_6'),
                1 if data.get('add_to_marketing') else 0,
                1 if data.get('add_to_debts') else 0,
                1 if data.get('plan_fbo') else 0
            ))
            new_id = cursor.lastrowid
        else:
            cursor.execute('''
                UPDATE supplies SET
                    sku = ?, product_name = ?, exit_plan_date = ?, order_qty_plan = ?,
                    exit_factory_date = ?, exit_factory_qty = ?,
                    arrival_warehouse_date = ?, arrival_warehouse_qty = ?,
                    logistics_cost_per_unit = ?, price_cny = ?, cost_plus_6 = ?,
                    add_to_marketing = ?, add_to_debts = ?, plan_fbo = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (
                data.get('sku', 0),
                data.get('product_name', ''),
                data.get('exit_plan_date', ''),
                data.get('order_qty_plan'),
                data.get('exit_factory_date', ''),
                data.get('exit_factory_qty'),
                data.get('arrival_warehouse_date', ''),
                data.get('arrival_warehouse_qty'),
                data.get('logistics_cost_per_unit'),
                data.get('price_cny'),
                data.get('cost_plus_6'),
                1 if data.get('add_to_marketing') else 0,
                1 if data.get('add_to_debts') else 0,
                1 if data.get('plan_fbo') else 0,
                int(supply_id)
            ))
            new_id = int(supply_id)

        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'id': new_id,
            'message': 'Поставка сохранена'
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/supplies/lock', methods=['POST'])
def lock_supply():
    """
    Заблокировать строку поставки (защита от редактирования).
    """
    try:
        data = request.json
        supply_id = data.get('id')

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('UPDATE supplies SET is_locked = 1 WHERE id = ?', (supply_id,))
        conn.commit()
        conn.close()

        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/supplies/unlock', methods=['POST'])
def unlock_supply():
    """
    Разблокировать строку поставки для редактирования.
    """
    try:
        data = request.json
        supply_id = data.get('id')

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('UPDATE supplies SET is_locked = 0 WHERE id = ?', (supply_id,))
        conn.commit()
        conn.close()

        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/supplies/delete', methods=['POST'])
def delete_supply():
    """
    Удалить строку поставки из базы данных.
    """
    try:
        data = request.json
        supply_id = data.get('id')

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM supplies WHERE id = ?', (supply_id,))
        conn.commit()
        conn.close()

        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ============================================================================

def main():
    print("\n" + "="*60)
    print("🌐 OZON ТОВАРЫ - ОСТАТКИ FBO")
    print("="*60)
    
    init_database()
    
    if sync_products():
        # ✅ Получаем хост и порт из .env или используем дефолтные
        host = os.getenv('FLASK_HOST', '0.0.0.0')  # 0.0.0.0 = доступно из сети
        port = int(os.getenv('FLASK_PORT', '5000'))

        print("\n" + "="*60)
        print("✅ ГОТОВО!")
        print("="*60)
        print(f"\n🌐 Сервер запущен на: http://{host}:{port}")
        print(f"📱 Доступ из сети: http://ВАШ-IP:{port}")
        print("\n⏹️  Для остановки: Ctrl+C\n")

        app.run(host=host, port=port, debug=True, use_reloader=False)
    else:
        print("\n❌ Ошибка при синхронизации!")
        sys.exit(1)


if __name__ == '__main__':
    main()