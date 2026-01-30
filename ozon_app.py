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
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template_string, jsonify, request

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

    conn.commit()
    conn.close()


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

        # API ограничение: до 1000 товаров за запрос
        batch_size = 1000
        for i in range(0, len(all_skus), batch_size):
            batch_skus = all_skus[i:i + batch_size]

            data = {
                "sku": batch_skus
            }

            response = requests.post(
                f"{OZON_HOST}/v3/product/info/list",
                json=data,
                headers=get_ozon_headers(),
                timeout=30
            )

            if response.status_code != 200:
                print(f"  ⚠️  Ошибка API (batch {i // batch_size + 1}): {response.status_code}")
                print(f"     {response.text[:200]}")
                continue

            result = response.json()
            # API /v3/product/info/list возвращает items напрямую, не в result
            items = result.get("items", [])

            for item in items:
                sku = item.get("sku")
                if not sku:
                    continue

                # DEBUG: выводим все поля для первого товара
                if sku == 1235819146:
                    print(f"\n  🔍 DEBUG для SKU {sku}:")
                    print(f"     price: {item.get('price')}")
                    print(f"     old_price: {item.get('old_price')}")
                    print(f"     min_price: {item.get('min_price')}")
                    print(f"     marketing_price: {item.get('marketing_price')}")
                    print(f"     promotions: {item.get('promotions')}")
                    print(f"     price_indexes: {item.get('price_indexes')}")

                # Цены из API
                # old_price - старая цена (базовая цена до скидки/акции)
                # price - текущая цена (с учетом акций) - это "Ваша цена" в ЛК
                old_price = item.get("old_price", 0)
                price = item.get("price", 0)

                # Конвертируем в float
                try:
                    # Цена в ЛК (текущая цена с учетом акций)
                    current_price = float(price) if price else 0
                    # Старая цена (до скидки)
                    base_price = float(old_price) if old_price else 0
                except (ValueError, TypeError):
                    current_price = 0
                    base_price = 0

                prices_by_sku[sku] = {
                    "price": current_price,  # Цена в ЛК (с учетом акций)
                    "marketing_price": base_price  # Пока используем old_price, потом разберемся
                }

            print(f"  ✓ Обработано {len(items)} товаров (batch {i // batch_size + 1})")

        print(f"  ✅ Загружено цен для {len(prices_by_sku)} товаров")

    except Exception as e:
        print(f"  ❌ Ошибка при загрузке цен: {e}")
        import traceback
        traceback.print_exc()

    return prices_by_sku


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
        
        # ✅ Очищаем старые данные
        cursor.execute('DELETE FROM products')
        conn.commit()
        
        print("\n📊 Загрузка остатков...")
        
        products_data = {}  # sku -> {name, fbo_stock}
        
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
            
            offset += 1000
        
        print(f"\n  ✅ Всего уникальных товаров: {len(products_data)}")

        # ============================================================================
        # ЗАГРУЗКА ДОПОЛНИТЕЛЬНЫХ ДАННЫХ
        # ============================================================================
        # ⚠️  ПРИ ДОБАВЛЕНИИ НОВЫХ ДАННЫХ: добавляй вызовы load_новые_данные() СЮДА

        # ✅ Загружаем заказы
        orders_by_sku = load_fbo_orders()

        # ✅ Загружаем заявки на поставку (В ПУТИ и В ЗАЯВКАХ)
        in_transit_by_sku, in_draft_by_sku = load_fbo_supply_orders()

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

            # Цены товара
            price_data = prices_by_sku.get(sku, {})
            price = price_data.get("price", 0)
            marketing_price = price_data.get("marketing_price", 0)

            # 1️⃣ Обновляем текущие остатки
            cursor.execute('''
                INSERT INTO products (sku, name, fbo_stock, orders_qty, price, marketing_price, hits_view_search, hits_view_search_pdp, search_ctr, hits_add_to_cart, cr1, cr2, adv_spend, in_transit, in_draft, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sku) DO UPDATE SET
                    name=excluded.name,
                    fbo_stock=excluded.fbo_stock,
                    orders_qty=excluded.orders_qty,
                    price=excluded.price,
                    marketing_price=excluded.marketing_price,
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
                data.get("fbo_stock", 0),
                orders_qty,
                price,
                marketing_price,
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
                INSERT INTO products_history (sku, name, fbo_stock, orders_qty, price, marketing_price, avg_position, hits_view_search, hits_view_search_pdp, search_ctr, hits_add_to_cart, cr1, cr2, adv_spend, in_transit, in_draft, snapshot_date, snapshot_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sku, snapshot_date) DO UPDATE SET
                    name=excluded.name,
                    fbo_stock=excluded.fbo_stock,
                    orders_qty=excluded.orders_qty,
                    price=excluded.price,
                    marketing_price=excluded.marketing_price,
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
                data.get("fbo_stock", 0),
                orders_qty,
                price,
                marketing_price,
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
            padding: 10px;
            border-radius: 12px;
            margin-bottom: 30px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            margin-left: -20px;
            margin-right: -20px;
            padding-left: 50px;
            padding-right: 50px;
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
            padding: 20px 30px;
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
            margin: 20px 0 0 0;
        }

        .tab-button {
            padding: 15px 30px;
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
            white-space: pre-wrap;
            font-size: 13px;
            line-height: 1.5;
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: space-between;
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
            max-width: 250px;
            position: relative;
        }

        .note-display {
            padding: 12px;
            background: #f8f9fa;
            border-radius: 4px;
            min-height: 80px;
            word-wrap: break-word;
            white-space: pre-wrap;
            font-size: 13px;
            line-height: 1.5;
            cursor: pointer;
            display: flex;
            align-items: flex-start;
            transition: background 0.2s;
            border: 1px solid #e9ecef;
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
            max-width: 250px;
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
            white-space: pre-wrap;
            min-height: 60px;
            cursor: pointer;
            transition: background-color 0.2s;
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
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
                <div>
                    <h1>Ежедневный отчет</h1>
                </div>
                <button class="refresh-btn" onclick="syncData()" id="sync-btn">Обновить данные</button>
            </div>
        </div>

        <div class="table-container">
            <div class="tabs">
                <button class="tab-button active" onclick="switchTab(event, 'history')">OZON</button>
                <button class="tab-button" onclick="switchTab(event, 'wb')">WB</button>
            </div>

            <!-- ТАБ: История товара -->
            <div id="history" class="tab-content active">
                <div class="table-header">
                    <div></div>
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

            <!-- ТАБ: Wildberries -->
            <div id="wb" class="tab-content">
                <div style="padding: 40px; text-align: center; color: #666;">
                    <h2>Wildberries</h2>
                    <p>Раздел в разработке</p>
                </div>
            </div>
        </div>
    </div>

    <script>
        let allProducts = [];

        document.addEventListener('DOMContentLoaded', function() {
            loadProductsList();
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

        // ✅ НОВЫЕ ФУНКЦИИ ДЛЯ ТАБОВ И ИСТОРИИ

        function switchTab(e, tab) {
            // Скрываем все табы
            document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
            document.querySelectorAll('.tab-button').forEach(el => el.classList.remove('active'));
            
            // Показываем нужный таб
            document.getElementById(tab).classList.add('active');
            e.target.classList.add('active');
            
            // Если открыли историю - загружаем список товаров
            if (tab === 'history') {
                loadProductsList();
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
                            option.textContent = `${p.name} (SKU: ${p.sku})`;
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
            html += '<th>FBO остаток</th>';
            html += '<th>Заказы</th>';
            html += '<th>Цена в ЛК</th>';
            html += '<th>Цена на сайте</th>';
            html += '<th>Ср. позиция</th>';
            html += '<th>Показы (поиск+кат.)</th>';
            html += '<th>Посещения</th>';
            html += '<th>CTR (%)</th>';
            html += '<th>Корзина</th>';
            html += '<th>CR1 (%)</th>';
            html += '<th>CR2 (%)</th>';
            html += '<th>Расходы</th>';
            html += '<th>CPO</th>';
            html += '<th>В ПУТИ</th>';
            html += '<th>В ЗАЯВКАХ</th>';
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
                html += `<td style="max-width: 280px;">
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
                html += `<td>${item.name}</td>`;
                html += `<td><span class="sku">${item.sku}</span></td>`;
                html += `<td><span class="${stockClass}">${formatNumber(item.fbo_stock)}</span></td>`;

                // Заказы (с стрелкой)
                html += `<td><span class="stock">${formatNumber(item.orders_qty || 0)}${getTrendArrow(item.orders_qty, prevItem?.orders_qty)}</span></td>`;

                // Цена в ЛК (с стрелкой, инвертированная логика: меньше = лучше)
                html += `<td><strong>${(item.price !== null && item.price !== undefined && item.price > 0) ? Math.round(item.price) + ' ₽' : '—'}${(item.price !== null && item.price !== undefined && item.price > 0) ? getTrendArrow(item.price, prevItem?.price, true) : ''}</strong></td>`;

                // Цена на сайте (с стрелкой, инвертированная логика: меньше = лучше)
                html += `<td><strong>${(item.marketing_price !== null && item.marketing_price !== undefined && item.marketing_price > 0) ? Math.round(item.marketing_price) + ' ₽' : '—'}${(item.marketing_price !== null && item.marketing_price !== undefined && item.marketing_price > 0) ? getTrendArrow(item.marketing_price, prevItem?.marketing_price, true) : ''}</strong></td>`;

                // Ср. позиция (с стрелкой, инвертированная логика: меньше = лучше)
                html += `<td><span class="position">${(item.avg_position !== null && item.avg_position !== undefined) ? item.avg_position.toFixed(1) : '—'}${(item.avg_position !== null && item.avg_position !== undefined) ? getTrendArrow(item.avg_position, prevItem?.avg_position, true) : ''}</span></td>`;

                // Показы (поиск+кат.) - с стрелкой
                html += `<td><strong>${formatNumber(item.hits_view_search || 0)}${getTrendArrow(item.hits_view_search, prevItem?.hits_view_search)}</strong></td>`;

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
                html += `<td><strong>${(item.adv_spend !== null && item.adv_spend !== undefined) ? Math.round(item.adv_spend) + ' ₽' : '—'}${(item.adv_spend !== null && item.adv_spend !== undefined) ? getTrendArrow(item.adv_spend, prevItem?.adv_spend) : ''}</strong></td>`;

                // CPO (Cost Per Order) - расходы/заказы с стрелкой (меньше = лучше)
                const cpo = (item.adv_spend !== null && item.adv_spend !== undefined && item.orders_qty > 0)
                    ? Math.round(item.adv_spend / item.orders_qty)
                    : null;
                const prevCpo = (prevItem?.adv_spend !== null && prevItem?.adv_spend !== undefined && prevItem?.orders_qty > 0)
                    ? Math.round(prevItem.adv_spend / prevItem.orders_qty)
                    : null;
                html += `<td><strong>${cpo !== null ? cpo + ' ₽' : '—'}${cpo !== null ? getTrendArrow(cpo, prevCpo, true) : ''}</strong></td>`;

                // В ПУТИ - товары из заявок со статусом "в пути"
                html += `<td><span class="stock">${formatNumber(item.in_transit || 0)}${getTrendArrow(item.in_transit, prevItem?.in_transit)}</span></td>`;

                // В ЗАЯВКАХ - товары из черновиков/новых заявок
                html += `<td><span class="stock">${formatNumber(item.in_draft || 0)}${getTrendArrow(item.in_draft, prevItem?.in_draft)}</span></td>`;

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
                    <button class="toggle-col-btn" onclick="toggleColumn(4)">FBO</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(5)">Заказы</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(6)">Ср. позиция</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(7)">Показы</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(8)">Посещения</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(9)">CTR</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(10)">Корзина</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(11)">CR1</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(12)">CR2</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(13)">Расходы</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(14)">CPO</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(15)">В ПУТИ</button>
                    <button class="toggle-col-btn" onclick="toggleColumn(16)">В ЗАЯВКАХ</button>
                </div>
                <div class="table-wrapper">
                    ${html}
                </div>
            `;
            
            historyContent.innerHTML = fullHtml;
            
            // Инициализирую изменение ширины столбцов
            initColumnResize();
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
        
        # Берём товары с последним известным названием (по дате)
        # SKU 1235819146 (ПЖД) первым, потом остальные по имени
        cursor.execute('''
            SELECT ph.sku, ph.name
            FROM products_history ph
            JOIN (
              SELECT sku, MAX(snapshot_date) AS max_date
              FROM products_history
              GROUP BY sku
            ) last
            ON last.sku = ph.sku AND last.max_date = ph.snapshot_date
            ORDER BY 
                CASE WHEN ph.sku = 1235819146 THEN 0 ELSE 1 END,
                ph.name
        ''')
        
        products = [{'sku': row['sku'], 'name': row['name']} for row in cursor.fetchall()]
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