#!/usr/bin/env python3
"""
============================================================================
ЛОКАЛЬНЫЙ ПАРСЕР РЕЙТИНГОВ OZON (через реальный Chrome)
============================================================================

Назначение:
    Парсит карточки товаров на Ozon через реальный браузер Chrome,
    извлекает рейтинг и количество отзывов, отправляет данные на сервер.

Как это работает:
    1. Запускает реальный Chrome с отдельным профилем и портом отладки
    2. Подключается к Chrome через CDP (Chrome DevTools Protocol) + Playwright
    3. Открывает страницу каждого товара на Ozon
    4. Извлекает рейтинг из JSON-LD разметки (самый надёжный способ)
    5. Fallback: regex поиск в HTML, затем в видимом тексте страницы
    6. Отправляет данные на сервер через API /api/update-rating/<sku>

Почему нужен реальный Chrome:
    Ozon использует агрессивную антибот-защиту (WAF/DataDome),
    которая блокирует requests, cloudscraper, headless Playwright/Selenium.
    Только реальный Chrome с обычным профилем проходит проверки.

Зависимости:
    pip install playwright requests
    python -m playwright install chromium

Использование:
    python update_ratings_local.py

@author OZON Tracker Team
@version 2.0.0
@lastUpdated 2026-02-02
"""

import asyncio
import subprocess
import time
import requests
import json
import re
import sqlite3
import sys
import os

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

# URL сервера для отправки данных рейтинга
SERVER_URL = "http://89.167.25.21"

# Путь к локальной базе данных
DB_PATH = "ozon_data.db"

# Путь к Chrome (стандартная установка Windows)
CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

# Порт для Chrome DevTools Protocol (не стандартный 9222, чтобы не конфликтовать)
CDP_PORT = 9333

# Директория для отдельного профиля Chrome (не затрагивает основной профиль)
CHROME_PROFILE_DIR = os.path.join(
    os.environ.get('LOCALAPPDATA', os.path.expanduser('~')),
    'ozon-scraper-chrome-profile'
)

# Задержка между запросами к Ozon (в секундах) — чтобы не вызывать подозрений
REQUEST_DELAY = 5

# Максимальное время ожидания загрузки страницы (мс)
PAGE_TIMEOUT = 30000


# ============================================================================
# УПРАВЛЕНИЕ CHROME
# ============================================================================

def ensure_chrome_running():
    """
    Проверяет, запущен ли Chrome с портом отладки.
    Если нет — запускает новый экземпляр с отдельным профилем.

    Возвращает:
        bool: True если Chrome готов к работе, False при ошибке
    """
    try:
        # Проверяем, уже ли запущен Chrome на нужном порту
        resp = requests.get(f'http://127.0.0.1:{CDP_PORT}/json/version', timeout=2)
        version = resp.json().get('Browser', 'Unknown')
        print(f"  ✅ Chrome уже запущен: {version}")
        return True
    except Exception:
        pass

    # Chrome не запущен — запускаем
    if not os.path.exists(CHROME_PATH):
        print(f"  ❌ Chrome не найден по пути: {CHROME_PATH}")
        print(f"     Укажите правильный путь в переменной CHROME_PATH")
        return False

    os.makedirs(CHROME_PROFILE_DIR, exist_ok=True)

    print(f"  🚀 Запускаю Chrome с портом отладки {CDP_PORT}...")
    subprocess.Popen(
        [
            CHROME_PATH,
            f'--remote-debugging-port={CDP_PORT}',
            f'--user-data-dir={CHROME_PROFILE_DIR}',
            '--no-first-run',
            '--no-default-browser-check',
            '--lang=ru-RU',
            'about:blank',
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Ждём пока Chrome запустится
    for attempt in range(10):
        time.sleep(1)
        try:
            resp = requests.get(f'http://127.0.0.1:{CDP_PORT}/json/version', timeout=2)
            version = resp.json().get('Browser', 'Unknown')
            print(f"  ✅ Chrome запущен: {version}")
            return True
        except Exception:
            continue

    print("  ❌ Chrome не удалось запустить")
    return False


def close_chrome():
    """
    Закрывает Chrome, запущенный на порту отладки.
    Отправляет команду через CDP.
    """
    try:
        # Получаем список целей (targets) и закрываем браузер
        resp = requests.get(f'http://127.0.0.1:{CDP_PORT}/json/version', timeout=2)
        ws_url = resp.json().get('webSocketDebuggerUrl')
        if ws_url:
            # Просто закрываем все страницы — Chrome завершится сам
            requests.put(f'http://127.0.0.1:{CDP_PORT}/json/close/all', timeout=2)
    except Exception:
        pass


# ============================================================================
# ТРАНСЛИТЕРАЦИЯ ДЛЯ ПОСТРОЕНИЯ URL
# ============================================================================

# Таблица транслитерации русских букв в латиницу (как на Ozon)
_TRANSLIT_MAP = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
    'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
    'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
    'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'shch',
    'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
}


def _transliterate_to_slug(name):
    """
    Транслитерирует название товара в URL-slug (как на Ozon).

    Пример:
        "ONSEN предпусковой подогреватель 12в" → "onsen-predpuskovoy-podogrevatel-12v"

    Аргументы:
        name (str): Название товара (русский + латиница)

    Возвращает:
        str: URL-slug для Ozon
    """
    text = name.lower()
    result = []
    for char in text:
        if char in _TRANSLIT_MAP:
            result.append(_TRANSLIT_MAP[char])
        elif char.isascii() and char.isalnum():
            result.append(char)
        else:
            # Пробелы, слеши, запятые и прочее → дефис
            result.append('-')
    # Убираем множественные дефисы и обрезаем крайние
    slug = re.sub(r'-+', '-', ''.join(result)).strip('-')
    return slug


def _build_product_url(sku, name=None):
    """
    Строит полный URL карточки товара на Ozon.

    Ozon использует формат: /product/{slug}-{sku}/
    Прямой URL /product/{sku}/ не всегда работает (особенно для товаров не в наличии).

    Аргументы:
        sku (int): SKU товара
        name (str): Название товара (если есть — строится полный URL с slug)

    Возвращает:
        str: URL карточки товара
    """
    if name:
        slug = _transliterate_to_slug(name)
        # Добавляем ?oos_search=false — позволяет открыть товары не в наличии
        return f'https://www.ozon.ru/product/{slug}-{sku}/?oos_search=false'
    return f'https://www.ozon.ru/product/{sku}/'


# ============================================================================
# ПАРСИНГ РЕЙТИНГА С КАРТОЧКИ ТОВАРА
# ============================================================================

async def parse_ratings_via_chrome(skus):
    """
    Парсит рейтинги и отзывы для списка SKU через реальный Chrome.

    Подключается к Chrome через CDP, открывает страницу каждого товара,
    извлекает данные из JSON-LD, regex или видимого текста.

    Аргументы:
        skus (list): Список SKU для парсинга

    Возвращает:
        dict: {sku: {'rating': float, 'review_count': int}} или {sku: None}
    """
    from playwright.async_api import async_playwright

    results = {}

    async with async_playwright() as p:
        print(f"\n  🔌 Подключаюсь к Chrome через CDP (порт {CDP_PORT})...")
        browser = await p.chromium.connect_over_cdp(f'http://127.0.0.1:{CDP_PORT}')
        context = browser.contexts[0]
        page = context.pages[0] if context.pages else await context.new_page()

        for i, sku in enumerate(skus, 1):
            print(f"\n  [{i}/{len(skus)}] SKU {sku}:")

            try:
                result = await _parse_single_product(page, sku)
                results[sku] = result
            except Exception as e:
                print(f"    ❌ Ошибка: {e}")
                results[sku] = None

            # Задержка между запросами
            if i < len(skus):
                await page.wait_for_timeout(REQUEST_DELAY * 1000)

        await browser.close()

    return results


async def _parse_single_product(page, sku):
    """
    Парсит одну карточку товара на Ozon.

    Стратегия URL:
    1. Сначала пробуем полный URL с slug: /product/{slug}-{sku}/
       (работает и для товаров не в наличии с ?oos_search=false)
    2. Если нет имени — пробуем короткий URL: /product/{sku}/
    3. Если не сработало — ищем товар через поиск Ozon

    Стратегия извлечения данных:
    1. JSON-LD разметка (самый надёжный — структурированные данные)
    2. Regex в HTML-исходнике (fallback)
    3. Видимый текст страницы (последний вариант)

    Аргументы:
        page: Playwright page object
        sku (int): SKU товара

    Возвращает:
        dict: {'rating': float, 'review_count': int} или None
    """
    rating = None
    review_count = None

    # --- Шаг 1: Открываем карточку товара ---
    # Строим полный URL с slug из названия (работает для товаров не в наличии)
    product_name = _get_product_name(sku)
    url = _build_product_url(sku, product_name)
    print(f"    📥 Открываю {url}...")

    resp = await page.goto(url, wait_until='domcontentloaded', timeout=PAGE_TIMEOUT)
    await page.wait_for_timeout(REQUEST_DELAY * 1000)

    title = await page.title()
    current_url = page.url

    # Проверяем что мы на карточке товара, а не на странице поиска/блокировки
    # Примечание: URL товара не в наличии содержит ?oos_search=false — это НЕ поиск
    is_product_page = (
        resp.status == 200
        and 'ограничен' not in title.lower()
        and '/product/' in current_url
        and '/search/' not in current_url
    )

    if not is_product_page:
        print(f"    ⚠️  Прямой URL не сработал (редирект на: {current_url[:80]})")
        print(f"    🔍 Пробую найти через поиск Ozon...")

        # Fallback: ищем товар через поиск
        product_url = await _find_product_via_search(page, sku)
        if product_url:
            resp = await page.goto(product_url, wait_until='domcontentloaded', timeout=PAGE_TIMEOUT)
            await page.wait_for_timeout(REQUEST_DELAY * 1000)
            title = await page.title()
            is_product_page = (
                resp.status == 200
                and 'ограничен' not in title.lower()
                and '/product/' in page.url
                and '/search/' not in page.url
            )

    if not is_product_page:
        print(f"    ❌ Не удалось открыть карточку товара")
        return None

    print(f"    📄 Страница: {title[:60]}...")

    # --- Шаг 2: Извлекаем рейтинг ---

    # Способ 1: JSON-LD разметка (самый надёжный)
    rating, review_count = await _extract_from_json_ld(page)

    # Способ 2: Regex в HTML
    if rating is None:
        content = await page.content()
        rating, review_count = _extract_from_html_regex(content)

    # Способ 3: Видимый текст
    if rating is None:
        rating, review_count = await _extract_from_visible_text(page)

    if rating is not None and review_count is not None:
        print(f"    ✅ Рейтинг: {rating}, Отзывов: {review_count}")
        return {'rating': rating, 'review_count': review_count}
    else:
        print(f"    ⚠️  Не удалось извлечь рейтинг")
        return None


async def _find_product_via_search(page, sku):
    """
    Ищет товар на Ozon через поисковую строку по SKU.

    Аргументы:
        page: Playwright page object
        sku (int): SKU товара

    Возвращает:
        str: URL карточки товара или None
    """
    # Получаем имя товара из БД для более точного поиска
    product_name = _get_product_name(sku)
    if not product_name:
        return None

    # Берём первые 5 слов из названия для поиска
    search_words = product_name.split()[:5]
    search_query = ' '.join(search_words)

    search_url = f'https://www.ozon.ru/search/?text={requests.utils.quote(search_query)}'
    print(f"    🔍 Поиск: {search_query}")

    await page.goto(search_url, wait_until='domcontentloaded', timeout=PAGE_TIMEOUT)
    await page.wait_for_timeout(REQUEST_DELAY * 1000)

    # Ищем ссылку на наш товар в результатах поиска
    # Проверяем по нескольким ключевым словам из названия
    key_words = [w.lower() for w in search_words[:3] if len(w) > 2]

    links = await page.evaluate('''
        (keyWords) => {
            const anchors = document.querySelectorAll('a[href*="/product/"]');
            const results = [];
            for (const a of anchors) {
                const text = a.textContent.toLowerCase();
                const matches = keyWords.filter(w => text.includes(w));
                if (matches.length >= 2) {
                    results.push({
                        href: a.href.split('?')[0],
                        text: a.textContent.substring(0, 100),
                        score: matches.length
                    });
                }
            }
            // Сортируем по количеству совпадений
            results.sort((a, b) => b.score - a.score);
            return results.slice(0, 3);
        }
    ''', key_words)

    if links:
        # Приоритет: ссылка, содержащая наш SKU в URL (точное совпадение)
        sku_str = str(sku)
        for link in links:
            if sku_str in link['href']:
                print(f"    ✅ Найден (SKU в URL): {link['href'][:80]}...")
                return link['href']
        # Если SKU не найден в URL — берём лучшее совпадение, но предупреждаем
        print(f"    ⚠️  SKU {sku} не найден в URL результатов, возможно другой товар")
        url = links[0]['href']
        print(f"    ⚠️  Используем: {url[:80]}...")
        return url

    print(f"    ❌ Товар не найден в поиске")
    return None


async def _extract_from_json_ld(page):
    """
    Извлекает рейтинг из JSON-LD разметки (schema.org).
    Это самый надёжный способ — структурированные данные для поисковиков.

    Пример JSON-LD:
        {
            "@type": "Product",
            "aggregateRating": {
                "ratingValue": "4.5",
                "reviewCount": "1402"
            }
        }

    Возвращает:
        tuple: (rating, review_count) или (None, None)
    """
    scripts_json = await page.evaluate('''
        () => {
            const scripts = document.querySelectorAll('script[type="application/ld+json"]');
            return Array.from(scripts).map(s => s.textContent);
        }
    ''')

    for s in scripts_json:
        try:
            data = json.loads(s)
            if isinstance(data, dict) and 'aggregateRating' in data:
                ar = data['aggregateRating']
                rating = float(ar.get('ratingValue', 0))
                review_count = int(ar.get('reviewCount', 0))
                if rating > 0:
                    print(f"    📊 Источник: JSON-LD (schema.org)")
                    return rating, review_count
        except (json.JSONDecodeError, ValueError, TypeError):
            continue

    return None, None


def _extract_from_html_regex(content):
    """
    Извлекает рейтинг через regex из HTML-исходника.
    Ищет паттерны ratingValue и reviewCount в JSON-подобных структурах.

    Аргументы:
        content (str): HTML-содержимое страницы

    Возвращает:
        tuple: (rating, review_count) или (None, None)
    """
    rating = None
    review_count = None

    m_rating = re.search(r'"ratingValue"[:\s]*"?([0-9]+[.,][0-9]+)', content)
    m_reviews = re.search(r'"reviewCount"[:\s]*"?(\d+)', content)

    if m_rating:
        rating = float(m_rating.group(1).replace(',', '.'))
    if m_reviews:
        review_count = int(m_reviews.group(1))

    if rating is not None and review_count is not None:
        print(f"    📊 Источник: HTML regex")
    return rating, review_count


async def _extract_from_visible_text(page):
    """
    Извлекает рейтинг из видимого текста страницы.
    Последний вариант — ищет паттерн вида "4.5  1402 отзыва".

    Возвращает:
        tuple: (rating, review_count) или (None, None)
    """
    body_text = await page.evaluate('() => document.body.innerText')

    for line in body_text.split('\n'):
        line = line.strip()
        # Ищем паттерн: рейтинг + число + "отзыв"
        m = re.search(r'([0-9]+[.,][0-9]+)\s+(\d[\d\s]*)\s*отзыв', line, re.IGNORECASE)
        if m:
            rating = float(m.group(1).replace(',', '.'))
            review_count = int(m.group(2).replace(' ', ''))
            if 1.0 <= rating <= 5.0 and review_count > 0:
                print(f"    📊 Источник: видимый текст")
                return rating, review_count

    return None, None


def _get_product_name(sku):
    """
    Получает название товара из локальной БД по SKU.

    Аргументы:
        sku (int): SKU товара

    Возвращает:
        str: Название товара или None
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT name FROM products WHERE sku = ?', (sku,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


# ============================================================================
# ОТПРАВКА ДАННЫХ НА СЕРВЕР
# ============================================================================

def save_to_local_db(sku, rating, review_count):
    """
    Сохраняет рейтинг в локальную БД (products_history).
    Обновляет запись за сегодняшнюю дату.

    Аргументы:
        sku (int): SKU товара
        rating (float): Рейтинг (1.0 - 5.0)
        review_count (int): Количество отзывов
    """
    try:
        from datetime import date
        today = date.today().isoformat()

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Обновляем в products_history за сегодня
        cursor.execute('''
            UPDATE products_history
            SET rating = ?, review_count = ?
            WHERE sku = ? AND snapshot_date = ?
        ''', (float(rating), int(review_count), sku, today))

        if cursor.rowcount > 0:
            print(f"    ✅ Сохранено в локальную БД (дата: {today})")
        else:
            print(f"    ⚠️  Нет записи в истории за {today} для SKU {sku}")

        conn.commit()
        conn.close()
    except Exception as e:
        print(f"    ❌ Ошибка записи в БД: {e}")


def send_to_server(sku, rating, review_count):
    """
    Отправляет данные рейтинга на сервер через API.

    API: POST /api/update-rating/<sku>
    Body: {"rating": 4.5, "review_count": 1402}

    Аргументы:
        sku (int): SKU товара
        rating (float): Рейтинг (1.0 - 5.0)
        review_count (int): Количество отзывов
    """
    try:
        url = f"{SERVER_URL}/api/update-rating/{sku}"
        data = {
            'rating': rating,
            'review_count': review_count
        }

        response = requests.post(url, json=data, timeout=10)

        # Проверяем что ответ — валидный JSON
        if response.headers.get('content-type', '').startswith('application/json'):
            result = response.json()
            if result.get('success'):
                print(f"    ✅ Данные отправлены на сервер")
            else:
                print(f"    ⚠️  Ошибка сервера: {result.get('error')}")
        else:
            print(f"    ⚠️  Сервер вернул не-JSON ответ (статус {response.status_code})")

    except requests.exceptions.ConnectionError:
        print(f"    ⚠️  Сервер недоступен ({SERVER_URL})")
    except Exception as e:
        print(f"    ❌ Ошибка при отправке: {e}")


# ============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ============================================================================

def main():
    """
    Основная функция:
    1. Получает список SKU из локальной БД
    2. Запускает Chrome
    3. Парсит рейтинги через Chrome CDP
    4. Отправляет результаты на сервер
    5. Закрывает Chrome
    """
    # Кодировка для Windows
    sys.stdout.reconfigure(encoding='utf-8')

    print("\n" + "=" * 70)
    print("📊 ПАРСЕР РЕЙТИНГОВ OZON (через реальный Chrome)")
    print("=" * 70)

    # --- Шаг 1: Получаем список SKU ---
    print("\n📦 Загрузка списка товаров из БД...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT sku FROM products ORDER BY sku')
    skus = [row[0] for row in cursor.fetchall()]
    conn.close()

    if not skus:
        print("  ⚠️  Нет товаров в БД")
        return 0, 0

    print(f"  Найдено товаров: {len(skus)}")

    # --- Шаг 2: Запускаем Chrome ---
    print("\n🌐 Подготовка Chrome...")
    if not ensure_chrome_running():
        print("\n❌ Не удалось запустить Chrome. Завершение.")
        return 0, 0

    # --- Шаг 3: Парсим рейтинги ---
    print("\n🔍 Парсинг рейтингов...")
    try:
        results = asyncio.run(parse_ratings_via_chrome(skus))
    except Exception as e:
        print(f"\n❌ Ошибка парсинга: {e}")
        import traceback
        traceback.print_exc()
        return 0, 0

    # --- Шаг 4: Сохраняем результаты ---
    print("\n📤 Сохранение результатов...")
    success_count = 0
    for sku, data in results.items():
        if data:
            # Сохраняем в локальную БД
            save_to_local_db(sku, data['rating'], data['review_count'])
            # Отправляем на сервер
            send_to_server(sku, data['rating'], data['review_count'])
            success_count += 1

    # --- Итоги ---
    print("\n" + "=" * 70)
    print(f"✅ Парсинг завершен!")
    print(f"   Всего товаров: {len(skus)}")
    print(f"   Успешно получено: {success_count}")
    print(f"   Не удалось: {len(skus) - success_count}")
    print("=" * 70)

    # Не закрываем Chrome — он может использоваться для следующего запуска
    print("\n💡 Chrome остаётся запущенным для последующих запусков.")
    print("   Закройте его вручную, если не нужен.")

    return success_count, len(skus) - success_count


# ============================================================================
# РЕЖИМ ОЖИДАНИЯ (--watch)
# ============================================================================
#
# Запускается командой: python update_ratings_local.py --watch
#
# В этом режиме скрипт работает в фоне, опрашивая сервер каждые 30 секунд.
# Когда пользователь нажимает кнопку "Парсить рейтинги" на сайте,
# сервер ставит флаг "requested". Скрипт видит флаг, запускает парсер,
# и отправляет результаты обратно на сервер.
# ============================================================================

# Интервал опроса сервера (секунды)
POLL_INTERVAL = 30


def notify_server(endpoint, data=None):
    """
    Отправляет уведомление на сервер (без авторизации — через nginx исключение).

    Аргументы:
        endpoint (str): Путь API (например, '/api/parse-start')
        data (dict): Данные для отправки (JSON)
    """
    try:
        url = f"{SERVER_URL}{endpoint}"
        response = requests.post(url, json=data or {}, timeout=10)
        return response.status_code == 200
    except Exception:
        return False


def check_parse_request():
    """
    Проверяет, есть ли запрос на парсинг от сервера.

    Возвращает:
        str: Статус ('idle', 'requested', 'running', 'completed')
    """
    try:
        url = f"{SERVER_URL}/api/parse-status"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json().get('status', 'idle')
    except Exception:
        pass
    return 'idle'


def watch_mode():
    """
    Режим ожидания: опрашивает сервер и запускает парсинг по запросу.

    Работает в бесконечном цикле:
    1. Каждые 30 секунд проверяет /api/parse-status
    2. Если status == 'requested' — запускает парсинг
    3. Уведомляет сервер о начале и завершении
    """
    sys.stdout.reconfigure(encoding='utf-8')

    print("\n" + "=" * 70)
    print("👀 ПАРСЕР РЕЙТИНГОВ — РЕЖИМ ОЖИДАНИЯ")
    print("=" * 70)
    print(f"\n  Сервер: {SERVER_URL}")
    print(f"  Опрос каждые {POLL_INTERVAL} сек.")
    print(f"  Нажмите Ctrl+C для выхода.")
    print(f"\n  Ожидание запроса с сайта...\n")

    while True:
        try:
            status = check_parse_request()

            if status == 'requested':
                print("\n" + "=" * 70)
                print("📨 Получен запрос на парсинг с сайта!")
                print("=" * 70)

                # Сообщаем серверу что начали
                notify_server('/api/parse-start')

                # Запускаем парсинг
                success, failed = main() or (0, 0)

                # Сообщаем серверу что закончили
                notify_server('/api/parse-complete', {
                    'success': success,
                    'failed': failed,
                    'message': f'Обновлено: {success}, не удалось: {failed}'
                })

                print(f"\n👀 Возвращаюсь в режим ожидания...\n")

            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            print("\n\n👋 Режим ожидания остановлен.")
            break
        except Exception as e:
            print(f"\n⚠️  Ошибка: {e}")
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    if '--watch' in sys.argv:
        watch_mode()
    else:
        main()
