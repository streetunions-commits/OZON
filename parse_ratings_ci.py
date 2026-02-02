#!/usr/bin/env python3
"""
============================================================================
ПАРСЕР РЕЙТИНГОВ OZON ДЛЯ CI/CD (GitHub Actions)
============================================================================

Назначение:
    Парсит рейтинги товаров с Ozon через реальный Chrome + Xvfb.
    Предназначен для запуска в GitHub Actions или любом Linux CI.

Как работает:
    1. Запускает Xvfb (виртуальный дисплей)
    2. Запускает реальный Google Chrome с портом отладки
    3. Подключается через CDP (Chrome DevTools Protocol) + Playwright
    4. Парсит рейтинги из JSON-LD разметки карточек товаров
    5. Отправляет данные на сервер через API

Отличия от update_ratings_local.py:
    - Работает на Linux без GUI (через Xvfb)
    - Получает список SKU через Ozon Seller API (не из локальной БД)
    - Конфигурация через переменные окружения
    - Не зависит от локальной БД

Переменные окружения:
    SERVER_URL - URL сервера (по умолчанию http://89.167.25.21)
    OZON_CLIENT_ID - Client ID для Seller API
    OZON_API_KEY - API Key для Seller API

@author OZON Tracker Team
@version 1.0.0
@lastUpdated 2026-02-02
"""

import asyncio
import subprocess
import time
import requests
import json
import re
import sys
import os
import signal

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

# URL сервера — берём из env или используем дефолт
SERVER_URL = os.environ.get("SERVER_URL", "http://89.167.25.21")

# Ozon Seller API — для получения списка SKU и названий товаров
OZON_CLIENT_ID = os.environ.get("OZON_CLIENT_ID", "138926")
OZON_API_KEY = os.environ.get("OZON_API_KEY", "a3d83a9a-d652-409a-9471-f09bd9b9b1bb")
OZON_HOST = "https://api-seller.ozon.ru"

# Chrome CDP
CDP_PORT = 9444
CHROME_PROFILE = "/tmp/ozon-chrome-profile"

# Задержка между запросами (секунды)
REQUEST_DELAY = 5

# Таймаут загрузки страницы (мс)
PAGE_TIMEOUT = 30000


# ============================================================================
# ПОЛУЧЕНИЕ СПИСКА ТОВАРОВ ЧЕРЕЗ SELLER API
# ============================================================================

def get_products_from_api():
    """
    Получает список SKU и названий товаров через Ozon Seller API.
    Не зависит от локальной БД — работает в любом окружении.

    API: POST /v2/analytics/stock_on_warehouses

    Возвращает:
        dict: {sku: name} — словарь SKU → название товара
    """
    print("\n📦 Загрузка списка товаров из Ozon API...")

    headers = {
        'Client-Id': OZON_CLIENT_ID,
        'Api-Key': OZON_API_KEY,
        'Content-Type': 'application/json'
    }

    products = {}  # {sku: name}
    offset = 0

    while True:
        resp = requests.post(
            f"{OZON_HOST}/v2/analytics/stock_on_warehouses",
            json={"warehouse_type": "FBO", "limit": 1000, "offset": offset},
            headers=headers,
            timeout=30
        )

        if resp.status_code != 200:
            print(f"  ❌ Ошибка API: {resp.status_code}")
            break

        rows = resp.json().get("result", {}).get("rows", [])
        if not rows:
            break

        for row in rows:
            sku = row.get("sku")
            name = row.get("item_name", "")
            if sku and sku not in products:
                products[sku] = name

        offset += 1000

    print(f"  ✅ Найдено товаров: {len(products)}")
    return products


# ============================================================================
# УПРАВЛЕНИЕ CHROME + XVFB
# ============================================================================

def start_xvfb_and_chrome():
    """
    Запускает Xvfb (виртуальный дисплей) и Google Chrome с портом отладки.

    Xvfb создаёт виртуальный экран 1920x1080, чтобы Chrome думал что есть монитор.
    Это позволяет запускать Chrome в "headed" режиме на сервере без GUI.

    Возвращает:
        tuple: (xvfb_process, chrome_process) или (None, None) при ошибке
    """
    os.makedirs(CHROME_PROFILE, exist_ok=True)

    # Запускаем Xvfb
    print("  🖥️  Запуск виртуального дисплея (Xvfb)...")
    xvfb = subprocess.Popen(
        ["Xvfb", ":99", "-screen", "0", "1920x1080x24", "-nolisten", "tcp"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    os.environ["DISPLAY"] = ":99"
    time.sleep(1)

    # Запускаем Chrome
    print("  🌐 Запуск Google Chrome...")
    chrome = subprocess.Popen(
        [
            "google-chrome",
            f"--remote-debugging-port={CDP_PORT}",
            f"--user-data-dir={CHROME_PROFILE}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-gpu",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--lang=ru-RU",
            "about:blank",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={**os.environ, "DISPLAY": ":99"},
    )

    # Ждём готовности Chrome
    for attempt in range(15):
        time.sleep(1)
        try:
            r = requests.get(f"http://127.0.0.1:{CDP_PORT}/json/version", timeout=2)
            version = r.json().get("Browser", "Unknown")
            print(f"  ✅ Chrome запущен: {version}")
            return xvfb, chrome
        except Exception:
            continue

    print("  ❌ Chrome не удалось запустить")
    xvfb.terminate()
    chrome.terminate()
    return None, None


def cleanup(xvfb, chrome):
    """Останавливает Chrome и Xvfb"""
    if chrome:
        chrome.terminate()
        chrome.wait(timeout=5)
    if xvfb:
        xvfb.terminate()
        xvfb.wait(timeout=5)


# ============================================================================
# ПАРСИНГ РЕЙТИНГОВ
# ============================================================================

async def parse_ratings(products):
    """
    Парсит рейтинги для всех товаров через Chrome CDP.

    Аргументы:
        products (dict): {sku: name}

    Возвращает:
        dict: {sku: {'rating': float, 'review_count': int}} или {sku: None}
    """
    from playwright.async_api import async_playwright

    results = {}

    async with async_playwright() as p:
        print(f"\n  🔌 Подключение к Chrome (порт {CDP_PORT})...")
        browser = await p.chromium.connect_over_cdp(f"http://127.0.0.1:{CDP_PORT}")
        context = browser.contexts[0]
        page = context.pages[0] if context.pages else await context.new_page()

        skus = list(products.keys())

        for i, sku in enumerate(skus, 1):
            name = products[sku]
            print(f"\n  [{i}/{len(skus)}] SKU {sku} ({name[:40]}...):")

            try:
                result = await _parse_single(page, sku, name)
                results[sku] = result
            except Exception as e:
                print(f"    ❌ Ошибка: {e}")
                results[sku] = None

            if i < len(skus):
                await page.wait_for_timeout(REQUEST_DELAY * 1000)

        await browser.close()

    return results


async def _parse_single(page, sku, name):
    """
    Парсит рейтинг одного товара.

    Стратегия:
    1. Прямой URL /product/{sku}/
    2. Поиск через Ozon по названию (fallback)
    3. Извлечение: JSON-LD → regex → видимый текст

    Возвращает:
        dict или None
    """
    # Попытка 1: Прямой URL
    url = f"https://www.ozon.ru/product/{sku}/"
    print(f"    📥 Открываю {url}...")

    resp = await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
    await page.wait_for_timeout(REQUEST_DELAY * 1000)

    title = await page.title()
    current_url = page.url

    is_product = (
        resp.status == 200
        and "ограничен" not in title.lower()
        and "antibot" not in title.lower()
        and "/product/" in current_url
        and "search" not in current_url
    )

    # Попытка 2: Поиск через Ozon
    if not is_product:
        print(f"    ⚠️  Прямой URL не сработал. Ищу через поиск...")
        product_url = await _search_product(page, name)
        if product_url:
            resp = await page.goto(product_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
            await page.wait_for_timeout(REQUEST_DELAY * 1000)
            title = await page.title()
            is_product = (
                resp.status == 200
                and "ограничен" not in title.lower()
                and "antibot" not in title.lower()
                and "/product/" in page.url
            )

    if not is_product:
        print(f"    ❌ Не удалось открыть карточку (возможно IP заблокирован)")
        return None

    print(f"    📄 {title[:60]}...")

    # Извлекаем рейтинг
    rating, reviews = await _extract_json_ld(page)

    if rating is None:
        content = await page.content()
        rating, reviews = _extract_regex(content)

    if rating is None:
        rating, reviews = await _extract_text(page)

    if rating is not None and reviews is not None:
        print(f"    ✅ Рейтинг: {rating}, Отзывов: {reviews}")
        return {"rating": rating, "review_count": reviews}

    print(f"    ⚠️  Рейтинг не найден на странице")
    return None


async def _search_product(page, name):
    """Ищет товар через поиск Ozon по первым 5 словам названия"""
    words = name.split()[:5]
    query = " ".join(words)
    key_words = [w.lower() for w in words[:3] if len(w) > 2]

    url = f"https://www.ozon.ru/search/?text={requests.utils.quote(query)}"
    print(f"    🔍 Поиск: {query}")

    await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
    await page.wait_for_timeout(REQUEST_DELAY * 1000)

    links = await page.evaluate("""
        (keyWords) => {
            const anchors = document.querySelectorAll('a[href*="/product/"]');
            const results = [];
            for (const a of anchors) {
                const text = a.textContent.toLowerCase();
                const matches = keyWords.filter(w => text.includes(w));
                if (matches.length >= 2) {
                    results.push({
                        href: a.href.split('?')[0],
                        score: matches.length
                    });
                }
            }
            results.sort((a, b) => b.score - a.score);
            return results.slice(0, 1);
        }
    """, key_words)

    if links:
        print(f"    ✅ Найден: {links[0]['href'][:80]}...")
        return links[0]["href"]
    return None


async def _extract_json_ld(page):
    """Извлекает рейтинг из JSON-LD (schema.org)"""
    scripts = await page.evaluate("""
        () => Array.from(
            document.querySelectorAll('script[type="application/ld+json"]')
        ).map(s => s.textContent)
    """)

    for s in scripts:
        try:
            data = json.loads(s)
            if isinstance(data, dict) and "aggregateRating" in data:
                ar = data["aggregateRating"]
                rating = float(ar.get("ratingValue", 0))
                reviews = int(ar.get("reviewCount", 0))
                if rating > 0:
                    print(f"    📊 Источник: JSON-LD")
                    return rating, reviews
        except (json.JSONDecodeError, ValueError, TypeError):
            continue
    return None, None


def _extract_regex(content):
    """Извлекает рейтинг через regex из HTML"""
    m_r = re.search(r'"ratingValue"[:\s]*"?([0-9]+[.,][0-9]+)', content)
    m_c = re.search(r'"reviewCount"[:\s]*"?(\d+)', content)

    if m_r and m_c:
        print(f"    📊 Источник: HTML regex")
        return float(m_r.group(1).replace(",", ".")), int(m_c.group(1))
    return None, None


async def _extract_text(page):
    """Извлекает рейтинг из видимого текста"""
    text = await page.evaluate("() => document.body.innerText")
    for line in text.split("\n"):
        m = re.search(r"([0-9]+[.,][0-9]+)\s+(\d[\d\s]*)\s*отзыв", line.strip(), re.IGNORECASE)
        if m:
            rating = float(m.group(1).replace(",", "."))
            reviews = int(m.group(2).replace(" ", ""))
            if 1.0 <= rating <= 5.0 and reviews > 0:
                print(f"    📊 Источник: видимый текст")
                return rating, reviews
    return None, None


# ============================================================================
# ОТПРАВКА НА СЕРВЕР
# ============================================================================

def send_to_server(sku, rating, review_count):
    """Отправляет рейтинг на сервер через API"""
    try:
        url = f"{SERVER_URL}/api/update-rating/{sku}"
        resp = requests.post(url, json={"rating": rating, "review_count": review_count}, timeout=10)

        if resp.headers.get("content-type", "").startswith("application/json"):
            result = resp.json()
            if result.get("success"):
                print(f"    ✅ Отправлено на сервер")
            else:
                print(f"    ⚠️  Ошибка: {result.get('error')}")
        else:
            print(f"    ⚠️  Сервер: статус {resp.status_code}")

    except requests.exceptions.ConnectionError:
        print(f"    ⚠️  Сервер недоступен ({SERVER_URL})")
    except Exception as e:
        print(f"    ❌ Ошибка: {e}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("\n" + "=" * 70)
    print("📊 ПАРСЕР РЕЙТИНГОВ OZON (CI/CD)")
    print("=" * 70)

    # Шаг 1: Получаем товары из API
    products = get_products_from_api()
    if not products:
        print("❌ Нет товаров. Завершение.")
        sys.exit(1)

    # Шаг 2: Запускаем Chrome + Xvfb
    print("\n🌐 Запуск Chrome + Xvfb...")
    xvfb, chrome = start_xvfb_and_chrome()
    if not xvfb or not chrome:
        print("❌ Chrome не запустился. Завершение.")
        sys.exit(1)

    try:
        # Шаг 3: Парсим рейтинги
        print("\n🔍 Парсинг рейтингов...")
        results = asyncio.run(parse_ratings(products))

        # Шаг 4: Отправляем на сервер
        print("\n📤 Отправка на сервер...")
        success = 0
        blocked = 0
        for sku, data in results.items():
            if data:
                send_to_server(sku, data["rating"], data["review_count"])
                success += 1
            else:
                blocked += 1

        # Итоги
        print("\n" + "=" * 70)
        print(f"✅ Завершено!")
        print(f"   Товаров: {len(products)}")
        print(f"   Успешно: {success}")
        print(f"   Не удалось: {blocked}")
        print("=" * 70)

        if blocked > 0 and success == 0:
            print("\n⚠️  Все запросы заблокированы — вероятно IP в блоклисте Ozon.")
            print("   Попробуйте запустить локально: python update_ratings_local.py")
            sys.exit(1)

    finally:
        cleanup(xvfb, chrome)
        print("🧹 Chrome и Xvfb остановлены.")


if __name__ == "__main__":
    main()
