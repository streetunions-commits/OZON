#!/usr/bin/env python3
"""
============================================================================
ЛОКАЛЬНЫЙ ПАРСЕР РЕЙТИНГОВ OZON
============================================================================

Парсит карточки товаров локально (без блокировки) и отправляет данные на сервер

Использование:
    python update_ratings_local.py

============================================================================
"""

import requests
from bs4 import BeautifulSoup
import json
import re
import sqlite3

# Конфигурация
SERVER_URL = "http://89.167.25.21"  # URL вашего сервера
DB_PATH = "ozon_data.db"  # Локальная БД


def parse_product_card(sku):
    """Парсит карточку товара и извлекает рейтинг и отзывы"""
    try:
        url = f"https://www.ozon.ru/product/-{sku}/"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        }

        print(f"  📥 Загружаю карточку SKU {sku}...")
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code != 200:
            print(f"  ⚠️  Ошибка: статус {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, 'html.parser')

        rating = None
        review_count = None

        # Вариант 1: JSON-LD данные
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and 'aggregateRating' in data:
                    rating = float(data['aggregateRating'].get('ratingValue', 0))
                    review_count = int(data['aggregateRating'].get('reviewCount', 0))
                    break
            except:
                continue

        # Вариант 2: Regex поиск
        if rating is None or review_count is None:
            rating_match = re.search(r'"ratingValue["\s:]+([0-9]+[.,][0-9]+)', response.text)
            if rating_match:
                rating = float(rating_match.group(1).replace(',', '.'))

            review_match = re.search(r'"reviewCount["\s:]+(\d+)', response.text)
            if review_match:
                review_count = int(review_match.group(1))

        if rating is not None and review_count is not None:
            print(f"  ✅ SKU {sku}: рейтинг={rating}, отзывов={review_count}")
            return {'rating': rating, 'review_count': review_count}
        else:
            print(f"  ⚠️  SKU {sku}: не удалось извлечь данные")
            return None

    except Exception as e:
        print(f"  ❌ Ошибка при парсинге SKU {sku}: {e}")
        return None


def send_to_server(sku, rating, review_count):
    """Отправляет данные на сервер"""
    try:
        url = f"{SERVER_URL}/api/update-rating/{sku}"
        data = {
            'rating': rating,
            'review_count': review_count
        }

        response = requests.post(url, json=data, timeout=10)
        result = response.json()

        if result.get('success'):
            print(f"  ✅ Данные отправлены на сервер")
        else:
            print(f"  ⚠️  Ошибка сервера: {result.get('error')}")

    except Exception as e:
        print(f"  ❌ Ошибка при отправке: {e}")


def main():
    """Основная функция"""
    print("\n" + "="*70)
    print("📊 ЛОКАЛЬНЫЙ ПАРСЕР РЕЙТИНГОВ OZON")
    print("="*70 + "\n")

    # Получаем список SKU из локальной БД
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT sku FROM products ORDER BY sku')
    skus = [row[0] for row in cursor.fetchall()]
    conn.close()

    print(f"📦 Найдено товаров: {len(skus)}\n")

    for i, sku in enumerate(skus, 1):
        print(f"[{i}/{len(skus)}] SKU {sku}:")

        # Парсим карточку
        card_data = parse_product_card(sku)

        if card_data:
            # Отправляем на сервер
            send_to_server(sku, card_data['rating'], card_data['review_count'])

        print()  # Пустая строка для разделения

    print("\n✅ Парсинг завершен!")


if __name__ == "__main__":
    main()
