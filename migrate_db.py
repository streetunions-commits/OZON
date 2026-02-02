#!/usr/bin/env python3
"""
============================================================================
СКРИПТ МИГРАЦИИ БАЗЫ ДАННЫХ
============================================================================

Назначение: Добавление столбца orders_plan в products_history

Использование:
    python migrate_db.py

============================================================================
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'ozon_data.db')

def ensure_column(cursor, table_name, column_name, alter_statement):
    """Проверяет наличие столбца и добавляет его если нет"""
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [column[1] for column in cursor.fetchall()]

    if column_name not in columns:
        cursor.execute(alter_statement)
        return True
    return False

def migrate():
    """Выполняет миграцию БД"""
    print("🔄 Начинаю миграцию базы данных...")
    print(f"📂 Путь к БД: {DB_PATH}")

    if not os.path.exists(DB_PATH):
        print("❌ База данных не найдена!")
        print(f"   Ожидается: {DB_PATH}")
        return False

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Добавляем столбец orders_plan
        if ensure_column(cursor, "products_history", "orders_plan",
                         "ALTER TABLE products_history ADD COLUMN orders_plan INTEGER DEFAULT NULL"):
            print("✅ Столбец orders_plan добавлен в products_history")
        else:
            print("ℹ️  Столбец orders_plan уже существует")

        # Добавляем столбец rating (рейтинг товара)
        if ensure_column(cursor, "products_history", "rating",
                         "ALTER TABLE products_history ADD COLUMN rating REAL DEFAULT NULL"):
            print("✅ Столбец rating добавлен в products_history")
        else:
            print("ℹ️  Столбец rating уже существует")

        # Добавляем столбец review_count (количество отзывов)
        if ensure_column(cursor, "products_history", "review_count",
                         "ALTER TABLE products_history ADD COLUMN review_count INTEGER DEFAULT NULL"):
            print("✅ Столбец review_count добавлен в products_history")
        else:
            print("ℹ️  Столбец review_count уже существует")

        # Добавляем столбец avg_delivery_hours (среднее время доставки в часах)
        if ensure_column(cursor, "products_history", "avg_delivery_hours",
                         "ALTER TABLE products_history ADD COLUMN avg_delivery_hours REAL DEFAULT NULL"):
            print("✅ Столбец avg_delivery_hours добавлен в products_history")
        else:
            print("ℹ️  Столбец avg_delivery_hours уже существует")

        if ensure_column(cursor, "products", "avg_delivery_hours",
                         "ALTER TABLE products ADD COLUMN avg_delivery_hours REAL DEFAULT NULL"):
            print("✅ Столбец avg_delivery_hours добавлен в products")
        else:
            print("ℹ️  Столбец avg_delivery_hours уже существует в products")

        # Добавляем столбец offer_id в products (артикул товара)
        if ensure_column(cursor, "products", "offer_id",
                         "ALTER TABLE products ADD COLUMN offer_id TEXT DEFAULT NULL"):
            print("✅ Столбец offer_id добавлен в products")
        else:
            print("ℹ️  Столбец offer_id уже существует в products")

        # ============================================================
        # Таблица fbo_warehouse_stock — остатки по складам/кластерам
        # ============================================================
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fbo_warehouse_stock (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku INTEGER NOT NULL,
                warehouse_name TEXT,
                stock INTEGER DEFAULT 0,
                snapshot_date DATE NOT NULL
            )
        ''')
        print("✅ Таблица fbo_warehouse_stock создана (или уже существует)")

        # ============================================================
        # Таблица fbo_analytics — аналитика по кластерам (ADS, IDC)
        # ============================================================
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
        print("✅ Таблица fbo_analytics создана (или уже существует)")

        conn.commit()
        conn.close()

        print("✅ Миграция завершена успешно!")
        return True

    except Exception as e:
        print(f"❌ Ошибка при миграции: {e}")
        return False

if __name__ == "__main__":
    migrate()
