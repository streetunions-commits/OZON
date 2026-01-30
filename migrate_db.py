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

DB_PATH = os.path.join(os.path.dirname(__file__), 'ozon_tracker.db')

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

        conn.commit()
        conn.close()

        print("✅ Миграция завершена успешно!")
        return True

    except Exception as e:
        print(f"❌ Ошибка при миграции: {e}")
        return False

if __name__ == "__main__":
    migrate()
