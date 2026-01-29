#!/usr/bin/env python3
"""
🗑️ Удаляет данные с 15 по 22 января 2026 из ozon_data.db
"""

import sqlite3
import sys

DB_PATH = 'ozon_data.db'

print("")
print("="*60)
print("🗑️ УДАЛЕНИЕ ДАННЫХ С 15 ПО 22 ЯНВАРЯ 2026")
print("="*60)
print("")

try:
    # Подключаемся к БД
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Получаем кол-во ДО удаления
    before = cursor.execute('SELECT COUNT(*) FROM products_history').fetchone()[0]
    print(f"📊 Записей ДО удаления: {before}")
    
    # Удаляем данные
    cursor.execute("DELETE FROM products_history WHERE snapshot_date BETWEEN '2026-01-15' AND '2026-01-22'")
    conn.commit()
    
    # Получаем кол-во ПОСЛЕ удаления
    after = cursor.execute('SELECT COUNT(*) FROM products_history').fetchone()[0]
    deleted = before - after
    
    print(f"🗑️  Удалено записей: {deleted}")
    print(f"📊 Осталось записей: {after}")
    
    # Показываем диапазон дат
    date_range = cursor.execute('SELECT MIN(snapshot_date), MAX(snapshot_date) FROM products_history').fetchone()
    if date_range[0]:
        print(f"📅 Диапазон дат: {date_range[0]} - {date_range[1]}")
    else:
        print(f"📅 БД пуста")
    
    print("")
    print("✅ ГОТОВО!")
    print("")
    
    conn.close()
    sys.exit(0)
    
except Exception as e:
    print(f"❌ ОШИБКА: {e}")
    print("")
    sys.exit(1)
