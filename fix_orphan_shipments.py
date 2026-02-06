#!/usr/bin/env python3
"""
============================================================================
СКРИПТ ДЛЯ УДАЛЕНИЯ СИРОТСКИХ ОТГРУЗОК
============================================================================

Назначение: Удаляет записи из warehouse_shipments, у которых нет
            связанного документа (doc_id IS NULL)

Использование:
    python fix_orphan_shipments.py          # Показать что будет удалено
    python fix_orphan_shipments.py --delete # Фактически удалить

@author OZON Tracker Team
@version 1.0.0
@lastUpdated 2026-02-06
"""

import sqlite3
import sys

DB_PATH = "ozon_data.db"


def main():
    delete_mode = "--delete" in sys.argv

    print("\n" + "=" * 60)
    print("🔍 ПОИСК СИРОТСКИХ ОТГРУЗОК (без документа)")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Проверяем существование таблицы
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='warehouse_shipments'
    """)
    if not cursor.fetchone():
        print("\n❌ Таблица warehouse_shipments не существует")
        print("   Возможно, база данных не инициализирована")
        conn.close()
        return

    # Находим сиротские записи
    cursor.execute("""
        SELECT
            s.id,
            s.sku,
            s.shipment_date,
            s.quantity,
            s.destination,
            p.name as product_name,
            p.offer_id
        FROM warehouse_shipments s
        LEFT JOIN products p ON s.sku = p.sku
        WHERE s.doc_id IS NULL
        ORDER BY s.shipment_date DESC
    """)

    orphans = cursor.fetchall()

    if not orphans:
        print("\n✅ Сиротских отгрузок не найдено!")
        print("   Все отгрузки привязаны к документам.")
        conn.close()
        return

    print(f"\n⚠️  Найдено {len(orphans)} сиротских отгрузок:\n")
    print("-" * 80)
    print(f"{'ID':<6} {'SKU':<12} {'Дата':<12} {'Кол-во':<8} {'Назначение':<15} {'Товар'}")
    print("-" * 80)

    for row in orphans:
        product_name = row['offer_id'] or row['product_name'] or f"SKU {row['sku']}"
        print(f"{row['id']:<6} {row['sku']:<12} {row['shipment_date']:<12} "
              f"{-row['quantity']:<8} {row['destination'] or '-':<15} {product_name[:30]}")

    print("-" * 80)

    if delete_mode:
        # Удаляем сиротские записи
        cursor.execute("DELETE FROM warehouse_shipments WHERE doc_id IS NULL")
        deleted_count = cursor.rowcount
        conn.commit()

        print(f"\n✅ Удалено {deleted_count} записей")
        print("   Остатки пересчитаются автоматически при следующем обновлении страницы")
    else:
        print("\n📋 Для удаления запустите:")
        print("   python fix_orphan_shipments.py --delete")
        print("\n⚠️  ВНИМАНИЕ: Это действие необратимо!")

    conn.close()


if __name__ == "__main__":
    main()
