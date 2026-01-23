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
from datetime import datetime
from flask import Flask, render_template_string, jsonify, request

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

# ✅ Проверяем что ключи установлены
if not OZON_CLIENT_ID or not OZON_API_KEY:
    import os.path
    print("\n❌ ОШИБКА: Не установлены переменные окружения!")
    print(f"   📂 Текущая папка: {os.getcwd()}")
    print(f"   📋 .env существует: {os.path.exists('.env')}")
    print(f"   📋 OZON_CLIENT_ID: {OZON_CLIENT_ID}")
    print(f"   📋 OZON_API_KEY: {OZON_API_KEY}")
    print("\n🔧 Способ 1 - PowerShell (в одной команде):")
    print("   $env:OZON_CLIENT_ID='138926'; $env:OZON_API_KEY='a3d83a9a-d652-409a-9471-f09bd9b9b1bb'; python ozon_app.py")
    print("\n🔧 Способ 2 - Создать .env файл в папке (рекомендуется):")
    print("   Содержимое .env:")
    print("   OZON_CLIENT_ID=138926")
    print("   OZON_API_KEY=a3d83a9a-d652-409a-9471-f09bd9b9b1bb")
    sys.exit(1)

OZON_HOST = "https://api-seller.ozon.ru"
DB_PATH = "ozon_data.db"

# ============================================================================
# СИНХРОНИЗАЦИЯ ДАННЫХ
# ============================================================================

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
    
    conn.commit()
    conn.close()


def get_ozon_headers():
    """Заголовки для запросов к Ozon API"""
    return {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json"
    }


def load_fbo_orders():
    """Загрузка активных FBO заказов: SKU -> qty (по отправлениям)"""
    print("\n📦 Загрузка активных заказов FBO...")

    orders_by_sku = {}

    # Статусы для FBO
    statuses = ["awaiting_packaging", "awaiting_deliver", "acceptance_in_progress"]

    from datetime import datetime, timedelta, timezone
    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(days=30)
    since_str = from_dt.isoformat().replace("+00:00", "Z")
    to_str = to_dt.isoformat().replace("+00:00", "Z")

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

            # Вытаскиваем posting_number
            for p in postings:
                pn = p.get("posting_number")
                if pn:
                    posting_numbers.append(pn)

            offset += 50

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


def sync_products():
    """Синхронизация товаров из Ozon"""
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
                
                # ✅ ПРАВИЛЬНО: проверяем на None, а не на "истинность"!
                # Иначе 0 считается False и мы берём значение из другого поля
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
        
        # ✅ Загружаем заказы
        orders_by_sku = load_fbo_orders()
        
        # Определяем дату снимка (YYYY-MM-DD)
        from datetime import date
        snapshot_date = date.today().isoformat()
        
        # ✅ Пишем в обе таблицы
        for sku, data in products_data.items():
            orders_qty = orders_by_sku.get(sku, 0)
            
            # 1️⃣ Обновляем текущие остатки
            cursor.execute('''
                INSERT INTO products (sku, name, fbo_stock, orders_qty, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(sku) DO UPDATE SET
                    name=excluded.name,
                    fbo_stock=excluded.fbo_stock,
                    orders_qty=excluded.orders_qty,
                    updated_at=excluded.updated_at
            ''', (
                sku,
                data.get("name", ""),
                data.get("fbo_stock", 0),
                orders_qty,
                datetime.now()
            ))
            
            # 2️⃣ Сохраняем в историю (один раз в день на SKU)
            cursor.execute('''
                INSERT INTO products_history (sku, name, fbo_stock, orders_qty, snapshot_date)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(sku, snapshot_date) DO UPDATE SET
                    name=excluded.name,
                    fbo_stock=excluded.fbo_stock,
                    orders_qty=excluded.orders_qty
            ''', (
                sku,
                data.get("name", ""),
                data.get("fbo_stock", 0),
                orders_qty,
                snapshot_date
            ))
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ Синхронизация завершена!")
        print(f"   📦 Товаров загружено: {len(products_data)}")
        print(f"   📅 История сохранена на дату: {snapshot_date}")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка подключения: {e}")
        return False
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
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
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
        }

        .header {
            background: white;
            padding: 30px;
            border-radius: 12px;
            margin-bottom: 30px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }

        .header h1 {
            color: #333;
            margin-bottom: 10px;
        }

        .header p {
            color: #666;
            font-size: 14px;
        }

        .stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }

        .stat-box {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }

        .stat-box .number {
            font-size: 32px;
            font-weight: bold;
            margin-bottom: 5px;
        }

        .stat-box .label {
            font-size: 14px;
            opacity: 0.9;
        }

        .table-container {
            background: white;
            border-radius: 12px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            overflow: hidden;
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
        }

        .note-input:focus {
            outline: none;
            border-color: #667eea;
            box-shadow: 0 0 0 2px rgba(102, 126, 234, 0.1);
        }

        .note-input::placeholder {
            color: #bbb;
        }
        }

        .refresh-btn {
            background: #667eea;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 14px;
        }

        .refresh-btn:hover {
            background: #5568d3;
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
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
                <div>
                    <h1>📦 Остатки на FBO складах Ozon</h1>
                    <p>Актуальные данные по остаткам товаров</p>
                </div>
                <button class="refresh-btn" onclick="location.reload()">🔄 Обновить</button>
            </div>

            <div class="stats">
                <div class="stat-box">
                    <div class="number" id="total-products">-</div>
                    <div class="label">Товаров</div>
                </div>
                <div class="stat-box">
                    <div class="number" id="total-stock">-</div>
                    <div class="label">Всего на FBO</div>
                </div>
            </div>
        </div>

        <div class="table-container">
            <div class="tabs">
                <button class="tab-button active" onclick="switchTab('current')">📊 Текущие остатки</button>
                <button class="tab-button" onclick="switchTab('history')">📅 История товара</button>
            </div>

            <!-- ТАБ 1: Текущие остатки -->
            <div id="current" class="tab-content active">
                <div class="table-header">
                    <h2>Список товаров</h2>
                    <div style="display: flex; gap: 10px; align-items: center;">
                        <div class="search-box">
                            <input 
                                type="text" 
                                id="search-input" 
                                placeholder="Поиск по названию или SKU..."
                                onkeyup="filterTable()"
                            >
                        </div>
                    </div>
                </div>
                <div id="table-content">
                    <div class="loading">Загрузка данных...</div>
                </div>
            </div>

            <!-- ТАБ 2: История товара -->
            <div id="history" class="tab-content">
                <div class="table-header">
                    <h2>История товара по датам</h2>
                    <div>
                        <label for="product-select" style="margin-right: 10px; font-weight: 500;">Выберите товар:</label>
                        <select 
                            id="product-select" 
                            class="history-select"
                            onchange="loadHistoryForProduct()"
                        >
                            <option value="">-- Загрузка товаров --</option>
                        </select>
                    </div>
                </div>
                <div id="history-content">
                    <div class="loading">Выберите товар из списка</div>
                </div>
            </div>
        </div>
    </div>

    <script>
        let allProducts = [];

        document.addEventListener('DOMContentLoaded', loadProducts);

        function loadProducts() {
            fetch('/api/products')
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        allProducts = data.products;
                        renderTable(allProducts);
                        document.getElementById('total-products').textContent = data.count;
                        document.getElementById('total-stock').textContent = data.total_stock;
                    } else {
                        showError(data.error);
                    }
                })
                .catch(error => showError('Ошибка при загрузке: ' + error));
        }

        function renderTable(products) {
            const tableContent = document.getElementById('table-content');

            if (products.length === 0) {
                tableContent.innerHTML = '<div class="empty-state"><h3>Товары не найдены</h3></div>';
                return;
            }

            let html = '<table><thead><tr><th>Название</th><th>SKU</th><th style="text-align: right;">Кол-во на FBO</th><th style="text-align: right;">Активные заказы</th></tr></thead><tbody>';

            products.forEach(p => {
                const stockClass = p.fbo_stock < 5 ? 'stock low' : 'stock';
                const ordersClass = p.orders_qty > 0 ? 'stock' : 'stock';
                html += `
                    <tr>
                        <td>${p.name || '(без названия)'}</td>
                        <td><span class="sku">${p.sku}</span></td>
                        <td style="text-align: right;"><span class="${stockClass}">${p.fbo_stock}</span></td>
                        <td style="text-align: right;"><span class="${ordersClass}">${p.orders_qty || 0}</span></td>
                    </tr>
                `;
            });

            html += '</tbody></table>';
            tableContent.innerHTML = html;
        }

        function filterTable() {
            const input = document.getElementById('search-input').value.toLowerCase();
            const filtered = allProducts.filter(p => {
                return (p.name && p.name.toLowerCase().includes(input)) || 
                       (p.sku && p.sku.toString().includes(input));
            });
            renderTable(filtered);
        }

        function loadProductsByDate() {
            const dateInput = document.getElementById('date-filter').value;
            
            if (!dateInput) {
                loadProducts();
                return;
            }
            
            const selectedDate = new Date(dateInput);
            const formattedDate = dateInput; // YYYY-MM-DD
            
            console.log('Загрузка данных за дату:', formattedDate);
            
            fetch(`/api/products?date=${formattedDate}`)
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        allProducts = data.products;
                        renderTable(allProducts);
                        document.getElementById('total-products').textContent = data.count;
                        document.getElementById('total-stock').textContent = data.total_stock;
                        
                        // Показываем дату в заголовке
                        const dateObj = new Date(selectedDate);
                        const dateStr = dateObj.toLocaleDateString('ru-RU');
                        console.log(`Данные за ${dateStr}: ${data.count} товаров, остаток ${data.total_stock}`);
                    } else {
                        showError(data.error || 'Нет данных за эту дату');
                    }
                })
                .catch(error => showError('Ошибка при загрузке: ' + error));
        }

        function showError(message) {
            document.getElementById('table-content').innerHTML = 
                '<div class="error"><strong>❌ Ошибка:</strong> ' + message + '</div>';
        }

        // ✅ НОВЫЕ ФУНКЦИИ ДЛЯ ТАБОВ И ИСТОРИИ

        function switchTab(tab) {
            // Скрываем все табы
            document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
            document.querySelectorAll('.tab-button').forEach(el => el.classList.remove('active'));
            
            // Показываем нужный таб
            document.getElementById(tab).classList.add('active');
            event.target.classList.add('active');
            
            // Если открыли историю - загружаем список товаров
            if (tab === 'history') {
                loadProductsList();
            }
        }

        function loadProductsList() {
            fetch('/api/products/list')
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        const select = document.getElementById('product-select');
                        select.innerHTML = '<option value="">-- Выберите товар --</option>';
                        
                        data.products.forEach(p => {
                            const option = document.createElement('option');
                            option.value = p.sku;
                            option.textContent = `${p.name} (SKU: ${p.sku})`;
                            select.appendChild(option);
                        });
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
            
            let html = '<table><thead><tr>';
            html += '<th>📝 Заметки</th>';
            html += '<th>📅 Дата</th>';
            html += '<th>Название</th>';
            html += '<th>SKU</th>';
            html += '<th style="text-align: right;">📦 FBO остаток</th>';
            html += '<th style="text-align: right;">📦 Заказы</th>';
            html += '</tr></thead><tbody>';
            
            data.history.forEach(item => {
                const date = new Date(item.snapshot_date);
                const dateStr = date.toLocaleDateString('ru-RU', { 
                    year: 'numeric', 
                    month: 'long', 
                    day: 'numeric' 
                });
                
                const stockClass = item.fbo_stock < 5 ? 'stock low' : 'stock';
                const uniqueId = `note_${data.product_sku}_${item.snapshot_date}`;
                const notes = item.notes || '';
                
                html += `<tr>`;
                html += `<td style="max-width: 200px;">
                    <input 
                        type="text" 
                        id="${uniqueId}"
                        class="note-input"
                        value="${notes.replace(/"/g, '&quot;')}"
                        placeholder="Добавьте заметку..."
                        onchange="saveNote(${data.product_sku}, '${item.snapshot_date}', this.value)"
                    >
                </td>`;
                html += `<td><strong>${dateStr}</strong></td>`;
                html += `<td>${item.name}</td>`;
                html += `<td><span class="sku">${item.sku}</span></td>`;
                html += `<td style="text-align: right;"><span class="${stockClass}">${item.fbo_stock}</span></td>`;
                html += `<td style="text-align: right;"><span class="stock">${item.orders_qty || 0}</span></td>`;
                html += `</tr>`;
            });
            
            html += '</tbody></table>';
            historyContent.innerHTML = html;
        }

        function saveNote(sku, date, text) {
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
                    console.log('✅ Заметка сохранена');
                } else {
                    alert('❌ Ошибка при сохранении: ' + data.error);
                }
            })
            .catch(error => console.error('Ошибка:', error));
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
        
        # Проверяем параметр даты
        date_filter = request.args.get('date')
        
        if date_filter:
            # Фильтруем по дате (берём данные за конкретный день)
            # ВАЖНО: в БД хранится timestamp, поэтому берём весь день
            cursor.execute('''
                SELECT * FROM products 
                WHERE DATE(updated_at) = ? 
                ORDER BY fbo_stock DESC, name
            ''', (date_filter,))
        else:
            # Берём самые свежие данные (последнее обновление)
            cursor.execute('SELECT * FROM products ORDER BY fbo_stock DESC, name')
        
        rows = cursor.fetchall()
        
        products = [dict(row) for row in rows]
        total_stock = sum(p['fbo_stock'] for p in products)
        
        conn.close()
        
        return jsonify({
            'success': True,
            'count': len(products),
            'total_stock': total_stock,
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
        
        # Берём уникальные товары из истории
        cursor.execute('''
            SELECT DISTINCT sku, name 
            FROM products_history 
            ORDER BY name
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


# ============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ============================================================================

def main():
    print("\n" + "="*60)
    print("🌐 OZON ТОВАРЫ - ОСТАТКИ FBO")
    print("="*60)
    
    init_database()
    
    if sync_products():
        print("\n" + "="*60)
        print("✅ ГОТОВО!")
        print("="*60)
        print("\n🌐 Откройте браузер: http://localhost:5000")
        print("\n⏹️  Для остановки: Ctrl+C\n")
        
        app.run(host='127.0.0.1', port=5000, debug=False)
    else:
        print("\n❌ Ошибка при синхронизации!")
        sys.exit(1)


if __name__ == '__main__':
    main()
