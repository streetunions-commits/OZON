#!/usr/bin/env python3
"""
🚀 Главный скрипт для Ozon FBO Tracker
Автоматически:
1. Проверяет обновления на GitHub каждую минуту
2. Если есть обновления - скачивает их
3. Flask автоматически перезагружает приложение (debug=True)
"""

import subprocess
import os
import time
import threading
from datetime import datetime

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

REPO_PATH = os.path.dirname(os.path.abspath(__file__))
CHECK_INTERVAL = 60  # Проверяем каждые 60 секунд
MAIN_SCRIPT = "ozon_app.py"

# ============================================================================
# ФУНКЦИИ
# ============================================================================

def log(message, prefix=""):
    """Логирование с timestamp"""
    ts = datetime.now().strftime("%H:%M:%S")
    if prefix:
        print(f"[{ts}] {prefix} {message}")
    else:
        print(f"[{ts}] {message}")

def git_pull_loop():
    """Фоновый поток для проверки обновлений"""
    log("🔄 Git sync поток запущен", "🔀")
    
    while True:
        try:
            time.sleep(CHECK_INTERVAL)
            
            result = subprocess.run(
                ["git", "pull", "origin", "main"],
                cwd=REPO_PATH,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                if "Already up to date" not in result.stdout and "Already up-to-date" not in result.stdout:
                    log("✅ ОБНОВЛЕНИЯ ПОЛУЧЕНЫ С GITHUB!", "🔀")
                    log("Flask автоматически перезагружает приложение...", "🔀")
                    log("Нажмите F5 в браузере чтобы обновить страницу", "🔀")
                else:
                    log("✓ Уже актуально", "🔀")
            else:
                log(f"⚠️  Git ошибка: {result.stderr[:100]}", "🔀")
                
        except Exception as e:
            log(f"❌ Ошибка при проверке обновлений: {e}", "🔀")

def main():
    """Главная функция"""
    os.chdir(REPO_PATH)
    
    print("\n" + "="*70)
    print("🚀 OZON FBO TRACKER - AUTO-UPDATE ВЕРСИЯ")
    print("="*70)
    log(f"📂 Папка: {REPO_PATH}", "📍")
    log(f"⏰ Проверка обновлений каждые {CHECK_INTERVAL} сек", "⚙️")
    print("="*70 + "\n")
    
    # Запускаем фоновый поток для Git синка
    git_thread = threading.Thread(target=git_pull_loop, daemon=True)
    git_thread.start()
    
    log("🌐 Запускаю Flask приложение...", "🚀")
    log("Откройте браузер: http://localhost:5000", "💻")
    log("", "")
    
    # Запускаем основное приложение
    try:
        subprocess.run(
            ["python", MAIN_SCRIPT],
            cwd=REPO_PATH,
            check=False
        )
    except KeyboardInterrupt:
        log("👋 Приложение остановлено", "🛑")
    except Exception as e:
        log(f"❌ Ошибка при запуске: {e}", "❌")

if __name__ == "__main__":
    main()
