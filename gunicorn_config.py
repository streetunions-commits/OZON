"""
============================================================================
GUNICORN КОНФИГУРАЦИЯ ДЛЯ PRODUCTION
============================================================================

Назначение: Настройки для Gunicorn WSGI сервера

Возможности:
- Автоматическая перезагрузка при изменении кода
- Логирование всех запросов
- Оптимальное количество workers

@version 1.0.0
@lastUpdated 2026-01-29
"""

import multiprocessing
import os

# Сервер
bind = "127.0.0.1:8000"  # Gunicorn слушает локально, Nginx проксирует
workers = multiprocessing.cpu_count() * 2 + 1  # Рекомендуемое количество
worker_class = "sync"
timeout = 300  # Таймаут для долгих запросов к Ozon API (синхронизация ~3-4 мин)

# Логирование
accesslog = "/var/log/ozon-tracker/access.log"
errorlog = "/var/log/ozon-tracker/error.log"
loglevel = "info"

# Автоперезагрузка при изменении файлов
# ОТКЛЮЧЕНО для production - при reload=True любое изменение файла прерывает синхронизацию
# Для разработки можно временно включить: reload = True
reload = False
# reload_extra_files = [".env"]

# Graceful shutdown - даём время на завершение синхронизации (до 5 минут)
graceful_timeout = 300

# Производительность
keepalive = 5
max_requests = 1000  # Перезапуск worker после N запросов (предотвращает утечки памяти)
max_requests_jitter = 50

# Безопасность
limit_request_line = 4096
limit_request_fields = 100
limit_request_field_size = 8190
