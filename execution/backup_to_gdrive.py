#!/usr/bin/env python3
"""
============================================================================
БЭКАП ПРОЕКТА НА GOOGLE DRIVE
============================================================================

Назначение: Создаёт архив проекта и загружает на Google Drive через rclone

Возможности:
- Архивирует весь проект (исключая .git, __pycache__, .env)
- Загружает архив на Google Drive
- Хранит последние N бэкапов (по умолчанию 7)
- Логирует все операции

Использование:
    python backup_to_gdrive.py              # Бэкап всего проекта
    python backup_to_gdrive.py --db-only    # Только база данных
    python backup_to_gdrive.py --dry-run    # Тест без загрузки

Зависимости:
- rclone (установить и настроить: rclone config)

@author OZON Tracker Team
@version 1.0.0
@lastUpdated 2026-02-06
"""

import os
import sys
import subprocess
import shutil
import argparse
from datetime import datetime
from pathlib import Path

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

# Имя remote в rclone (настраивается через rclone config)
RCLONE_REMOTE = "gdrive"

# Папка на Google Drive для бэкапов
GDRIVE_BACKUP_FOLDER = "ТОВАРКА/БЭКАПЫ"

# Сколько бэкапов хранить (старые удаляются)
MAX_BACKUPS = 7

# Файлы и папки для исключения из архива
EXCLUDE_PATTERNS = [
    ".git",
    "__pycache__",
    "*.pyc",
    ".env",
    "*.log",
    "node_modules",
    ".venv",
    "venv",
    "*.tmp",
    ".claude/settings.local.json",
]

# Путь к проекту (автоматически определяется)
PROJECT_DIR = Path(__file__).parent.parent.resolve()

# Временная папка для архивов
TEMP_DIR = Path("/tmp/ozon_backups")


# ============================================================================
# ФУНКЦИИ
# ============================================================================

def log(message: str, level: str = "INFO"):
    """Логирование с временной меткой"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


def check_rclone():
    """Проверить, что rclone установлен и настроен"""
    try:
        result = subprocess.run(
            ["rclone", "listremotes"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode != 0:
            log("rclone не установлен или не настроен", "ERROR")
            return False

        remotes = result.stdout.strip().split("\n")
        remote_name = f"{RCLONE_REMOTE}:"

        if remote_name not in remotes:
            log(f"Remote '{RCLONE_REMOTE}' не найден в rclone", "ERROR")
            log(f"Доступные remotes: {remotes}", "INFO")
            log("Запустите: rclone config", "INFO")
            return False

        log(f"rclone настроен, remote '{RCLONE_REMOTE}' найден", "INFO")
        return True

    except FileNotFoundError:
        log("rclone не установлен. Установите: apt install rclone", "ERROR")
        return False
    except Exception as e:
        log(f"Ошибка проверки rclone: {e}", "ERROR")
        return False


def create_archive(db_only: bool = False) -> Path:
    """
    Создать архив проекта.

    Параметры:
        db_only: Если True — архивируем только базу данных

    Возвращает: Path к созданному архиву
    """
    # Создаём временную папку
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    # Имя архива с датой
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    if db_only:
        archive_name = f"ozon_db_backup_{timestamp}.tar.gz"
        files_to_archive = ["ozon_data.db"]
    else:
        archive_name = f"ozon_full_backup_{timestamp}.tar.gz"
        files_to_archive = None  # Весь проект

    archive_path = TEMP_DIR / archive_name

    log(f"Создаю архив: {archive_name}", "INFO")

    # Формируем команду tar
    tar_cmd = ["tar", "-czf", str(archive_path)]

    # Добавляем исключения
    for pattern in EXCLUDE_PATTERNS:
        tar_cmd.extend(["--exclude", pattern])

    # Переходим в папку проекта
    tar_cmd.extend(["-C", str(PROJECT_DIR)])

    if files_to_archive:
        # Только конкретные файлы
        for f in files_to_archive:
            if (PROJECT_DIR / f).exists():
                tar_cmd.append(f)
            else:
                log(f"Файл {f} не найден, пропускаю", "WARNING")
    else:
        # Весь проект
        tar_cmd.append(".")

    try:
        result = subprocess.run(tar_cmd, capture_output=True, text=True, timeout=300)

        if result.returncode != 0:
            log(f"Ошибка создания архива: {result.stderr}", "ERROR")
            return None

        # Проверяем размер
        size_mb = archive_path.stat().st_size / (1024 * 1024)
        log(f"Архив создан: {size_mb:.2f} МБ", "INFO")

        return archive_path

    except Exception as e:
        log(f"Ошибка при создании архива: {e}", "ERROR")
        return None


def upload_to_gdrive(archive_path: Path, dry_run: bool = False) -> bool:
    """
    Загрузить архив на Google Drive через rclone.

    Параметры:
        archive_path: Путь к архиву
        dry_run: Если True — только показать команду, не выполнять

    Возвращает: True если успешно
    """
    remote_path = f"{RCLONE_REMOTE}:{GDRIVE_BACKUP_FOLDER}/{archive_path.name}"

    cmd = ["rclone", "copy", str(archive_path), f"{RCLONE_REMOTE}:{GDRIVE_BACKUP_FOLDER}/"]

    if dry_run:
        log(f"[DRY-RUN] Команда: {' '.join(cmd)}", "INFO")
        return True

    log(f"Загружаю на Google Drive: {GDRIVE_BACKUP_FOLDER}/{archive_path.name}", "INFO")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)

        if result.returncode != 0:
            log(f"Ошибка загрузки: {result.stderr}", "ERROR")
            return False

        log("Загрузка завершена успешно!", "INFO")
        return True

    except Exception as e:
        log(f"Ошибка при загрузке: {e}", "ERROR")
        return False


def cleanup_old_backups(dry_run: bool = False):
    """
    Удалить старые бэкапы, оставив только последние MAX_BACKUPS.

    Параметры:
        dry_run: Если True — только показать что будет удалено
    """
    log(f"Проверяю старые бэкапы (лимит: {MAX_BACKUPS})...", "INFO")

    try:
        # Получаем список файлов в папке бэкапов
        result = subprocess.run(
            ["rclone", "lsf", f"{RCLONE_REMOTE}:{GDRIVE_BACKUP_FOLDER}/", "--files-only"],
            capture_output=True,
            text=True,
            timeout=60
        )

        if result.returncode != 0:
            log("Не удалось получить список бэкапов", "WARNING")
            return

        files = sorted(result.stdout.strip().split("\n"), reverse=True)
        files = [f for f in files if f.endswith(".tar.gz")]

        if len(files) <= MAX_BACKUPS:
            log(f"Бэкапов: {len(files)}, удаление не требуется", "INFO")
            return

        # Удаляем старые
        files_to_delete = files[MAX_BACKUPS:]

        for filename in files_to_delete:
            remote_file = f"{RCLONE_REMOTE}:{GDRIVE_BACKUP_FOLDER}/{filename}"

            if dry_run:
                log(f"[DRY-RUN] Удалить: {filename}", "INFO")
            else:
                log(f"Удаляю старый бэкап: {filename}", "INFO")
                subprocess.run(
                    ["rclone", "deletefile", remote_file],
                    capture_output=True,
                    timeout=30
                )

        log(f"Удалено {len(files_to_delete)} старых бэкапов", "INFO")

    except Exception as e:
        log(f"Ошибка при очистке: {e}", "WARNING")


def cleanup_local_archive(archive_path: Path):
    """Удалить локальный архив после загрузки"""
    try:
        if archive_path and archive_path.exists():
            archive_path.unlink()
            log("Локальный архив удалён", "INFO")
    except Exception as e:
        log(f"Не удалось удалить локальный архив: {e}", "WARNING")


def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(description="Бэкап проекта на Google Drive")
    parser.add_argument("--db-only", action="store_true", help="Только база данных")
    parser.add_argument("--dry-run", action="store_true", help="Тест без загрузки")
    parser.add_argument("--no-cleanup", action="store_true", help="Не удалять старые бэкапы")

    args = parser.parse_args()

    log("=" * 60, "INFO")
    log("БЭКАП OZON TRACKER НА GOOGLE DRIVE", "INFO")
    log("=" * 60, "INFO")

    # Проверяем rclone
    if not check_rclone():
        log("Настройте rclone перед использованием!", "ERROR")
        sys.exit(1)

    # Создаём архив
    archive_path = create_archive(db_only=args.db_only)
    if not archive_path:
        log("Не удалось создать архив", "ERROR")
        sys.exit(1)

    # Загружаем на Google Drive
    success = upload_to_gdrive(archive_path, dry_run=args.dry_run)

    # Удаляем локальный архив
    if not args.dry_run:
        cleanup_local_archive(archive_path)

    # Удаляем старые бэкапы
    if success and not args.no_cleanup and not args.dry_run:
        cleanup_old_backups()

    if success:
        log("=" * 60, "INFO")
        log("БЭКАП ЗАВЕРШЁН УСПЕШНО!", "INFO")
        log("=" * 60, "INFO")
    else:
        log("Бэкап завершён с ошибками", "ERROR")
        sys.exit(1)


if __name__ == "__main__":
    main()
