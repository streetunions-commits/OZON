# Бэкап на Google Drive

## Обзор

Автоматический ежедневный бэкап всего проекта на Google Drive через rclone.

## Настройка на сервере (один раз)

### 1. Установка rclone

```bash
ssh root@89.167.25.21

# Установка rclone
curl https://rclone.org/install.sh | sudo bash

# Проверка установки
rclone version
```

### 2. Настройка Google Drive

```bash
# Запустить мастер настройки
rclone config

# Следовать шагам:
# n) New remote
# name> gdrive
# Storage> drive (выбрать Google Drive)
# client_id> (оставить пустым, Enter)
# client_secret> (оставить пустым, Enter)
# scope> 1 (Full access)
# root_folder_id> (оставить пустым)
# service_account_file> (оставить пустым)
# Edit advanced config> n
# Use auto config> n (на сервере без GUI)
```

### 3. Авторизация (на сервере без GUI)

Когда rclone попросит авторизацию:

1. На **локальном ПК** (с браузером) установить rclone:
   - Windows: `winget install Rclone.Rclone` или скачать с https://rclone.org/downloads/

2. На локальном ПК выполнить:
   ```bash
   rclone authorize "drive"
   ```

3. Откроется браузер → войти в Google аккаунт → разрешить доступ

4. В терминале появится токен вида:
   ```
   {"access_token":"ya29...","token_type":"Bearer",...}
   ```

5. Скопировать ВЕСЬ токен (включая фигурные скобки) и вставить на сервере

6. Завершить настройку:
   ```
   y) Yes this is OK
   q) Quit config
   ```

### 4. Проверка подключения

```bash
# Проверить что remote работает
rclone lsd gdrive:

# Должен показать список папок на Google Drive
```

### 5. Настройка cron для ежедневного бэкапа

```bash
# Открыть crontab
crontab -e

# Добавить строку (бэкап каждый день в 03:00)
0 3 * * * cd /root/OZON && /usr/bin/python3 execution/backup_to_gdrive.py >> /var/log/ozon_backup.log 2>&1
```

## Использование

### Ручной запуск

```bash
cd /root/OZON

# Полный бэкап
python3 execution/backup_to_gdrive.py

# Только база данных
python3 execution/backup_to_gdrive.py --db-only

# Тест без загрузки
python3 execution/backup_to_gdrive.py --dry-run
```

### Просмотр бэкапов

```bash
# Список бэкапов на Google Drive
rclone ls gdrive:OZON_Backups/

# Скачать конкретный бэкап
rclone copy gdrive:OZON_Backups/ozon_full_backup_2026-02-06_03-00-00.tar.gz ./
```

### Восстановление из бэкапа

```bash
# Скачать последний бэкап
rclone copy gdrive:OZON_Backups/ /tmp/restore/ --include "*2026-02-06*"

# Распаковать
cd /tmp/restore
tar -xzf ozon_full_backup_*.tar.gz

# Восстановить базу данных
cp ozon_data.db /root/OZON/
```

## Конфигурация

В файле `execution/backup_to_gdrive.py`:

| Параметр | Значение | Описание |
|----------|----------|----------|
| `RCLONE_REMOTE` | `gdrive` | Имя remote в rclone |
| `GDRIVE_BACKUP_FOLDER` | `OZON_Backups` | Папка на Google Drive |
| `MAX_BACKUPS` | `7` | Сколько бэкапов хранить |

## Мониторинг

```bash
# Посмотреть логи бэкапа
tail -f /var/log/ozon_backup.log

# Проверить последний бэкап
rclone ls gdrive:OZON_Backups/ | tail -1
```

## Troubleshooting

### Ошибка "Remote not found"
```bash
rclone config  # Заново настроить remote с именем "gdrive"
```

### Ошибка авторизации
```bash
rclone config reconnect gdrive:  # Обновить токен
```

### Бэкап не запускается по cron
```bash
# Проверить путь к python
which python3

# Проверить права на скрипт
chmod +x /root/OZON/execution/backup_to_gdrive.py

# Проверить логи cron
grep CRON /var/log/syslog
```
