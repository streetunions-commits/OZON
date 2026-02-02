# ============================================================================
# НАСТРОЙКА ПЛАНИРОВЩИКА WINDOWS ДЛЯ ПАРСЕРА РЕЙТИНГОВ OZON
# ============================================================================
#
# Что делает: Создаёт задачу в Планировщике Windows, которая:
#   - Запускает парсер рейтингов каждый день в 09:00
#   - Если ПК был выключен в 09:00 — запустится при включении
#   - Работает полностью автоматически
#
# Как запустить: Открыть PowerShell от имени администратора и выполнить:
#   powershell -ExecutionPolicy Bypass -File setup_scheduler.ps1
#
# Как удалить задачу:
#   Unregister-ScheduledTask -TaskName "OzonRatingsParser" -Confirm:$false
# ============================================================================

$taskName = "OzonRatingsParser"
$description = "Парсинг рейтингов товаров Ozon (09:00). Если ПК выключен - запустится при включении."
$workingDir = "C:\Users\stree\Documents\GIT_OZON"
$batFile = "C:\Users\stree\Documents\GIT_OZON\run_parser.bat"

# --- Удаляем старую задачу, если есть ---
$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "Удаляю существующую задачу '$taskName'..." -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
}

# Также удалим старое имя задачи, если осталось
$oldTask = Get-ScheduledTask -TaskName "OzonRatingParser" -ErrorAction SilentlyContinue
if ($oldTask) {
    Write-Host "Удаляю старую задачу 'OzonRatingParser'..." -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName "OzonRatingParser" -Confirm:$false
}

# --- Триггер: каждый день в 09:00 ---
$trigger = New-ScheduledTaskTrigger -Daily -At "09:00"

# --- Действие: запустить bat-файл ---
$action = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c `"$batFile`"" `
    -WorkingDirectory $workingDir

# --- Настройки задачи ---
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -DontStopOnIdleEnd `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30)

# StartWhenAvailable = если ПК был выключен в 09:00, задача запустится
# как только ПК включится (в течение суток после пропуска)

# --- Регистрируем задачу ---
Register-ScheduledTask `
    -TaskName $taskName `
    -Description $description `
    -Trigger $trigger `
    -Action $action `
    -Settings $settings `
    -RunLevel Highest

Write-Host ""
Write-Host "Задача '$taskName' успешно создана!" -ForegroundColor Green
Write-Host ""
Write-Host "  Расписание: каждый день в 09:00" -ForegroundColor Cyan
Write-Host "  Если ПК выключен: запустится при включении" -ForegroundColor Cyan
Write-Host "  Лог: $workingDir\ratings_parser.log" -ForegroundColor Cyan
Write-Host ""
Write-Host "Проверить:        Get-ScheduledTask -TaskName '$taskName'" -ForegroundColor Gray
Write-Host "Запустить вручную: Start-ScheduledTask -TaskName '$taskName'" -ForegroundColor Gray
Write-Host "Удалить:          Unregister-ScheduledTask -TaskName '$taskName' -Confirm:`$false" -ForegroundColor Gray
