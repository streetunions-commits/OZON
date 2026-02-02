@echo off
REM ============================================================================
REM ЗАПУСК ПАРСЕРА РЕЙТИНГОВ OZON
REM ============================================================================
REM Запускается автоматически через Планировщик Windows (ежедневно в 09:00 MSK)
REM Если ПК был выключен — запустится при включении
REM Логи пишутся в ratings_parser.log
REM ============================================================================

chcp 65001 >nul
cd /d "C:\Users\stree\Documents\GIT_OZON"

echo [%date% %time%] Запуск парсера рейтингов... >> ratings_parser.log
python update_ratings_local.py >> ratings_parser.log 2>&1
echo [%date% %time%] Парсер завершён (код: %ERRORLEVEL%) >> ratings_parser.log
