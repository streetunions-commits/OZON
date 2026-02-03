@echo off
REM ============================================================================
REM ZAPUSK PARSERA REYTINGOV OZON
REM ============================================================================
REM Zapuskaetsya avtomaticheski cherez Planirovshchik Windows (ezhednevno v 09:00 MSK)
REM Esli PK byl vyklyuchen - zapustitsya pri vklyuchenii
REM Logi pishutsya v ratings_parser.log
REM ============================================================================

cd /d "C:\Users\stree\Documents\GIT_OZON"

set PYTHON_EXE=C:\Users\stree\AppData\Local\Python\pythoncore-3.14-64\python.exe

echo [%date% %time%] Zapusk parsera reytingov... >> ratings_parser.log
"%PYTHON_EXE%" update_ratings_local.py >> ratings_parser.log 2>&1
echo [%date% %time%] Parser zavershen (kod: %ERRORLEVEL%) >> ratings_parser.log
