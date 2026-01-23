@echo off
REM ============================================================================
REM 🚀 AUTO COMMIT & PUSH - Батник для запуска
REM ============================================================================

REM Разрешаем выполнение PowerShell скриптов
powershell -ExecutionPolicy Bypass -File "%~dp0auto_commit.ps1" -CommitMessage "Update: automatic commit"

pause
