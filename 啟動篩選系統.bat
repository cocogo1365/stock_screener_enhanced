@echo off
chcp 65001 > nul
echo ========================================
echo 台股智能篩選系統 - 增強版
echo ========================================
echo.

cd /d "%~dp0"

echo 檢查 Python 安裝...
python --version >nul 2>&1
if errorlevel 1 (
    echo [錯誤] 找不到 Python，請先安裝 Python 3.8 以上版本
    pause
    exit /b 1
)

echo 安裝相依套件...
pip install -r requirements.txt

echo.
echo 啟動篩選系統...
python stock_screener_enhanced.py

pause
