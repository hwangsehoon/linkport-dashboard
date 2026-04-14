@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo [Sync] Loading new data...
python sync_data.py
echo.
echo [Dashboard] Starting...
python -m streamlit run demo.py --server.port 9000
pause
