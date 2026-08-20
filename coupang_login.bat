@echo off
cd /d "%~dp0"
echo ============================================================
echo  Coupang Ads login + collect (attach mode)
echo ============================================================
echo.
echo  Chrome will open. Log in via "Coupang WING", open the Ads
echo  REPORT page so the numbers show, then come back and press Enter.
echo.
"C:\Users\%USERNAME%\AppData\Local\Python\pythoncore-3.14-64\python.exe" -X utf8 "%~dp0coupang_crawler.py" --login --days 16
echo.
pause
