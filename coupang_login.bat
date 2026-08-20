@echo off
cd /d "%~dp0"
echo ============================================================
echo  Coupang Ads login (attach mode - keep Chrome OPEN)
echo ============================================================
echo.
echo  Cleaning up leftover coupang Chrome (so the debug port opens)...
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='chrome.exe'\" | Where-Object { $_.CommandLine -like '*coupang_profile*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"
timeout /t 1 >nul
echo.
echo  1) A Chrome window opens. Log in via "Coupang WING".
echo  2) Open the Ads REPORT page and make sure the numbers show.
echo  3) Keep this Chrome OPEN (do NOT close it).
echo  4) When "press any key" appears below, press it to collect.
echo.
start "" "C:\Program Files\Google\Chrome\Application\chrome.exe" --user-data-dir="%~dp0coupang_profile" --remote-debugging-port=9222 --window-position=60,60 --window-size=1280,900 "https://advertising.coupang.com/marketing-reporting/billboard/one-pager"
echo  Chrome launched with debugging port 9222.
echo  After login + report is visible, keep Chrome open and press any key...
pause >nul
echo  Collecting via attach...
"C:\Users\%USERNAME%\AppData\Local\Python\pythoncore-3.14-64\python.exe" -X utf8 "%~dp0coupang_crawler.py" --attach --days 14
echo.
echo  Done. You can close Chrome now.
timeout /t 3 >nul
