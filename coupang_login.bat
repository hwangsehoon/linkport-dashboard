@echo off
cd /d "%~dp0"
echo ============================================================
echo  Coupang Ads login (dedicated crawler profile)
echo ============================================================
echo.
echo  A Chrome window will open. Log in to Coupang Ads there.
echo  When done: close that Chrome window, then press any key here.
echo.
start "" "C:\Program Files\Google\Chrome\Application\chrome.exe" --user-data-dir="%~dp0coupang_profile" "https://advertising.coupang.com/"
pause >nul
echo  Login profile saved. You can run the crawler now.
timeout /t 2 >nul
