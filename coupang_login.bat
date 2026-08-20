@echo off
cd /d "%~dp0"
echo ============================================================
echo  Coupang Ads login (attach mode - keep Chrome OPEN)
echo ============================================================
echo.
echo  기존 coupang 크롬 정리 중... (포트가 안 열리는 원인 방지)
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='chrome.exe'\" | Where-Object { $_.CommandLine -like '*coupang_profile*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"
timeout /t 1 >nul
echo.
echo  1) 크롬이 열립니다. "쿠팡 wing" 으로 로그인하세요.
echo  2) 광고 리포트 화면에서 숫자(광고비/매출)가 보이는지 확인.
echo  3) 이 크롬은 그대로 열어두세요 (닫지 마세요).
echo  4) 아래 "아무 키나" 뜨면 누르세요 - 자동으로 수집합니다.
echo.
start "" "C:\Program Files\Google\Chrome\Application\chrome.exe" --user-data-dir="%~dp0coupang_profile" --remote-debugging-port=9222 --window-position=60,60 --window-size=1280,900 "https://advertising.coupang.com/marketing-reporting/billboard/one-pager"
echo  크롬을 디버깅 포트 9222로 실행했습니다.
echo  로그인/리포트 확인 후, 크롬은 열어둔 채로 아무 키나 누르세요...
pause >nul
echo  수집(attach) 중...
"C:\Users\%USERNAME%\AppData\Local\Python\pythoncore-3.14-64\python.exe" -X utf8 "%~dp0coupang_crawler.py" --attach --days 14
echo.
echo  완료. 이제 크롬을 닫아도 됩니다.
timeout /t 3 >nul
