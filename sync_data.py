"""
데이터 동기화 스크립트
- 최근 7일 데이터를 API에서 가져와 DB에 저장
- 대시보드와 별도로 실행 (스케줄러 또는 수동)
- start.bat에서 대시보드 시작 전에 자동 실행
"""
import os
import sys
import io
import time
from pathlib import Path
from datetime import date, timedelta

# 콘솔이 cp949면 메시지의 '—' 같은 문자에서 UnicodeEncodeError로 죽는다.
# (pythonw로 돌 땐 stdout이 없으므로 None 체크)
if sys.stdout is not None:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr is not None:
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from config import is_configured
from api.db import get_missing_dates, mark_fetched, save_sales, save_ads
from api.token_manager import check_and_refresh_all

# 쿠팡 광고 크롤러는 Chrome을 띄우므로 자주 돌리면 부담 → 최소 간격(분)
COUPANG_ADS_MIN_INTERVAL_MIN = 60


def _set_status(source: str, state: str, message: str = ""):
    """수집 상태를 DB에 남긴다. 대시보드가 이걸 읽어 '쿠팡 세션 만료' 같은 걸 바로 보여준다.
    (지금까지는 실패가 로그에만 남아, 며칠 멈춰도 눈치채기 어려웠다)"""
    try:
        from api.db import _get_conn
        c = _get_conn(); cur = c.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS sync_status (
            소스 TEXT PRIMARY KEY, 상태 TEXT, 메시지 TEXT, 갱신시각 TIMESTAMPTZ DEFAULT now())""")
        cur.execute("""INSERT INTO sync_status (소스,상태,메시지,갱신시각)
                       VALUES (%s,%s,%s,now())
                       ON CONFLICT (소스) DO UPDATE SET
                       상태=EXCLUDED.상태, 메시지=EXCLUDED.메시지, 갱신시각=now()""",
                    (source, state, message))
        c.commit(); c.close()
    except Exception:
        pass   # 상태 기록 실패가 동기화를 막으면 안 된다


def _sync_coupang_ads(days):
    """쿠팡 광고 크롤러 실행 (로컬·세션 있을 때만 — CI/세션없음은 자동 스킵).
    5분 동기화에 매번 Chrome을 띄우지 않도록 1시간에 한 번만 실행한다."""
    if os.getenv("GITHUB_ACTIONS"):
        return  # CI에는 브라우저/로그인 세션 없음
    profile = Path(__file__).parent / "coupang_profile"
    if not profile.exists():
        print("  쿠팡 광고: 프로필 없음 — 스킵 (최초 1회 `python coupang_crawler.py`로 로그인)")
        return
    marker = Path(__file__).parent / ".coupang_ads_last"
    if marker.exists() and (time.time() - marker.stat().st_mtime) < COUPANG_ADS_MIN_INTERVAL_MIN * 60:
        print(f"  쿠팡 광고: 최근 실행됨 — 스킵 ({COUPANG_ADS_MIN_INTERVAL_MIN}분 주기)")
        return
    try:
        marker.write_text(str(time.time()))  # 시도 시각 기록 (성공·실패 무관, 주기 유지)
    except Exception:
        pass
    try:
        from coupang_crawler import crawl, SessionExpired
        print("  쿠팡 광고: 크롤러 실행 중...")
        try:
            crawl(days=min(days, 7), auto=True)  # 창 숨김, 만료 시 조용히 스킵
            _set_status("쿠팡광고", "ok", "")
        except SessionExpired as e:
            print(f"  쿠팡 광고: {e}")
            _set_status("쿠팡광고", "session_expired",
                        "쿠팡 로그인 세션 만료 — coupang_login.bat 실행 후 재로그인 필요")
    except Exception as e:
        print(f"  쿠팡 광고 실패: {e}")
        _set_status("쿠팡광고", "error", str(e)[:200])


# 반품 집계는 무거우므로(특히 스마트스토어 일자순회) 12시간에 한 번만
RETURNS_MIN_INTERVAL_MIN = 60 * 12


def _sync_returns():
    """월별 반품 집계 — 현재 연도 최근 2개월 재집계(반품은 며칠 뒤 확정되므로).
    쿠팡/스마트스토어 반품 API가 IP 제한이라 로컬에서만 실행."""
    if os.getenv("GITHUB_ACTIONS"):
        return  # 쿠팡·스마트스토어 IP 차단 — 로컬만
    marker = Path(__file__).parent / ".returns_last"
    if marker.exists() and (time.time() - marker.stat().st_mtime) < RETURNS_MIN_INTERVAL_MIN * 60:
        return
    try:
        marker.write_text(str(time.time()))
    except Exception:
        pass
    try:
        from api.returns import collect_returns, save_monthly_returns
        today = date.today()
        months = sorted({max(1, today.month - 1), today.month})  # 최근 2개월
        print(f"  반품 집계: {today.year}년 {months} 재집계 중...")
        df = collect_returns(today.year, months, include_smartstore=True)
        save_monthly_returns(df)
        print(f"  반품 집계: {len(df)}행 저장")
    except Exception as e:
        print(f"  반품 집계 실패: {e}")


def _dates_to_fetch(service, start, end, force_recent_days=7):
    """fetch_log에 있어도 최근 N일은 항상 재수집 (당일 부분 데이터 갱신/일시 누락 자가복구용).
    동기화 당시 API가 빈 응답을 줬는데 '수집됨'으로 기록돼 누락이 굳는 것을 막기 위해
    최근 1주일은 매번 다시 받아온다."""
    missing = set(get_missing_dates(service, start, end))
    force_from = max(start, date.today() - timedelta(days=force_recent_days))
    cur = force_from
    while cur <= end:
        missing.add(cur)
        cur += timedelta(days=1)
    return sorted(missing)


def sync_recent(days=7):
    """최근 N일 데이터 동기화"""
    end = date.today()
    start = end - timedelta(days=days)

    print(f"데이터 동기화: {start} ~ {end}")
    check_and_refresh_all()

    # 매출
    if is_configured("cafe24"):
        missing = _dates_to_fetch("cafe24", start, end)
        if missing:
            print(f"  카페24: {len(missing)}일 수집 중...")
            try:
                from api.cafe24 import fetch_all_cafe24
                df = fetch_all_cafe24(min(missing), max(missing))
                if not df.empty:
                    save_sales(df)
                    mark_fetched("cafe24", df["날짜"].unique().tolist())
                    print(f"  카페24: {len(df)}건 저장")
            except Exception as e:
                print(f"  카페24 실패: {e}")

    if is_configured("smartstore"):
        missing = _dates_to_fetch("smartstore", start, end)
        if missing:
            print(f"  스마트스토어: {len(missing)}일 수집 중...")
            try:
                from api.smartstore import fetch_smartstore
                df = fetch_smartstore(min(missing), max(missing))
                if not df.empty:
                    save_sales(df)
                    mark_fetched("smartstore", df["날짜"].unique().tolist())
                    print(f"  스마트스토어: {len(df)}건 저장")
                    _set_status("스마트스토어", "ok", "")
                else:
                    # fetch_smartstore는 IP 차단 시 메시지만 찍고 빈 값을 준다 → 여기서 상태로 남긴다
                    _set_status("스마트스토어", "no_data",
                                "수집 결과 없음 — 커머스API센터 허용 IP 등록 확인 필요")
            except Exception as e:
                print(f"  스마트스토어 실패: {e}")
                _set_status("스마트스토어", "error", str(e)[:200])

    if is_configured("coupang"):
        missing = _dates_to_fetch("coupang", start, end)
        if missing:
            print(f"  쿠팡: {len(missing)}일 수집 중...")
            try:
                from api.coupang import fetch_coupang
                df = fetch_coupang(min(missing), max(missing))
                if not df.empty:
                    save_sales(df)
                    mark_fetched("coupang", df["날짜"].unique().tolist())
                    print(f"  쿠팡: {len(df)}건 저장")
            except Exception as e:
                print(f"  쿠팡 실패: {e}")

    # 광고
    if is_configured("meta"):
        missing = _dates_to_fetch("meta", start, end)
        if missing:
            print(f"  Meta 광고: {len(missing)}일 수집 중...")
            try:
                from api.meta_ads import fetch_meta_ads
                df = fetch_meta_ads(min(missing), max(missing))
                if not df.empty:
                    save_ads(df)
                    mark_fetched("meta", df["날짜"].unique().tolist())
                    print(f"  Meta: {len(df)}건 저장")
            except Exception as e:
                print(f"  Meta 실패: {e}")

    if is_configured("naver_sa"):
        missing = _dates_to_fetch("naver_sa", start, end)
        if missing:
            print(f"  네이버SA: {len(missing)}일 수집 중...")
            try:
                from api.naver_sa import fetch_naver_sa
                df = fetch_naver_sa(min(missing), max(missing))
                if not df.empty:
                    save_ads(df)
                    mark_fetched("naver_sa", df["날짜"].unique().tolist())
                    print(f"  네이버SA: {len(df)}건 저장")
            except Exception as e:
                print(f"  네이버SA 실패: {e}")

    # 쿠팡 광고 (브라우저 크롤러 — 로컬에서만 실행됨)
    _sync_coupang_ads(days)

    # 월별 반품 집계 (12시간 주기 — 로컬에서만)
    _sync_returns()

    print("동기화 완료!")


if __name__ == "__main__":
    import sys
    from datetime import datetime
    # 스케줄러(pythonw)는 콘솔이 없어 stdout이 None → 로그파일로 출력
    if sys.stdout is None:
        _log = open(Path(__file__).parent / "sync_auto.log", "a", encoding="utf-8")
        sys.stdout = sys.stderr = _log
    print(f"\n===== {datetime.now():%Y-%m-%d %H:%M:%S} 동기화 시작 =====")
    sync_recent(7)
    try:
        sys.stdout.flush()
    except Exception:
        pass
