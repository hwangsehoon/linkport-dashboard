"""2026 전체 데이터 백필
- 2026-01-01부터 어제까지 모든 API를 1일씩 강제 호출
- 기존 데이터 덮어씀 (fetch_log 무시)
- 실패한 일자는 스킵 후 마지막에 출력
- 매 호출 후 1초 대기 (rate limit 보호)
"""
import sys
import time
import sqlite3
from datetime import date, timedelta

from config import is_configured
from api.db import save_sales, save_ads, mark_fetched
from api.token_manager import check_and_refresh_all

DB_PATH = "dashboard_data.db"
START = date(2026, 1, 1)
END = date.today() - timedelta(days=1)  # 어제까지

# 서비스별 fetch 함수 (lazy import)
SERVICES = [
    ("cafe24", "매출", "api.cafe24", "fetch_all_cafe24"),
    ("smartstore", "매출", "api.smartstore", "fetch_smartstore"),
    ("coupang", "매출", "api.coupang", "fetch_coupang"),
    ("meta", "광고", "api.meta_ads", "fetch_meta_ads"),
    ("naver_sa", "광고", "api.naver_sa", "fetch_naver_sa"),
    ("coupang_ads", "광고", "api.coupang_ads", "fetch_coupang_ads"),
]


def import_fn(module, fname):
    mod = __import__(module, fromlist=[fname])
    return getattr(mod, fname)


def clear_fetch_log_2026():
    """2026 이후 fetch_log 삭제 (재수집 위해)"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM fetch_log WHERE 날짜 >= '2026-01-01'")
    conn.commit()
    conn.close()


def daterange(start, end):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def backfill_service(service_name, kind, module, fname):
    if not is_configured(service_name):
        print(f"\n[{service_name}] 설정 안됨 - 스킵")
        return
    print(f"\n[{service_name}] {kind} 백필 시작 ({START} ~ {END})")
    fetch = import_fn(module, fname)
    save = save_sales if kind == "매출" else save_ads
    failed = []
    success = 0
    total = (END - START).days + 1
    for i, d in enumerate(daterange(START, END), 1):
        try:
            df = fetch(d, d)
            if not df.empty:
                save(df)
                mark_fetched(service_name, [d])
                success += 1
            print(f"  [{i:3d}/{total}] {d}: {len(df)}건", flush=True)
        except Exception as e:
            failed.append((d, str(e)[:60]))
            print(f"  [{i:3d}/{total}] {d}: 실패 ({str(e)[:60]})", flush=True)
        time.sleep(0.3)  # rate limit
    print(f"  → 성공 {success}/{total}, 실패 {len(failed)}")
    if failed:
        print(f"  실패 일자:")
        for d, err in failed[:10]:
            print(f"    {d}: {err}")


def main():
    services_filter = sys.argv[1:] if len(sys.argv) > 1 else None
    print(f"=== 2026 백필: {START} ~ {END} ===")
    if services_filter:
        print(f"  대상 서비스: {services_filter}")
    print("토큰 갱신 중...")
    check_and_refresh_all()
    clear_fetch_log_2026()
    for sv_name, kind, module, fname in SERVICES:
        if services_filter and sv_name not in services_filter:
            continue
        try:
            backfill_service(sv_name, kind, module, fname)
        except Exception as e:
            print(f"  [{sv_name}] 치명적 에러: {e}")
    print("\n=== 백필 완료 ===")
    print("\n월별 검증:")
    conn = sqlite3.connect(DB_PATH)
    print("  매출:")
    for row in conn.execute(
        "SELECT substr(날짜,1,7) m, SUM(매출) FROM sales "
        "WHERE 날짜 >= '2026-01-01' GROUP BY m ORDER BY m"
    ):
        print(f"    {row[0]}: {row[1]:,}")
    print("  광고비:")
    for row in conn.execute(
        "SELECT substr(날짜,1,7) m, 광고채널, SUM(광고비) FROM ads "
        "WHERE 날짜 >= '2026-01-01' GROUP BY m, 광고채널 ORDER BY m, 광고채널"
    ):
        print(f"    {row[0]} {row[1]}: {row[2]:,}")
    conn.close()


if __name__ == "__main__":
    main()
