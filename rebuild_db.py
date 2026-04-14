"""
DB 완전 재구축 스크립트
- 기존 데이터 전부 삭제
- 모든 소스에서 정확한 기간/브랜드로 재수집
- 각 단계마다 검증
"""
import sqlite3
import time
from datetime import date, timedelta

DB_PATH = "dashboard_data.db"


def reset_db():
    """DB 완전 초기화"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM sales")
    conn.execute("DELETE FROM ads")
    conn.execute("DELETE FROM fetch_log")
    conn.commit()
    conn.close()
    print("=== DB 초기화 완료 ===\n")


def verify_sales(label, expected_min=0):
    """매출 데이터 검증"""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT 스토어, 브랜드, COUNT(DISTINCT 날짜), SUM(매출), SUM(주문건수) FROM sales GROUP BY 스토어, 브랜드"
    ).fetchall()
    conn.close()

    print(f"\n[검증] {label}")
    total = 0
    for store, brand, days, rev, cnt in rows:
        rev = int(rev or 0)
        cnt = int(cnt or 0)
        total += rev
        print(f"  {store} ({brand or '미분류'}): {days}일, {rev:,}원 ({cnt}건)")
    print(f"  → 매출 합계: {total:,}원")
    if total < expected_min:
        print(f"  ⚠️ 예상보다 적음! (최소 {expected_min:,}원 예상)")
    return total


def verify_ads(label):
    """광고 데이터 검증"""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT 광고채널, 브랜드, COUNT(DISTINCT 날짜), SUM(광고비) FROM ads GROUP BY 광고채널, 브랜드"
    ).fetchall()
    conn.close()

    print(f"\n[검증] {label}")
    total = 0
    for ch, brand, days, cost in rows:
        cost = int(cost or 0)
        total += cost
        print(f"  {ch} ({brand or '미분류'}): {days}일, {cost:,}원")
    print(f"  → 광고비 합계: {total:,}원")
    return total


def collect_cafe24():
    """카페24 3개 스토어 수집 (CA API - 방문자수 포함)"""
    from api.cafe24 import fetch_all_cafe24
    from api.db import save_sales, mark_fetched

    # CA API는 2023년부터 데이터 제공
    periods = [
        (date(2023, 1, 1), date(2023, 6, 30)),
        (date(2023, 7, 1), date(2023, 12, 31)),
        (date(2024, 1, 1), date(2024, 6, 30)),
        (date(2024, 7, 1), date(2024, 12, 31)),
        (date(2025, 1, 1), date(2025, 6, 30)),
        (date(2025, 7, 1), date(2025, 12, 31)),
        (date(2026, 1, 1), date.today()),
    ]

    print("=== 카페24 수집 시작 ===")
    for start, end in periods:
        print(f"  {start} ~ {end}...", end=" ", flush=True)
        try:
            df = fetch_all_cafe24(start, end)
            if not df.empty:
                save_sales(df)
                fetched = df["날짜"].unique().tolist()
                mark_fetched("cafe24", fetched)
                print(f"OK ({len(df)}건)")
            else:
                print("데이터 없음")
        except Exception as e:
            print(f"에러: {e}")
        time.sleep(2)

    verify_sales("카페24")


def collect_smartstore():
    """스마트스토어 수집"""
    from api.smartstore import fetch_smartstore
    from api.db import save_sales, mark_fetched

    # 6개월 단위로 수집
    periods = [
        (date(2023, 1, 1), date(2023, 6, 30)),
        (date(2023, 7, 1), date(2023, 12, 31)),
        (date(2024, 1, 1), date(2024, 6, 30)),
        (date(2024, 7, 1), date(2024, 12, 31)),
        (date(2025, 1, 1), date(2025, 6, 30)),
        (date(2025, 7, 1), date(2025, 12, 31)),
        (date(2026, 1, 1), date.today()),
    ]

    print("\n=== 스마트스토어 수집 시작 ===")
    for start, end in periods:
        print(f"  {start} ~ {end}...", end=" ", flush=True)
        try:
            df = fetch_smartstore(start, end)
            if not df.empty:
                save_sales(df)
                fetched = df["날짜"].unique().tolist()
                mark_fetched("smartstore", fetched)
                print(f"OK ({df['주문건수'].sum()}건)")
            else:
                print("데이터 없음")
        except Exception as e:
            print(f"에러: {e}")
        time.sleep(2)

    verify_sales("스마트스토어")


def collect_coupang():
    """쿠팡 수집 (로켓그로스 + 마켓플레이스, 브랜드 분류)"""
    from api.coupang import fetch_coupang
    from api.db import save_sales, mark_fetched

    # 3개월 단위
    periods = [
        (date(2024, 8, 1), date(2024, 10, 31)),
        (date(2024, 11, 1), date(2025, 1, 31)),
        (date(2025, 2, 1), date(2025, 4, 30)),
        (date(2025, 5, 1), date(2025, 7, 31)),
        (date(2025, 8, 1), date(2025, 10, 31)),
        (date(2025, 11, 1), date(2026, 1, 31)),
        (date(2026, 2, 1), date.today()),
    ]

    print("\n=== 쿠팡 수집 시작 ===")
    for start, end in periods:
        print(f"  {start} ~ {end}...", end=" ", flush=True)
        try:
            df = fetch_coupang(start, end)
            if not df.empty:
                save_sales(df)
                fetched = df["날짜"].unique().tolist()
                mark_fetched("coupang", fetched)
                brands = ", ".join(f"{b}:{int(r):,}" for b, r in df.groupby("브랜드")["매출"].sum().items())
                print(f"OK ({df['주문건수'].sum()}건) [{brands}]")
            else:
                print("데이터 없음")
        except Exception as e:
            print(f"에러: {e}")
        time.sleep(10)

    verify_sales("쿠팡")


def collect_meta():
    """Meta 광고 수집 (캠페인별 브랜드 분류)"""
    from api.meta_ads import fetch_meta_ads
    from api.db import save_ads, mark_fetched

    # 6개월 단위
    periods = [
        (date(2023, 3, 1), date(2023, 8, 31)),
        (date(2023, 9, 1), date(2024, 2, 29)),
        (date(2024, 3, 1), date(2024, 8, 31)),
        (date(2024, 9, 1), date(2025, 2, 28)),
        (date(2025, 3, 1), date(2025, 8, 31)),
        (date(2025, 9, 1), date(2026, 2, 28)),
        (date(2026, 3, 1), date.today()),
    ]

    print("\n=== Meta 광고 수집 시작 ===")
    for start, end in periods:
        print(f"  {start} ~ {end}...", end=" ", flush=True)
        try:
            df = fetch_meta_ads(start, end)
            if not df.empty:
                save_ads(df)
                fetched = df["날짜"].unique().tolist()
                mark_fetched("meta", fetched)
                print(f"OK ({len(df)}건)")
            else:
                print("데이터 없음")
        except Exception as e:
            print(f"에러: {e}")
        time.sleep(2)

    verify_ads("Meta 광고")


def collect_naver_sa():
    """네이버SA 수집 (캠페인별 브랜드 분류 + 전환 리포트)"""
    from api.naver_sa import fetch_naver_sa
    from api.db import save_ads, mark_fetched

    # 하루씩 리포트 생성 필요 → 느림
    start = date(2025, 4, 1)
    end = date.today()

    print(f"\n=== 네이버SA 수집 시작 ({start} ~ {end}) ===")
    current = start
    while current <= end:
        try:
            df = fetch_naver_sa(current, current)
            if not df.empty:
                save_ads(df)
                mark_fetched("naver_sa", [current])
            current += timedelta(days=1)
            time.sleep(1)
        except Exception as e:
            print(f"  {current} 에러: {e}")
            current += timedelta(days=1)

    verify_ads("네이버SA")


def final_verify():
    """최종 검증"""
    print("\n" + "=" * 60)
    print("=== 최종 검증 ===")
    print("=" * 60)
    verify_sales("전체 매출")
    verify_ads("전체 광고")

    # 월별 요약
    conn = sqlite3.connect(DB_PATH)
    print("\n[월별 매출 요약]")
    rows = conn.execute("""
        SELECT substr(날짜, 1, 7) as 월, SUM(매출)
        FROM sales
        GROUP BY substr(날짜, 1, 7)
        ORDER BY 월 DESC
        LIMIT 12
    """).fetchall()
    for month, rev in rows:
        print(f"  {month}: {int(rev):,}원")

    print("\n[월별 광고비 요약]")
    rows2 = conn.execute("""
        SELECT substr(날짜, 1, 7) as 월, SUM(광고비)
        FROM ads
        GROUP BY substr(날짜, 1, 7)
        ORDER BY 월 DESC
        LIMIT 12
    """).fetchall()
    for month, cost in rows2:
        print(f"  {month}: {int(cost):,}원")

    conn.close()
    print("\n=== 재구축 완료! ===")


if __name__ == "__main__":
    reset_db()

    # 매출 수집
    collect_cafe24()
    collect_smartstore()
    collect_coupang()

    # 광고 수집
    collect_meta()
    collect_naver_sa()

    # 최종 검증
    final_verify()
