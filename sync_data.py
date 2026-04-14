"""
데이터 동기화 스크립트
- 최근 7일 데이터를 API에서 가져와 DB에 저장
- 대시보드와 별도로 실행 (스케줄러 또는 수동)
- start.bat에서 대시보드 시작 전에 자동 실행
"""
from datetime import date, timedelta
from config import is_configured
from api.db import get_missing_dates, mark_fetched, save_sales, save_ads
from api.token_manager import check_and_refresh_all


def sync_recent(days=7):
    """최근 N일 데이터 동기화"""
    end = date.today()
    start = end - timedelta(days=days)

    print(f"데이터 동기화: {start} ~ {end}")
    check_and_refresh_all()

    # 매출
    if is_configured("cafe24"):
        missing = get_missing_dates("cafe24", start, end)
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
        missing = get_missing_dates("smartstore", start, end)
        if missing:
            print(f"  스마트스토어: {len(missing)}일 수집 중...")
            try:
                from api.smartstore import fetch_smartstore
                df = fetch_smartstore(min(missing), max(missing))
                if not df.empty:
                    save_sales(df)
                    mark_fetched("smartstore", df["날짜"].unique().tolist())
                    print(f"  스마트스토어: {len(df)}건 저장")
            except Exception as e:
                print(f"  스마트스토어 실패: {e}")

    if is_configured("coupang"):
        missing = get_missing_dates("coupang", start, end)
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
        missing = get_missing_dates("meta", start, end)
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
        missing = get_missing_dates("naver_sa", start, end)
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

    print("동기화 완료!")


if __name__ == "__main__":
    sync_recent(7)
