"""
과거 데이터 수집 스크립트
- 대시보드와 별도로 실행
- 각 API에서 과거 데이터를 가져와 DB에 저장
- 중간에 중단해도 이미 저장된 데이터는 유지됨
"""
import time
from datetime import date, timedelta

from config import is_configured
from api.db import get_missing_dates, mark_fetched, save_sales, save_ads
from api.cafe24 import fetch_all_cafe24
from api.smartstore import fetch_smartstore
from api.coupang import fetch_coupang
from api.meta_ads import fetch_meta_ads
from api.naver_sa import fetch_naver_sa


START_DATE = date(2020, 1, 1)  # 수집 시작일 (최대한 과거부터)
END_DATE = date.today() - timedelta(days=1)  # 어제까지


def fetch_in_chunks(service, fetch_fn, save_fn, data_type, chunk_days=7):
    """chunk_days 단위로 나눠서 조회 + 저장"""
    missing = get_missing_dates(service, START_DATE, END_DATE)
    if not missing:
        print(f"  [{service}] 이미 수집 완료")
        return

    print(f"  [{service}] {len(missing)}일 미수집 → 수집 시작")

    current_start = min(missing)
    while current_start <= max(missing):
        current_end = min(current_start + timedelta(days=chunk_days - 1), END_DATE)

        try:
            df = fetch_fn(current_start, current_end)
            if not df.empty:
                save_fn(df)
                fetched_dates = df["날짜"].unique().tolist()
                mark_fetched(service, fetched_dates)
                total_rows = len(df)
                print(f"  [{service}] {current_start} ~ {current_end}: {total_rows}건 저장")
            else:
                print(f"  [{service}] {current_start} ~ {current_end}: 데이터 없음")
        except Exception as e:
            print(f"  [{service}] {current_start} ~ {current_end}: 에러 - {e}")

        current_start = current_end + timedelta(days=1)
        time.sleep(1)


def main():
    print(f"=== 과거 데이터 수집 ({START_DATE} ~ {END_DATE}) ===\n")

    # 매출 데이터
    if is_configured("cafe24"):
        print("[카페24]")
        fetch_in_chunks("cafe24", fetch_all_cafe24, save_sales, "sales")

    if is_configured("smartstore"):
        print("[스마트스토어]")
        fetch_in_chunks("smartstore", fetch_smartstore, save_sales, "sales", chunk_days=1)

    if is_configured("coupang"):
        print("[쿠팡]")
        fetch_in_chunks("coupang", fetch_coupang, save_sales, "sales", chunk_days=30)

    # 광고 데이터
    if is_configured("meta"):
        print("[Meta 광고]")
        fetch_in_chunks("meta", fetch_meta_ads, save_ads, "ads", chunk_days=30)

    if is_configured("naver_sa"):
        print("[네이버 검색광고]")
        fetch_in_chunks("naver_sa", fetch_naver_sa, save_ads, "ads", chunk_days=1)

    print("\n=== 수집 완료 ===")


if __name__ == "__main__":
    main()
