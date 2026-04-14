"""
통합 데이터 로더
- 로컬 DB에서 저장된 데이터를 먼저 불러옴
- 아직 조회하지 않은 날짜만 API에서 가져와서 DB에 저장
- 두 번째 실행부터는 빠름!
"""
import pandas as pd
import streamlit as st
from datetime import date, timedelta

from config import is_configured
from api.db import (
    get_missing_dates, mark_fetched,
    save_sales, save_ads,
    load_sales, load_ads,
)
from api.cafe24 import fetch_all_cafe24
from api.smartstore import fetch_smartstore
from api.coupang import fetch_coupang
from api.meta_ads import fetch_meta_ads
from api.naver_sa import fetch_naver_sa
from api.coupang_ads import fetch_coupang_ads


SALES_COLUMNS = ["날짜", "스토어", "채널", "주문건수", "매출", "객단가", "순방문자수", "전환율"]
ADS_COLUMNS = ["날짜", "광고채널", "광고비", "노출수", "클릭수", "전환수", "전환매출"]


def _empty_sales() -> pd.DataFrame:
    return pd.DataFrame(columns=SALES_COLUMNS)


def _empty_ads() -> pd.DataFrame:
    return pd.DataFrame(columns=ADS_COLUMNS)


def _ensure_numeric(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def _fetch_and_save_sales(service: str, fetch_fn, start_date: date, end_date: date, status: dict):
    """매출 API 호출 → DB 저장 (새 날짜만)"""
    if not is_configured(service):
        status[service] = "미설정"
        return

    missing = get_missing_dates(service, start_date, end_date)
    if not missing:
        status[service] = "DB 로드"
        return

    try:
        fetch_start = min(missing)
        fetch_end = max(missing)
        df = fetch_fn(fetch_start, fetch_end)
        if not df.empty:
            save_sales(df)
            # 데이터가 있는 날짜만 완료 처리
            fetched_dates = df["날짜"].unique().tolist()
            mark_fetched(service, fetched_dates)
            status[service] = f"연결됨 (+{len(fetched_dates)}일)"
        else:
            status[service] = "데이터 없음"
    except Exception as e:
        status[service] = f"실패: {e}"


def _fetch_and_save_ads(service: str, fetch_fn, start_date: date, end_date: date, status: dict):
    """광고 API 호출 → DB 저장 (새 날짜만)"""
    if not is_configured(service):
        status[service] = "미설정"
        return

    missing = get_missing_dates(service, start_date, end_date)
    if not missing:
        status[service] = "DB 로드"
        return

    try:
        fetch_start = min(missing)
        fetch_end = max(missing)
        df = fetch_fn(fetch_start, fetch_end)
        if not df.empty:
            save_ads(df)
            fetched_dates = df["날짜"].unique().tolist()
            mark_fetched(service, fetched_dates)
            status[service] = f"연결됨 (+{len(fetched_dates)}일)"
        else:
            status[service] = "데이터 없음"
    except Exception as e:
        status[service] = f"실패: {e}"


@st.cache_data(ttl=3600)
def load_data(start_date: date, end_date: date) -> tuple:
    """
    데이터 로드:
    1. 아직 조회 안 한 날짜만 API에서 가져와서 DB에 저장
    2. DB에서 전체 기간 데이터 불러오기
    """
    status = {}

    # ── 새 데이터 API 조회 + DB 저장 (진행 상태 표시) ──
    progress = st.progress(0, text="데이터 로딩 중...")
    steps = [
        ("cafe24", "카페24", "sales", fetch_all_cafe24),
        ("smartstore", "스마트스토어", "sales", fetch_smartstore),
        ("coupang", "쿠팡", "sales", fetch_coupang),
        ("meta", "Meta 광고", "ads", fetch_meta_ads),
        ("naver_sa", "네이버 검색광고", "ads", fetch_naver_sa),
        ("coupang_ads", "쿠팡 광고", "ads", fetch_coupang_ads),
    ]
    for i, (service, label, data_type, fetch_fn) in enumerate(steps):
        progress.progress((i) / len(steps), text=f"{label} 로딩 중...")
        if data_type == "sales":
            _fetch_and_save_sales(service, fetch_fn, start_date, end_date, status)
        else:
            _fetch_and_save_ads(service, fetch_fn, start_date, end_date, status)
    progress.progress(1.0, text="로딩 완료!")
    progress.empty()

    # ── DB에서 전체 데이터 불러오기 ──
    df_sales = load_sales(start_date, end_date)
    df_ads = load_ads(start_date, end_date)

    # 타입 보정
    if not df_sales.empty:
        df_sales = _ensure_numeric(df_sales, ["주문건수", "매출", "객단가", "순방문자수", "전환율"])
    else:
        df_sales = _empty_sales()

    if not df_ads.empty:
        df_ads = _ensure_numeric(df_ads, ["광고비", "노출수", "클릭수", "전환수", "전환매출"])
    else:
        df_ads = _empty_ads()

    # 연결 상태 저장
    st.session_state["api_status"] = status

    return df_sales, df_ads


def get_api_status() -> dict:
    """현재 API 연결 상태 반환"""
    return st.session_state.get("api_status", {})
