import os
import db_compat as sqlite3
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta, date, timezone
import random

# KST 기준 오늘 날짜 (Streamlit Cloud는 UTC)
_KST = timezone(timedelta(hours=9))


def _today_kst():
    return datetime.now(_KST).date()
import calendar
import streamlit.components.v1 as components

from config import is_configured
from api.db import load_sales, load_ads

st.set_page_config(
    page_title="LINKPORT Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════
# CSS - Claude Design Tone
# ══════════════════════════════════════════════
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;600;700&display=swap');

    /* 전체 배경 - 클로드 크림 */
    .stApp, [data-testid="stAppViewContainer"] {
        background-color: #FAF9F6 !important;
    }
    section[data-testid="stSidebar"] > div:first-child {
        background: #2D2B28 !important;
    }
    section[data-testid="stSidebar"] * {
        color: #E8E4DE !important;
    }
    section[data-testid="stSidebar"] [data-testid="stMarkdown"] p {
        color: #E8E4DE !important;
    }
    section[data-testid="stSidebar"] .stRadio label span {
        color: #E8E4DE !important;
    }
    section[data-testid="stSidebar"] .stRadio label[data-checked="true"] span {
        color: #FFFFFF !important;
        font-weight: 700 !important;
    }
    /* 사이드바 메뉴 카드형 */
    section[data-testid="stSidebar"] .stRadio {
        padding: 0 8px !important;
    }
    section[data-testid="stSidebar"] .stRadio > div {
        gap: 6px !important;
    }
    section[data-testid="stSidebar"] .stRadio > div > label {
        display: flex !important;
        align-items: center !important;
        justify-content: flex-start !important;
        background: #3D3B38 !important;
        border: 1px solid #4A4745 !important;
        border-radius: 10px !important;
        padding: 12px 18px !important;
        margin: 0 !important;
        width: 100% !important;
        height: 46px !important;
        transition: all 0.18s ease !important;
        cursor: pointer !important;
        box-shadow: 0 1px 2px rgba(0,0,0,0.15);
        box-sizing: border-box !important;
    }
    section[data-testid="stSidebar"] .stRadio > div > label > div:last-child {
        flex: 1 !important;
        text-align: left !important;
    }
    section[data-testid="stSidebar"] .stRadio > div > label:hover {
        background: #4A4745 !important;
        transform: translateX(3px);
        box-shadow: 0 4px 10px rgba(0,0,0,0.25);
    }
    section[data-testid="stSidebar"] .stRadio > div > label[data-checked="true"] {
        background: linear-gradient(135deg, #D97757 0%, #C4694D 100%) !important;
        border-color: #FFB088 !important;
        border-width: 1px !important;
        border-left: 4px solid #FFFFFF !important;
        box-shadow: 0 6px 20px rgba(217,119,87,0.55), inset 0 0 0 1px rgba(255,255,255,0.15);
        transform: translateX(4px);
        padding-left: 14px !important;
    }
    section[data-testid="stSidebar"] .stRadio > div > label[data-checked="true"]::after {
        content: "▸";
        margin-left: auto;
        font-size: 1.1rem;
        color: #FFFFFF;
        font-weight: 700;
    }
    section[data-testid="stSidebar"] .stRadio > div > label[data-checked="true"] span {
        font-size: 1rem !important;
        font-weight: 700 !important;
        letter-spacing: 0.02em;
    }
    /* 라디오 동그라미 점 숨김 */
    section[data-testid="stSidebar"] .stRadio > div > label > div:first-child {
        display: none !important;
    }
    section[data-testid="stSidebar"] .stRadio > div > label span {
        font-size: 0.95rem !important;
        font-weight: 500 !important;
    }
    section[data-testid="stSidebar"] hr {
        border-color: #4A4745 !important;
    }
    section[data-testid="stSidebar"] .stCaption p {
        color: #8C8680 !important;
    }

    /* 폰트 */
    html, body, [class*="css"], [data-testid="stMarkdown"],
    [data-testid="stMetric"], input, button, select, textarea,
    h1, h2, h3, h4, h5, h6, p, div, label {
        font-family: 'Noto Sans KR', sans-serif !important;
    }
    span:not([data-testid="stIconMaterial"]) {
        font-family: 'Noto Sans KR', sans-serif !important;
    }

    /* 메트릭 카드 (기본) - 다른 페이지용 */
    [data-testid="stMetric"] {
        background: #FFFFFF;
        border: 1px solid #E8E4DE;
        border-radius: 12px;
        padding: 18px 22px;
        box-shadow: 0 1px 3px rgba(45,43,40,0.06);
        min-height: 100px;
        transition: box-shadow 0.2s;
    }
    [data-testid="stMetric"]:hover { box-shadow: 0 4px 12px rgba(45,43,40,0.1); }
    [data-testid="stMetricLabel"] { font-size: 0.78rem; color: #8C8680 !important; font-weight: 500; letter-spacing: 0.02em; }
    [data-testid="stMetricValue"] { font-size: 1.35rem; color: #2D2B28 !important; font-weight: 700; }
    [data-testid="stMetricDelta"] { font-size: 0.72rem; }
    [data-testid="stMetricDelta"] svg { width: 0.7rem; height: 0.7rem; }

    /* 신규 KPI 카드 */
    .kpi-card {
        background: #FFFFFF;
        border: 1px solid #E8E4DE;
        border-radius: 14px;
        padding: 18px 20px 14px 20px;
        box-shadow: 0 1px 3px rgba(45,43,40,0.05);
        min-height: 138px;
        transition: all 0.2s ease;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    .kpi-card:hover {
        box-shadow: 0 6px 18px rgba(45,43,40,0.1);
        transform: translateY(-2px);
    }
    .kpi-label {
        font-size: 0.78rem;
        color: #8C8680;
        font-weight: 500;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        margin-bottom: 6px;
    }
    .kpi-value {
        font-size: 1.55rem;
        color: #2D2B28;
        font-weight: 700;
        line-height: 1.2;
        margin-bottom: 4px;
    }
    .kpi-delta {
        font-size: 0.78rem;
        font-weight: 600;
        display: flex;
        align-items: center;
        gap: 4px;
    }
    .kpi-delta-pos { color: #4A8C5F; }
    .kpi-delta-neg { color: #C4694D; }
    .kpi-delta-neu { color: #8C8680; }
    .kpi-spark {
        margin-top: 8px;
        height: 28px;
        display: flex;
        align-items: end;
    }
    .kpi-progress-wrap {
        margin-top: 8px;
    }
    .kpi-progress-track {
        background: #F0EDE8;
        border-radius: 10px;
        height: 6px;
        overflow: hidden;
    }
    .kpi-progress-fill {
        background: linear-gradient(90deg, #D97757, #E89373);
        height: 100%;
        border-radius: 10px;
        transition: width 0.6s ease;
    }
    .kpi-progress-text {
        font-size: 0.68rem;
        color: #8C8680;
        margin-top: 3px;
        display: flex;
        justify-content: space-between;
    }

    /* 탭 스타일 */
    div[data-testid="stTabs"] button {
        font-size: 0.92rem;
        font-weight: 500;
        color: #8C8680;
        border-radius: 8px 8px 0 0;
    }
    div[data-testid="stTabs"] button[aria-selected="true"] {
        color: #D97757;
        border-bottom-color: #D97757;
        font-weight: 600;
    }

    /* 데이터프레임 / 테이블 */
    [data-testid="stDataFrame"] {
        border: 1px solid #E8E4DE;
        border-radius: 10px;
        overflow: hidden;
    }
    [data-testid="stExpander"] {
        border: 1px solid #E8E4DE;
        border-radius: 10px;
        background: #FFFFFF;
    }

    /* 구분선 */
    hr { border-color: #E8E4DE !important; }

    /* 제목 */
    h1 { color: #2D2B28 !important; font-weight: 700 !important; font-size: 1.8rem !important; }
    h2 { color: #2D2B28 !important; font-weight: 600 !important; }
    h3 { color: #3D3B38 !important; font-size: 1.1rem !important; font-weight: 600 !important; margin-top: 0.5rem !important; }

    /* 버튼 */
    .stButton > button[kind="primary"] {
        background-color: #D97757 !important;
        border-color: #D97757 !important;
        color: #FFFFFF !important;
        border-radius: 8px !important;
        font-weight: 500 !important;
    }
    .stButton > button[kind="primary"]:hover {
        background-color: #C4694D !important;
        border-color: #C4694D !important;
    }
    .stButton > button {
        border-radius: 8px !important;
        background-color: #3D3B38 !important;
        border-color: #3D3B38 !important;
        color: #FFFFFF !important;
        font-weight: 500 !important;
        transition: all 0.15s ease !important;
    }
    .stButton > button:hover {
        background-color: #1F1E1C !important;
        border-color: #1F1E1C !important;
        color: #FFFFFF !important;
        font-weight: 700 !important;
        transform: translateY(-1px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.15);
    }
    .stButton > button:active {
        transform: translateY(0);
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }

    /* selectbox / input */
    [data-testid="stSelectbox"] > div > div,
    .stTextInput > div > div > input,
    .stNumberInput > div > div > input {
        border-color: #D9D4CE !important;
        border-radius: 8px !important;
    }

    /* 섹션 카드 느낌 */
    .section-card {
        background: #FFFFFF;
        border: 1px solid #E8E4DE;
        border-radius: 12px;
        padding: 24px;
        margin-bottom: 16px;
        box-shadow: 0 1px 3px rgba(45,43,40,0.04);
    }
    .section-title {
        color: #2D2B28;
        font-size: 1.05rem;
        font-weight: 600;
        margin: 28px 0 16px 0;
        padding-bottom: 8px;
        border-bottom: 2px solid #D97757;
        display: inline-block;
    }
    .accent-text { color: #D97757; font-weight: 600; }
    .muted-text { color: #8C8680; font-size: 0.85rem; }

    /* 페이지 헤더 */
    .page-header {
        margin-bottom: 24px;
        padding-bottom: 16px;
        border-bottom: 1px solid #E8E4DE;
    }

    /* 빈 상태 UI */
    .empty-state {
        background: #FFFFFF;
        border: 1px dashed #D9D4CE;
        border-radius: 12px;
        padding: 48px 24px;
        text-align: center;
        color: #8C8680;
    }

    /* 차트 컨테이너 (호버 효과) */
    .stPlotlyChart {
        background: #FFFFFF;
        border-radius: 12px;
        padding: 8px;
        border: 1px solid #E8E4DE;
        transition: box-shadow 0.2s;
    }
    .stPlotlyChart:hover {
        box-shadow: 0 4px 12px rgba(45,43,40,0.08);
    }

    /* 컬럼 사이 간격 */
    [data-testid="column"] { padding: 0 6px; }

    /* 사이드바 토글 버튼 더 잘 보이게 */
    [data-testid="stSidebarCollapseButton"] {
        background: #D97757 !important;
        border-radius: 50% !important;
    }

    /* 데이터프레임 hover */
    [data-testid="stDataFrame"] tbody tr:hover {
        background-color: #F4F1EC !important;
    }

    /* 반응형 */
    @media (max-width: 768px) {
        [data-testid="stMetric"] { padding: 10px 14px; min-height: auto; }
        [data-testid="stMetricValue"] { font-size: 1.1rem; }
        .kpi-card { min-height: 110px; padding: 12px 14px; }
        .kpi-value { font-size: 1.2rem !important; }
        h1 { font-size: 1.4rem !important; }
        [data-testid="column"] { padding: 0 3px; }
    }
    .js-plotly-plot { width: 100% !important; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════
# Plotly 테마 - 클로드 톤
# ══════════════════════════════════════════════
def _is_dark():
    try: return st.session_state.get("dark_mode_toggle", False)
    except: return False


def _theme_colors():
    if _is_dark():
        return dict(
            bg="#1A1916", area="#2D2B28",
            text="#E8E4DE", muted="#8C8680",
            grid="#3D3B38",
        )
    return dict(
        bg="#FFFFFF", area="#FAF9F6",
        text="#3D3B38", muted="#8C8680",
        grid="#E8E4DE",
    )


PLOT_BG = "#FFFFFF"
PLOT_AREA = "#FAF9F6"
PLOT_FONT = dict(color="#3D3B38", family="Noto Sans KR")
HOVER_STYLE = dict(
    bgcolor="rgba(45,43,40,0.95)", font_size=12, font_color="#FAF9F6",
    bordercolor="#D97757", font_family="Noto Sans KR",
)
GRID_COLOR = "#E8E4DE"

# 브랜드 컬러
BRAND_COLORS = {
    "아자차": "#D97757",      # 테라코타 (클로드 메인)
    "반드럽": "#6B9B7A",      # 세이지 그린
    "웰바이오젠": "#7B8DBF",   # 소프트 블루
    "윈토르": "#B8956A",      # 골드
    "자르오": "#9B7A8D",      # 모브
    "기타": "#A8A29E",        # 웜 그레이
}

STORE_COLORS = {
    "아자차(카페24)": "#D97757",
    "반드럽(카페24)": "#6B9B7A",
    "웰바이오젠(카페24)": "#7B8DBF",
    "마르문(스마트스토어)": "#D4956A",
    "링포(쿠팡)": "#B8956A",
    "기타(아자차)": "#E8A88C",
    "기타(반드럽)": "#9BBFA8",
    "기타(웰바이오젠)": "#A3B1D4",
    "기타(윈토르)": "#D4C09A",
}

AD_COLORS = {
    "Meta": "#5B7FC7",
    "Naver SA": "#6B9B7A",
    "쿠팡 광고": "#D4956A",
}

STORE_LIST = list(STORE_COLORS.keys())


# ══════════════════════════════════════════════
# 데이터 로드
# ══════════════════════════════════════════════
_any_configured = any(
    is_configured(s) for s in ["cafe24", "smartstore", "coupang", "meta", "naver_sa", "coupang_ads"]
)

if _any_configured:
    from api.db import load_sales as _ls, load_ads as _la

    @st.cache_data(ttl=300, show_spinner="데이터 불러오는 중...")
    def _cached_sales(start, end):
        return _ls(start, end)

    @st.cache_data(ttl=300, show_spinner=False)
    def _cached_ads(start, end):
        return _la(start, end)

    _db_start = date(2020, 1, 1)
    df_sales = _cached_sales(_db_start, _today_kst())
    df_ads = _cached_ads(_db_start, _today_kst())
    _data_mode = "API"
else:
    # 샘플 데이터 (generate_data 함수는 demo_backup.py에 있음)
    df_sales = pd.DataFrame(columns=["날짜","스토어","채널","주문건수","매출","객단가","순방문자수","전환율"])
    df_ads = pd.DataFrame(columns=["날짜","광고채널","광고비","노출수","클릭수","전환수","전환매출"])
    _data_mode = "DEMO"

# 타입 보정
for col in ["주문건수", "매출", "객단가", "순방문자수", "전환율"]:
    if col in df_sales.columns:
        df_sales[col] = pd.to_numeric(df_sales[col], errors="coerce").fillna(0)
for col in ["광고비", "노출수", "클릭수", "전환수", "전환매출"]:
    if col in df_ads.columns:
        df_ads[col] = pd.to_numeric(df_ads[col], errors="coerce").fillna(0)


# ══════════════════════════════════════════════
# 유틸 함수
# ══════════════════════════════════════════════
def fmt(val):
    if pd.isna(val) or val == 0: return "₩0"
    if abs(val) >= 100_000_000: return f"₩{val/100_000_000:.1f}억"
    if abs(val) >= 1_000_000: return f"₩{val/10_000:,.0f}만"
    return f"₩{val:,.0f}"

def fmt_full(val):
    if pd.isna(val) or val is None: return "₩0"
    return f"₩{int(val):,}"

def pct(val):
    if pd.isna(val): return "0.0%"
    return f"{val:.1f}%"

def krw_hover(val):
    if abs(val) >= 100_000_000: return f"{val/100_000_000:.1f}억원"
    if abs(val) >= 10_000: return f"{val/10_000:,.0f}만원"
    return f"{val:,.0f}원"

def fmt_axis(val):
    if abs(val) >= 100_000_000: return f"{val/100_000_000:.1f}억"
    if abs(val) >= 10_000: return f"{val/10_000:,.0f}만"
    return f"{val:,.0f}"

def fmt_date(d):
    if isinstance(d, str): return d.replace("-", ".")
    return d.strftime("%Y.%m.%d")

def delta_str(curr, prev):
    if prev == 0: return None
    return f"{(curr - prev) / abs(prev) * 100:+.1f}%"


def empty_state(msg, icon="📭"):
    """빈 상태 안내"""
    st.markdown(
        f'<div class="empty-state"><div style="font-size:2.5rem;">{icon}</div>'
        f'<div style="margin-top:8px;font-size:0.95rem;color:#3D3B38;">{msg}</div></div>',
        unsafe_allow_html=True
    )


def download_csv_button(df, filename, label="📥 CSV 다운로드", key=None):
    """데이터프레임 CSV 다운로드"""
    if df is None or (hasattr(df, "empty") and df.empty):
        return
    csv = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button(
        label=label, data=csv, file_name=filename,
        mime="text/csv", key=key, use_container_width=False,
    )


def _spark_svg(values, color="#D97757", width=140, height=28):
    """미니 스파크라인 SVG 생성"""
    if not values or len(values) < 2:
        return ""
    vals = [float(v) for v in values]
    vmin, vmax = min(vals), max(vals)
    rng = vmax - vmin if vmax != vmin else 1
    n = len(vals)
    pts = []
    for i, v in enumerate(vals):
        x = i * (width / (n - 1))
        y = height - 2 - ((v - vmin) / rng) * (height - 4)
        pts.append(f"{x:.1f},{y:.1f}")
    poly = " ".join(pts)
    last_x = (n - 1) * (width / (n - 1))
    last_y = height - 2 - ((vals[-1] - vmin) / rng) * (height - 4)
    area = f"M0,{height} L{poly.replace(' ', ' L')} L{last_x},{height} Z"
    return (
        f'<svg width="100%" viewBox="0 0 {width} {height}" preserveAspectRatio="none" style="display:block;">'
        f'<path d="{area}" fill="{color}" opacity="0.12"/>'
        f'<polyline points="{poly}" fill="none" stroke="{color}" stroke-width="1.6" stroke-linejoin="round" stroke-linecap="round"/>'
        f'<circle cx="{last_x:.1f}" cy="{last_y:.1f}" r="2.2" fill="{color}"/>'
        '</svg>'
    )


def kpi_card(label, value, delta_pct=None, spark=None, target_pct=None,
             invert_delta=False, spark_color="#D97757"):
    """고도화 KPI 카드 (HTML)
    - delta_pct: 전기 대비 % (양수=상승)
    - invert_delta: True면 양수가 빨강 (광고비처럼 적을수록 좋음)
    - spark: 시계열 값 리스트 (스파크라인)
    - target_pct: 목표 진행률 % (0~100+)
    """
    delta_html = ""
    if delta_pct is not None:
        is_pos = delta_pct >= 0
        good = (not is_pos) if invert_delta else is_pos
        cls = "kpi-delta-pos" if good else "kpi-delta-neg"
        if abs(delta_pct) < 0.05:
            cls = "kpi-delta-neu"
        arrow = "▲" if is_pos else "▼"
        delta_html = f'<div class="kpi-delta {cls}">{arrow} {abs(delta_pct):.1f}% <span style="color:#8C8680;font-weight:400;margin-left:2px;">전기대비</span></div>'

    bottom = ""
    if target_pct is not None:
        fill_w = max(0, min(100, target_pct))
        bottom = (
            f'<div class="kpi-progress-wrap">'
            f'<div class="kpi-progress-track"><div class="kpi-progress-fill" style="width:{fill_w}%;"></div></div>'
            f'<div class="kpi-progress-text"><span>목표</span><span>{target_pct:.0f}%</span></div>'
            f'</div>'
        )
    elif spark:
        bottom = f'<div class="kpi-spark">{_spark_svg(spark, spark_color)}</div>'

    html = (
        f'<div class="kpi-card">'
        f'<div><div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div>'
        f'{delta_html}</div>'
        f'{bottom}'
        f'</div>'
    )
    st.markdown(html, unsafe_allow_html=True)

def get_daily(date_from, date_to, store_filter=None):
    ds = df_sales[(df_sales["날짜"] >= date_from) & (df_sales["날짜"] <= date_to)]
    if store_filter and store_filter != "전체":
        ds = ds[ds["스토어"].isin(store_filter) if isinstance(store_filter, list) else ds["스토어"] == store_filter]
    da = df_ads[(df_ads["날짜"] >= date_from) & (df_ads["날짜"] <= date_to)]
    daily = ds.groupby("날짜").agg({"매출": "sum", "주문건수": "sum"}).reset_index()
    daily_ad = da.groupby("날짜").agg({"광고비": "sum", "전환매출": "sum"}).reset_index()
    merged = daily.merge(daily_ad, on="날짜", how="outer").fillna(0)
    merged["ROAS"] = (merged["전환매출"] / merged["광고비"].replace(0, 1) * 100)
    merged["B.ROAS"] = (merged["매출"] / merged["광고비"].replace(0, 1) * 100)
    merged["객단가"] = (merged["매출"] / merged["주문건수"].replace(0, 1)).astype(int)
    return merged

def apply_plotly_theme(fig):
    """클로드 톤 plotly 테마 적용 + 한글 금액 축 (다크모드 자동 전환)"""
    c = _theme_colors()
    fig.update_layout(
        plot_bgcolor=c["area"],
        paper_bgcolor=c["bg"],
        font=dict(color=c["text"], family="Noto Sans KR"),
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0,
            font=dict(size=11, color=c["muted"]),
            bgcolor="rgba(0,0,0,0)",
        ),
        xaxis=dict(
            gridcolor=c["grid"], showline=False, color=c["text"],
            tickformatstops=[
                dict(dtickrange=[None, 86400000*7], value="%-m/%-d"),
                dict(dtickrange=[86400000*7, 86400000*60], value="%-m월 %-d일"),
                dict(dtickrange=[86400000*60, "M12"], value="%Y년 %-m월"),
                dict(dtickrange=["M12", None], value="%Y년"),
            ],
        ),
        yaxis=dict(gridcolor=c["grid"], showline=False, color=c["text"]),
        hoverlabel=HOVER_STYLE,
    )
    # Y축 한글 금액 표기
    for axis_name in ['yaxis', 'yaxis2']:
        axis = fig.layout[axis_name] if axis_name in dir(fig.layout) else None
        try:
            axis = getattr(fig.layout, axis_name)
            if axis and axis.title and axis.title.text and '금액' in axis.title.text:
                axis.tickformat = None
                axis.ticksuffix = None
        except:
            pass
    return fig

def apply_korean_yaxis(fig, secondary=False):
    """Y축 금액을 한글로 표기 (만, 억)"""
    axis_key = "yaxis2" if secondary else "yaxis"
    try:
        axis = getattr(fig.layout, axis_key)
        if axis:
            axis.tickformat = None
            axis.tickprefix = ""
            axis.ticksuffix = ""
    except:
        pass

    # 데이터에서 최대값을 찾아서 적절한 tick 생성
    max_val = 0
    for trace in fig.data:
        if hasattr(trace, 'y') and trace.y is not None:
            try:
                vals = [v for v in trace.y if v is not None and not pd.isna(v)]
                if vals:
                    trace_max = max(vals)
                    if not secondary and not hasattr(trace, 'yaxis'):
                        max_val = max(max_val, trace_max)
                    elif not secondary:
                        max_val = max(max_val, trace_max)
            except:
                pass

    if max_val > 0:
        if max_val >= 100_000_000:
            step = 50_000_000
        elif max_val >= 10_000_000:
            step = 5_000_000
        elif max_val >= 1_000_000:
            step = 1_000_000
        elif max_val >= 100_000:
            step = 200_000
        else:
            step = 50_000

        ticks = list(range(0, int(max_val * 1.3) + step, step))
        labels = [fmt_axis(v) for v in ticks]
        fig.update_layout(**{axis_key: dict(tickvals=ticks, ticktext=labels)})

    return fig


def store_filter_ui(key_prefix=""):
    view = st.radio("보기", ["전체", "스토어 선택"], horizontal=True, key=f"{key_prefix}_view")
    if view == "스토어 선택":
        selected = st.multiselect("스토어", STORE_LIST, default=STORE_LIST, key=f"{key_prefix}_stores")
        return selected if selected else STORE_LIST
    return "전체"


# ══════════════════════════════════════════════
# 사이드바
# ══════════════════════════════════════════════
with st.sidebar:
    st.markdown("""
    <div style='text-align:center; padding: 16px 0 12px 0;'>
        <span style='font-size: 1.2rem; font-weight: 700; letter-spacing: 2px; color:#D97757;'>LINKPORT</span><br>
        <span style='font-size: 0.7rem; color:#8C8680; letter-spacing: 1px;'>E-COMMERCE DASHBOARD</span>
    </div>
    """, unsafe_allow_html=True)
    st.divider()

    page = st.radio(
        "메뉴",
        ["📊 대시보드", "🏷️ 브랜드 분석", "📆 월별 분석", "🏪 채널 분석", "⚙️ 설정"],
        label_visibility="collapsed",
    )

    st.divider()

    # 자동 새로고침 (5분, 깜빡임 최소화)
    _auto_refresh = st.toggle("자동 새로고침 (5분)", value=False, key="auto_refresh_toggle")
    if _auto_refresh:
        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=5 * 60 * 1000, key="autorefresher")
        except ImportError:
            st.caption("⚠ streamlit-autorefresh 미설치")

    # 다크 모드 토글
    _dark = st.toggle("🌙 다크 모드", value=False, key="dark_mode_toggle")
    if _dark:
        st.markdown("""
        <style>
        .stApp, [data-testid="stAppViewContainer"] { background-color: #1A1916 !important; }
        .kpi-card, .stPlotlyChart, [data-testid="stMetric"], [data-testid="stExpander"], [data-testid="stDataFrame"] {
            background: #2D2B28 !important;
            border-color: #3D3B38 !important;
            color: #E8E4DE !important;
        }
        .kpi-value, h1, h2, h3, .section-title { color: #FAF9F6 !important; }
        .kpi-label, .muted-text { color: #B8B2AA !important; }
        [data-testid="stMetricValue"] { color: #FAF9F6 !important; }
        .empty-state { background: #2D2B28 !important; color: #B8B2AA !important; }
        </style>
        """, unsafe_allow_html=True)

    st.divider()
    if _data_mode == "API":
        st.caption(f"LIVE · {_today_kst().strftime('%Y.%m.%d')}")
        if st.button("🔄 오늘 매출 갱신", use_container_width=True):
            with st.spinner("API에서 가져오는 중..."):
                from datetime import date as _d
                from api.token_manager import check_and_refresh_all as _rt
                from api.db import save_sales as _ss, save_ads as _sa, mark_fetched as _mf
                _today = _today_kst()
                _msgs = []
                try:
                    _rt()
                except Exception as e:
                    _msgs.append(f"토큰: {e}")
                # 매출 (오늘만)
                for _svc, _mod, _fn in [
                    ("cafe24","api.cafe24","fetch_all_cafe24"),
                    ("smartstore","api.smartstore","fetch_smartstore"),
                    ("coupang","api.coupang","fetch_coupang"),
                ]:
                    if not is_configured(_svc): continue
                    try:
                        _m = __import__(_mod, fromlist=[_fn])
                        _df = getattr(_m, _fn)(_today, _today)
                        if not _df.empty:
                            _ss(_df); _mf(_svc, [_today])
                            _msgs.append(f"{_svc}: {len(_df)}건")
                        else:
                            _msgs.append(f"{_svc}: 0건")
                    except Exception as e:
                        _msgs.append(f"{_svc}: 실패 {str(e)[:40]}")
                # 광고 (오늘만)
                for _svc, _mod, _fn in [
                    ("meta","api.meta_ads","fetch_meta_ads"),
                    ("naver_sa","api.naver_sa","fetch_naver_sa"),
                ]:
                    if not is_configured(_svc): continue
                    try:
                        _m = __import__(_mod, fromlist=[_fn])
                        _df = getattr(_m, _fn)(_today, _today)
                        if not _df.empty:
                            _sa(_df); _mf(_svc, [_today])
                            _msgs.append(f"{_svc}: {len(_df)}건")
                        else:
                            _msgs.append(f"{_svc}: 0건")
                    except Exception as e:
                        _msgs.append(f"{_svc}: 실패 {str(e)[:40]}")
            for _m in _msgs:
                st.caption(_m)
            st.cache_data.clear()
            st.success("갱신 완료")
            st.rerun()
    else:
        st.caption("DEMO MODE")


# ══════════════════════════════════════════════
# PAGE 1: 대시보드
# ══════════════════════════════════════════════
if page == "📊 대시보드":
    today = _today_kst()
    yesterday = today - timedelta(days=1)
    weekday_kr = ["월", "화", "수", "목", "금", "토", "일"][today.weekday()]

    st.markdown(f"""
    <div style='margin-bottom: 8px;'>
        <span style='font-size: 1.6rem; font-weight: 700; color: #2D2B28;'>대시보드</span>
        <span style='font-size: 0.9rem; color: #8C8680; margin-left: 12px;'>{fmt_date(today)} ({weekday_kr})</span>
    </div>
    """, unsafe_allow_html=True)

    sf = store_filter_ui("dash")

    # 오늘/어제 집계
    t_s = df_sales[df_sales["날짜"] == today]
    y_s = df_sales[df_sales["날짜"] == yesterday]
    if sf != "전체":
        t_s = t_s[t_s["스토어"].isin(sf)]
        y_s = y_s[y_s["스토어"].isin(sf)]
    t_a = df_ads[df_ads["날짜"] == today]
    y_a = df_ads[df_ads["날짜"] == yesterday]

    t_rev, y_rev = t_s["매출"].sum(), y_s["매출"].sum()
    t_orders, y_orders = t_s["주문건수"].sum(), y_s["주문건수"].sum()
    t_aov = int(t_rev / max(1, t_orders))
    y_aov = int(y_rev / max(1, y_orders))
    t_ad, y_ad = t_a["광고비"].sum(), y_a["광고비"].sum()
    t_conv, y_conv = t_a["전환매출"].sum(), y_a["전환매출"].sum()
    t_roas = t_conv / max(1, t_ad) * 100
    y_roas = y_conv / max(1, y_ad) * 100
    t_broas = t_rev / max(1, t_ad) * 100
    y_broas = y_rev / max(1, y_ad) * 100

    # KPI 카드 (스파크라인 7일치 데이터)
    st.markdown('<div class="section-title">오늘 매출 현황</div>', unsafe_allow_html=True)
    _spark_data = get_daily(today - timedelta(6), today, sf)
    spark_rev = _spark_data["매출"].tolist() if not _spark_data.empty else []
    spark_ord = _spark_data["주문건수"].tolist() if not _spark_data.empty else []
    spark_aov = (_spark_data["매출"] / _spark_data["주문건수"].replace(0, 1)).astype(int).tolist() if not _spark_data.empty else []
    spark_ad = _spark_data["광고비"].tolist() if not _spark_data.empty else []
    spark_roas = (_spark_data["전환매출"] / _spark_data["광고비"].replace(0, 1) * 100).round(1).tolist() if not _spark_data.empty else []
    spark_broas = (_spark_data["매출"] / _spark_data["광고비"].replace(0, 1) * 100).round(1).tolist() if not _spark_data.empty else []

    def _d(c, p):
        if p == 0: return None
        return (c - p) / abs(p) * 100

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1: kpi_card("매출", fmt(t_rev), _d(t_rev, y_rev), spark=spark_rev)
    with c2: kpi_card("주문건수", f"{t_orders}건", _d(t_orders, y_orders), spark=spark_ord, spark_color="#7B8DBF")
    with c3: kpi_card("객단가", fmt(t_aov), _d(t_aov, y_aov), spark=spark_aov, spark_color="#A88B6E")
    with c4: kpi_card("광고비", fmt(t_ad), _d(t_ad, y_ad), spark=spark_ad, invert_delta=True, spark_color="#B8B2AA")
    with c5: kpi_card("ROAS", pct(t_roas), _d(t_roas, y_roas), spark=spark_roas, spark_color="#4A8C5F")
    with c6: kpi_card("B.ROAS", pct(t_broas), _d(t_broas, y_broas), spark=spark_broas, spark_color="#4A8C5F")

    st.markdown("<br>", unsafe_allow_html=True)

    # 월 목표 달성
    _month_key = today.strftime("%Y-%m")
    _month_label = f"{today.month}월"
    try:
        _tgt_c = sqlite3.connect("dashboard_data.db")
        _tgt_row = _tgt_c.execute("SELECT 목표매출 FROM monthly_targets WHERE 월=?", (_month_key,)).fetchone()
        target = int(_tgt_row[0]) if _tgt_row else 35_000_000
        _tgt_c.close()
    except Exception:
        target = 35_000_000

    month_data = get_daily(date(today.year, today.month, 1), today)
    m_rev = month_data["매출"].sum()
    achieve = m_rev / target * 100 if target else 0
    month_days = calendar.monthrange(today.year, today.month)[1]
    remaining = month_days - today.day
    daily_needed = (target - m_rev) / remaining if remaining > 0 else 0

    st.markdown(f'<div class="section-title">{_month_label} 목표 달성</div>', unsafe_allow_html=True)
    m0, m1, m2, m3, m4 = st.columns(5)
    with m0: kpi_card("달성률", pct(achieve), target_pct=achieve)
    with m1: kpi_card("누적 매출", fmt(m_rev), target_pct=achieve)
    with m2: kpi_card("남은 일수", f"{remaining}일")
    with m3: kpi_card("일평균 필요매출", fmt(int(daily_needed)))
    _cur_avg = m_rev / max(1, today.day)
    _pace_pos = _cur_avg >= daily_needed
    pace = "순조" if _pace_pos else "부족"
    pace_value = f'<span style="color:{"#4A8C5F" if _pace_pos else "#C4694D"}">{pace}</span> · {fmt(int(_cur_avg))}'
    with m4: kpi_card("진행 상태", pace_value)

    st.markdown("<br>", unsafe_allow_html=True)

    # 매출/광고비 추이
    st.markdown('<div class="section-title">매출 / 광고비 추이</div>', unsafe_allow_html=True)
    period_options = {"최근 7일": 7, "최근 14일": 14, "최근 30일": 30, "최근 90일": 90, "직접 설정": 0}
    col_p, col_f, col_t = st.columns([1, 1, 1])
    with col_p:
        period_sel = st.selectbox("기간", list(period_options.keys()), index=2, key="dash_period")
    if period_sel == "직접 설정":
        with col_f:
            d_from = st.date_input("시작", today - timedelta(30), key="dash_from", format="YYYY/MM/DD")
        with col_t:
            d_to = st.date_input("종료", today, key="dash_to", format="YYYY/MM/DD")
    else:
        d_from = today - timedelta(period_options[period_sel])
        d_to = today

    chart_data = get_daily(d_from, d_to, sf)
    if chart_data.empty:
        empty_state(f"{d_from} ~ {d_to} 기간 데이터가 없어요. 사이드바에서 '🔄 오늘 매출 갱신'을 눌러보세요.", icon="📊")
    else:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=chart_data["날짜"], y=chart_data["매출"], name="매출",
                             marker_color="#D97757", opacity=0.8,
                             hovertemplate="%{x|%Y.%m.%d}<br>매출: %{y:,.0f}원<extra></extra>"), secondary_y=False)
        fig.add_trace(go.Bar(x=chart_data["날짜"], y=chart_data["광고비"], name="광고비",
                             marker_color="#B8B2AA", opacity=0.6,
                             hovertemplate="%{x|%Y.%m.%d}<br>광고비: %{y:,.0f}원<extra></extra>"), secondary_y=False)
        fig.add_trace(go.Scatter(x=chart_data["날짜"], y=chart_data["ROAS"], name="ROAS",
                                 line=dict(color="#7B8DBF", width=2), mode="lines+markers",
                                 marker=dict(size=4),
                                 hovertemplate="%{x|%Y.%m.%d}<br>ROAS: %{y:.1f}%<extra></extra>"), secondary_y=True)
        fig.update_yaxes(title_text="금액 (원)", secondary_y=False, gridcolor=GRID_COLOR)
        fig.update_yaxes(title_text="ROAS (%)", secondary_y=True, gridcolor=GRID_COLOR)
        fig = apply_plotly_theme(fig)
        fig = apply_korean_yaxis(fig)
        fig.update_layout(barmode="group", height=380)
        st.plotly_chart(fig, use_container_width=True)

    # 스토어별 오늘 매출
    st.markdown('<div class="section-title">스토어별 오늘 매출</div>', unsafe_allow_html=True)
    t_st = t_s.groupby("스토어").agg({"매출": "sum", "주문건수": "sum"}).reset_index()
    y_st = y_s.groupby("스토어").agg({"매출": "sum"}).reset_index().rename(columns={"매출": "전일"})
    t_st = t_st.merge(y_st, on="스토어", how="left").fillna(0)
    t_st["전일대비"] = ((t_st["매출"] - t_st["전일"]) / t_st["전일"].replace(0, 1) * 100).round(1)
    t_st["객단가"] = (t_st["매출"] / t_st["주문건수"].replace(0, 1)).astype(int)

    if len(t_st) > 0:
        cols = st.columns(min(len(t_st), 5))
        for i, (_, row) in enumerate(t_st.iterrows()):
            with cols[i % 5]:
                st.metric(row["스토어"], fmt_full(int(row["매출"])), f"{row['전일대비']:+.1f}%")
                st.caption(f"주문 {int(row['주문건수'])}건 · 객단가 {fmt(int(row['객단가']))}")

    # 매출 캘린더 히트맵 (최근 12주 = 잔디 스타일)
    st.markdown('<div class="section-title">매출 캘린더 (최근 12주)</div>', unsafe_allow_html=True)
    _cal_end = today
    _cal_start = today - timedelta(weeks=12)
    _cal_data = get_daily(_cal_start, _cal_end, sf)
    if not _cal_data.empty:
        _cal_data = _cal_data.set_index("날짜")
        # 12주 x 7일 grid
        import numpy as np
        _all_dates = pd.date_range(_cal_start, _cal_end)
        _values = []
        _texts = []
        _xs = []
        _ys = []
        WEEKDAYS = ["월","화","수","목","금","토","일"]
        for d in _all_dates:
            dd = d.date()
            v = float(_cal_data.loc[dd, "매출"]) if dd in _cal_data.index else 0
            wk = (dd - _cal_start).days // 7
            wd = d.weekday()
            _xs.append(wk)
            _ys.append(6 - wd)  # 월요일 위로
            _values.append(v)
            _texts.append(f"{dd.strftime('%Y.%m.%d')} ({WEEKDAYS[wd]})<br>매출 {fmt_full(int(v))}")
        _vmax = max(_values) if _values else 1
        fig_cal = go.Figure(data=[go.Heatmap(
            x=_xs, y=_ys, z=_values, text=_texts, hoverinfo="text",
            colorscale=[[0,"#F4F1EC"],[0.25,"#F0CBB8"],[0.5,"#E89373"],[1,"#D97757"]],
            zmin=0, zmax=_vmax,
            xgap=3, ygap=3, showscale=False,
        )])
        fig_cal.update_layout(
            height=200, margin=dict(l=40, r=20, t=20, b=20),
            plot_bgcolor="#FAF9F6", paper_bgcolor="#FAF9F6",
            xaxis=dict(showticklabels=False, showgrid=False, zeroline=False),
            yaxis=dict(tickmode="array", tickvals=[6,5,4,3,2,1,0],
                       ticktext=WEEKDAYS, showgrid=False, zeroline=False,
                       tickfont=dict(size=10, color="#8C8680")),
        )
        st.plotly_chart(fig_cal, use_container_width=True, config={"displayModeBar": False})


# ══════════════════════════════════════════════
# PAGE 2: 브랜드 분석
# ══════════════════════════════════════════════
elif page == "🏷️ 브랜드 분석":
    st.markdown("""
    <div style='margin-bottom: 8px;'>
        <span style='font-size: 1.6rem; font-weight: 700; color: #2D2B28;'>브랜드 분석</span>
    </div>
    """, unsafe_allow_html=True)

    # 기간 선택
    col1, col2 = st.columns(2)
    with col1:
        b_from = st.date_input("시작일", _today_kst() - timedelta(30), key="brand_from", format="YYYY/MM/DD")
    with col2:
        b_to = st.date_input("종료일", _today_kst(), key="brand_to", format="YYYY/MM/DD")

    # 브랜드별 매출 집계 (카페24=스토어명, 스마트스토어=아자차, 쿠팡=브랜드 컬럼)
    bs = df_sales[(df_sales["날짜"] >= b_from) & (df_sales["날짜"] <= b_to)].copy()
    ba = df_ads[(df_ads["날짜"] >= b_from) & (df_ads["날짜"] <= b_to)].copy()

    # 매출 브랜드 매핑
    def map_brand_sales(row):
        if "아자차" in str(row.get("스토어", "")):
            return "아자차"
        if "반드럽" in str(row.get("스토어", "")):
            return "반드럽"
        if "웰바이오젠" in str(row.get("스토어", "")):
            return "웰바이오젠"
        if "마르문" in str(row.get("스토어", "")):
            return "아자차"
        brand = str(row.get("브랜드", ""))
        if brand and brand != "nan" and brand != "":
            return brand
        return "기타"

    bs["_브랜드"] = bs.apply(map_brand_sales, axis=1)

    # 광고 브랜드 매핑
    if "브랜드" in ba.columns:
        ba["_브랜드"] = ba["브랜드"].fillna("기타").replace("", "기타")
    else:
        ba["_브랜드"] = "기타"

    # 브랜드별 KPI
    brand_sales = bs.groupby("_브랜드").agg({"매출": "sum", "주문건수": "sum"}).reset_index()
    brand_ads = ba.groupby("_브랜드").agg({"광고비": "sum", "전환매출": "sum"}).reset_index()
    brand_kpi = brand_sales.merge(brand_ads, left_on="_브랜드", right_on="_브랜드", how="outer").fillna(0)
    brand_kpi["ROAS"] = (brand_kpi["전환매출"] / brand_kpi["광고비"].replace(0, 1) * 100).round(1)
    brand_kpi["B.ROAS"] = (brand_kpi["매출"] / brand_kpi["광고비"].replace(0, 1) * 100).round(1)

    # 주요 브랜드만 표시
    main_brands = ["아자차", "반드럽", "웰바이오젠"]

    st.markdown('<div class="section-title">브랜드별 성과 요약</div>', unsafe_allow_html=True)
    brand_cols = st.columns(len(main_brands))
    for i, brand in enumerate(main_brands):
        row = brand_kpi[brand_kpi["_브랜드"] == brand]
        with brand_cols[i]:
            color = BRAND_COLORS.get(brand, "#A8A29E")
            st.markdown(f"""
            <div style='background: #FFFFFF; border: 1px solid #E8E4DE; border-radius: 12px;
                        padding: 20px; border-left: 4px solid {color};'>
                <div style='font-size: 1.1rem; font-weight: 700; color: {color}; margin-bottom: 12px;'>{brand}</div>
            </div>
            """, unsafe_allow_html=True)
            if not row.empty:
                r = row.iloc[0]
                st.metric("매출", fmt(r["매출"]))
                st.metric("광고비", fmt(r["광고비"]))
                st.metric("ROAS", pct(r["ROAS"]))
                st.metric("B.ROAS", pct(r["B.ROAS"]))
            else:
                st.caption("데이터 없음")

    st.markdown("<br>", unsafe_allow_html=True)

    # 브랜드별 매출 추이
    st.markdown('<div class="section-title">브랜드별 매출 추이</div>', unsafe_allow_html=True)
    daily_brand = bs.groupby(["날짜", "_브랜드"]).agg({"매출": "sum"}).reset_index()
    if not daily_brand.empty:
        fig = go.Figure()
        for brand in main_brands:
            bd = daily_brand[daily_brand["_브랜드"] == brand]
            if not bd.empty:
                fig.add_trace(go.Scatter(
                    x=bd["날짜"], y=bd["매출"], name=brand,
                    line=dict(color=BRAND_COLORS.get(brand, "#A8A29E"), width=2),
                    mode="lines",
                    hovertemplate=f"{brand}<br>%{{x|%Y.%m.%d}}<br>매출: %{{y:,.0f}}원<extra></extra>",
                ))
        fig = apply_plotly_theme(fig)
        fig = apply_korean_yaxis(fig)
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    # 브랜드별 광고비 추이
    st.markdown('<div class="section-title">브랜드별 광고비 추이</div>', unsafe_allow_html=True)
    daily_brand_ad = ba.groupby(["날짜", "_브랜드"]).agg({"광고비": "sum"}).reset_index()
    if not daily_brand_ad.empty:
        fig2 = go.Figure()
        for brand in main_brands:
            bd = daily_brand_ad[daily_brand_ad["_브랜드"] == brand]
            if not bd.empty:
                fig2.add_trace(go.Bar(
                    x=bd["날짜"], y=bd["광고비"], name=brand,
                    marker_color=BRAND_COLORS.get(brand, "#A8A29E"),
                    opacity=0.8,
                    hovertemplate=f"{brand}<br>%{{x|%Y.%m.%d}}<br>광고비: %{{y:,.0f}}원<extra></extra>",
                ))
        fig2 = apply_plotly_theme(fig2)
        fig2 = apply_korean_yaxis(fig2)
        fig2.update_layout(barmode="stack", height=350)
        st.plotly_chart(fig2, use_container_width=True)


# ══════════════════════════════════════════════
# PAGE 3: 월별 분석
# ══════════════════════════════════════════════
elif page == "📆 월별 분석":
    st.markdown("""
    <div style='margin-bottom: 8px;'>
        <span style='font-size: 1.6rem; font-weight: 700; color: #2D2B28;'>월별 분석</span>
    </div>
    """, unsafe_allow_html=True)

    today = _today_kst()
    period_map = {"최근 7일": 7, "최근 14일": 14, "최근 30일": 30, "최근 90일": 90, "직접 설정": 0}
    col_p, col_f, col_t = st.columns([1, 1, 1])
    with col_p:
        ch_period = st.selectbox("기간", list(period_map.keys()), index=2, key="month_period")
    if ch_period == "직접 설정":
        with col_f:
            ch_from = st.date_input("시작", today - timedelta(30), key="month_from", format="YYYY/MM/DD")
        with col_t:
            ch_to = st.date_input("종료", today, key="month_to", format="YYYY/MM/DD")
    else:
        ch_from = today - timedelta(period_map[ch_period])
        ch_to = today

    sf = store_filter_ui("month")
    merged = get_daily(ch_from, ch_to, sf)

    total_rev = merged["매출"].sum()
    total_orders = merged["주문건수"].sum()
    total_aov = int(total_rev / max(1, total_orders))
    total_ad = merged["광고비"].sum()
    total_conv = merged["전환매출"].sum()

    st.markdown('<div class="section-title">기간 요약</div>', unsafe_allow_html=True)
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: st.metric("총 매출", fmt(total_rev))
    with c2: st.metric("총 주문건수", f"{total_orders:,.0f}건")
    with c3: st.metric("객단가", fmt(total_aov))
    with c4: st.metric("총 광고비", fmt(total_ad))
    with c5: st.metric("ROAS", pct(total_conv / total_ad * 100 if total_ad else 0))

    st.markdown("<br>", unsafe_allow_html=True)

    # 일별 추이
    st.markdown('<div class="section-title">일별 추이</div>', unsafe_allow_html=True)
    if not merged.empty:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=merged["날짜"], y=merged["매출"], name="매출",
                             marker_color="#D97757", opacity=0.8), secondary_y=False)
        fig.add_trace(go.Bar(x=merged["날짜"], y=merged["광고비"], name="광고비",
                             marker_color="#B8B2AA", opacity=0.6), secondary_y=False)
        fig.add_trace(go.Scatter(x=merged["날짜"], y=merged["ROAS"], name="ROAS",
                                 line=dict(color="#7B8DBF", width=2), mode="lines+markers",
                                 marker=dict(size=4)), secondary_y=True)
        fig.update_yaxes(title_text="금액", secondary_y=False)
        fig.update_yaxes(title_text="ROAS (%)", secondary_y=True)
        fig = apply_plotly_theme(fig)
        fig = apply_korean_yaxis(fig)
        fig.update_layout(barmode="group", height=380)
        st.plotly_chart(fig, use_container_width=True)

    # 스토어별 매출
    st.markdown('<div class="section-title">스토어별 매출</div>', unsafe_allow_html=True)
    ds = df_sales[(df_sales["날짜"] >= ch_from) & (df_sales["날짜"] <= ch_to)]
    if sf != "전체":
        ds = ds[ds["스토어"].isin(sf)]
    ch = ds.groupby("스토어").agg({"매출": "sum", "주문건수": "sum"}).reset_index()
    ch["객단가"] = (ch["매출"] / ch["주문건수"].replace(0, 1)).astype(int)
    ch["매출비중"] = (ch["매출"] / ch["매출"].sum() * 100).round(1)

    col_tbl, col_pie = st.columns([2, 1])
    with col_tbl:
        display_ch = ch.copy()
        display_ch["매출"] = display_ch["매출"].apply(lambda x: f"₩{int(x):,}")
        display_ch["객단가"] = display_ch["객단가"].apply(lambda x: f"₩{int(x):,}")
        display_ch["매출비중"] = display_ch["매출비중"].apply(lambda x: f"{x}%")
        st.dataframe(display_ch, width="stretch", hide_index=True)
        download_csv_button(ch, f"스토어매출_{ch_from}_{ch_to}.csv", key="dl_store")
    with col_pie:
        if not ch.empty:
            colors = [STORE_COLORS.get(s, "#A8A29E") for s in ch["스토어"]]
            fig_pie = go.Figure(data=[go.Pie(
                labels=ch["스토어"], values=ch["매출"],
                marker=dict(colors=colors),
                hole=0.4,
                textinfo="label+percent",
                textfont=dict(size=12, color="#3D3B38"),
                textposition="outside",
                pull=[0.02] * 10,
            )])
            fig_pie = apply_plotly_theme(fig_pie)
            fig_pie.update_layout(height=350, margin=dict(l=40, r=40, t=30, b=30))
            st.plotly_chart(fig_pie, use_container_width=True)

    # 광고채널별 성과
    st.markdown('<div class="section-title">광고채널별 성과</div>', unsafe_allow_html=True)
    da = df_ads[(df_ads["날짜"] >= ch_from) & (df_ads["날짜"] <= ch_to)]
    ad_ch = da.groupby("광고채널").agg({"광고비": "sum", "노출수": "sum", "클릭수": "sum", "전환수": "sum", "전환매출": "sum"}).reset_index()
    if not ad_ch.empty:
        ad_ch["CTR"] = (ad_ch["클릭수"] / ad_ch["노출수"].replace(0, 1) * 100).round(2)
        ad_ch["ROAS"] = (ad_ch["전환매출"] / ad_ch["광고비"].replace(0, 1) * 100).round(1)

        # 광고 퍼널 + 효율 버블 차트 (좌/우)
        col_funnel, col_bubble = st.columns([1, 1])
        with col_funnel:
            st.caption(f"광고 퍼널 (전체 채널 합계, 전환매출 ₩{int(ad_ch['전환매출'].sum()):,})")
            _imp = int(ad_ch["노출수"].sum())
            _click = int(ad_ch["클릭수"].sum())
            _conv = int(ad_ch["전환수"].sum())
            _ctr = (_click / max(1, _imp) * 100)
            _cvr = (_conv / max(1, _click) * 100)
            # 시각적 비율 고정 (100 / 65 / 35) - 실제 값은 텍스트로 표시
            fig_funnel = go.Figure(go.Funnel(
                y=["노출수", "클릭수", "전환수"],
                x=[100, 65, 35],
                text=[f"{_imp:,}",
                      f"{_click:,} · CTR {_ctr:.2f}%",
                      f"{_conv:,}건 · CVR {_cvr:.2f}%"],
                textinfo="text",
                textposition="inside",
                textfont=dict(size=13, color="#FFFFFF", family="Noto Sans KR"),
                marker={"color": ["#7B8DBF", "#E89373", "#D97757"]},
                connector={"line": {"color": "#E8E4DE"}},
            ))
            fig_funnel.update_layout(
                height=320, margin=dict(l=20, r=20, t=10, b=10),
                plot_bgcolor="#FAF9F6", paper_bgcolor="#FAF9F6",
                font=dict(family="Noto Sans KR", size=12, color="#3D3B38"),
                xaxis=dict(visible=False),
            )
            st.plotly_chart(fig_funnel, use_container_width=True, config={"displayModeBar": False})

        with col_bubble:
            st.caption("채널 효율 (X:광고비 / Y:ROAS / 크기:전환매출)")
            CHANNEL_COLORS = {
                "Meta": "#1877F2", "Naver SA": "#03C75A", "쿠팡": "#FF6F61",
                "카페제휴": "#9B59B6", "기타": "#A88B6E",
            }
            ad_ch_pos = ad_ch[ad_ch["광고비"] > 0].copy()
            if not ad_ch_pos.empty:
                fig_bub = go.Figure()
                for _, r in ad_ch_pos.iterrows():
                    fig_bub.add_trace(go.Scatter(
                        x=[r["광고비"]], y=[r["ROAS"]],
                        mode="markers+text",
                        text=[r["광고채널"]],
                        textposition="top center",
                        textfont=dict(size=11, color="#3D3B38"),
                        marker=dict(
                            size=max(15, min(60, (r["전환매출"] / max(1, ad_ch_pos["전환매출"].max()) * 60))),
                            color=CHANNEL_COLORS.get(r["광고채널"], "#8C8680"),
                            opacity=0.7, line=dict(width=1.5, color="#FFFFFF"),
                        ),
                        hovertemplate=f"<b>{r['광고채널']}</b><br>광고비: ₩{int(r['광고비']):,}<br>ROAS: {r['ROAS']}%<br>전환매출: ₩{int(r['전환매출']):,}<extra></extra>",
                        showlegend=False,
                    ))
                # ROAS 100% 기준선
                fig_bub.add_hline(y=100, line=dict(color="#C4694D", width=1, dash="dash"),
                                  annotation_text="손익분기 (ROAS 100%)",
                                  annotation_position="top right",
                                  annotation_font=dict(size=10, color="#C4694D"))
                fig_bub.update_layout(
                    height=320, margin=dict(l=40, r=20, t=10, b=40),
                    plot_bgcolor="#FAF9F6", paper_bgcolor="#FAF9F6",
                    font=dict(family="Noto Sans KR", size=10, color="#3D3B38"),
                    xaxis=dict(title="광고비 (₩)", gridcolor="#E8E4DE", tickformat=",.0f"),
                    yaxis=dict(title="ROAS (%)", gridcolor="#E8E4DE"),
                )
                st.plotly_chart(fig_bub, use_container_width=True, config={"displayModeBar": False})

        display_ad = ad_ch.copy()
        display_ad["광고비"] = display_ad["광고비"].apply(lambda x: f"₩{int(x):,}")
        display_ad["전환매출"] = display_ad["전환매출"].apply(lambda x: f"₩{int(x):,}")
        display_ad["노출수"] = display_ad["노출수"].apply(lambda x: f"{int(x):,}")
        display_ad["클릭수"] = display_ad["클릭수"].apply(lambda x: f"{int(x):,}")
        display_ad["CTR"] = display_ad["CTR"].apply(lambda x: f"{x}%")
        display_ad["ROAS"] = display_ad["ROAS"].apply(lambda x: f"{x}%")
        st.dataframe(display_ad, width="stretch", hide_index=True)
        download_csv_button(ad_ch, f"광고채널성과_{ch_from}_{ch_to}.csv", key="dl_ad_ch")


# ══════════════════════════════════════════════
# PAGE 4: 채널(스토어) 분석
# ══════════════════════════════════════════════
elif page == "🏪 채널 분석":
    st.markdown("""
    <div style='margin-bottom: 8px;'>
        <span style='font-size: 1.6rem; font-weight: 700; color: #2D2B28;'>채널 분석</span>
    </div>
    """, unsafe_allow_html=True)

    today = _today_kst()
    period_map = {"최근 3개월": 90, "최근 6개월": 180, "최근 1년": 365, "직접 설정": 0}
    col_p, col_f, col_t = st.columns([1, 1, 1])
    with col_p:
        ch_period = st.selectbox("기간", list(period_map.keys()), index=0, key="ch_period")
    if ch_period == "직접 설정":
        with col_f:
            ch_from = st.date_input("시작", today - timedelta(90), key="ch_from", format="YYYY/MM/DD")
        with col_t:
            ch_to = st.date_input("종료", today, key="ch_to", format="YYYY/MM/DD")
    else:
        ch_from = today - timedelta(period_map[ch_period])
        ch_to = today

    ds = df_sales[(df_sales["날짜"] >= ch_from) & (df_sales["날짜"] <= ch_to)]

    # 스토어별 매출 비중
    st.markdown('<div class="section-title">스토어별 매출 비중</div>', unsafe_allow_html=True)
    store_summary = ds.groupby("스토어").agg({"매출": "sum", "주문건수": "sum"}).reset_index()
    store_summary["매출비중"] = (store_summary["매출"] / store_summary["매출"].sum() * 100).round(1)

    col_chart, col_tbl = st.columns([1, 2])
    with col_chart:
        if not store_summary.empty:
            colors = [STORE_COLORS.get(s, "#A8A29E") for s in store_summary["스토어"]]
            fig = go.Figure(data=[go.Pie(
                labels=store_summary["스토어"], values=store_summary["매출"],
                marker=dict(colors=colors), hole=0.4,
                textinfo="label+percent",
                textfont=dict(size=12, color="#3D3B38"),
                textposition="outside",
                pull=[0.02] * 10,
            )])
            fig = apply_plotly_theme(fig)
            fig.update_layout(height=380, showlegend=False, margin=dict(l=40, r=40, t=30, b=30))
            st.plotly_chart(fig, use_container_width=True)
    with col_tbl:
        if not store_summary.empty:
            display_ss = store_summary.copy()
            display_ss["매출"] = display_ss["매출"].apply(lambda x: f"₩{int(x):,}")
            display_ss["객단가"] = (store_summary["매출"].astype(float) / store_summary["주문건수"].replace(0, 1)).astype(int).apply(lambda x: f"₩{x:,}")
            display_ss["매출비중"] = display_ss["매출비중"].apply(lambda x: f"{x}%")
            st.dataframe(display_ss, width="stretch", hide_index=True)

    # 스토어별 매출 추이
    st.markdown('<div class="section-title">스토어별 매출 추이</div>', unsafe_allow_html=True)
    daily_store = ds.groupby(["날짜", "스토어"]).agg({"매출": "sum"}).reset_index()
    if not daily_store.empty:
        fig2 = go.Figure()
        for store in daily_store["스토어"].unique():
            sd = daily_store[daily_store["스토어"] == store]
            fig2.add_trace(go.Scatter(
                x=sd["날짜"], y=sd["매출"], name=store,
                line=dict(color=STORE_COLORS.get(store, "#A8A29E"), width=2),
                mode="lines",
            ))
        fig2 = apply_plotly_theme(fig2)
        fig2 = apply_korean_yaxis(fig2)
        fig2.update_layout(height=350)
        st.plotly_chart(fig2, use_container_width=True)

    # 카페24 순방문자수 / 전환율
    cafe24_data = ds[ds["채널"] == "카페24"]
    if not cafe24_data.empty and cafe24_data["순방문자수"].sum() > 0:
        st.markdown('<div class="section-title">카페24 순방문자수 / 전환율</div>', unsafe_allow_html=True)

        cafe_stores = cafe24_data["스토어"].unique().tolist()
        sel_store = st.selectbox("카페24 스토어", ["전체"] + cafe_stores, key="cafe_store")

        if sel_store != "전체":
            vdata = cafe24_data[cafe24_data["스토어"] == sel_store]
        else:
            vdata = cafe24_data

        vdaily = vdata.groupby("날짜").agg({"순방문자수": "sum", "주문건수": "sum"}).reset_index()
        vdaily["전환율"] = (vdaily["주문건수"] / vdaily["순방문자수"].replace(0, 1) * 100).round(2)

        fig3 = make_subplots(specs=[[{"secondary_y": True}]])
        fig3.add_trace(go.Bar(x=vdaily["날짜"], y=vdaily["순방문자수"], name="순방문자수",
                              marker_color="#D97757", opacity=0.6), secondary_y=False)
        fig3.add_trace(go.Scatter(x=vdaily["날짜"], y=vdaily["전환율"], name="전환율",
                                  line=dict(color="#7B8DBF", width=2), mode="lines+markers",
                                  marker=dict(size=4)), secondary_y=True)
        fig3.update_yaxes(title_text="순방문자수", secondary_y=False)
        fig3.update_yaxes(title_text="전환율 (%)", secondary_y=True)
        fig3 = apply_plotly_theme(fig3)
        fig3 = apply_korean_yaxis(fig3)
        fig3.update_layout(height=350)
        st.plotly_chart(fig3, use_container_width=True)

    # 스토어별 기간 요약
    st.markdown('<div class="section-title">스토어별 기간 요약</div>', unsafe_allow_html=True)
    if not store_summary.empty:
        cols = st.columns(min(len(store_summary), 5))
        for i, (_, row) in enumerate(store_summary.iterrows()):
            with cols[i % 5]:
                st.markdown(f"""
                <div style='background: #FFFFFF; border: 1px solid #E8E4DE; border-radius: 12px;
                            padding: 16px; border-top: 3px solid {STORE_COLORS.get(row["스토어"], "#A8A29E")};'>
                    <div style='font-size: 0.85rem; font-weight: 600; color: #3D3B38; margin-bottom: 8px;'>{row["스토어"]}</div>
                </div>
                """, unsafe_allow_html=True)
                st.metric("매출", fmt(row["매출"]))
                st.metric("주문건수", f"{int(row['주문건수']):,}건")
                aov = int(row["매출"] / max(1, row["주문건수"]))
                st.metric("객단가", fmt(aov))


# ══════════════════════════════════════════════
# PAGE 5: 설정
# ══════════════════════════════════════════════
elif page == "⚙️ 설정":
    st.markdown("""
    <div style='margin-bottom: 8px;'>
        <span style='font-size: 1.6rem; font-weight: 700; color: #2D2B28;'>설정</span>
    </div>
    """, unsafe_allow_html=True)

    tab_api, tab_upload, tab_manual, tab_target, tab_brand = st.tabs(
        ["API 연동", "엑셀 업로드", "기타 광고비", "월 목표", "브랜드 매핑"]
    )

    with tab_api:
        st.markdown('<div class="section-title">판매채널 API 연동</div>', unsafe_allow_html=True)
        st.caption("`.env` 파일에 API 키를 입력하면 자동으로 연결됩니다.")

        # 카페24
        from api.cafe24 import Cafe24Client, authenticate_store
        cafe24_stores = [("아자차(카페24)", "linkport"), ("반드럽(카페24)", "linkport3"), ("웰바이오젠(카페24)", "linkport5")]
        if is_configured("cafe24"):
            for i, (store, mall_id) in enumerate(cafe24_stores):
                client = Cafe24Client(mall_id, store)
                with st.expander(store, expanded=(not client.is_authenticated())):
                    if client.is_authenticated():
                        st.success("인증 완료")
                    else:
                        st.warning("인증 필요")
                        auth_url = client.get_auth_url()
                        st.code(auth_url, language=None)
                        code = st.text_input("인증 코드", key=f"cafe24_code_{i}")
                        if code and st.button(f"인증하기", key=f"cafe24_auth_{i}"):
                            if authenticate_store(store, code):
                                st.success("인증 성공!")
                                st.rerun()

        # 스마트스토어
        with st.expander("마르문 (스마트스토어)"):
            if is_configured("smartstore"):
                st.success("연결됨")
            else:
                st.warning("미설정")

        # 쿠팡
        with st.expander("링포 (쿠팡)"):
            if is_configured("coupang"):
                st.success("연결됨")
            else:
                st.warning("미설정")

        st.divider()
        st.markdown('<div class="section-title">광고채널 API 연동</div>', unsafe_allow_html=True)

        for channel, service in [("Meta 광고", "meta"), ("네이버 검색광고", "naver_sa")]:
            with st.expander(channel):
                if is_configured(service):
                    st.success("연결됨")
                else:
                    st.warning("미설정")

        if st.button("🔄 데이터 새로고침", type="primary"):
            st.cache_data.clear()
            st.rerun()

    with tab_upload:
        st.markdown('<div class="section-title">쿠팡 광고 엑셀 업로드</div>', unsafe_allow_html=True)
        st.caption("쿠팡 광고센터에서 다운받은 엑셀 파일을 올려주세요.")

        uploaded_files = st.file_uploader("엑셀 선택 (여러 파일 가능)", type=["xlsx", "xls"], accept_multiple_files=True)
        if uploaded_files:
            from brand_config import detect_brand
            total_saved = 0
            for uploaded_file in uploaded_files:
                try:
                    udf = pd.read_excel(uploaded_file)
                    cols = list(udf.columns)
                    # 형식 자동 인식
                    camp_col = '캠페인명' if '캠페인명' in cols else ('캠페인 이름' if '캠페인 이름' in cols else None)
                    cost_col = '광고비' if '광고비' in cols else ('집행 광고비' if '집행 광고비' in cols else None)
                    date_col = '날짜' if '날짜' in cols else cols[0]
                    if not camp_col or not cost_col:
                        raise ValueError(f"필수 컬럼 없음 (캠페인/광고비). 컬럼: {cols[:8]}")

                    udf["_브랜드"] = udf[camp_col].apply(lambda x: detect_brand(str(x)) or "기타")
                    udf["_광고비"] = pd.to_numeric(udf[cost_col], errors='coerce').fillna(0).astype(int)
                    imp_col = '노출수' if '노출수' in cols else None
                    click_col = '클릭수' if '클릭수' in cols else None
                    conv_col = '총 주문수(1일)' if '총 주문수(1일)' in cols else None
                    conv_rev_col = ('총 전환매출액(1일)' if '총 전환매출액(1일)' in cols
                                    else ('첫구매를 통한 광고 전환 매출' if '첫구매를 통한 광고 전환 매출' in cols else None))
                    metric_cols = [c for c in [imp_col, click_col, conv_col, conv_rev_col] if c]
                    for c in metric_cols:
                        udf[f"_{c}"] = pd.to_numeric(udf[c], errors='coerce').fillna(0).astype(int)

                    agg_dict = {"_광고비": "sum"}
                    for c in metric_cols:
                        agg_dict[f"_{c}"] = "sum"
                    daily = udf.groupby([date_col, "_브랜드"]).agg(agg_dict).reset_index()

                    _upload_conn = sqlite3.connect("dashboard_data.db")
                    for _, row in daily.iterrows():
                        d = row[date_col]
                        ds = str(int(d)) if not isinstance(d, str) else str(d).strip()
                        if len(ds) == 8 and ds.isdigit():
                            formatted_date = f"{ds[:4]}-{ds[4:6]}-{ds[6:8]}"
                        else:
                            formatted_date = pd.to_datetime(d).date().isoformat()
                        ad = int(row["_광고비"])
                        if ad <= 0:
                            continue
                        imp = int(row[f"_{imp_col}"]) if imp_col else 0
                        click = int(row[f"_{click_col}"]) if click_col else 0
                        conv = int(row[f"_{conv_col}"]) if conv_col else 0
                        conv_rev = int(row[f"_{conv_rev_col}"]) if conv_rev_col else 0
                        _upload_conn.execute(
                            """INSERT OR REPLACE INTO ads
                               (날짜, 광고채널, 광고비, 노출수, 클릭수, 전환수, 전환매출, 브랜드)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                            (formatted_date, "쿠팡", ad, imp, click, conv, conv_rev, row["_브랜드"]),
                        )
                    _upload_conn.commit()
                    _upload_conn.close()
                    total_saved += len(daily)
                    st.success(f"'{uploaded_file.name}' - {len(daily)}건 저장")
                except Exception as e:
                    st.error(f"'{uploaded_file.name}' 처리 실패: {e}")

            if total_saved > 0:
                if st.button("대시보드 새로고침", type="primary"):
                    st.cache_data.clear()
                    st.rerun()

        st.markdown("---")
        st.markdown('<div class="section-title">광고일지 업로드 (카페제휴/바이럴)</div>', unsafe_allow_html=True)
        st.caption("링포 광고일지_YYYY.xlsx 파일을 올리면 각 브랜드의 카페제휴/바이럴/기타 광고비를 자동 추출합니다.")
        diary_file = st.file_uploader("링포 광고일지 xlsx", type=["xlsx"], key="diary_upload")
        if diary_file:
            try:
                import tempfile
                from import_cafe_affiliate import extract_from_sheet, LABEL_TO_BRAND
                # 연도 추출 (파일명에서)
                import re as _re
                m = _re.search(r'(\d{4})', diary_file.name)
                year = int(m.group(1)) if m else 2026
                # 임시 파일에 저장
                with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
                    tmp.write(diary_file.getvalue())
                    tmp_path = tmp.name
                xls = pd.ExcelFile(tmp_path)
                sheets = [s for s in xls.sheet_names if '월' in s and '정산' not in s and '목표' not in s]
                all_rows = []
                for sh in sheets:
                    try:
                        month = int(sh.replace('월', '').strip())
                    except:
                        continue
                    rows = extract_from_sheet(tmp_path, sh, year, month)
                    all_rows.extend(rows)
                os.unlink(tmp_path)

                # 적재
                _diary_conn = sqlite3.connect("dashboard_data.db")
                _diary_conn.execute(f"DELETE FROM ads WHERE 광고채널='기타' AND substr(날짜,1,4)='{year}'")
                _diary_conn.execute(f"DELETE FROM sales WHERE 채널='기타' AND substr(날짜,1,4)='{year}'")
                agg = {}
                for r in all_rows:
                    k = (r['date'], r['brand'])
                    if k not in agg:
                        agg[k] = {'ad': 0, 'rev': 0}
                    agg[k]['ad'] += r['ad']
                    agg[k]['rev'] += r['rev']
                inserted = 0
                for (d, brand), v in agg.items():
                    if v['ad'] > 0:
                        _diary_conn.execute(
                            """INSERT OR REPLACE INTO ads
                               (날짜, 광고채널, 광고비, 노출수, 클릭수, 전환수, 전환매출, 브랜드)
                               VALUES (?,?,?,?,?,?,?,?)""",
                            (d, '기타', v['ad'], 0, 0, 0, 0, brand)
                        )
                        inserted += 1
                    if v['rev'] > 0:
                        _diary_conn.execute(
                            """INSERT OR REPLACE INTO sales
                               (날짜, 스토어, 채널, 주문건수, 매출, 객단가, 순방문자수, 전환율, 브랜드)
                               VALUES (?,?,?,?,?,?,?,?,?)""",
                            (d, f'{brand}(기타)', '기타', 0, v['rev'], 0, 0, 0.0, brand)
                        )
                _diary_conn.commit()
                _diary_conn.close()
                st.success(f"{year}년 카페제휴 데이터 {inserted}건 적재 완료")
                if st.button("대시보드 새로고침", type="primary", key="refresh_diary"):
                    st.cache_data.clear()
                    st.rerun()
            except Exception as e:
                st.error(f"처리 실패: {e}")

    with tab_manual:
        st.markdown('<div class="section-title">기타 광고비 입력</div>', unsafe_allow_html=True)
        st.caption("API로 가져올 수 없는 광고비를 직접 입력하세요.")

        _man_conn = sqlite3.connect("dashboard_data.db")

        _man_mode = st.radio("입력 단위", ["월별", "일별"], horizontal=True, key="man_mode")

        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            if _man_mode == "월별":
                _man_period = st.text_input("월 (YYYY-MM)", value=_today_kst().strftime("%Y-%m"), key="man_period_m")
            else:
                _man_period = st.date_input("날짜", value=_today_kst(), key="man_period_d", format="YYYY/MM/DD")
        with col2:
            _man_channel = st.text_input("광고 채널명", value="카페 광고", key="man_channel")
        with col3:
            _man_brand = st.selectbox("브랜드", ["아자차", "반드럽", "웰바이오젠", "윈토르", "자르오", "기타"], key="man_brand")
        with col4:
            _man_cost = st.number_input("광고비 (원)", min_value=0, value=0, step=10000, key="man_cost")
        with col5:
            _man_rev = st.number_input("전환매출 (원)", min_value=0, value=0, step=10000, key="man_rev")

        if st.button("광고비 추가", type="primary", key="add_manual"):
            if _man_channel and _man_cost > 0:
                if _man_mode == "월별":
                    _man_date = f"{_man_period}-01"
                    _label = _man_period
                else:
                    _man_date = _man_period.isoformat()
                    _label = _man_date
                _man_conn.execute(
                    """INSERT OR REPLACE INTO ads (날짜, 광고채널, 광고비, 노출수, 클릭수, 전환수, 전환매출, 브랜드)
                       VALUES (?, ?, ?, 0, 0, 0, ?, ?)""",
                    (_man_date, _man_channel, _man_cost, _man_rev, _man_brand),
                )
                _man_conn.commit()
                st.success(f"{_label} {_man_channel} {_man_cost:,}원 추가!")
                st.rerun()

        _man_existing = pd.read_sql_query(
            "SELECT 날짜, 광고채널, 브랜드, 광고비, 전환매출 FROM ads WHERE 광고채널 NOT IN ('Meta', 'Naver SA', '쿠팡 광고') ORDER BY 날짜 DESC",
            _man_conn,
        )
        if not _man_existing.empty:
            st.divider()
            st.markdown("**입력된 기타 광고비**")
            st.dataframe(_man_existing, width="stretch", hide_index=True)
        _man_conn.close()

    with tab_target:
        st.markdown('<div class="section-title">월별 매출 목표</div>', unsafe_allow_html=True)
        st.caption("목표 금액을 직접 수정하고 '저장' 버튼을 누르세요.")

        _tgt_conn = sqlite3.connect("dashboard_data.db")
        _tgt_conn.execute("CREATE TABLE IF NOT EXISTS monthly_targets (월 TEXT PRIMARY KEY, 목표매출 INTEGER)")
        _tgt_existing = pd.read_sql_query("SELECT * FROM monthly_targets ORDER BY 월", _tgt_conn)
        if _tgt_existing.empty:
            _tgt_existing = pd.DataFrame([{"월": "2026-04", "목표매출": 35000000}])

        _tgt_edited = st.data_editor(
            _tgt_existing, width="stretch", hide_index=True, num_rows="dynamic",
            column_config={
                "월": st.column_config.TextColumn("월 (YYYY-MM)"),
                "목표매출": st.column_config.NumberColumn("목표 매출", format="₩%d"),
            },
        )
        if st.button("목표 저장", type="primary"):
            _tgt_conn.execute("DELETE FROM monthly_targets")
            for _, row in _tgt_edited.iterrows():
                if row["월"] and row["목표매출"]:
                    _tgt_conn.execute("INSERT OR REPLACE INTO monthly_targets (월, 목표매출) VALUES (?, ?)",
                                      (str(row["월"]), int(row["목표매출"])))
            _tgt_conn.commit()
            st.success("저장 완료!")
            st.rerun()
        _tgt_conn.close()

    with tab_brand:
        st.markdown('<div class="section-title">브랜드 매핑 관리</div>', unsafe_allow_html=True)
        st.caption("상품명/캠페인명에서 브랜드를 자동 분류합니다.")

        _br_conn = sqlite3.connect("dashboard_data.db")
        _br_conn.execute("CREATE TABLE IF NOT EXISTS brand_mapping (키워드 TEXT PRIMARY KEY, 브랜드 TEXT, 플랫폼 TEXT, 유형 TEXT)")

        st.markdown("**키워드 → 브랜드 매핑**")
        _kw_df = pd.read_sql_query(
            "SELECT 키워드, 브랜드 FROM brand_mapping WHERE 플랫폼 = '전체' ORDER BY 브랜드, 키워드", _br_conn)
        if _kw_df.empty:
            _kw_df = pd.DataFrame(columns=["키워드", "브랜드"])

        _kw_edited = st.data_editor(
            _kw_df, width="stretch", hide_index=True, num_rows="dynamic",
            column_config={
                "키워드": st.column_config.TextColumn("키워드"),
                "브랜드": st.column_config.SelectboxColumn("브랜드",
                    options=["아자차", "반드럽", "웰바이오젠", "윈토르", "자르오", "기타"]),
            },
        )
        if st.button("키워드 매핑 저장", type="primary", key="save_kw"):
            _br_conn.execute("DELETE FROM brand_mapping WHERE 플랫폼 = '전체'")
            for _, row in _kw_edited.iterrows():
                if row["키워드"] and row["브랜드"]:
                    _br_conn.execute(
                        "INSERT OR REPLACE INTO brand_mapping (키워드, 브랜드, 플랫폼, 유형) VALUES (?,?,?,?)",
                        (str(row["키워드"]), str(row["브랜드"]), "전체", "키워드"))
            _br_conn.commit()
            st.success("저장 완료!")

        st.divider()
        st.markdown("**네이버 검색광고 캠페인 매핑**")
        _sa_df = pd.read_sql_query(
            "SELECT 키워드 as 캠페인ID, 브랜드, 유형 as 캠페인명 FROM brand_mapping WHERE 플랫폼 = 'Naver SA' ORDER BY 브랜드", _br_conn)
        if not _sa_df.empty:
            _sa_df["캠페인명"] = _sa_df["캠페인명"].str.replace("캠페인: ", "", regex=False)

        _sa_edited = st.data_editor(
            _sa_df, width="stretch", hide_index=True, num_rows="dynamic",
            column_config={
                "캠페인ID": st.column_config.TextColumn("캠페인 ID", disabled=True),
                "캠페인명": st.column_config.TextColumn("캠페인명", disabled=True),
                "브랜드": st.column_config.SelectboxColumn("브랜드",
                    options=["아자차", "반드럽", "웰바이오젠", "윈토르", "자르오", "기타"]),
            },
        )
        if st.button("캠페인 매핑 저장", type="primary", key="save_sa"):
            _br_conn.execute("DELETE FROM brand_mapping WHERE 플랫폼 = 'Naver SA'")
            for _, row in _sa_edited.iterrows():
                if row["캠페인ID"] and row["브랜드"]:
                    _br_conn.execute(
                        "INSERT OR REPLACE INTO brand_mapping (키워드, 브랜드, 플랫폼, 유형) VALUES (?,?,?,?)",
                        (str(row["캠페인ID"]), str(row["브랜드"]), "Naver SA", f"캠페인: {row['캠페인명']}"))
            _br_conn.commit()
            st.success("저장 완료!")
        _br_conn.close()
