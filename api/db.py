"""
데이터 저장소 (Postgres on Supabase)
- API에서 받은 데이터를 저장
- 이미 저장된 날짜는 다시 조회하지 않음
"""
import os
import psycopg2
import psycopg2.extras
import pandas as pd
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv(interpolate=False)

# Streamlit secrets 우선 (배포 환경), 없으면 .env
def _get_db_url():
    try:
        import streamlit as st
        if hasattr(st, "secrets") and "SUPABASE_DB_URL" in st.secrets:
            return st.secrets["SUPABASE_DB_URL"]
    except Exception:
        pass
    return os.getenv("SUPABASE_DB_URL", "")


DB_URL = _get_db_url()


def _get_conn():
    return psycopg2.connect(DB_URL)


def init_db():
    """테이블 생성 (이미 있으면 스킵)"""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            날짜 TEXT NOT NULL,
            스토어 TEXT NOT NULL,
            채널 TEXT,
            주문건수 INTEGER DEFAULT 0,
            매출 BIGINT DEFAULT 0,
            객단가 INTEGER DEFAULT 0,
            순방문자수 INTEGER DEFAULT 0,
            전환율 REAL DEFAULT 0,
            브랜드 TEXT DEFAULT '',
            PRIMARY KEY (날짜, 스토어, 브랜드)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ads (
            날짜 TEXT NOT NULL,
            광고채널 TEXT NOT NULL,
            광고비 BIGINT DEFAULT 0,
            노출수 BIGINT DEFAULT 0,
            클릭수 INTEGER DEFAULT 0,
            전환수 INTEGER DEFAULT 0,
            전환매출 BIGINT DEFAULT 0,
            브랜드 TEXT DEFAULT '',
            PRIMARY KEY (날짜, 광고채널, 브랜드)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fetch_log (
            서비스 TEXT NOT NULL,
            날짜 TEXT NOT NULL,
            조회시각 TEXT,
            PRIMARY KEY (서비스, 날짜)
        )
    """)
    conn.commit()
    conn.close()


def get_missing_dates(service: str, start_date: date, end_date: date) -> list:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 날짜 FROM fetch_log WHERE 서비스 = %s", (service,))
    fetched = {row[0] for row in cur.fetchall()}
    conn.close()

    missing = []
    current = start_date
    while current <= end_date:
        if current.isoformat() not in fetched:
            missing.append(current)
        current += timedelta(days=1)
    return missing


def mark_fetched(service: str, dates: list):
    conn = _get_conn()
    cur = conn.cursor()
    now = date.today().isoformat()
    rows = [(service, d.isoformat() if isinstance(d, date) else d, now) for d in dates]
    psycopg2.extras.execute_values(
        cur,
        """INSERT INTO fetch_log (서비스, 날짜, 조회시각) VALUES %s
           ON CONFLICT (서비스, 날짜) DO UPDATE SET 조회시각=EXCLUDED.조회시각""",
        rows
    )
    conn.commit()
    conn.close()


def save_sales(df: pd.DataFrame):
    if df.empty:
        return
    conn = _get_conn()
    cur = conn.cursor()
    rows = []
    for _, row in df.iterrows():
        brand = str(row.get("브랜드", "")) if "브랜드" in df.columns else ""
        rows.append((
            str(row["날짜"]),
            row["스토어"],
            row.get("채널", ""),
            int(row.get("주문건수", 0)),
            int(row.get("매출", 0)),
            int(row.get("객단가", 0)),
            int(row.get("순방문자수", 0)),
            float(row.get("전환율", 0)),
            brand,
        ))
    psycopg2.extras.execute_values(
        cur,
        """INSERT INTO sales (날짜,스토어,채널,주문건수,매출,객단가,순방문자수,전환율,브랜드)
           VALUES %s ON CONFLICT (날짜,스토어,브랜드) DO UPDATE SET
           채널=EXCLUDED.채널, 주문건수=EXCLUDED.주문건수, 매출=EXCLUDED.매출,
           객단가=EXCLUDED.객단가, 순방문자수=EXCLUDED.순방문자수, 전환율=EXCLUDED.전환율""",
        rows, page_size=500
    )
    conn.commit()
    conn.close()


def save_ads(df: pd.DataFrame):
    if df.empty:
        return
    conn = _get_conn()
    cur = conn.cursor()
    rows = []
    for _, row in df.iterrows():
        brand = str(row.get("브랜드", "")) if "브랜드" in df.columns else ""
        rows.append((
            str(row["날짜"]),
            row["광고채널"],
            int(row.get("광고비", 0)),
            int(row.get("노출수", 0)),
            int(row.get("클릭수", 0)),
            int(row.get("전환수", 0)),
            int(row.get("전환매출", 0)),
            brand,
        ))
    psycopg2.extras.execute_values(
        cur,
        """INSERT INTO ads (날짜,광고채널,광고비,노출수,클릭수,전환수,전환매출,브랜드)
           VALUES %s ON CONFLICT (날짜,광고채널,브랜드) DO UPDATE SET
           광고비=EXCLUDED.광고비, 노출수=EXCLUDED.노출수, 클릭수=EXCLUDED.클릭수,
           전환수=EXCLUDED.전환수, 전환매출=EXCLUDED.전환매출""",
        rows, page_size=500
    )
    conn.commit()
    conn.close()


def load_sales(start_date: date, end_date: date) -> pd.DataFrame:
    conn = _get_conn()
    df = pd.read_sql_query(
        "SELECT * FROM sales WHERE 날짜 >= %s AND 날짜 <= %s",
        conn,
        params=(start_date.isoformat(), end_date.isoformat()),
    )
    conn.close()
    if not df.empty:
        df["날짜"] = pd.to_datetime(df["날짜"]).dt.date
    return df


def load_ads(start_date: date, end_date: date) -> pd.DataFrame:
    conn = _get_conn()
    df = pd.read_sql_query(
        "SELECT * FROM ads WHERE 날짜 >= %s AND 날짜 <= %s",
        conn,
        params=(start_date.isoformat(), end_date.isoformat()),
    )
    conn.close()
    if not df.empty:
        df["날짜"] = pd.to_datetime(df["날짜"]).dt.date
    return df


def execute(sql: str, params: tuple = None):
    """범용 실행 (DELETE 등에 사용)"""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(sql, params or ())
    conn.commit()
    conn.close()
