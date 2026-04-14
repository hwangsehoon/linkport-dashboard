"""SQLite → Supabase Postgres 마이그레이션
- sales, ads, fetch_log 테이블 스키마 생성
- 모든 데이터 옮김 (배치 INSERT)
"""
import os
import sqlite3
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(interpolate=False, override=True)

SQLITE_DB = "dashboard_data.db"
PG_URL = os.getenv("SUPABASE_DB_URL")

SCHEMA = """
DROP TABLE IF EXISTS sales CASCADE;
DROP TABLE IF EXISTS ads CASCADE;
DROP TABLE IF EXISTS fetch_log CASCADE;

CREATE TABLE sales (
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
);

CREATE TABLE ads (
    날짜 TEXT NOT NULL,
    광고채널 TEXT NOT NULL,
    광고비 BIGINT DEFAULT 0,
    노출수 BIGINT DEFAULT 0,
    클릭수 INTEGER DEFAULT 0,
    전환수 INTEGER DEFAULT 0,
    전환매출 BIGINT DEFAULT 0,
    브랜드 TEXT DEFAULT '',
    PRIMARY KEY (날짜, 광고채널, 브랜드)
);

CREATE TABLE fetch_log (
    서비스 TEXT NOT NULL,
    날짜 TEXT NOT NULL,
    조회시각 TEXT,
    PRIMARY KEY (서비스, 날짜)
);

CREATE INDEX idx_sales_date ON sales(날짜);
CREATE INDEX idx_sales_brand ON sales(브랜드);
CREATE INDEX idx_ads_date ON ads(날짜);
CREATE INDEX idx_ads_brand ON ads(브랜드);
CREATE INDEX idx_ads_channel ON ads(광고채널);
"""


def main():
    print("[Postgres 스키마 생성]")
    pg = psycopg2.connect(PG_URL)
    pg.autocommit = True
    cur = pg.cursor()
    for stmt in SCHEMA.strip().split(';'):
        s = stmt.strip()
        if s:
            cur.execute(s)
    print("  스키마 OK")

    sl = sqlite3.connect(SQLITE_DB)
    sl.row_factory = sqlite3.Row

    # sales
    rows = sl.execute("SELECT * FROM sales").fetchall()
    print(f"\n[sales] {len(rows)}건 마이그레이션 중...")
    data = [(r['날짜'], r['스토어'], r['채널'], r['주문건수'], r['매출'],
             r['객단가'], r['순방문자수'], r['전환율'], r['브랜드'] or '')
            for r in rows]
    psycopg2.extras.execute_values(
        cur,
        """INSERT INTO sales (날짜,스토어,채널,주문건수,매출,객단가,순방문자수,전환율,브랜드)
           VALUES %s ON CONFLICT (날짜,스토어,브랜드) DO UPDATE SET
           채널=EXCLUDED.채널, 주문건수=EXCLUDED.주문건수, 매출=EXCLUDED.매출,
           객단가=EXCLUDED.객단가, 순방문자수=EXCLUDED.순방문자수, 전환율=EXCLUDED.전환율""",
        data, page_size=500
    )
    print(f"  적재 완료")

    # ads
    rows = sl.execute("SELECT * FROM ads").fetchall()
    print(f"\n[ads] {len(rows)}건 마이그레이션 중...")
    data = [(r['날짜'], r['광고채널'], r['광고비'], r['노출수'], r['클릭수'],
             r['전환수'], r['전환매출'], r['브랜드'] or '')
            for r in rows]
    psycopg2.extras.execute_values(
        cur,
        """INSERT INTO ads (날짜,광고채널,광고비,노출수,클릭수,전환수,전환매출,브랜드)
           VALUES %s ON CONFLICT (날짜,광고채널,브랜드) DO UPDATE SET
           광고비=EXCLUDED.광고비, 노출수=EXCLUDED.노출수, 클릭수=EXCLUDED.클릭수,
           전환수=EXCLUDED.전환수, 전환매출=EXCLUDED.전환매출""",
        data, page_size=500
    )
    print(f"  적재 완료")

    # fetch_log
    rows = sl.execute("SELECT * FROM fetch_log").fetchall()
    print(f"\n[fetch_log] {len(rows)}건 마이그레이션 중...")
    data = [(r['서비스'], r['날짜'], r['조회시각']) for r in rows]
    psycopg2.extras.execute_values(
        cur,
        """INSERT INTO fetch_log (서비스,날짜,조회시각) VALUES %s
           ON CONFLICT (서비스,날짜) DO UPDATE SET 조회시각=EXCLUDED.조회시각""",
        data, page_size=500
    )
    print(f"  적재 완료")

    # 검증
    print("\n=== 검증 ===")
    for tbl in ['sales','ads','fetch_log']:
        cur.execute(f"SELECT COUNT(*) FROM {tbl}")
        n_pg = cur.fetchone()[0]
        n_sl = sl.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  {tbl}: SQLite {n_sl}, Postgres {n_pg} {'✓' if n_pg==n_sl else '✗'}")

    sl.close()
    pg.close()


if __name__ == "__main__":
    main()
