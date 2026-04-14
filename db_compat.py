"""sqlite3 호환 인터페이스 (Postgres 백엔드)
- demo.py 등 기존 sqlite3 코드를 최소 변경으로 Postgres 사용 가능하게 해줌
- `?` placeholder → `%s` 자동 변환
- `INSERT OR REPLACE` → `INSERT ... ON CONFLICT DO UPDATE` 변환
"""
import os
import re
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(interpolate=False)


def _get_db_url():
    try:
        import streamlit as st
        if hasattr(st, "secrets") and "SUPABASE_DB_URL" in st.secrets:
            return st.secrets["SUPABASE_DB_URL"]
    except Exception:
        pass
    return os.getenv("SUPABASE_DB_URL", "")


# 테이블별 PK (INSERT OR REPLACE 변환용)
_TABLE_PK = {
    "sales": ["날짜", "스토어", "브랜드"],
    "ads": ["날짜", "광고채널", "브랜드"],
    "fetch_log": ["서비스", "날짜"],
    "monthly_targets": ["월"],
    "brand_mapping": ["키워드"],
}


def _translate_sql(sql: str) -> str:
    s = sql

    # CREATE TABLE 변환
    if re.search(r'^\s*CREATE\s+TABLE', s, re.IGNORECASE):
        s = re.sub(r'\bINTEGER\s+PRIMARY\s+KEY\b', 'BIGINT PRIMARY KEY', s, flags=re.IGNORECASE)
        # SQLite-specific 무시할 부분 (대부분 그대로 작동)

    # INSERT OR REPLACE INTO tbl (...) VALUES → INSERT INTO ... ON CONFLICT DO UPDATE
    m = re.match(
        r'\s*INSERT\s+OR\s+REPLACE\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)\s*$',
        s, re.IGNORECASE | re.DOTALL,
    )
    if m:
        tbl = m.group(1)
        cols_str = m.group(2)
        vals_str = m.group(3)
        cols = [c.strip() for c in cols_str.split(',')]
        pk = _TABLE_PK.get(tbl, [cols[0]])
        non_pk = [c for c in cols if c not in pk]
        update_clause = ", ".join(f"{c}=EXCLUDED.{c}" for c in non_pk) or f"{cols[0]}=EXCLUDED.{cols[0]}"
        pk_clause = ", ".join(pk)
        s = (f"INSERT INTO {tbl} ({cols_str}) VALUES ({vals_str}) "
             f"ON CONFLICT ({pk_clause}) DO UPDATE SET {update_clause}")

    # ? placeholder → %s
    s = s.replace('?', '%s')

    return s


class _Cursor:
    def __init__(self, cur):
        self._cur = cur

    def execute(self, sql, params=None):
        translated = _translate_sql(sql)
        self._cur.execute(translated, params or ())
        return self

    def executemany(self, sql, seq):
        translated = _translate_sql(sql)
        self._cur.executemany(translated, seq)
        return self

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    def fetchmany(self, size=None):
        return self._cur.fetchmany(size) if size else self._cur.fetchmany()

    def close(self):
        self._cur.close()

    def __iter__(self):
        return iter(self._cur)

    def __getattr__(self, name):
        # description, rowcount, arraysize 등 DBAPI 속성 forwarding
        return getattr(self._cur, name)


class _Conn:
    def __init__(self):
        self._conn = psycopg2.connect(_get_db_url())

    def execute(self, sql, params=None):
        cur = self._conn.cursor()
        c = _Cursor(cur).execute(sql, params)
        return c

    def cursor(self):
        return _Cursor(self._conn.cursor())

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __getattr__(self, name):
        return getattr(self._conn, name)


def connect(_path=None):
    """sqlite3.connect() 호환 - 경로 인자 무시하고 Postgres 연결"""
    return _Conn()
