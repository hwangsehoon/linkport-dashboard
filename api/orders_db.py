# -*- coding: utf-8 -*-
"""주문 이력 DB (재구매 분석용) — 바탕화면 엑셀 의존을 없앤다.

order_history 테이블에 3개 카페24 스토어 + 스마트스토어 주문을 '건별'로 저장하고,
build_performance 가 이 테이블에서 재구매(식별주문/재구매주문)를 계산한다.
파일 삭제/월말 수동추출로 데이터가 조용히 틀어지던 문제를 원천 제거한다.

- 백필:    sync_orders(date(2023,1,1), date.today())   # 최초 1회
- 증분:    sync_orders(date.today()-60일, date.today()) # 동기화에서 반복 호출
- 재구매:  load_orders()  → build_performance._orders() 가 쓰는 것과 동일한 DataFrame
"""
from datetime import date, timedelta
import calendar

import pandas as pd
from psycopg2.extras import execute_values

from api.db import _get_conn
# 데이터 추출 로직은 export_orders 의 검증된 함수를 그대로 재사용(중복/불일치 방지)
from export_orders import fetch_month, fetch_smartstore, _norm_phone, _f, STORES
from api.cafe24 import Cafe24Client

SS = "마르문(스마트스토어)"   # build_performance.SS 와 동일해야 함
TABLE = "order_history"


def ensure_table():
    c = _get_conn(); cur = c.cursor()
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        스토어 TEXT, 주문번호 TEXT, 주문일 DATE, 회원ID TEXT,
        주문자명 TEXT, 주문자휴대폰 TEXT, 주문자이메일 TEXT,
        수령인명 TEXT, 수령인휴대폰 TEXT, 결제금액 BIGINT,
        취소여부 TEXT,
        PRIMARY KEY (스토어, 주문번호))""")
    cur.execute(f"CREATE INDEX IF NOT EXISTS ix_oh_date ON {TABLE}(주문일)")
    c.commit(); c.close()


def upsert_orders(rows):
    """rows: dict 리스트. (스토어,주문번호) 기준 upsert."""
    if not rows:
        return 0
    ensure_table()
    vals = []
    for r in rows:
        od = (r.get("주문일") or "").strip() or None
        vals.append((r["스토어"], str(r["주문번호"]), od, str(r.get("회원ID") or ""),
                     r.get("주문자명") or "", r.get("주문자휴대폰") or "", r.get("주문자이메일") or "",
                     r.get("수령인명") or "", r.get("수령인휴대폰") or "",
                     int(r.get("결제금액") or 0), r.get("취소여부") or ""))
    c = _get_conn(); cur = c.cursor()
    execute_values(cur, f"""INSERT INTO {TABLE}
        (스토어,주문번호,주문일,회원ID,주문자명,주문자휴대폰,주문자이메일,수령인명,수령인휴대폰,결제금액,취소여부)
        VALUES %s ON CONFLICT (스토어,주문번호) DO UPDATE SET
        주문일=EXCLUDED.주문일, 회원ID=EXCLUDED.회원ID, 주문자명=EXCLUDED.주문자명,
        주문자휴대폰=EXCLUDED.주문자휴대폰, 주문자이메일=EXCLUDED.주문자이메일,
        수령인명=EXCLUDED.수령인명, 수령인휴대폰=EXCLUDED.수령인휴대폰,
        결제금액=EXCLUDED.결제금액, 취소여부=EXCLUDED.취소여부""", vals, page_size=500)
    c.commit(); c.close()
    return len(vals)


def _map_cafe24(o, store):
    rc = (o.get("receivers") or [{}])[0]
    by = o.get("buyer") or {}
    actual = o.get("actual_order_amount") or {}
    amount = max(0, int(_f(actual.get("payment_amount") or o.get("payment_amount"))
                        + _f(o.get("naver_point"))))
    od = (o.get("payment_date") or o.get("order_date") or "")[:10]
    return {
        "주문일": od, "스토어": store, "주문번호": o.get("order_id", ""),
        "회원ID": (by.get("member_id") or o.get("member_id") or ""),
        "주문자명": by.get("name") or o.get("billing_name") or "",
        "주문자휴대폰": _norm_phone(by.get("cellphone") or by.get("phone")),
        "주문자이메일": by.get("email") or o.get("member_email") or "",
        "수령인명": rc.get("name", "") or "",
        "수령인휴대폰": _norm_phone(rc.get("cellphone") or rc.get("phone")),
        "결제금액": amount,
        "취소여부": "취소" if o.get("canceled") == "T" else "",
    }


def _months(start, end):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield y, m
        m += 1
        if m > 12:
            y, m = y + 1, 1


def sync_orders(start: date, end: date, include_smartstore: bool = True):
    """[start, end] 기간의 주문을 카페24 3스토어 + 스마트스토어에서 받아 upsert.
    백필은 넓게, 증분은 최근 N일로 호출한다."""
    ensure_table()
    total = 0
    for store, mall in STORES:
        cli = Cafe24Client(mall, store)
        if not cli.is_authenticated():
            print(f"  {store}: 미인증 — 스킵")
            continue
        rows = []
        for (yy, mm) in _months(start, end):
            orders = fetch_month(cli, yy, mm)
            rows += [_map_cafe24(o, store) for o in orders]
        n = upsert_orders(rows)
        total += n
        print(f"  {store}: {n}건 upsert")
    if include_smartstore:
        try:
            ss = fetch_smartstore(max(start, date(2025, 12, 1)), end)
            for r in ss:
                r.setdefault("스토어", SS)
            n = upsert_orders(ss)
            total += n
            print(f"  {SS}: {n}건 upsert")
        except Exception as e:
            print(f"  스마트스토어 스킵: {e}")
    return total


def load_orders():
    """build_performance._orders() 와 '동일한' 식별주문 DataFrame 을 DB에서 만든다.
    반환 컬럼: 스토어, 주문일(datetime), 회원ID, 주문자휴대폰, 고객키 (고객키 비어있으면 제외)."""
    c = _get_conn(); cur = c.cursor()
    cur.execute(f"SELECT 스토어,주문번호,주문일,회원ID,주문자휴대폰,취소여부 FROM {TABLE}")
    rows = cur.fetchall(); c.close()
    if not rows:
        return None
    d = pd.DataFrame(rows, columns=["스토어", "주문번호", "주문일", "회원ID", "주문자휴대폰", "취소여부"])
    for col in ("회원ID", "주문자휴대폰", "주문번호"):
        d[col] = d[col].astype(str).str.strip().replace("nan", "")
    d["취소여부"] = d["취소여부"].fillna("")
    d = d[d["취소여부"] == ""].copy()
    d["주문일"] = pd.to_datetime(d["주문일"], errors="coerce")
    d = d.dropna(subset=["주문일"])
    ss = d[d["스토어"] == SS].drop_duplicates(subset=["주문번호"])
    d = pd.concat([d[d["스토어"] != SS], ss], ignore_index=True)
    d["고객키"] = d.apply(
        lambda r: ("N:" + r["회원ID"]) if r["스토어"] == SS and r["회원ID"]
        else (("P:" + r["주문자휴대폰"]) if r["주문자휴대폰"] else ""), axis=1)
    return d[d["고객키"] != ""].sort_values("주문일").reset_index(drop=True)


if __name__ == "__main__":
    import sys
    a = sys.argv[1:]
    if a and a[0] == "backfill":
        print("백필: 2023-01 ~ 오늘")
        n = sync_orders(date(2023, 1, 1), date.today())
        print(f"완료: 총 {n}건 upsert")
    elif a and a[0] == "sync":
        days = int(a[1]) if len(a) > 1 else 60
        print(f"증분: 최근 {days}일")
        n = sync_orders(date.today() - timedelta(days=days), date.today())
        print(f"완료: 총 {n}건 upsert")
    else:
        print("사용: python -m api.orders_db backfill | sync [days]")
