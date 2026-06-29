# -*- coding: utf-8 -*-
"""
월별 반품 집계 (반품 분석용)

채널별 월별 구매건수/반품건수를 모아 monthly_returns 테이블에 저장한다.
대시보드(배포)는 이 테이블만 읽고, 수집은 로컬 동기화에서 돈다
(쿠팡·스마트스토어는 허용 IP에서만 됨 — 매출 수집과 동일 제약).

  포함: 카페24(웰바이오젠/반드럽/아자차), 스마트스토어(마르문), 쿠팡 '일반(마켓플레이스)'
  제외: 쿠팡 '로켓그로스' — 표준 API에 반품 없음 + 윙은 봇차단으로 자동수집 불안정
        → 페이지에 "쿠팡은 로켓 제외" 반드시 명시
"""
import calendar
import time
import urllib.parse
from datetime import date

import requests
import pandas as pd

from api.cafe24 import Cafe24Client, CAFE24_MALLS
from api.coupang import CoupangClient, COUPANG_VENDOR_ID
from api.db import _get_conn
from brand_config import detect_brand

# 카페24 스토어 → 브랜드
_CAFE24_BRAND = {
    "아자차(카페24)": "아자차",
    "반드럽(카페24)": "반드럽",
    "웰바이오젠(카페24)": "웰바이오젠",
}


def _month_bounds(year, m):
    last = calendar.monthrange(year, m)[1]
    return date(year, m, 1), date(year, m, last)


# ── 카페24 ─────────────────────────────────────────────
def _cafe24_orders(cli, start, end):
    """카페24 admin/orders 원본 주문 목록 (canceled/return_confirmed_date 포함)."""
    url = f"https://{cli.mall_id}.cafe24api.com/api/v2/admin/orders"
    headers = {"Authorization": f"Bearer {cli.access_token}"}
    out, offset, limit = [], 0, 1000
    while True:
        r = requests.get(url, headers=headers, params={
            "start_date": start.isoformat(), "end_date": end.isoformat(),
            "limit": limit, "offset": offset,
        }, timeout=30)
        if r.status_code != 200:
            break
        batch = r.json().get("orders", [])
        out.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return out


def cafe24_returns(year, months):
    rows = []
    for store, mall in CAFE24_MALLS.items():
        if not mall:
            continue
        cli = Cafe24Client(mall, store)
        if not cli.is_authenticated():
            continue
        brand = _CAFE24_BRAND.get(store, store)
        for m in months:
            s, e = _month_bounds(year, m)
            orders = _cafe24_orders(cli, s, e)
            total = len(orders)
            returns = sum(1 for o in orders if o.get("return_confirmed_date") or o.get("canceled") == "T")
            rows.append({"년": year, "월": m, "채널": "카페24", "브랜드": brand,
                         "구매건수": total, "반품건수": returns})
    return rows


# ── 쿠팡 일반(마켓플레이스) — 로켓 제외 ──────────────────
def coupang_market_returns(year, months):
    cli = CoupangClient()
    path = f"/v2/providers/openapi/apis/api/v4/vendors/{COUPANG_VENDOR_ID}/returnRequests"
    rows = []
    for m in months:
        s, e = _month_bounds(year, m)
        # 구매(주문) — 일반 마켓플레이스만 (fetch_marketplace는 로켓 제외)
        mk = cli.fetch_marketplace(s, e)
        buy = int(mk["주문건수"].sum()) if not mk.empty else 0
        # 반품 — returnRequests (상태별 합집합, orderId 중복제거)
        ret_ids = set()
        for st in ["RU", "CC", "PR", "UC"]:
            params = {"searchType": "timeFrame",
                      "createdAtFrom": f"{s}T00:00", "createdAtTo": f"{e}T23:59",
                      "status": st, "maxPerPage": 50}
            for _ in range(4):
                h = cli._generate_headers("GET", path, params)
                r = requests.get(f"{cli.BASE_URL}{path}?{urllib.parse.urlencode(params)}",
                                 headers=h, timeout=30)
                if r.status_code == 429:
                    time.sleep(3)
                    continue
                break
            try:
                for x in (r.json().get("data") or []):
                    ret_ids.add(x.get("orderId"))
            except Exception:
                pass
            time.sleep(0.3)
        rows.append({"년": year, "월": m, "채널": "쿠팡(일반)", "브랜드": "전체",
                     "구매건수": buy, "반품건수": len(ret_ids)})
    return rows


# ── 스마트스토어(마르문) ────────────────────────────────
def smartstore_returns(year, months):
    from api.smartstore import SmartStoreClient
    cli = SmartStoreClient()
    if not cli.authenticate():
        return []
    rows = []
    for m in months:
        s, e = _month_bounds(year, m)
        ids = []
        cur = s
        while cur <= e:
            ids += [x.get("productOrderId") for x in cli._fetch_day_orders(cur) if x.get("productOrderId")]
            cur = date.fromordinal(cur.toordinal() + 1)
            time.sleep(0.25)
        ids = list({i for i in ids if i})
        total = retn = 0
        for i in range(0, len(ids), 100):
            batch = ids[i:i + 100]
            try:
                resp = requests.post(f"{cli.BASE_URL}/v1/pay-order/seller/product-orders/query",
                                     headers=cli._headers(), json={"productOrderIds": batch}, timeout=30)
                if resp.status_code == 429:
                    time.sleep(2)
                    resp = requests.post(f"{cli.BASE_URL}/v1/pay-order/seller/product-orders/query",
                                         headers=cli._headers(), json={"productOrderIds": batch}, timeout=30)
                details = resp.json().get("data", [])
            except Exception:
                details = []
            for o in details:
                po = o.get("productOrder", {})
                st = po.get("productOrderStatus", "")
                cs = po.get("claimStatus", "") or ""
                # 주문일이 해당 월인 것만 카운트
                od = (po.get("placeOrderDate") or po.get("paymentDate") or "")[:10]
                if not od.startswith(f"{year}-{m:02d}"):
                    continue
                total += 1
                if st == "RETURNED" or cs in ("RETURN_DONE", "RETURN_REQUEST", "RETURN_REJECT", "COLLECT_DONE"):
                    retn += 1
            time.sleep(0.3)
        rows.append({"년": year, "월": m, "채널": "스마트스토어", "브랜드": "아자차",
                     "구매건수": total, "반품건수": retn})
    return rows


# ── 통합 수집 + 저장/조회 ──────────────────────────────
def collect_returns(year, months, include_smartstore=True):
    rows = []
    rows += cafe24_returns(year, months)
    rows += coupang_market_returns(year, months)
    if include_smartstore:
        try:
            rows += smartstore_returns(year, months)
        except Exception as e:
            print(f"[반품] 스마트스토어 수집 실패(스킵): {e}")
    return pd.DataFrame(rows)


def _ensure_table(cur):
    cur.execute("""CREATE TABLE IF NOT EXISTS monthly_returns (
        년 INT, 월 INT, 채널 TEXT, 브랜드 TEXT,
        구매건수 INT, 반품건수 INT,
        PRIMARY KEY (년, 월, 채널, 브랜드))""")


def save_monthly_returns(df):
    if df is None or df.empty:
        return
    conn = _get_conn(); cur = conn.cursor()
    _ensure_table(cur)
    import psycopg2.extras
    rows = [(int(r["년"]), int(r["월"]), r["채널"], r["브랜드"],
             int(r["구매건수"]), int(r["반품건수"])) for _, r in df.iterrows()]
    psycopg2.extras.execute_values(cur,
        """INSERT INTO monthly_returns (년,월,채널,브랜드,구매건수,반품건수) VALUES %s
           ON CONFLICT (년,월,채널,브랜드) DO UPDATE SET
           구매건수=EXCLUDED.구매건수, 반품건수=EXCLUDED.반품건수""", rows)
    conn.commit(); conn.close()


def load_monthly_returns(year):
    conn = _get_conn(); cur = conn.cursor()
    _ensure_table(cur)
    cur.execute("SELECT 년,월,채널,브랜드,구매건수,반품건수 FROM monthly_returns WHERE 년=%s", (year,))
    data = cur.fetchall(); conn.close()
    return pd.DataFrame(data, columns=["년", "월", "채널", "브랜드", "구매건수", "반품건수"])
