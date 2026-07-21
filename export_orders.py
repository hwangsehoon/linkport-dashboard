# -*- coding: utf-8 -*-
"""
카페24 전체 주문내역 엑셀 추출 (재구매율 분석용)

- 3개 스토어(아자차/반드럽/웰바이오젠) 2026-01-01 ~ 오늘
- 고객 식별자: 회원ID, 주문자명, 수령인명, 휴대폰(cellphone), 이메일
- 시트: [주문내역] 건별 원본 / [재구매율요약] 스토어별·전체 재구매율
- 재구매 판정: 휴대폰번호(숫자만) 기준, 없으면 '이름+이메일' 보조키

사용: python export_orders.py
결과: 바탕화면\링크포트_주문내역_2026.xlsx
"""
import sys, io, re, calendar, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from datetime import date
from collections import Counter

import requests
import pandas as pd

from api.cafe24 import Cafe24Client

STORES = [("아자차(카페24)", "linkport"),
          ("반드럽(카페24)", "linkport3"),
          ("웰바이오젠(카페24)", "linkport5")]
# 재구매 판정 정확도를 위해 개점 시점(2023년)부터 전체 이력을 받는다.
# (과거 구매자가 최근 재구매하면 '신규'로 오분류되는 것 방지 — 실측상 2023-06부터 주문 존재)
START_YEAR, START_MONTH = 2023, 1
OUT = r"C:\Users\조현우\Desktop\링크포트_주문내역_전체.xlsx"


def _norm_phone(s):
    """휴대폰 → 숫자만. 없으면 빈 문자열."""
    if not s:
        return ""
    d = re.sub(r"\D", "", str(s))
    return d if len(d) >= 9 else ""


def _f(v):
    try:
        return float(v or 0)
    except Exception:
        return 0.0


def fetch_month(cli, y, m):
    """한 달치 주문 (buyer=주문자, receivers=수령인 포함), 페이지네이션"""
    last = calendar.monthrange(y, m)[1]
    url = f"https://{cli.mall_id}.cafe24api.com/api/v2/admin/orders"
    headers = {"Authorization": f"Bearer {cli.access_token}"}
    out, offset, limit = [], 0, 1000
    while True:
        r = requests.get(url, headers=headers, params={
            "start_date": f"{y}-{m:02d}-01", "end_date": f"{y}-{m:02d}-{last:02d}",
            "limit": limit, "offset": offset, "embed": "receivers,buyer",
        }, timeout=40)
        if r.status_code != 200:
            print(f"    [{cli.store_name} {y}-{m:02d}] HTTP {r.status_code}")
            break
        batch = r.json().get("orders", [])
        out.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(0.2)
    return out


def fetch_smartstore(start: date, end: date):
    """스마트스토어(마르문) 주문 — 구매자번호(ordererNo)/전화(ordererTel) 포함.
    last-changed-statuses는 하루 단위라 날짜를 순회한다."""
    from api.smartstore import SmartStoreClient
    cli = SmartStoreClient()
    if not cli.authenticate():
        print("  스마트스토어: 인증 실패 — 스킵")
        return []
    ids, cur = set(), start
    while cur <= end:
        for x in cli._fetch_day_orders(cur):
            pid = x.get("productOrderId")
            if pid:
                ids.add(pid)
        cur = date.fromordinal(cur.toordinal() + 1)
        time.sleep(0.2)
    ids = list(ids)
    print(f"  스마트스토어: 주문ID {len(ids)}건 상세 조회 중...")
    out = []
    for i in range(0, len(ids), 100):
        batch = ids[i:i + 100]
        try:
            r = requests.post(f"{cli.BASE_URL}/v1/pay-order/seller/product-orders/query",
                              headers=cli._headers(), json={"productOrderIds": batch}, timeout=40)
            if r.status_code == 429:
                time.sleep(2)
                r = requests.post(f"{cli.BASE_URL}/v1/pay-order/seller/product-orders/query",
                                  headers=cli._headers(), json={"productOrderIds": batch}, timeout=40)
            det = r.json().get("data", [])
        except Exception as e:
            print(f"    상세 조회 실패: {e}")
            det = []
        for o in det:
            od = o.get("order", {}) or {}
            po = o.get("productOrder", {}) or {}
            st = po.get("productOrderStatus", "") or ""
            cs = po.get("claimStatus", "") or ""
            canceled = st in ("CANCELED", "RETURNED") or cs in ("CANCEL_DONE", "RETURN_DONE")
            dt_ = (po.get("placeOrderDate") or po.get("paymentDate") or od.get("orderDate") or "")[:10]
            if not dt_:
                continue
            out.append({
                "주문일": dt_,
                "스토어": "마르문(스마트스토어)",
                "주문번호": od.get("orderId", "") or po.get("productOrderId", ""),
                "회원ID": str(od.get("ordererNo") or ""),
                "주문자명": od.get("ordererName", "") or "",
                "주문자휴대폰": _norm_phone(od.get("ordererTel")),
                "주문자이메일": "",
                "주문자우편번호": "",
                "수령인명": "",
                "수령인휴대폰": "",
                "결제금액": int(_f(po.get("totalPaymentAmount"))),
                "주문경로": "스마트스토어",
                "취소여부": "취소" if canceled else "",
                "반품확정일": "",
            })
        time.sleep(0.25)
    return out


def main():
    today = date.today()
    months = []
    y, m = START_YEAR, START_MONTH
    while (y, m) <= (today.year, today.month):
        months.append((y, m))
        m += 1
        if m > 12:
            y, m = y + 1, 1

    rows = []
    for store, mall in STORES:
        cli = Cafe24Client(mall, store)
        if not cli.is_authenticated():
            print(f"  {store}: 미인증 — 스킵")
            continue
        for (yy, mm) in months:
            orders = fetch_month(cli, yy, mm)
            print(f"  {store} {yy}-{mm:02d}: {len(orders)}건")
            for o in orders:
                rc = (o.get("receivers") or [{}])[0]
                by = o.get("buyer") or {}
                actual = o.get("actual_order_amount") or {}
                amount = max(0, int(_f(actual.get("payment_amount") or o.get("payment_amount"))
                                    + _f(o.get("naver_point"))))
                od = (o.get("payment_date") or o.get("order_date") or "")[:10]
                buyer_phone = _norm_phone(by.get("cellphone") or by.get("phone"))
                recv_phone = _norm_phone(rc.get("cellphone") or rc.get("phone"))
                rows.append({
                    "주문일": od,
                    "스토어": store,
                    "주문번호": o.get("order_id", ""),
                    "회원ID": (by.get("member_id") or o.get("member_id") or ""),
                    "주문자명": by.get("name") or o.get("billing_name") or "",
                    "주문자휴대폰": buyer_phone,
                    "주문자이메일": by.get("email") or o.get("member_email") or "",
                    "주문자우편번호": by.get("buyer_zipcode") or "",
                    "수령인명": rc.get("name", "") or "",
                    "수령인휴대폰": recv_phone,
                    "결제금액": amount,
                    "주문경로": o.get("order_place_name", "") or "",
                    "취소여부": "취소" if o.get("canceled") == "T" else "",
                    "반품확정일": (o.get("return_confirmed_date") or "")[:10],
                })
            time.sleep(0.2)

    # 스마트스토어(마르문) — 2025-12 개설, 하루 단위 순회라 시작일을 넉넉히
    print("스마트스토어(마르문) 수집 중...")
    ss_rows = fetch_smartstore(date(2025, 12, 1), today)
    print(f"  스마트스토어: {len(ss_rows)}건")
    rows += ss_rows

    if not rows:
        print("주문 없음 — 종료")
        return

    df = pd.DataFrame(rows).sort_values(["주문일", "스토어"]).reset_index(drop=True)

    # ── 재구매율 계산 ─────────────────────────────
    # 유효 주문 = 취소 제외.
    # 기준A(권장) = 주문자 휴대폰  /  기준B(참고) = 수령인 휴대폰
    valid = df[df["취소여부"] == ""].copy()

    def keyA(r):  # 주문자 기준
        if r["주문자휴대폰"]:
            return "P:" + r["주문자휴대폰"]
        if r["주문자명"] and r["주문자이메일"]:
            return "E:" + r["주문자명"] + "|" + r["주문자이메일"]
        return ""

    valid["고객키_주문자"] = valid.apply(keyA, axis=1)
    valid["고객키_수령인"] = valid["수령인휴대폰"].apply(lambda p: ("R:" + p) if p else "")

    def summarize(keycol, label):
        idf = valid[valid[keycol] != ""]
        out = []
        store_list = ["(전체·채널통합)"] + sorted(idf["스토어"].unique().tolist())
        for store in store_list:
            sub = idf if store == "(전체·채널통합)" else idf[idf["스토어"] == store]
            if sub.empty:
                continue
            cnt = Counter(sub[keycol])
            buyers = len(cnt)
            repeat = sum(1 for v in cnt.values() if v >= 2)
            out.append({
                "기준": label,
                "구분": store,
                "유효주문": len(sub),
                "고유 고객수": buyers,
                "재구매 고객수(2회+)": repeat,
                "재구매율(%)": round(repeat / buyers * 100, 2) if buyers else 0,
                "인당 평균주문": round(len(sub) / buyers, 2) if buyers else 0,
            })
        return out

    sdf = pd.DataFrame(summarize("고객키_주문자", "주문자 휴대폰(권장)")
                       + summarize("고객키_수령인", "수령인 휴대폰(참고)"))

    idfA = valid[valid["고객키_주문자"] != ""]
    cov = pd.DataFrame([{
        "전체 주문": len(df),
        "취소 제외 주문": len(valid),
        "주문자 식별 가능": len(idfA),
        "주문자 식별률(%)": round(len(idfA) / len(valid) * 100, 1) if len(valid) else 0,
        "주문자휴대폰 보유": int((valid["주문자휴대폰"] != "").sum()),
        "수령인휴대폰 보유": int((valid["수령인휴대폰"] != "").sum()),
        "주문자≠수령인 번호": int((valid["주문자휴대폰"] != valid["수령인휴대폰"]).sum()),
        "회원ID 보유": int((valid["회원ID"] != "").sum()),
    }])

    # ── 코호트: 첫 구매월 기준 재구매율 + 재구매까지 걸린 일수 ──
    ia = valid[valid["고객키_주문자"] != ""].copy()
    ia["주문일"] = pd.to_datetime(ia["주문일"], errors="coerce")
    ia = ia.dropna(subset=["주문일"]).sort_values("주문일")

    g = ia.groupby("고객키_주문자")["주문일"]
    first = g.min()
    n_ord = g.count()
    # 2번째 주문일 (없으면 NaT) — 고객키로 인덱싱되게 apply 사용
    second = g.apply(lambda s: s.sort_values().iloc[1] if len(s) >= 2 else pd.NaT)

    cust = pd.DataFrame({"첫구매": first, "주문수": n_ord})
    cust["재구매"] = cust["주문수"] >= 2
    cust["재구매일"] = second
    cust["재구매까지_일수"] = (cust["재구매일"] - cust["첫구매"]).dt.days
    cust["첫구매월"] = cust["첫구매"].dt.strftime("%Y-%m")

    coh = cust.groupby("첫구매월").agg(
        신규고객=("주문수", "size"),
        재구매고객=("재구매", "sum"),
    ).reset_index()
    coh["재구매율(%)"] = (coh["재구매고객"] / coh["신규고객"] * 100).round(2)
    coh["평균 재구매까지(일)"] = cust.groupby("첫구매월")["재구매까지_일수"].mean().round(1).values
    coh["관찰기간(일)"] = (pd.Timestamp(today) - pd.to_datetime(coh["첫구매월"] + "-01")).dt.days

    gap = cust["재구매까지_일수"].dropna()
    gapstat = pd.DataFrame([{
        "재구매 고객수": int(cust["재구매"].sum()),
        "재구매까지 평균(일)": round(gap.mean(), 1) if len(gap) else 0,
        "중앙값(일)": int(gap.median()) if len(gap) else 0,
        "30일 이내(%)": round((gap <= 30).mean() * 100, 1) if len(gap) else 0,
        "90일 이내(%)": round((gap <= 90).mean() * 100, 1) if len(gap) else 0,
        "180일 이내(%)": round((gap <= 180).mean() * 100, 1) if len(gap) else 0,
    }])

    with pd.ExcelWriter(OUT, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="주문내역", index=False)
        sdf.to_excel(w, sheet_name="재구매율요약", index=False)
        coh.to_excel(w, sheet_name="코호트(첫구매월)", index=False)
        gapstat.to_excel(w, sheet_name="재구매소요기간", index=False)
        cov.to_excel(w, sheet_name="식별커버리지", index=False)

    print("\n[코호트] 첫구매월별 재구매율")
    print(coh.to_string(index=False))
    print("\n[재구매 소요기간]")
    print(gapstat.to_string(index=False))

    print(f"\n저장 완료: {OUT}")
    print(f"총 주문 {len(df):,}건 / 취소제외 {len(valid):,}건 / 주문자식별 {len(idfA):,}건")
    print(sdf.to_string(index=False))


if __name__ == "__main__":
    main()
