# -*- coding: utf-8 -*-
"""
재구매율 재계산 (이미 추출된 엑셀 기반, 재수집 불필요)

채널별 최적 고객키:
  - 카페24        : 주문자 휴대폰 (buyer.cellphone) — 100% 보유
  - 스마트스토어   : ordererNo(고유 구매자번호) — 100% 보유, 마스킹 안 됨
                    (ordererTel은 79%가 개인정보 마스킹돼 사용 불가)

주의: 두 채널은 키 체계가 달라 '동일인 통합'은 불가.
      따라서 (전체)는 채널별 고객 단순 합산 → 양쪽에서 산 사람은 2명으로 셈.
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from collections import Counter

import pandas as pd

XL = r"C:\Users\조현우\Desktop\링크포트_주문내역_전체.xlsx"
SS = "마르문(스마트스토어)"


def load():
    d = pd.read_excel(XL, "주문내역", dtype={"주문자휴대폰": str, "회원ID": str, "주문번호": str})
    for c in ("주문자휴대폰", "회원ID", "주문번호"):
        d[c] = d[c].astype(str).str.strip().replace("nan", "")
    d["취소여부"] = d["취소여부"].fillna("")
    d = d[d["취소여부"] == ""].copy()          # 취소 제외
    d["주문일"] = pd.to_datetime(d["주문일"], errors="coerce")
    d = d.dropna(subset=["주문일"])
    # 스마트스토어는 상품주문 단위 → 주문(orderId) 단위로 중복 제거
    ss = d[d["스토어"] == SS].drop_duplicates(subset=["주문번호"])
    c24 = d[d["스토어"] != SS]
    d = pd.concat([c24, ss], ignore_index=True)

    def key(r):
        if r["스토어"] == SS:
            return ("N:" + r["회원ID"]) if r["회원ID"] else ""
        return ("P:" + r["주문자휴대폰"]) if r["주문자휴대폰"] else ""

    d["고객키"] = d.apply(key, axis=1)
    return d[d["고객키"] != ""].sort_values("주문일").reset_index(drop=True)


def repeat_summary(d):
    rows = []
    for store in ["(전체)"] + sorted(d["스토어"].unique()):
        sub = d if store == "(전체)" else d[d["스토어"] == store]
        c = Counter(sub["고객키"])
        b = len(c)
        r = sum(1 for v in c.values() if v >= 2)
        rows.append({"구분": store, "유효주문": len(sub), "고유 고객수": b,
                     "재구매 고객수": r, "재구매율(%)": round(r / b * 100, 2) if b else 0,
                     "인당 평균주문": round(len(sub) / b, 2) if b else 0})
    return pd.DataFrame(rows)


def timing(d):
    rows = []
    for store in ["(전체)"] + sorted(d["스토어"].unique()):
        sub = d if store == "(전체)" else d[d["스토어"] == store]
        g = sub.groupby("고객키")["주문일"]
        first = g.min()
        second = g.apply(lambda s: s.sort_values().iloc[1] if len(s) >= 2 else pd.NaT)
        gap = (second - first).dt.days.dropna()
        if len(gap) == 0:
            continue
        rows.append({"구분": store, "고객": sub["고객키"].nunique(), "재구매": len(gap),
                     "재구매율(%)": round(len(gap) / sub["고객키"].nunique() * 100, 2),
                     "평균(일)": round(gap.mean(), 1), "중앙값(일)": int(gap.median()),
                     "30일내(%)": round((gap <= 30).mean() * 100, 1),
                     "90일내(%)": round((gap <= 90).mean() * 100, 1),
                     "180일내(%)": round((gap <= 180).mean() * 100, 1)})
    return pd.DataFrame(rows)


def monthly(d):
    d = d.copy()
    d["순번"] = d.groupby("고객키").cumcount() + 1
    d["월"] = d["주문일"].dt.strftime("%Y-%m")
    d["재구매주문"] = d["순번"] >= 2
    g = d.groupby("월").agg(주문=("순번", "size"), 재구매주문=("재구매주문", "sum")).reset_index()
    g["재구매주문율(%)"] = (g["재구매주문"] / g["주문"] * 100).round(2)
    firstm = d.groupby("고객키")["월"].min()
    rows = []
    for m, sub in d.groupby("월"):
        u = sub["고객키"].unique()
        old = sum(1 for k in u if firstm[k] < m)
        rows.append({"월": m, "구매고객": len(u), "기존고객": old,
                     "신규고객": len(u) - old,
                     "기존고객비율(%)": round(old / len(u) * 100, 2)})
    return g.merge(pd.DataFrame(rows), on="월")


if __name__ == "__main__":
    d = load()
    print(f"분석 대상: {len(d):,}건 ({d['주문일'].min().date()} ~ {d['주문일'].max().date()})\n")
    s, t, m = repeat_summary(d), timing(d), monthly(d)
    print("[재구매율 요약]"); print(s.to_string(index=False))
    print("\n[재구매 타이밍]"); print(t.to_string(index=False))
    print("\n[월별 (2026)]"); print(m[m["월"] >= "2026-01"].to_string(index=False))

    # 엑셀에 시트 갱신
    orig = pd.read_excel(XL, "주문내역")
    with pd.ExcelWriter(XL, engine="openpyxl") as w:
        orig.to_excel(w, sheet_name="주문내역", index=False)
        s.to_excel(w, sheet_name="재구매율요약", index=False)
        t.to_excel(w, sheet_name="재구매타이밍", index=False)
        m.to_excel(w, sheet_name="월별재구매", index=False)
    print(f"\n엑셀 갱신 완료: {XL}")
