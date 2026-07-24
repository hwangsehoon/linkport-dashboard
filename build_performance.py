# -*- coding: utf-8 -*-
"""
일별 성과 데이터 생성 (성과 분석 대시보드용) — ★ 웰바이오젠 전용

검색량(활신경제)·블로그·카페가 전부 웰바이오젠 지표라, 매출·방문자·광고비도
웰바이오젠만 뽑아야 분자·분모가 맞는다. 전 브랜드 합계로 보면 반드럽 트래픽
(웰바이오젠의 3.5배)에 묻혀 성과 변화가 보이지 않는다.

일(day) 단위 원시 카운트를 저장하고, 대시보드에서 일/주/월로 집계한다.
  ※ 전환율·ROAS·재구매율 같은 '비율'은 반드시 합계로 재계산해야 정확하므로
     비율이 아닌 분자/분모(원시 카운트)를 저장한다.

합치는 것 (전부 웰바이오젠):
  1) 방문자·주문   ← DB sales, 스토어 '웰바이오젠(카페24)' (순방문자수는 카페24만 존재)
  2) 매출          ← DB sales, 웰바이오젠 전 채널
  3) 광고비        ← DB ads, 브랜드='웰바이오젠'
  4) 식별주문·재구매주문 ← 주문내역 엑셀, 웰바이오젠(카페24)
  5) 검색량 "활신경제"  ← 검색광고 keywordstool × 데이터랩 트렌드 (일별)
  6) 블로그 방문자      ← 헤르메스 추적기 blogs.json (total 증가분, 웰바이오젠)

주의 — sales 스토어 구조:
  · 2025년: '웰바이오젠(합계)' 행만 존재 (채널 구분 없음, 방문자·주문 없음)
  · 2026년~: '웰바이오젠(카페24)' / '웰바이오젠(기타)' 채널별 행
  두 형태가 같은 날짜에 함께 있는 경우는 0건이라 LIKE '웰바이오젠%'로 합쳐도 이중집계 없음.

주의 — ROAS:
  웰바이오젠은 ads.전환매출이 전 기간 0이다(카페제휴·블로그 중심, 전환추적 없음).
  따라서 ROAS는 전환매출÷광고비가 아니라 **매출 ÷ 총마케팅비**로 계산한다(앱에서).

결과: daily_performance 테이블 + repeat_timing + exports 엑셀 보관
"""
import sys, io, json, os
# pythonw(스케줄러·창 없음)에서는 sys.stdout이 None이라 .buffer 접근 시 죽는다 →
# 콘솔이 없으면 로그 파일로 보내고, 있으면 UTF-8로 감싼다.
if sys.stdout is None:
    _logp = os.path.join(os.path.dirname(os.path.abspath(__file__)), "build_performance.log")
    sys.stdout = open(_logp, "a", encoding="utf-8", errors="replace")
    sys.stderr = sys.stdout
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
from datetime import date
from collections import defaultdict
from pathlib import Path

import pandas as pd
import psycopg2.extras

from api.db import _get_conn

ROOT = Path(__file__).resolve().parent
ORDER_XL = r"C:\Users\조현우\Desktop\링크포트_주문내역_전체.xlsx"
# 블로그 방문자: 헤르메스 추적기(로컬)가 원본. 계정 추가도 여기서 하므로 항상 최신 집합.
# (구드라이브 H: 사본은 폴더가 재편돼 접근이 끊긴 적이 있어 로컬로 고정)
BLOG_JSON = r"C:\Users\조현우\Desktop\헤르메스\01_프로그램\05_블로그방문자추적기\blogs.json"
KEYWORD = "활신경제"
SS = "마르문(스마트스토어)"
START = "2025-01-01"

BRAND = "웰바이오젠"
BRAND_STORE = "웰바이오젠(카페24)"       # 방문자·주문·재구매가 있는 유일한 스토어
# 매출: 카페24 + 기타 + 쿠팡(링포, 브랜드 컬럼에 표기) 전부
# ※ 파라미터 없이 f-string으로 넣으므로 % 이스케이프 불필요
SALES_WHERE = "(스토어 LIKE '웰바이오젠%' OR 브랜드 = '웰바이오젠')"


# ── 주문 엑셀 → 고객키 부여 ──────────────────────────────
def _find_order_xl():
    """주문내역 엑셀 경로. 바탕화면 원본이 없으면 exports 아카이브의 최신본을 쓴다.
    (2026-07-24: 바탕화면 파일이 사라져 빌드 전체가 죽었음 → 폴백 추가)"""
    if Path(ORDER_XL).exists():
        return ORDER_XL
    arch = sorted((ROOT / "exports" / "주문내역").rglob("주문내역_전체_*.xlsx"))
    if arch:
        print(f"  ⚠ 주문 엑셀 원본 없음 → 아카이브 사용: {arch[-1].name}")
        return str(arch[-1])
    return None


def _orders():
    """주문 엑셀을 못 찾으면 None. 재구매 지표만 건너뛰고 나머지는 정상 진행한다."""
    path = _find_order_xl()
    if path is None:
        print("  ⚠ 주문내역 엑셀 없음 — 재구매 지표 건너뜀(기존 값 보존)")
        return None
    d = pd.read_excel(path, "주문내역",
                      dtype={"주문자휴대폰": str, "회원ID": str, "주문번호": str})
    for c in ("주문자휴대폰", "회원ID", "주문번호"):
        d[c] = d[c].astype(str).str.strip().replace("nan", "")
    d["취소여부"] = d["취소여부"].fillna("")
    d = d[d["취소여부"] == ""].copy()
    d["주문일"] = pd.to_datetime(d["주문일"], errors="coerce")
    d = d.dropna(subset=["주문일"])
    ss = d[d["스토어"] == SS].drop_duplicates(subset=["주문번호"])   # 상품주문 → 주문 단위
    d = pd.concat([d[d["스토어"] != SS], ss], ignore_index=True)
    d["고객키"] = d.apply(
        lambda r: ("N:" + r["회원ID"]) if r["스토어"] == SS and r["회원ID"]
        else (("P:" + r["주문자휴대폰"]) if r["주문자휴대폰"] else ""), axis=1)
    return d[d["고객키"] != ""].sort_values("주문일").reset_index(drop=True)


def repeat_daily(d):
    """일별 식별주문 / 재구매주문 (그 고객의 2번째 이후 주문)"""
    d = d.copy()
    d["순번"] = d.groupby("고객키").cumcount() + 1
    d["일"] = d["주문일"].dt.strftime("%Y-%m-%d")
    g = d.groupby("일").agg(식별주문=("순번", "size"),
                           재구매주문=("순번", lambda s: int((s >= 2).sum())))
    return g.to_dict("index")


def repeat_timing_rows(d, stores):
    rows = []
    for store in stores:
        sub = d[d["스토어"] == store]
        g = sub.groupby("고객키")["주문일"]
        first, second = g.min(), g.apply(lambda s: s.sort_values().iloc[1] if len(s) >= 2 else pd.NaT)
        gap = (second - first).dt.days.dropna()
        n = sub["고객키"].nunique()
        if not n or not len(gap):
            continue
        rows.append((store, int(n), int(len(gap)), round(len(gap) / n * 100, 2),
                     round(float(gap.mean()), 1), int(gap.median()),
                     round(float((gap <= 30).mean() * 100), 1),
                     round(float((gap <= 90).mean() * 100), 1),
                     round(float((gap <= 180).mean() * 100), 1)))
    return rows


# 광고비 분류 — 쿠팡·과거광고비는 이 대시보드에서 제외(자사몰 성과 기준)
PAID_AD = ["Meta", "Naver SA", "스마트스토어 AI 광고", "네이버 쿠키광고"]
OTHER_AD = ["카페제휴", "카페 제휴(해돌)", "카페 광고", "아이디", "계정 구매",
            "카페 제휴(부산맘)", "기타"]


# ── DB (일별) ────────────────────────────────────────────
def db_daily():
    c = _get_conn(); cur = c.cursor()
    # 방문자·주문 — 순방문자수는 카페24에만 존재
    cur.execute("""SELECT 날짜::text, SUM(순방문자수), SUM(주문건수)
                   FROM sales WHERE 스토어 = %s GROUP BY 날짜""", (BRAND_STORE,))
    vis = {r[0]: (int(r[1] or 0), int(r[2] or 0)) for r in cur.fetchall()}
    # 매출 — 웰바이오젠 전 채널 (2025년 '(합계)' 행 포함, 겹치는 날 없음)
    cur.execute(f"SELECT 날짜::text, SUM(매출) FROM sales WHERE {SALES_WHERE} GROUP BY 날짜")
    rev = {r[0]: int(r[1] or 0) for r in cur.fetchall()}
    # 유료광고 — 웰바이오젠 전환매출은 전 기간 0이라 ROAS는 앱에서 매출÷총마케팅비로 계산
    cur.execute("""SELECT 날짜::text, SUM(광고비), SUM(전환매출) FROM ads
                   WHERE 브랜드 = %s AND 광고채널 = ANY(%s) GROUP BY 날짜""", (BRAND, PAID_AD))
    paid = {r[0]: (int(r[1] or 0), int(r[2] or 0)) for r in cur.fetchall()}
    # 기타 광고비 (카페제휴/계정/카페광고 등 — 웰바이오젠 마케팅비의 대부분)
    cur.execute("""SELECT 날짜::text, SUM(광고비) FROM ads
                   WHERE 브랜드 = %s AND 광고채널 = ANY(%s) GROUP BY 날짜""", (BRAND, OTHER_AD))
    other = {r[0]: int(r[1] or 0) for r in cur.fetchall()}
    c.close()
    return vis, rev, paid, other


# ── 검색량 (일별) ────────────────────────────────────────
def search_daily():
    import tomllib
    sec = Path(r"C:\Users\조현우\Desktop\keyword-search-trend\.streamlit\secrets.toml")
    if not sec.exists():
        print("  검색량: secrets.toml 없음 — 스킵")
        return {}
    sys.path.insert(0, str(sec.parent.parent))
    with open(sec, "rb") as f:
        s = tomllib.load(f)
    creds = {"ad_api_key": s["NAVER_AD_API_KEY"], "ad_secret_key": s["NAVER_AD_SECRET_KEY"],
             "ad_customer_id": s["NAVER_AD_CUSTOMER_ID"],
             "client_id": s["NAVER_CLIENT_ID"], "client_secret": s["NAVER_CLIENT_SECRET"]}
    try:
        from naver_volume import estimate_daily_volume
        res = estimate_daily_volume(KEYWORD, creds, start_date=START)
    except Exception as e:
        print(f"  검색량 실패: {e}")
        return {}
    return {r["date"]: int(r["volume"]) for r in res["daily"]}


# ── 블로그 (일별, total 증가분) ──────────────────────────
def blog_daily():
    # 1순위: 헤르메스 추적기 blogs.json (로컬 C드라이브)
    #   · 사장님이 계정을 여기에 추가하므로 항상 최신 집합(2026-07 기준 566개)
    #   · 이 파일의 last_total 합계가 ★블로그 시트의 '누적 방문자'와 정확히 일치 → 시트의 원본
    #   · 로컬이라 H드라이브(구글드라이브) 미마운트 문제 없음
    p = Path(BLOG_JSON)
    if p.exists():
        blogs = json.load(open(p, encoding="utf-8"))["blogs"]
        day = defaultdict(int)
        for b in blogs:
            h = b.get("history") or {}
            prev = None
            for dt in sorted(h):
                t = int(h[dt].get("total") or 0)
                if prev is not None and t >= prev:   # 감소(글삭제/정지)는 방문 아님
                    day[dt] += t - prev
                prev = t
        if day:
            cum = sum(int(b.get("last_total") or 0) for b in blogs)
            print(f"  블로그: 헤르메스 추적기 {len(blogs)}개 · 기록 {len(day)}일 · 누적 {cum:,}명")
            return dict(day), len(day), cum

    # 2순위(백업): ★블로그 구글시트 — 추적기 파일이 없을 때만.
    #   시트는 4/21부터 있어 추적기(5/27~)보다 앞 구간을 채워준다.
    try:
        from api.blog_sheet import blog_daily_map
        d2, n2, t2 = blog_daily_map()
        if d2:
            print(f"  블로그: (대체) 구글시트에서 {n2}일 · 합계 {t2:,}명")
            return d2, n2, t2
    except Exception as e:
        print(f"  ⚠ 블로그 시트 대체 실패: {type(e).__name__}")

    # 둘 다 실패 — save_daily가 기존 DB값을 보존하므로 데이터 유실은 없음
    print("  ⚠ 블로그: 추적기·시트 모두 접근 불가 — 갱신 중단(기존 값 보존)")
    return {}, 0, 0


# ── 저장 ────────────────────────────────────────────────
def save_daily(df, update_repeat: bool = True):
    """update_repeat=False면 식별주문·재구매주문을 UPDATE에서 빼 기존 값을 보존한다.
    (주문 엑셀을 못 읽었을 때 0으로 덮어쓰는 것을 막기 위함)"""
    repeat_set = ("식별주문=EXCLUDED.식별주문, 재구매주문=EXCLUDED.재구매주문,"
                  if update_repeat else "")
    c = _get_conn(); cur = c.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS daily_performance (
        날짜 DATE PRIMARY KEY, 방문자 INT, 주문 INT, 매출 BIGINT,
        광고비 BIGINT, 전환매출 BIGINT, 식별주문 INT, 재구매주문 INT,
        검색량 INT, 블로그방문자 INT)""")
    cur.execute("ALTER TABLE daily_performance ADD COLUMN IF NOT EXISTS 기타광고비 BIGINT DEFAULT 0")
    rows = [(r["날짜"], int(r["방문자"]), int(r["주문"]), int(r["매출"]),
             int(r["광고비"]), int(r["기타광고비"]), int(r["전환매출"]), int(r["식별주문"]),
             int(r["재구매주문"]), int(r["검색량"]), int(r["블로그방문자"]))
            for _, r in df.iterrows()]
    psycopg2.extras.execute_values(cur,
        """INSERT INTO daily_performance
           (날짜,방문자,주문,매출,광고비,기타광고비,전환매출,식별주문,재구매주문,검색량,블로그방문자)
           VALUES %s ON CONFLICT (날짜) DO UPDATE SET
           방문자=EXCLUDED.방문자, 주문=EXCLUDED.주문, 매출=EXCLUDED.매출,
           광고비=EXCLUDED.광고비, 기타광고비=EXCLUDED.기타광고비,
           전환매출=EXCLUDED.전환매출,
           """ + repeat_set + """
           -- 검색량·블로그는 외부 소스(네이버 API·구글드라이브 blogs.json)라 접근 실패 시 0이 된다.
           -- 그대로 덮으면 과거 데이터가 통째로 지워지므로(2026-07-21 실제 발생),
           -- 새 값이 0이면 기존 값을 보존한다.
           검색량 = CASE WHEN EXCLUDED.검색량 > 0
                        THEN EXCLUDED.검색량 ELSE daily_performance.검색량 END,
           블로그방문자 = CASE WHEN EXCLUDED.블로그방문자 > 0
                          THEN EXCLUDED.블로그방문자 ELSE daily_performance.블로그방문자 END""",
        rows, page_size=500)
    c.commit(); c.close()


def save_timing(rows):
    c = _get_conn(); cur = c.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS repeat_timing (
        구분 TEXT PRIMARY KEY, 고객 INT, 재구매 INT, 재구매율 REAL,
        평균일 REAL, 중앙값일 INT, d30 REAL, d90 REAL, d180 REAL)""")
    # upsert만 하면 예전 브랜드 행(아자차·반드럽·(전체))이 남아 섞인다 → 먼저 비운다
    keep = [r[0] for r in rows]
    cur.execute("DELETE FROM repeat_timing WHERE 구분 <> ALL(%s)", (keep,))
    print(f"   repeat_timing 정리: {cur.rowcount}행 삭제 (웰바이오젠 외)")
    psycopg2.extras.execute_values(cur,
        """INSERT INTO repeat_timing (구분,고객,재구매,재구매율,평균일,중앙값일,d30,d90,d180)
           VALUES %s ON CONFLICT (구분) DO UPDATE SET
           고객=EXCLUDED.고객, 재구매=EXCLUDED.재구매, 재구매율=EXCLUDED.재구매율,
           평균일=EXCLUDED.평균일, 중앙값일=EXCLUDED.중앙값일,
           d30=EXCLUDED.d30, d90=EXCLUDED.d90, d180=EXCLUDED.d180""", rows)
    c.commit(); c.close()


def main():
    print(f"★ 웰바이오젠 전용 (매출·방문자·광고비·재구매 모두 {BRAND}만)")
    print("1) DB 일별 집계...")
    print(f"   유료광고: {', '.join(PAID_AD)}  (쿠팡·과거광고비 제외)")
    print(f"   기타광고비: {', '.join(OTHER_AD)}")
    vis, rev, paid, other = db_daily()
    print("2) 주문/재구매 일별...")
    od = _orders()
    if od is None:                                   # 엑셀 없음 → 재구매만 건너뜀
        od_w, rp = None, {}
    else:
        od_w = od[od["스토어"] == BRAND_STORE]       # 일별 재구매율은 웰바이오젠만
        print(f"   웰바이오젠 주문 {len(od_w):,}건 / 전체 {len(od):,}건")
        rp = repeat_daily(od_w)
    print("3) 검색량 일별...")
    sv = search_daily()
    print("4) 블로그 일별...")
    bv, nblog, bcum = blog_daily()
    print(f"   블로그 기록 {nblog}일 · 방문자 합계 {bcum:,}명 (웰바이오젠)")

    days = sorted({d for d in set(vis) | set(rev) | set(paid) | set(other)
                   | set(rp) | set(sv) | set(bv) if d >= START})
    out = []
    for dd in days:
        v, o = vis.get(dd, (0, 0))
        ad, conv = paid.get(dd, (0, 0))
        r = rp.get(dd, {})
        out.append({"날짜": dd, "방문자": v, "주문": o, "매출": rev.get(dd, 0),
                    "광고비": ad, "기타광고비": other.get(dd, 0), "전환매출": conv,
                    "식별주문": int(r.get("식별주문", 0)), "재구매주문": int(r.get("재구매주문", 0)),
                    "검색량": int(sv.get(dd, 0)), "블로그방문자": int(bv.get(dd, 0))})
    df = pd.DataFrame(out)

    save_daily(df, update_repeat=od_w is not None)
    print(f"\ndaily_performance 저장: {len(df):,}일 ({df['날짜'].min()} ~ {df['날짜'].max()})")
    if od_w is None:
        print("5) 재구매 타이밍: 주문 엑셀 없어 건너뜀 (기존 값 유지)")
    else:
        print("5) 재구매 타이밍...")
        print(f"   웰바이오젠 주문 이력: {od_w['주문일'].min():%Y-%m-%d} ~ {od_w['주문일'].max():%Y-%m-%d}")
        tr = repeat_timing_rows(od_w, [BRAND_STORE])
        save_timing(tr)
        print(f"   repeat_timing {len(tr)}행 (웰바이오젠만)")

    today = date.today()
    outdir = ROOT / "exports" / "월별성과" / f"{today.year}" / f"{today:%Y-%m}"
    outdir.mkdir(parents=True, exist_ok=True)
    xl = outdir / f"일별성과_{today:%Y%m%d}.xlsx"
    with pd.ExcelWriter(xl, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="일별성과", index=False)
    print(f"엑셀 보관: {xl}")

    # 월별 요약 미리보기 (비율은 합계로 재계산)
    m = df.copy()
    m["월"] = m["날짜"].str[:7]
    g = m.groupby("월").sum(numeric_only=True)
    g["전환율"] = (g["주문"] / g["방문자"].replace(0, 1) * 100).round(2)
    g["총마케팅비"] = g["광고비"] + g["기타광고비"]
    # 웰바이오젠은 전환매출이 0이므로 매출 기준 '배수'. 마케팅비 0이면 정의 불가(NaN)
    # ※ replace(0, pd.NA)는 int 컬럼을 object로 만들어 .round()가 터진다 → .where 사용
    g["ROAS배수"] = (g["매출"] / g["총마케팅비"].where(g["총마케팅비"] > 0)).round(1)
    g["재구매율"] = (g["재구매주문"] / g["식별주문"].replace(0, 1) * 100).round(2)
    print("\n[월별 요약] ※ ROAS = 매출 ÷ 총마케팅비 (배수)")
    print(g.loc[g.index >= "2026-01",
                ["방문자", "주문", "전환율", "매출", "광고비", "기타광고비", "총마케팅비",
                 "ROAS배수", "재구매율", "검색량", "블로그방문자"]].to_string())


if __name__ == "__main__":
    main()
