# -*- coding: utf-8 -*-
"""
블로그 방문자 수집 (구글 스프레드시트 ★블로그)

  시트 : ★블로그 → 탭 '상위노출 데이터 분석'
  대상 : 웰바이오젠(활신경제 등) 블로그
  인증 : 서비스 계정 hermes-bot@... (뷰어 공유됨)

왜 blogs.json(H드라이브) 대신 이 시트를 쓰나:
  · H:(구글드라이브)가 미마운트되면 접근이 끊기고, 실제로 2026-07-21에
    수집이 0으로 덮여 과거 데이터가 유실될 뻔했다.
  · 시트는 헤르메스가 매일 00시 기준으로 갱신하므로 안정적이다.

어느 컬럼을 쓰나 — '누적 방문자'의 증가분 (일방문자 컬럼 아님):
  · '일방문자(당일0시~)'는 기록이 빠진 날이 많다(6월 30일 중 21일만).
    그 컬럼만 더하면 6월 13,436명으로 실제의 1/7 수준으로 축소된다.
  · '누적 방문자' 증가분은 6월 90,645명으로, 기존 blogs.json(89,800)과 일치 → 이쪽이 정확.
  · 기록이 빠진 날의 방문자는 다음 기록일에 합산되므로 '일별'은 들쭉날쭉하지만
    '월 합계'는 정확하다.

주의: 글 삭제 등으로 누적이 감소하는 날이 있다(예 2026-05-08 '상위노출 외 글 삭제').
      감소분은 방문자가 아니므로 증가분만 센다.
"""
import os
import re
from datetime import date

import pandas as pd

KEY_PATH = os.getenv(
    "GOOGLE_SA_KEY",
    r"C:\Users\조현우\Desktop\헤르메스\04_백업자료\google_sheets_api_key.json")
SHEET_ID = "1OKRPlMiYdI8tt1bXAHC3mZiQLn8e6sosEpkQULEyynI"
TAB = "상위노출 데이터 분석"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

COL_DATE = 1        # 날짜        (예: '2026. 4. 21')
COL_CUM = 8         # 누적 방문자
COL_KEYWORD = 11    # 검색량


def _num(v):
    if v is None:
        return None
    s = str(v).strip().replace(",", "").replace("%", "")
    if not s or s in ("-", "#DIV/0!", "#N/A"):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _parse_date(v):
    """'2026. 4. 21' → date(2026,4,21)"""
    if not v:
        return None
    m = re.match(r"\s*(\d{4})\s*\.\s*(\d{1,2})\s*\.\s*(\d{1,2})", str(v))
    if not m:
        return None
    try:
        return date(*map(int, m.groups()))
    except ValueError:
        return None


def fetch_blog_daily() -> pd.DataFrame:
    """일별 블로그 방문자(누적 증가분) + 검색량. 컬럼: 날짜, 블로그방문자, 블로그검색량"""
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build

    cred = Credentials.from_service_account_file(KEY_PATH, scopes=SCOPES)
    svc = build("sheets", "v4", credentials=cred)
    vals = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{TAB}'!A1:N500").execute().get("values", [])

    recs = []
    for r in vals:
        r = list(r) + [""] * (14 - len(r))
        d = _parse_date(r[COL_DATE])
        if not d or d > date.today():        # 미래 예정행 제외
            continue
        recs.append({"날짜": d, "누적": _num(r[COL_CUM]), "블로그검색량": _num(r[COL_KEYWORD])})
    if not recs:
        return pd.DataFrame()

    df = pd.DataFrame(recs).drop_duplicates(subset=["날짜"], keep="last").sort_values("날짜")
    # 누적이 기록된 날만 골라 증가분 계산 (감소=글삭제 → 0 처리)
    cum = df.dropna(subset=["누적"]).copy()
    cum["블로그방문자"] = cum["누적"].diff()
    cum.loc[cum["블로그방문자"] < 0, "블로그방문자"] = 0
    df = df.merge(cum[["날짜", "블로그방문자"]], on="날짜", how="left")
    df["블로그방문자"] = df["블로그방문자"].fillna(0).astype(int)
    return df[["날짜", "블로그방문자", "블로그검색량"]].reset_index(drop=True)


def blog_daily_map():
    """build_performance용: {'YYYY-MM-DD': 방문자} + (블로그수 대신) 기록일수, 최신 누적"""
    df = fetch_blog_daily()
    if df.empty:
        return {}, 0, 0
    day = {d.isoformat(): int(v) for d, v in zip(df["날짜"], df["블로그방문자"]) if v > 0}
    return day, len(day), int(df["블로그방문자"].sum())


if __name__ == "__main__":
    import sys, io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    d = fetch_blog_daily()
    print(f"블로그 시트 {len(d)}일 ({d['날짜'].min()} ~ {d['날짜'].max()})")
    print(d.tail(8).to_string(index=False))
    m = d.copy()
    m["월"] = m["날짜"].map(lambda x: f"{x.year}-{x.month:02d}")
    print("\n월별 방문자 합계")
    print(m.groupby("월")["블로그방문자"].sum().to_string())
