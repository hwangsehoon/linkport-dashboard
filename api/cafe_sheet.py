# -*- coding: utf-8 -*-
"""
카페 제휴 성과 시트 수집 (구글 스프레드시트)

  시트  : ★N카페_제휴  →  탭 '4)데이터 분석'
  대상  : 전부 웰바이오젠(트라핀) 기준
  인증  : 서비스 계정 hermes-bot@charged-chess-501304-n0 (뷰어 공유됨)

컬럼(B열부터):
  날짜 | D+1 노출 키워드 검색량 | 게시글 조회수 합계 | 조회수당 매출
       | 순방문수 | 순방문율 | 구매건수 | 전환율 | 매출 | 평균객단가 | 비고

가져오는 것 = 카페 고유 지표뿐:
  카페검색량(D+1 노출 키워드) · 카페조회수(게시글) · 카페순방문(게시글 기준)

가져오지 않는 것:
  - 매출·구매건수 → 시트는 '카페24 관리자 통계' 기준이라 우리 API(실결제) 값과 불일치.
    소스를 섞으면 데이터가 틀어지므로 매출/주문은 **항상 API 값만** 쓴다.
  - 순방문율·전환율·조회수당매출·평균객단가 → 시트가 계산한 파생값. 비율은 집계 후 재계산.
"""
import os
import re
from datetime import date

import pandas as pd
import psycopg2.extras

from api.db import _get_conn

KEY_PATH = os.getenv(
    "GOOGLE_SA_KEY",
    r"C:\Users\조현우\Desktop\헤르메스\04_백업자료\google_sheets_api_key.json")
SHEET_ID = "1oSHBSuOtKJkiuWstVBcegPP5VYgLudH4MyvDl5Hvxng"
TAB = "4)데이터 분석"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def _num(v):
    """'1,786,500' → 1786500 / 빈값·'-' → None"""
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
    """'2026. 3. 24' → date(2026,3,24)"""
    if not v:
        return None
    m = re.match(r"\s*(\d{4})\s*\.\s*(\d{1,2})\s*\.\s*(\d{1,2})", str(v))
    if not m:
        return None
    y, mo, d = map(int, m.groups())
    try:
        return date(y, mo, d)
    except ValueError:
        return None


def fetch_cafe_daily() -> pd.DataFrame:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build

    cred = Credentials.from_service_account_file(KEY_PATH, scopes=SCOPES)
    svc = build("sheets", "v4", credentials=cred)
    vals = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{TAB}'!A1:L500").execute().get("values", [])

    rows = []
    for r in vals:
        r = list(r) + [""] * (12 - len(r))          # 길이 맞춤
        dt = _parse_date(r[1])
        if not dt:
            continue                                 # 헤더/평균/빈 줄 스킵
        rows.append({
            "날짜": dt,
            "카페검색량": _num(r[2]),                # D+1 노출 키워드 검색량
            "카페조회수": _num(r[3]),                # 게시글 조회수 합계
            "카페순방문": _num(r[5]),                # 게시글 기준 순방문
            # 매출·구매건수(r[9], r[7])는 의도적으로 미수집 — API 값으로 통일
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # 빈칸은 0이 아니라 '미기록'. 0으로 채우면 없는 하락이 생긴다 (검색량은 절반이 빈칸)
    for c in ("카페검색량", "카페조회수", "카페순방문"):
        df[c] = df[c].astype("Int64")
    # 시트 미래 예정행 제외 + 같은 날 재작성 행은 나중 것 채택(6/4 '부산맘 삭제 후')
    df = df[df["날짜"] <= date.today()]
    df = df.drop_duplicates(subset=["날짜"], keep="last")
    return df.sort_values("날짜").reset_index(drop=True)


def save_cafe_daily(df: pd.DataFrame):
    if df is None or df.empty:
        return
    c = _get_conn(); cur = c.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS cafe_daily (
        날짜 DATE PRIMARY KEY,
        카페검색량 BIGINT, 카페조회수 INT, 카페순방문 INT)""")
    def _v(x):
        return None if pd.isna(x) else int(x)
    rows = [(r["날짜"], _v(r["카페검색량"]), _v(r["카페조회수"]), _v(r["카페순방문"]))
            for _, r in df.iterrows()]
    psycopg2.extras.execute_values(cur,
        """INSERT INTO cafe_daily (날짜,카페검색량,카페조회수,카페순방문)
           VALUES %s ON CONFLICT (날짜) DO UPDATE SET
           카페검색량=EXCLUDED.카페검색량, 카페조회수=EXCLUDED.카페조회수,
           카페순방문=EXCLUDED.카페순방문""", rows)
    c.commit(); c.close()


if __name__ == "__main__":
    import sys, io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    d = fetch_cafe_daily()
    print(f"카페 시트 {len(d)}일 ({d['날짜'].min()} ~ {d['날짜'].max()})")
    save_cafe_daily(d)
    print("cafe_daily 저장 완료\n")
    print(d.tail(8).to_string(index=False))
    print("\n항목        기록일수   합계")
    for c in ("카페검색량", "카페조회수", "카페순방문"):
        s = d[c].dropna()
        print(f"{c:<10} {len(s):>4}/{len(d)}일  {int(s.sum()):>12,}")
