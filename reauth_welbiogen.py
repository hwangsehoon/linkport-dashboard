# -*- coding: utf-8 -*-
"""웰바이오젠 카페24 재인증 + 최근 매출 백필 (인증코드 만료 전 원샷).

사용: python reauth_welbiogen.py <인증코드 또는 콜백URL전체>
  인증코드는 카페24 발급 후 ~1분이면 만료되므로 받는 즉시 실행한다.
"""
import sys, io, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from datetime import date, timedelta
from config import CAFE24_MALLS
from api.cafe24 import Cafe24Client, fetch_all_cafe24
from api.db import save_sales

STORE = "웰바이오젠(카페24)"


def extract_code(arg: str) -> str:
    m = re.search(r"[?&]code=([^&\s]+)", arg)
    return m.group(1) if m else arg.strip()


def main():
    if len(sys.argv) < 2:
        print("사용법: python reauth_welbiogen.py <code 또는 콜백URL>")
        return
    code = extract_code(sys.argv[1])
    mall = CAFE24_MALLS[STORE]
    cl = Cafe24Client(mall, STORE)
    if not cl.authenticate_with_code(code):
        print("✗ 토큰 교환 실패 — 코드가 만료됐을 수 있음. 새 코드로 다시 시도.")
        return
    print("✓ 토큰 재발급 완료")

    end = date.today()
    start = end - timedelta(days=10)
    print(f"백필 수집(전체 카페24 몰): {start} ~ {end}")
    df = fetch_all_cafe24(start, end)          # 인증된 몰만 자동 처리
    if df is None or df.empty:
        print("수집 결과 없음")
        return
    save_sales(df)
    w = df[(df["스토어"] == STORE) & (df["매출"] > 0)]
    print(f"저장 {len(df)}행 · 웰바이오젠 매출일 {len(w)}건")
    for _, r in w.sort_values("날짜").iterrows():
        print(f"   {r['날짜']}  매출 {int(r['매출']):>10,}  주문 {int(r['주문건수']):>3}")


if __name__ == "__main__":
    main()
