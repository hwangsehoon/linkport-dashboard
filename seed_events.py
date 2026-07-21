# -*- coding: utf-8 -*-
"""
마케팅 이벤트 기록 (성과 해석용)

채널 시작/중단/정책변경 등을 기록해 두면, 성과 그래프의 변곡점을 설명할 수 있다.
새 이벤트는 EVENTS에 추가하고 다시 실행하면 됨 (날짜+내용이 같으면 갱신).
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

import psycopg2.extras
from api.db import _get_conn

# (날짜, 구분, 채널, 내용)
#   구분: 시작 / 중단 / 변경
# ※ 스레드 2~3월은 정확한 일자를 몰라 월 경계로 넣음 — 알게 되면 수정 후 재실행
EVENTS = [
    ("2026-02-01", "시작", "스레드", "스레드(Threads) 운영 시작"),
    ("2026-03-31", "중단", "스레드", "스레드(Threads) 운영 종료"),
    ("2026-05-07", "시작", "블로그", "블로그 '활신경제' 콘텐츠 시작"),
    ("2026-05-27", "중단", "카페", "네이버 카페 답글 작성 차단 (정책 변경)"),
    ("2026-07-01", "시작", "블로그", "헤르메스로 블로그 발행 완전 자동화"),
]


def main():
    c = _get_conn()
    cur = c.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS marketing_events (
        날짜 DATE, 구분 TEXT, 채널 TEXT, 내용 TEXT,
        PRIMARY KEY (날짜, 채널, 내용))""")
    psycopg2.extras.execute_values(
        cur,
        """INSERT INTO marketing_events (날짜, 구분, 채널, 내용) VALUES %s
           ON CONFLICT (날짜, 채널, 내용) DO UPDATE SET 구분 = EXCLUDED.구분""",
        EVENTS)
    c.commit()

    cur.execute("SELECT 날짜, 구분, 채널, 내용 FROM marketing_events ORDER BY 날짜")
    print("등록된 마케팅 이벤트:")
    for r in cur.fetchall():
        print(f"  {r[0]}  [{r[1]}] {r[2]:<6} {r[3]}")
    c.close()


if __name__ == "__main__":
    main()
