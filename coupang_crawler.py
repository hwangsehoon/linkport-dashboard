"""
쿠팡 광고센터 일별 광고비 자동 수집

사용법:
    python coupang_crawler.py              # 첫 실행 (headless=False, 직접 보면서 셀렉터 검증)
    python coupang_crawler.py --headless   # 자동화 (작업스케줄러용)

준비:
    pip install playwright python-dotenv
    playwright install chromium
    .env 파일에 COUPANG_ID, COUPANG_PW 설정
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from playwright.sync_api import Page, sync_playwright, TimeoutError as PWTimeout

from brand_config import detect_brand

ROOT = Path(__file__).parent
DB_PATH = ROOT / "dashboard_data.db"
SESSION_PATH = ROOT / "coupang_session.json"
LOG_PATH = ROOT / "crawl.log"

LOGIN_URL = "https://xauth.coupang.com/login/seller"
REPORT_URL = "https://advertising.coupang.com/report/performance"  # TODO: 실제 리포트 URL 확인

# ------------------------------------------------------------------ logging
logger = logging.getLogger("coupang")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
sh = logging.StreamHandler(sys.stdout); sh.setFormatter(fmt); logger.addHandler(sh)
fh = logging.FileHandler(LOG_PATH, encoding="utf-8"); fh.setFormatter(fmt); logger.addHandler(fh)


# ------------------------------------------------------------------ helpers
def screenshot_and_die(page: Page, msg: str) -> None:
    fname = ROOT / f"coupang_error_{datetime.now():%Y%m%d_%H%M%S}.png"
    try:
        page.screenshot(path=str(fname), full_page=True)
        logger.error(f"{msg} — 스크린샷 저장: {fname}")
    except Exception as e:
        logger.error(f"{msg} — 스크린샷 실패: {e}")
    sys.exit(1)


def parse_int(s: str) -> int:
    s = re.sub(r"[^\d\-]", "", s or "")
    return int(s) if s else 0


# ------------------------------------------------------------------ login
def ensure_login(page: Page) -> None:
    """세션이 없거나 만료됐으면 로그인."""
    page.goto(REPORT_URL, wait_until="domcontentloaded")
    # 로그인 페이지로 리다이렉트되었는지 검사
    if "login" in page.url.lower():
        logger.info("세션 없음/만료 — 로그인 시도")
        cid = os.getenv("COUPANG_ID"); cpw = os.getenv("COUPANG_PW")
        if not cid or not cpw:
            screenshot_and_die(page, ".env에 COUPANG_ID/COUPANG_PW 없음")

        try:
            # TODO: 실제 셀렉터 확인 필요 (첫 실행 때 DevTools로)
            page.fill('input[name="username"], input[name="loginId"], input[type="text"]', cid)
            page.fill('input[name="password"], input[type="password"]', cpw)
            page.click('button[type="submit"], button:has-text("로그인")')
            page.wait_for_url(re.compile(r".*advertising\.coupang\.com.*"), timeout=30_000)
        except PWTimeout:
            screenshot_and_die(page, "로그인 후 리포트 페이지로 이동 실패 (캡차/2FA?)")
        logger.info("로그인 성공")
    else:
        logger.info("세션 재사용 OK")


# ------------------------------------------------------------------ report scraping
def fetch_daily_costs(page: Page, start: str, end: str) -> list[dict]:
    """
    리포트 페이지에서 일자별 (날짜, 캠페인명, 광고비, 노출, 클릭, 전환, 전환매출) 추출.

    TODO (첫 실행 때 확정):
      - 기간 선택 컨트롤 셀렉터
      - 일자별 보기 토글
      - 테이블/행 셀렉터, 각 셀 인덱스
    """
    page.goto(REPORT_URL, wait_until="networkidle")

    # --- 1) 기간을 어제~오늘로 ----------------------------------------
    # 예시 — 실제 UI 보고 수정:
    # page.click('button:has-text("기간")')
    # page.fill('input[name="startDate"]', start)
    # page.fill('input[name="endDate"]', end)
    # page.click('button:has-text("적용")')
    # page.wait_for_load_state("networkidle")

    # --- 2) 일자별 + 캠페인별 보기로 토글 -----------------------------
    # page.click('text=일자별')

    # --- 3) 테이블 행 파싱 --------------------------------------------
    rows = page.query_selector_all("table tbody tr")  # TODO: 실제 셀렉터
    if not rows:
        screenshot_and_die(page, "리포트 테이블 행을 찾지 못함 — 셀렉터 확인 필요")

    out: list[dict] = []
    for r in rows:
        tds = [c.inner_text().strip() for c in r.query_selector_all("td")]
        if len(tds) < 6:
            continue
        # TODO: 컬럼 순서를 실제 페이지에 맞게 매핑
        # 가정: [날짜, 캠페인명, 노출, 클릭, 광고비, 전환, 전환매출]
        try:
            out.append({
                "날짜": tds[0],
                "캠페인": tds[1],
                "노출": parse_int(tds[2]),
                "클릭": parse_int(tds[3]),
                "광고비": parse_int(tds[4]),
                "전환": parse_int(tds[5]),
                "전환매출": parse_int(tds[6]) if len(tds) > 6 else 0,
            })
        except Exception as e:
            logger.warning(f"행 파싱 실패: {tds} ({e})")
    logger.info(f"파싱된 행: {len(out)}")
    return out


# ------------------------------------------------------------------ DB
def upsert_ads(rows: list[dict]) -> None:
    """캠페인 행들을 (날짜, 브랜드)로 합산해서 ads 테이블에 INSERT OR REPLACE."""
    agg: dict[tuple[str, str], dict] = {}
    for r in rows:
        brand = detect_brand(r["캠페인"]) or "기타"
        key = (r["날짜"], brand)
        a = agg.setdefault(key, {"광고비": 0, "노출수": 0, "클릭수": 0, "전환수": 0, "전환매출": 0})
        a["광고비"]   += r["광고비"]
        a["노출수"]   += r["노출"]
        a["클릭수"]   += r["클릭"]
        a["전환수"]   += r["전환"]
        a["전환매출"] += r["전환매출"]

    conn = sqlite3.connect(DB_PATH)
    try:
        for (date, brand), v in agg.items():
            conn.execute(
                "INSERT OR REPLACE INTO ads "
                "(날짜, 광고채널, 광고비, 노출수, 클릭수, 전환수, 전환매출, 브랜드) "
                "VALUES (?, '쿠팡', ?, ?, ?, ?, ?, ?)",
                (date, v["광고비"], v["노출수"], v["클릭수"], v["전환수"], v["전환매출"], brand),
            )
        conn.commit()
        logger.info(f"DB 업서트 완료: {len(agg)}건")
    finally:
        conn.close()


# ------------------------------------------------------------------ main
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--headless", action="store_true", help="헤드리스 모드 (셀렉터 검증 후 사용)")
    ap.add_argument("--days", type=int, default=1, help="오늘로부터 며칠 전부터 수집 (기본 1=어제)")
    args = ap.parse_args()

    load_dotenv(ROOT / ".env")
    today = datetime.now().date()
    start = (today - timedelta(days=args.days)).isoformat()
    end = today.isoformat()
    logger.info(f"수집 기간: {start} ~ {end} (headless={args.headless})")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        ctx_kw = {"storage_state": str(SESSION_PATH)} if SESSION_PATH.exists() else {}
        context = browser.new_context(**ctx_kw)
        page = context.new_page()
        try:
            ensure_login(page)
            context.storage_state(path=str(SESSION_PATH))  # 갱신
            rows = fetch_daily_costs(page, start, end)
            if not rows:
                screenshot_and_die(page, "수집 결과 0건")
            upsert_ads(rows)
        except SystemExit:
            raise
        except Exception as e:
            logger.exception("예외 발생")
            screenshot_and_die(page, f"예외: {e}")
        finally:
            context.close(); browser.close()

    logger.info("완료")


if __name__ == "__main__":
    main()
