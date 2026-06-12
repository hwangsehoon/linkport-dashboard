"""
쿠팡 광고센터 일별 광고비 자동 수집 (내부 GraphQL API 방식)

쿠팡은 셀러용 광고 리포트 공개 API를 제공하지 않으므로,
광고센터(advertising.coupang.com)에 로그인한 브라우저 세션으로
내부 GraphQL 엔드포인트를 호출해 일자별·캠페인별 광고 성과를 받아온다.
DOM 스크래핑이 아니라 페이지가 실제로 쓰는 API를 그대로 호출하므로 안정적이다.

  엔드포인트 : POST /marketing-reporting/v2/graphql  (쿠키 인증, Akamai 봇 보호)
  쿼리       : 매출성장 getCampaignsTotalAdPerformance + 신규구매확보 getNcaAllCampaignOnePager
               (둘 다 하루 단위로 호출해 캠페인별 합산 → 두 광고 유형 합쳐서 저장)
  브랜드     : 캠페인명 → brand_config.detect_brand
               (마르문→아자차, 반드럽/풋쉐이버→반드럽)
  저장       : Supabase ads 테이블, 광고채널="쿠팡"

중요: 쿠팡은 헤드리스(창 없는) 브라우저를 봇으로 차단하므로 항상 헤드풀로 띄운다.
      자동 모드(--auto)에서는 창을 화면 밖(-2400,-2400)으로 보내 안 보이게 한다.
      → 작업 스케줄러는 "사용자가 로그인했을 때만 실행"으로 등록해야 함 (창 표시 필요).

사용법:
    python coupang_crawler.py                # 로그인 모드(창 표시): 최초/재로그인
    python coupang_crawler.py --days 7       # 최근 7일 + 30일 내 누락분 수집
    python coupang_crawler.py --auto         # 자동 모드(창 숨김): 스케줄러/sync_data용

준비:
    pip install playwright python-dotenv
    playwright install chromium
    최초 1회 `python coupang_crawler.py`(로그인 모드) → 광고센터 로그인 → coupang_profile/ 에 세션 유지.
    이후 세션 만료되면 다시 로그인 모드로 실행해 재로그인.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from brand_config import detect_brand
from api.db import save_ads, mark_fetched, get_missing_dates

ROOT = Path(__file__).parent
PROFILE_DIR = ROOT / "coupang_profile"   # 지속 브라우저 프로필 (쿠키·토큰 유지)
LOG_PATH = ROOT / "crawl.log"


class SessionExpired(Exception):
    """로그인 세션이 없거나 만료됨 — 헤드풀 재로그인 필요."""

REPORT_URL = "https://advertising.coupang.com/marketing-reporting/billboard/one-pager"
GRAPHQL_URL = "https://advertising.coupang.com/marketing-reporting/v2/graphql"

# 매출 성장 캠페인 — 하루(캠페인별) 광고 성과
CAMPAIGN_QUERY = (
    "query ($page: Int!, $pageSize: Int!, $startDate: String!, $endDate: String!) {"
    "  report: getCampaignsTotalAdPerformance(page: $page, pageSize: $pageSize,"
    "  startDate: $startDate, endDate: $endDate) {"
    "    totalCount"
    "    total { campaignId campaignName impressions clicks orders adGmv adCostSum }"
    "  }"
    "}"
)

# 신규 구매 고객 확보(NCA) 캠페인 — 하루(캠페인별) 광고 성과 (지표 이름이 다름)
NCA_QUERY = (
    "query ($startDate: String!, $endDate: String!) {"
    "  report: getNcaAllCampaignOnePager(startDate: $startDate, endDate: $endDate) {"
    "    campaignPerformance {"
    "      campaign_name nca_ad_cost_sum view_count billable_event_count"
    "      new_to_brand_users_12mo repeat_purchaser_count total_gmv_from_nca"
    "    }"
    "  }"
    "}"
)

# ------------------------------------------------------------------ logging
logger = logging.getLogger("coupang_ads")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
_sh = logging.StreamHandler(sys.stdout); _sh.setFormatter(_fmt); logger.addHandler(_sh)
_fh = logging.FileHandler(LOG_PATH, encoding="utf-8"); _fh.setFormatter(_fmt); logger.addHandler(_fh)


def _kst_day_bounds(d: date) -> tuple[str, str]:
    """KST 하루 D → API가 쓰는 UTC 경계 문자열 (start=end=(D-1)T15:00:00.000Z)."""
    iso = (d - timedelta(days=1)).strftime("%Y-%m-%dT15:00:00.000Z")
    return iso, iso


def _logged_in_url(page) -> bool:
    u = page.url.lower()
    return "advertising.coupang.com" in u and "login" not in u and "xauth" not in u


def _wait_settle(page, total_ms: int = 9000) -> None:
    """SPA의 클라이언트측 인증 리다이렉트가 끝날 때까지 폴링 대기."""
    waited = 0
    while waited < total_ms:
        page.wait_for_timeout(1000)
        waited += 1000
        if not _logged_in_url(page):
            return  # 이미 로그인 화면으로 튕김 — 더 기다릴 필요 없음
    # advertising 도메인에 머물러 있으면 로그인 상태로 간주


def ensure_session(page, auto: bool) -> None:
    """세션 확인. 만료면 예외 → coupang_login.bat 로 직접 로그인해야 함.
    (쿠팡 로그인은 Akamai 봇차단이라 자동 로그인 금지 — 자동화로 로그인 시도하면
     IP까지 차단될 수 있어 절대 시도하지 않는다. 로그인은 평범한 Chrome으로만.)"""
    page.goto(REPORT_URL, wait_until="domcontentloaded")
    _wait_settle(page)
    if _logged_in_url(page):
        logger.info("세션 재사용 OK")
        return
    raise SessionExpired(
        "세션 만료 — `coupang_login.bat` 실행 후 쿠팡에 직접 로그인하고 Chrome을 닫으세요. "
        "(자동 로그인은 Akamai 차단으로 불가)")
    logger.info("로그인 성공")


def _gql(page, query: str, variables: dict):
    """GraphQL 호출. 반환 data 딕셔너리, 오류/봇차단이면 None."""
    payload = [{"variables": variables, "query": query}]
    result = page.evaluate(
        """async (args) => {
            const r = await fetch(args.url, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(args.payload),
                credentials: 'include',
            });
            return {status: r.status, text: await r.text()};
        }""",
        {"url": GRAPHQL_URL, "payload": payload},
    )
    if result["status"] != 200:
        logger.warning(f"  HTTP {result['status']} — {result['text'][:120]}")
        return None
    body = json.loads(result["text"])
    entry = body[0] if isinstance(body, list) and body else body
    data = (entry or {}).get("data") or {}
    if data.get("report") is None:
        return None  # report=null → 봇 차단/인증 실패/당일 미집계
    return data["report"]


def fetch_day(page, d: date) -> tuple[bool, list[dict]]:
    """하루치 광고 성과 = 매출성장 + 신규구매확보(NCA) 캠페인 합산.
    반환 (ok, rows): ok=True면 두 쿼리 모두 정상 응답(광고 0개여도 완료),
    ok=False면 둘 중 하나라도 오류/미집계 → 기록 안 하고 다음 실행에 재시도."""
    start, end = _kst_day_bounds(d)
    rows: list[dict] = []

    # ① 매출 성장 캠페인
    rep = _gql(page, CAMPAIGN_QUERY,
               {"page": 0, "pageSize": 500, "startDate": start, "endDate": end})
    if rep is None:
        return False, []
    for c in rep.get("total") or []:
        rows.append({
            "날짜": d.isoformat(), "광고채널": "쿠팡",
            "광고비": int(c.get("adCostSum", 0)),
            "노출수": int(c.get("impressions", 0)),
            "클릭수": int(c.get("clicks", 0)),
            "전환수": int(c.get("orders", 0)),
            "전환매출": int(c.get("adGmv", 0)),
            "브랜드": detect_brand(c.get("campaignName", "")),
        })

    # ② 신규 구매 고객 확보(NCA) 캠페인 — 지표 이름이 달라 별도 매핑
    nca = _gql(page, NCA_QUERY, {"startDate": start, "endDate": end})
    if nca is None:
        return False, []
    for c in nca.get("campaignPerformance") or []:
        rows.append({
            "날짜": d.isoformat(), "광고채널": "쿠팡",
            "광고비": int(c.get("nca_ad_cost_sum", 0)),
            "노출수": int(c.get("view_count", 0)),
            "클릭수": int(c.get("billable_event_count", 0)),
            "전환수": int(c.get("new_to_brand_users_12mo", 0)) + int(c.get("repeat_purchaser_count", 0)),
            "전환매출": int(c.get("total_gmv_from_nca", 0)),
            "브랜드": detect_brand(c.get("campaign_name", "")),
        })

    return True, rows


def _dates_to_fetch(days: int, lookback: int) -> list[date]:
    """수집할 날짜 = (최근 lookback일 중 fetch_log에 없는 누락분) ∪ (최근 days일 항상 재수집).
    → 일시적 쿠팡 오류로 빈 날도 lookback 기간 안에 자동으로 다시 채워짐.
    → 최근 days일은 쿠팡의 사후 수정/정산 반영 위해 매번 덮어씀."""
    today = datetime.now().date()
    missing = set(get_missing_dates("coupang_ads", today - timedelta(days=lookback), today))
    for i in range(days):
        missing.add(today - timedelta(days=i))
    return sorted(d for d in missing if d <= today)


def crawl(days: int, auto: bool, lookback: int = 30) -> None:
    """auto=True: 창을 화면 밖으로 숨김(스케줄러/sync용). auto=False: 창 표시(디버깅용).
    어느 쪽이든 세션이 없으면 예외 — 로그인은 coupang_login.bat로만 한다.
    쿠팡이 헤드리스를 차단하므로 항상 헤드풀 + 진짜 Chrome으로 띄운다."""
    targets = _dates_to_fetch(days, lookback)
    logger.info(f"수집 대상 {len(targets)}일 "
                f"(최근 {days}일 + 최근 {lookback}일 내 누락분), auto={auto}")

    # Akamai 봇 차단 회피: 헤드리스 금지 + 자동화 흔적 숨김 + 진짜 Chrome 사용.
    # 자동 모드면 창을 화면 밖으로 보내 안 보이게.
    stealth_args = ["--disable-blink-features=AutomationControlled"]
    if auto:
        stealth_args += ["--window-position=-2400,-2400", "--window-size=1280,900"]

    all_rows, ok_dates = [], []
    with sync_playwright() as p:
        # 지속 프로필 + 진짜 Chrome(channel) + 자동화 플래그 제거 → 봇 탐지 회피
        context = p.chromium.launch_persistent_context(
            str(PROFILE_DIR), headless=False, channel="chrome",
            args=stealth_args, ignore_default_args=["--enable-automation"],
        )
        # navigator.webdriver 흔적 제거
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        page = context.pages[0] if context.pages else context.new_page()
        try:
            ensure_session(page, auto)

            for cur in targets:
                ok, day_rows = fetch_day(page, cur)
                if not ok:
                    continue  # 오류/미집계 → 기록 안 함, 다음 실행에 재시도
                ok_dates.append(cur)
                if day_rows:
                    total = sum(r["광고비"] for r in day_rows)
                    logger.info(f"  {cur}: {len(day_rows)}개 브랜드, 광고비 {total:,}원")
                    all_rows.extend(day_rows)
                else:
                    logger.info(f"  {cur}: 광고 없음 (정상)")
        finally:
            context.close()

    if all_rows:
        df = pd.DataFrame(all_rows)
        df = df.groupby(["날짜", "광고채널", "브랜드"], as_index=False).agg(
            광고비=("광고비", "sum"), 노출수=("노출수", "sum"), 클릭수=("클릭수", "sum"),
            전환수=("전환수", "sum"), 전환매출=("전환매출", "sum"),
        )
        save_ads(df)
    # API가 정상 응답한 날만 완료 기록 (광고 0원인 날도 포함 → 헛스캔 방지)
    if ok_dates:
        mark_fetched("coupang_ads", [d.isoformat() for d in ok_dates])
    logger.info(f"완료: 정상 {len(ok_dates)}일, 저장 {len(all_rows)}행, "
                f"실패/미집계 {len(targets) - len(ok_dates)}일")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=3, help="오늘 포함 최근 며칠 수집 (기본 3)")
    ap.add_argument("--auto", action="store_true",
                    help="자동 모드: 창 숨김, 세션 만료 시 그냥 종료 (스케줄러/sync용). "
                         "생략하면 로그인 모드(창 표시).")
    args = ap.parse_args()
    try:
        crawl(args.days, args.auto)
    except SessionExpired as e:
        logger.error(str(e))
        sys.exit(2)


if __name__ == "__main__":
    main()
