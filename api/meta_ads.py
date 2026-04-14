"""
Meta Marketing API 연동 모듈
- 여러 광고 계정 인사이트 동시 조회
- 캠페인별 브랜드 분류
- 일별 광고비/노출/클릭/전환 데이터

API 문서: https://developers.facebook.com/docs/marketing-api/insights/
"""
import requests
import pandas as pd
from datetime import date
from config import META_AD_ACCOUNT_ID
from api.token_manager import get_meta_access_token as _get_meta_token
from brand_config import detect_brand

# Meta 광고 계정 → 브랜드 매핑 (계정 전체가 단일 브랜드인 경우)
ACCOUNT_BRAND = {
    "1809677422517700": "반드럽",       # 03_반드럽
    "290400492505668": "윈토르",        # 04_윈토르
    "886770819805448": "반드럽",        # L_반드럽
    "471404312082072": "윈토르",        # 윈토르
}

# 캠페인 접두사 → 브랜드 매핑 (아자차/웰바이오젠 합산 계정용)
CAMPAIGN_PREFIX_BRAND = {
    "M_": "아자차",
    "T_": "웰바이오젠",
    "F_": "반드럽",
    "B_": "반드럽",
    "BF_": "반드럽",
}

# 건너뛸 계정 (사용 안 함)
SKIP_ACCOUNTS = {"708072902861524", "901145563971818", "319218092918051", "217810891099881"}


def _detect_campaign_brand(campaign_name: str) -> str:
    """캠페인명에서 브랜드 감지 (접두사 우선, 없으면 키워드 매칭)"""
    for prefix, brand in CAMPAIGN_PREFIX_BRAND.items():
        if campaign_name.startswith(prefix):
            return brand
    return detect_brand(campaign_name)


class MetaAdsClient:
    BASE_URL = "https://graph.facebook.com/v21.0"

    def __init__(self):
        pass

    def get_all_ad_accounts(self) -> list:
        """접근 가능한 광고 계정 목록 조회 (활성 계정만)"""
        url = f"{self.BASE_URL}/me/adaccounts"
        params = {
            "access_token": _get_meta_token(),
            "fields": "name,account_id,amount_spent",
            "limit": 100,
        }
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            accounts = resp.json().get("data", [])
            return [
                a for a in accounts
                if int(a.get("amount_spent", 0)) > 0
                and a.get("account_id") not in SKIP_ACCOUNTS
            ]
        except Exception as e:
            print(f"[Meta 광고] 계정 목록 조회 실패: {e}")
            return []

    def fetch_account_insights(self, account_id: str, account_brand: str,
                                start_date: date, end_date: date) -> list:
        """특정 광고 계정의 캠페인별 일별 인사이트 조회"""
        # 계정 전체가 단일 브랜드면 캠페인 분류 불필요
        if account_brand:
            return self._fetch_account_level(account_id, account_brand, start_date, end_date)
        else:
            return self._fetch_campaign_level(account_id, start_date, end_date)

    def _fetch_account_level(self, account_id: str, brand: str,
                              start_date: date, end_date: date) -> list:
        """계정 단위 조회 (단일 브랜드 계정)"""
        url = f"{self.BASE_URL}/act_{account_id}/insights"
        all_rows = []
        params = {
            "access_token": _get_meta_token(),
            "time_range": f'{{"since":"{start_date.isoformat()}","until":"{end_date.isoformat()}"}}',
            "time_increment": 1,
            "fields": "spend,impressions,clicks,actions,action_values",
            "limit": 500,
        }
        self._collect_insights(url, params, brand, all_rows)
        return all_rows

    def _fetch_campaign_level(self, account_id: str,
                               start_date: date, end_date: date) -> list:
        """캠페인 단위 조회 (아자차/웰바이오젠 합산 계정)"""
        url = f"{self.BASE_URL}/act_{account_id}/insights"
        all_rows = []
        params = {
            "access_token": _get_meta_token(),
            "time_range": f'{{"since":"{start_date.isoformat()}","until":"{end_date.isoformat()}"}}',
            "time_increment": 1,
            "level": "campaign",
            "fields": "campaign_name,spend,impressions,clicks,actions,action_values",
            "limit": 500,
        }
        self._collect_insights(url, params, None, all_rows)
        return all_rows

    def _collect_insights(self, url: str, params: dict, fixed_brand: str, all_rows: list):
        """인사이트 수집 (페이징 포함)"""
        try:
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            insights = data.get("data", [])
            while insights:
                for row in insights:
                    conversions = 0
                    conversion_revenue = 0

                    for action in row.get("actions", []):
                        if action.get("action_type") == "purchase":
                            conversions = int(action.get("value", 0))
                            break

                    for av in row.get("action_values", []):
                        if av.get("action_type") == "purchase":
                            conversion_revenue = int(float(av.get("value", 0)))
                            break

                    # 브랜드 결정
                    if fixed_brand:
                        brand = fixed_brand
                    else:
                        campaign_name = row.get("campaign_name", "")
                        brand = _detect_campaign_brand(campaign_name)

                    all_rows.append({
                        "날짜": pd.to_datetime(row["date_start"]).date(),
                        "광고채널": "Meta",
                        "브랜드": brand,
                        "광고비": int(float(row.get("spend", 0))),
                        "노출수": int(row.get("impressions", 0)),
                        "클릭수": int(row.get("clicks", 0)),
                        "전환수": conversions,
                        "전환매출": conversion_revenue,
                    })

                next_url = data.get("paging", {}).get("next")
                if next_url:
                    resp = requests.get(next_url, timeout=60)
                    resp.raise_for_status()
                    data = resp.json()
                    insights = data.get("data", [])
                else:
                    break

        except Exception as e:
            print(f"[Meta 광고] 인사이트 조회 실패: {e}")

    def fetch_insights(self, start_date: date, end_date: date) -> pd.DataFrame:
        """모든 광고 계정의 인사이트 조회 (브랜드별)"""
        accounts = self.get_all_ad_accounts()
        if not accounts:
            return pd.DataFrame()

        all_rows = []
        for account in accounts:
            acc_id = account.get("account_id", "")
            acc_brand = ACCOUNT_BRAND.get(acc_id)  # 단일 브랜드 계정이면 값 있음
            rows = self.fetch_account_insights(acc_id, acc_brand, start_date, end_date)
            all_rows.extend(rows)

        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)
        df = df.groupby(["날짜", "광고채널", "브랜드"]).agg(
            광고비=("광고비", "sum"),
            노출수=("노출수", "sum"),
            클릭수=("클릭수", "sum"),
            전환수=("전환수", "sum"),
            전환매출=("전환매출", "sum"),
        ).reset_index()
        return df


def fetch_meta_ads(start_date: date, end_date: date) -> pd.DataFrame:
    """Meta 광고 데이터 조회"""
    client = MetaAdsClient()
    return client.fetch_insights(start_date, end_date)
