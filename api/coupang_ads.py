"""
쿠팡 광고 API 연동 모듈
- 광고 성과 리포트 조회

참고: 쿠팡 광고 API는 공식 문서가 제한적입니다.
      광고센터에서 발급한 API 키를 사용합니다.
"""
import requests
import pandas as pd
import hmac
import hashlib
import datetime as dt
from datetime import date
from config import COUPANG_ADS_ACCESS_KEY, COUPANG_ADS_SECRET_KEY, COUPANG_ADS_ADVERTISER_ID


class CoupangAdsClient:
    BASE_URL = "https://api.ads.coupang.com"

    def __init__(self):
        pass

    def _generate_auth(self, method: str, path: str) -> dict:
        """인증 헤더 생성"""
        timestamp = dt.datetime.utcnow().strftime("%y%m%dT%H%M%SZ")
        message = f"{timestamp}{method}{path}"
        signature = hmac.new(
            COUPANG_ADS_SECRET_KEY.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        return {
            "Authorization": f"CEA algorithm=HmacSHA256, access-key={COUPANG_ADS_ACCESS_KEY}, signed-date={timestamp}, signature={signature}",
            "Content-Type": "application/json",
        }

    def fetch_report(self, start_date: date, end_date: date) -> pd.DataFrame:
        """광고 성과 리포트 조회"""
        path = f"/v1/reports/advertisers/{COUPANG_ADS_ADVERTISER_ID}/campaigns"
        headers = self._generate_auth("POST", path)

        body = {
            "reportType": "CAMPAIGN",
            "timeUnit": "DAY",
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "metrics": ["impressions", "clicks", "cost", "conversions", "conversionRevenue"],
        }

        try:
            resp = requests.post(
                f"{self.BASE_URL}{path}",
                headers=headers,
                json=body,
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json().get("data", [])
        except Exception as e:
            print(f"[쿠팡 광고] 리포트 조회 실패: {e}")
            return pd.DataFrame()

        if not data:
            return pd.DataFrame()

        rows = []
        for row in data:
            rows.append({
                "날짜": pd.to_datetime(row.get("date", "")).date(),
                "광고채널": "쿠팡 광고",
                "광고비": int(row.get("cost", 0)),
                "노출수": int(row.get("impressions", 0)),
                "클릭수": int(row.get("clicks", 0)),
                "전환수": int(row.get("conversions", 0)),
                "전환매출": int(row.get("conversionRevenue", 0)),
            })

        # 일별 집계
        df = pd.DataFrame(rows)
        df = df.groupby(["날짜", "광고채널"]).agg(
            광고비=("광고비", "sum"),
            노출수=("노출수", "sum"),
            클릭수=("클릭수", "sum"),
            전환수=("전환수", "sum"),
            전환매출=("전환매출", "sum"),
        ).reset_index()
        return df


def fetch_coupang_ads(start_date: date, end_date: date) -> pd.DataFrame:
    """쿠팡 광고 데이터 조회"""
    client = CoupangAdsClient()
    return client.fetch_report(start_date, end_date)
