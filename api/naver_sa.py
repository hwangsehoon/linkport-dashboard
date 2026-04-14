"""
네이버 검색광고 API 연동 모듈
- HMAC 인증
- AD 리포트 (노출/클릭/광고비) + AD_CONVERSION 리포트 (전환수/전환매출) 조합

API 문서: https://naver.github.io/searchad-apidoc/
"""
import requests
import pandas as pd
import hmac
import hashlib
import base64
import time
from datetime import date, timedelta
from config import NAVER_SA_API_KEY, NAVER_SA_SECRET_KEY, NAVER_SA_CUSTOMER_ID

from brand_config import get_brand_by_campaign_id


class NaverSAClient:
    BASE_URL = "https://api.searchad.naver.com"

    def __init__(self):
        pass

    def _generate_headers(self, method: str, path: str) -> dict:
        timestamp = str(int(time.time() * 1000))
        sign = f"{timestamp}.{method}.{path}"
        signature = base64.b64encode(
            hmac.new(
                NAVER_SA_SECRET_KEY.encode("utf-8"),
                sign.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")
        return {
            "X-Timestamp": timestamp,
            "X-API-KEY": NAVER_SA_API_KEY,
            "X-Customer": NAVER_SA_CUSTOMER_ID,
            "X-Signature": signature,
            "Content-Type": "application/json",
        }

    def _request_and_download_report(self, report_type: str, stat_date: date) -> str:
        """리포트 생성 → 빌드 대기 → 다운로드 → TSV 텍스트 반환"""
        # 리포트 생성
        path = "/stat-reports"
        headers = self._generate_headers("POST", path)
        body = {"reportTp": report_type, "statDt": stat_date.isoformat()}

        try:
            resp = requests.post(f"{self.BASE_URL}{path}", headers=headers, json=body, timeout=30)
            if resp.status_code != 200:
                return ""
            job_id = resp.json().get("reportJobId")
            if not job_id:
                return ""

            # 빌드 대기
            for _ in range(10):
                time.sleep(3)
                check_path = f"/stat-reports/{job_id}"
                check_headers = self._generate_headers("GET", check_path)
                check_resp = requests.get(
                    f"{self.BASE_URL}{check_path}", headers=check_headers, timeout=30
                )
                if check_resp.status_code == 200:
                    data = check_resp.json()
                    if data.get("status") == "BUILT":
                        download_url = data.get("downloadUrl", "")
                        if download_url:
                            dl_headers = self._generate_headers("GET", "/report-download")
                            dl_resp = requests.get(download_url, headers=dl_headers, timeout=60)
                            if dl_resp.status_code == 200:
                                return dl_resp.text
                        return ""
                    elif data.get("status") in ("REGIST", "RUNNING"):
                        continue
                    else:
                        return ""
            return ""
        except Exception as e:
            print(f"[네이버SA] {report_type} 리포트 실패 ({stat_date}): {e}")
            return ""

    def _parse_ad_report(self, tsv_text: str) -> dict:
        """AD 리포트 TSV → 날짜+브랜드별 {광고비, 노출수, 클릭수} 집계"""
        # [0]Date [2]CampaignID [9]Impression [10]Click [11]Cost
        result = {}
        for line in tsv_text.strip().split("\n"):
            parts = line.split("\t")
            if len(parts) < 12:
                continue
            dt = parts[0]
            campaign_id = parts[2]
            brand = get_brand_by_campaign_id(campaign_id)
            try:
                cost = int(float(parts[11]))
            except ValueError:
                cost = 0
            impression = int(parts[9]) if parts[9].isdigit() else 0
            click = int(parts[10]) if parts[10].isdigit() else 0

            key = (dt, brand)
            if key not in result:
                result[key] = {"광고비": 0, "노출수": 0, "클릭수": 0}
            result[key]["광고비"] += cost
            result[key]["노출수"] += impression
            result[key]["클릭수"] += click
        return result

    def _parse_conversion_report(self, tsv_text: str) -> dict:
        """AD_CONVERSION 리포트 TSV → 날짜+브랜드별 {전환수, 전환매출} 집계"""
        # [0]Date [2]CampaignID [11]ConversionCount [12]SalesByConversion
        result = {}
        for line in tsv_text.strip().split("\n"):
            parts = line.split("\t")
            if len(parts) < 13:
                continue
            dt = parts[0]
            campaign_id = parts[2]
            brand = get_brand_by_campaign_id(campaign_id)
            try:
                conv_count = int(parts[11]) if parts[11].isdigit() else 0
                conv_sales = int(float(parts[12])) if parts[12] else 0
            except (ValueError, IndexError):
                conv_count = 0
                conv_sales = 0

            key = (dt, brand)
            if key not in result:
                result[key] = {"전환수": 0, "전환매출": 0}
            result[key]["전환수"] += conv_count
            result[key]["전환매출"] += conv_sales
        return result

    def fetch_stats(self, start_date: date, end_date: date) -> pd.DataFrame:
        """일별 광고 성과 + 전환 데이터 조합"""
        all_rows = []

        current = start_date
        while current <= end_date:
            # AD 리포트 (노출/클릭/광고비)
            ad_tsv = self._request_and_download_report("AD", current)
            ad_data = self._parse_ad_report(ad_tsv) if ad_tsv else {}

            # AD_CONVERSION 리포트 (전환수/전환매출)
            conv_tsv = self._request_and_download_report("AD_CONVERSION", current)
            conv_data = self._parse_conversion_report(conv_tsv) if conv_tsv else {}

            # 합치기 (키 = (날짜, 브랜드))
            all_keys = set(list(ad_data.keys()) + list(conv_data.keys()))
            for key in all_keys:
                dt, brand = key
                ad = ad_data.get(key, {"광고비": 0, "노출수": 0, "클릭수": 0})
                conv = conv_data.get(key, {"전환수": 0, "전환매출": 0})
                all_rows.append({
                    "날짜": pd.to_datetime(dt).date(),
                    "광고채널": "Naver SA",
                    "브랜드": brand,
                    "광고비": ad["광고비"],
                    "노출수": ad["노출수"],
                    "클릭수": ad["클릭수"],
                    "전환수": conv["전환수"],
                    "전환매출": conv["전환매출"],
                })

            current += timedelta(days=1)
            time.sleep(0.5)

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


def fetch_naver_sa(start_date: date, end_date: date) -> pd.DataFrame:
    client = NaverSAClient()
    return client.fetch_stats(start_date, end_date)
