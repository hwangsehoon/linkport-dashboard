"""
쿠팡 WING API 연동 모듈
- HMAC-SHA256 서명 인증
- 로켓그로스 + 일반 마켓플레이스 주문 조회

API 문서: https://developers.coupangcorp.com/
"""
import os
import time
import requests
import pandas as pd
import hmac
import hashlib
import urllib.parse
from datetime import date, datetime, timedelta
from config import COUPANG_ACCESS_KEY, COUPANG_SECRET_KEY, COUPANG_VENDOR_ID

from brand_config import detect_brand as _detect_brand


class CoupangClient:
    BASE_URL = "https://api-gateway.coupang.com"

    def __init__(self):
        pass

    def _generate_headers(self, method: str, path: str, query_params: dict = None) -> dict:
        """HMAC-SHA256 서명 헤더 생성"""
        from datetime import datetime as dt_class, timezone as tz
        utc_now = dt_class.now(tz.utc)
        datetime_str = utc_now.strftime('%y%m%d') + 'T' + utc_now.strftime('%H%M%S') + 'Z'

        if query_params:
            query = urllib.parse.urlencode(query_params)
        else:
            query = ""

        message = datetime_str + method + path + query

        signature = hmac.new(
            COUPANG_SECRET_KEY.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        authorization = (
            f"CEA algorithm=HmacSHA256, "
            f"access-key={COUPANG_ACCESS_KEY}, "
            f"signed-date={datetime_str}, "
            f"signature={signature}"
        )
        return {
            "Authorization": authorization,
            "Content-Type": "application/json;charset=UTF-8",
        }

    def fetch_rocket_growth(self, start_date: date, end_date: date) -> pd.DataFrame:
        """로켓그로스 주문 조회"""
        path = f"/v2/providers/rg_open_api/apis/api/v1/vendors/{COUPANG_VENDOR_ID}/rg/orders"
        all_orders = []

        # 최대 30일 단위로 조회
        current_start = start_date
        while current_start <= end_date:
            current_end = min(current_start + timedelta(days=29), end_date)

            query_params = {
                "paidDateFrom": current_start.strftime("%Y%m%d"),
                "paidDateTo": current_end.strftime("%Y%m%d"),
            }

            next_token = ""
            while True:
                params = dict(query_params)
                if next_token:
                    params["nextToken"] = next_token

                headers = self._generate_headers("GET", path, params)
                query_string = urllib.parse.urlencode(params)
                url = f"{self.BASE_URL}{path}?{query_string}"

                try:
                    resp = requests.get(url, headers=headers, timeout=30)
                    if resp.status_code != 200:
                        print(f"[쿠팡 로켓그로스] 조회 실패 ({resp.status_code}): {resp.text[:200]}")
                        break
                    result = resp.json()
                    data = result.get("data", [])
                    if data:
                        all_orders.extend(data)
                    next_token = result.get("nextToken", "")
                    if not next_token:
                        break
                except Exception as e:
                    print(f"[쿠팡 로켓그로스] {current_start}~{current_end} 조회 실패: {e}")
                    break
                time.sleep(0.3)

            current_start = current_end + timedelta(days=1)

        if not all_orders:
            return pd.DataFrame()

        rows = []
        for order in all_orders:
            # paidAt은 밀리초 타임스탬프
            paid_at = order.get("paidAt", 0)
            if paid_at:
                order_date = datetime.fromtimestamp(paid_at / 1000).date()
            else:
                continue

            for item in order.get("orderItems", []):
                price = int(float(item.get("unitSalesPrice", 0)))
                qty = int(item.get("salesQuantity", 1))
                product_name = item.get("productName", "")
                rows.append({
                    "날짜": order_date,
                    "스토어": "링포(쿠팡)",
                    "채널": "쿠팡",
                    "주문건수": 1,
                    "매출": price * qty,
                    "브랜드": _detect_brand(product_name),
                })

        df = pd.DataFrame(rows)
        df = df.groupby(["날짜", "스토어", "채널", "브랜드"]).agg(
            주문건수=("주문건수", "sum"),
            매출=("매출", "sum"),
        ).reset_index()
        df["객단가"] = (df["매출"] / df["주문건수"]).astype(int)
        df["순방문자수"] = 0
        df["전환율"] = 0.0
        return df

    def fetch_marketplace(self, start_date: date, end_date: date) -> pd.DataFrame:
        """일반 마켓플레이스 주문 조회"""
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{COUPANG_VENDOR_ID}/ordersheets"
        all_orders = []

        current_start = start_date
        while current_start <= end_date:
            current_end = min(current_start + timedelta(days=6), end_date)

            query_params = {
                "createdAtFrom": current_start.isoformat(),
                "createdAtTo": current_end.isoformat(),
                "status": "ACCEPT",
            }

            headers = self._generate_headers("GET", path, query_params)
            query_string = urllib.parse.urlencode(query_params)
            url = f"{self.BASE_URL}{path}?{query_string}"

            try:
                resp = requests.get(url, headers=headers, timeout=30)
                if resp.status_code == 200:
                    data = resp.json().get("data", [])
                    if data:
                        all_orders.extend(data)
            except Exception as e:
                print(f"[쿠팡 마켓] {current_start}~{current_end} 조회 실패: {e}")

            current_start = current_end + timedelta(days=1)
            time.sleep(0.3)

        if not all_orders:
            return pd.DataFrame()

        rows = []
        for order in all_orders:
            order_date_str = order.get("orderedAt", "")[:10]
            sale_price = int(order.get("orderPrice", 0))

            rows.append({
                "날짜": pd.to_datetime(order_date_str).date(),
                "스토어": "링포(쿠팡)",
                "채널": "쿠팡",
                "주문건수": 1,
                "매출": sale_price,
                "브랜드": "기타",
            })

        df = pd.DataFrame(rows)
        df = df.groupby(["날짜", "스토어", "채널", "브랜드"]).agg(
            주문건수=("주문건수", "sum"),
            매출=("매출", "sum"),
        ).reset_index()
        df["객단가"] = (df["매출"] / df["주문건수"]).astype(int)
        df["순방문자수"] = 0
        df["전환율"] = 0.0
        return df

    def fetch_all_orders(self, start_date: date, end_date: date) -> pd.DataFrame:
        """로켓그로스 + 마켓플레이스 주문 통합 조회"""
        dfs = []

        # 로켓그로스
        rg_df = self.fetch_rocket_growth(start_date, end_date)
        if not rg_df.empty:
            dfs.append(rg_df)

        # 일반 마켓플레이스
        mp_df = self.fetch_marketplace(start_date, end_date)
        if not mp_df.empty:
            dfs.append(mp_df)

        if dfs:
            combined = pd.concat(dfs, ignore_index=True)
            if "브랜드" not in combined.columns:
                combined["브랜드"] = "기타"
            # 날짜+브랜드별 재집계
            combined = combined.groupby(["날짜", "스토어", "채널", "브랜드"]).agg(
                주문건수=("주문건수", "sum"),
                매출=("매출", "sum"),
            ).reset_index()
            combined["객단가"] = (combined["매출"] / combined["주문건수"]).astype(int)
            combined["순방문자수"] = 0
            combined["전환율"] = 0.0
            return combined
        return pd.DataFrame()


def fetch_coupang(start_date: date, end_date: date) -> pd.DataFrame:
    """쿠팡 전체 주문 데이터 조회"""
    client = CoupangClient()
    return client.fetch_all_orders(start_date, end_date)
