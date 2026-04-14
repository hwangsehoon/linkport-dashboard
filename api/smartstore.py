"""
네이버 스마트스토어 커머스API 연동 모듈
- bcrypt 기반 인증 토큰 발급
- 주문 데이터 조회 (24시간 단위 제한)

API 문서: https://apicenter.commerce.naver.com/
"""
import requests
import pandas as pd
import bcrypt
import base64
import time
import urllib.parse
from datetime import date, datetime, timedelta, timezone
from config import SMARTSTORE_CLIENT_ID, SMARTSTORE_CLIENT_SECRET

KST = timezone(timedelta(hours=9))


class SmartStoreClient:
    TOKEN_URL = "https://api.commerce.naver.com/external/v1/oauth2/token"
    BASE_URL = "https://api.commerce.naver.com/external"

    def __init__(self):
        self.access_token = None

    def _make_signature(self) -> tuple:
        """bcrypt 기반 client_secret_sign 생성"""
        timestamp = int(time.time() * 1000)
        password = f"{SMARTSTORE_CLIENT_ID}_{timestamp}"
        hashed = bcrypt.hashpw(
            password.encode("utf-8"),
            SMARTSTORE_CLIENT_SECRET.encode("utf-8"),
        )
        client_secret_sign = base64.urlsafe_b64encode(hashed).decode("utf-8")
        return str(timestamp), client_secret_sign

    def authenticate(self) -> bool:
        """인증 토큰 발급"""
        timestamp, sign = self._make_signature()
        try:
            resp = requests.post(self.TOKEN_URL, data={
                "client_id": SMARTSTORE_CLIENT_ID,
                "timestamp": timestamp,
                "client_secret_sign": sign,
                "grant_type": "client_credentials",
                "type": "SELF",
            }, timeout=10)
            if resp.status_code == 403:
                print(f"[스마트스토어] IP 차단됨 - 커머스API센터에서 허용 IP를 등록하세요")
                return False
            if resp.status_code != 200:
                print(f"[스마트스토어] 인증 실패 (HTTP {resp.status_code}): {resp.text[:200]}")
                return False
            self.access_token = resp.json()["access_token"]
            return True
        except Exception as e:
            print(f"[스마트스토어] 인증 실패: {e}")
            return False

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def _fetch_day_orders(self, target_date: date) -> list:
        """하루치 변경 주문 조회 (24시간 이내만 가능)"""
        # URL에 직접 인코딩 (+를 %2B로)
        from_str = f"{target_date.isoformat()}T00%3A00%3A00.000%2B09%3A00"
        to_str = f"{target_date.isoformat()}T23%3A59%3A59.000%2B09%3A00"

        base = f"{self.BASE_URL}/v1/pay-order/seller/product-orders/last-changed-statuses"
        url = f"{base}?lastChangedFrom={from_str}&lastChangedTo={to_str}"

        try:
            resp = requests.get(url, headers=self._headers(), timeout=30)
            if resp.status_code == 401:
                if self.authenticate():
                    resp = requests.get(url, headers=self._headers(), timeout=30)
                else:
                    return []
            if resp.status_code == 429:
                time.sleep(2)
                resp = requests.get(url, headers=self._headers(), timeout=30)
            if resp.status_code != 200:
                return []
            data = resp.json()
            return data.get("data", {}).get("lastChangeStatuses", [])
        except Exception as e:
            print(f"[스마트스토어] {target_date} 조회 실패: {e}")
            return []

    def fetch_orders(self, start_date: date, end_date: date) -> pd.DataFrame:
        """주문 데이터 조회 - 하루씩 반복"""
        if not self.access_token and not self.authenticate():
            return pd.DataFrame()

        all_product_order_ids = []
        current = start_date

        while current <= end_date:
            statuses = self._fetch_day_orders(current)
            for s in statuses:
                pid = s.get("productOrderId")
                if pid:
                    all_product_order_ids.append(pid)
            current += timedelta(days=1)
            time.sleep(0.3)  # API 속도 제한 방지

        if not all_product_order_ids:
            return pd.DataFrame()

        # 주문 상세 조회 (100개씩 배치)
        all_orders = []
        for i in range(0, len(all_product_order_ids), 100):
            batch = all_product_order_ids[i:i+100]
            detail_url = f"{self.BASE_URL}/v1/pay-order/seller/product-orders/query"
            try:
                resp = requests.post(detail_url, headers=self._headers(), json={
                    "productOrderIds": batch,
                }, timeout=30)
                if resp.status_code == 429:
                    time.sleep(2)
                    resp = requests.post(detail_url, headers=self._headers(), json={
                        "productOrderIds": batch,
                    }, timeout=30)
                resp.raise_for_status()
                details = resp.json().get("data", [])
                all_orders.extend(details)
            except Exception as e:
                print(f"[스마트스토어] 주문 상세 조회 실패: {e}")
                continue

        rows = []
        for order in all_orders:
            product_order = order.get("productOrder", {})
            # 취소/환불 주문 제외
            status = product_order.get("productOrderStatus", "")
            claim_status = product_order.get("claimStatus", "") or ""
            if status in ("CANCELED", "RETURNED"):
                continue
            if claim_status in ("CANCEL_DONE", "RETURN_DONE"):
                continue
            # placeOrderDate(주문일) 사용, 없으면 decisionDate
            order_date = (
                product_order.get("placeOrderDate", "") or
                product_order.get("paymentDate", "") or
                product_order.get("decisionDate", "")
            )[:10]
            if not order_date:
                continue
            total_amount = int(product_order.get("totalPaymentAmount", 0))

            rows.append({
                "날짜": pd.to_datetime(order_date).date(),
                "스토어": "마르문(스마트스토어)",
                "채널": "스마트스토어",
                "주문건수": 1,
                "매출": total_amount,
            })

        df = pd.DataFrame(rows)
        df = df.groupby(["날짜", "스토어", "채널"]).agg(
            주문건수=("주문건수", "sum"),
            매출=("매출", "sum"),
        ).reset_index()
        df["객단가"] = (df["매출"] / df["주문건수"]).astype(int)
        df["순방문자수"] = 0
        df["전환율"] = 0.0
        return df


def fetch_smartstore(start_date: date, end_date: date) -> pd.DataFrame:
    """스마트스토어 주문 데이터 조회"""
    client = SmartStoreClient()
    return client.fetch_orders(start_date, end_date)
