"""
카페24 CA API (Analytics API) 연동 모듈
- OAuth 2.0 인증 (기존 토큰 재사용)
- CA API로 매출/방문자 데이터 조회 (카페24 관리자와 일치)
- 주문 API는 CA API 미지원 시 폴백용

API 문서: https://developers.cafe24.com/data/front/cafe24dataapi
"""
import os
import json
import base64
import requests
import pandas as pd
from datetime import date, datetime
from config import CAFE24_CLIENT_ID, CAFE24_CLIENT_SECRET, CAFE24_MALLS

TOKEN_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".tokens")
CA_API_BASE = "https://ca-api.cafe24data.com"


def _token_path(mall_id: str) -> str:
    os.makedirs(TOKEN_DIR, exist_ok=True)
    return os.path.join(TOKEN_DIR, f"cafe24_{mall_id}.json")


def _save_token(mall_id: str, token_data: dict):
    with open(_token_path(mall_id), "w") as f:
        json.dump(token_data, f)


def _load_token(mall_id: str) -> dict:
    path = _token_path(mall_id)
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {}


class Cafe24Client:
    def __init__(self, mall_id: str, store_name: str):
        self.mall_id = mall_id
        self.store_name = store_name
        self.access_token = None
        self.refresh_token = None
        self._load_saved_token()

    def _load_saved_token(self):
        data = _load_token(self.mall_id)
        self.access_token = data.get("access_token")
        self.refresh_token = data.get("refresh_token")

    def _basic_auth(self) -> str:
        return base64.b64encode(
            f"{CAFE24_CLIENT_ID}:{CAFE24_CLIENT_SECRET}".encode()
        ).decode()

    def get_auth_url(self) -> str:
        scope = "mall.read_order,mall.read_store,mall.read_salesreport,mall.read_analytics"
        return (
            f"https://{self.mall_id}.cafe24.com/api/v2/oauth/authorize"
            f"?response_type=code&client_id={CAFE24_CLIENT_ID}"
            f"&redirect_uri=https://ajacha.com/callback&scope={scope}"
        )

    def authenticate_with_code(self, auth_code: str) -> bool:
        url = f"https://{self.mall_id}.cafe24api.com/api/v2/oauth/token"
        try:
            resp = requests.post(url,
                headers={
                    "Authorization": f"Basic {self._basic_auth()}",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                data={
                    "grant_type": "authorization_code",
                    "code": auth_code,
                    "redirect_uri": "https://ajacha.com/callback",
                },
                timeout=10,
            )
            resp.raise_for_status()
            token_data = resp.json()
            self.access_token = token_data["access_token"]
            self.refresh_token = token_data.get("refresh_token")
            _save_token(self.mall_id, token_data)
            return True
        except Exception as e:
            print(f"[카페24] {self.store_name} 토큰 발급 실패: {e}")
            return False

    def refresh_access_token(self) -> bool:
        if not self.refresh_token:
            return False
        url = f"https://{self.mall_id}.cafe24api.com/api/v2/oauth/token"
        try:
            resp = requests.post(url,
                headers={
                    "Authorization": f"Basic {self._basic_auth()}",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token,
                },
                timeout=10,
            )
            resp.raise_for_status()
            token_data = resp.json()
            self.access_token = token_data["access_token"]
            self.refresh_token = token_data.get("refresh_token", self.refresh_token)
            _save_token(self.mall_id, token_data)
            return True
        except Exception as e:
            print(f"[카페24] {self.store_name} 토큰 갱신 실패: {e}")
            return False

    def is_authenticated(self) -> bool:
        return bool(self.access_token)

    def _ca_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def _ca_request(self, endpoint: str, params: dict) -> dict:
        """CA API 요청 (토큰 만료 시 자동 갱신)"""
        params["mall_id"] = self.mall_id
        url = f"{CA_API_BASE}{endpoint}"
        resp = requests.get(url, headers=self._ca_headers(), params=params, timeout=30)
        if resp.status_code == 401 and self.refresh_access_token():
            resp = requests.get(url, headers=self._ca_headers(), params=params, timeout=30)
        if resp.status_code != 200:
            return {}
        return resp.json()

    def fetch_sales(self, start_date: date, end_date: date) -> pd.DataFrame:
        """CA API로 매출 데이터 조회 (결제완료 기준, 카페24 관리자와 일치)"""
        if not self.is_authenticated():
            return pd.DataFrame()

        data = self._ca_request("/sales/orderdetails", {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "limit": 1000,
        })

        details = data.get("orderdetails", [])
        if not details:
            return pd.DataFrame()

        rows = []
        for d in details:
            order_date = d.get("order_date", "")[:10]
            amount = int(d.get("order_amount", 0))
            rows.append({
                "날짜": pd.to_datetime(order_date).date(),
                "스토어": self.store_name,
                "채널": "카페24",
                "주문건수": 1,
                "매출": amount,
            })

        df = pd.DataFrame(rows)
        df = df.groupby(["날짜", "스토어", "채널"]).agg(
            주문건수=("주문건수", "sum"),
            매출=("매출", "sum"),
        ).reset_index()
        df["객단가"] = (df["매출"] / df["주문건수"]).astype(int)
        return df

    def fetch_visitors(self, start_date: date, end_date: date) -> pd.DataFrame:
        """CA API로 순방문자수 조회"""
        if not self.is_authenticated():
            return pd.DataFrame()

        data = self._ca_request("/visitors/unique", {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        visitors = data.get("unique", [])
        if not visitors:
            return pd.DataFrame()

        rows = []
        for v in visitors:
            visit_date = v.get("date", "")[:10]
            count = int(v.get("unique_visit_count", 0))
            rows.append({
                "날짜": pd.to_datetime(visit_date).date(),
                "스토어": self.store_name,
                "순방문자수": count,
            })

        return pd.DataFrame(rows)


def get_auth_urls() -> dict:
    urls = {}
    for store_name, mall_id in CAFE24_MALLS.items():
        if not mall_id:
            continue
        client = Cafe24Client(mall_id, store_name)
        if not client.is_authenticated():
            urls[store_name] = client.get_auth_url()
    return urls


def authenticate_store(store_name: str, auth_code: str) -> bool:
    mall_id = CAFE24_MALLS.get(store_name)
    if not mall_id:
        return False
    client = Cafe24Client(mall_id, store_name)
    return client.authenticate_with_code(auth_code)


def fetch_all_cafe24(start_date: date, end_date: date) -> pd.DataFrame:
    """모든 카페24 스토어의 매출+방문자 데이터 통합 조회 (CA API)"""
    all_dfs = []

    for store_name, mall_id in CAFE24_MALLS.items():
        if not mall_id:
            continue
        client = Cafe24Client(mall_id, store_name)
        if not client.is_authenticated():
            print(f"[카페24] {store_name} 미인증 - 설정 페이지에서 인증해주세요")
            continue

        sales_df = client.fetch_sales(start_date, end_date)
        visitors_df = client.fetch_visitors(start_date, end_date)

        if not sales_df.empty and not visitors_df.empty:
            merged = sales_df.merge(visitors_df, on=["날짜", "스토어"], how="left")
            merged["순방문자수"] = merged["순방문자수"].fillna(0).astype(int)
            merged["전환율"] = (
                merged["주문건수"] / merged["순방문자수"].replace(0, 1) * 100
            ).round(2)
            all_dfs.append(merged)
        elif not sales_df.empty:
            sales_df["순방문자수"] = 0
            sales_df["전환율"] = 0.0
            all_dfs.append(sales_df)

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return pd.DataFrame()
