"""
토큰 자동 갱신 관리 (Supabase 저장)
- Meta: 60일 토큰 만료 10일 전 자동 갱신
- 카페24: refresh_token으로 access_token 자동 갱신 (2시간마다)
"""
import os
import json
import requests
import base64
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv
from config import (
    META_APP_ID, META_APP_SECRET,
    CAFE24_CLIENT_ID, CAFE24_CLIENT_SECRET, CAFE24_MALLS,
)

load_dotenv(interpolate=False)


def _get_db_url():
    try:
        import streamlit as st
        if hasattr(st, "secrets") and "SUPABASE_DB_URL" in st.secrets:
            return st.secrets["SUPABASE_DB_URL"]
    except Exception:
        pass
    return os.getenv("SUPABASE_DB_URL", "")


def _conn():
    return psycopg2.connect(_get_db_url())


def get_token(service: str) -> dict:
    """Supabase에서 토큰 조회"""
    c = _conn()
    cur = c.cursor()
    cur.execute("SELECT 데이터 FROM tokens WHERE 서비스=%s", (service,))
    row = cur.fetchone()
    c.close()
    if row:
        v = row[0]
        return v if isinstance(v, dict) else json.loads(v)
    return {}


def set_token(service: str, data: dict):
    """Supabase에 토큰 저장 (덮어쓰기)"""
    c = _conn()
    cur = c.cursor()
    cur.execute("""
        INSERT INTO tokens (서비스, 데이터, 갱신시각) VALUES (%s, %s, NOW())
        ON CONFLICT (서비스) DO UPDATE SET 데이터=EXCLUDED.데이터, 갱신시각=NOW()
    """, (service, json.dumps(data)))
    c.commit()
    c.close()


def get_meta_access_token() -> str:
    """Meta access_token 반환 (Supabase 우선, 없으면 환경변수)"""
    data = get_token("meta")
    if data and "access_token" in data:
        return data["access_token"]
    return os.getenv("META_ACCESS_TOKEN", "")


def refresh_meta_token():
    current = get_meta_access_token()
    if not current or not META_APP_ID or not META_APP_SECRET:
        return
    try:
        resp = requests.get("https://graph.facebook.com/v21.0/debug_token", params={
            "input_token": current,
            "access_token": current,
        }, timeout=10)
        if resp.status_code != 200:
            return
        expires_at = resp.json().get("data", {}).get("expires_at", 0)
        now = int(datetime.now(timezone.utc).timestamp())
        days_left = (expires_at - now) / 86400 if expires_at else 999
        if days_left > 10:
            print(f"[Meta] 토큰 만료까지 {days_left:.0f}일 - 갱신 불필요")
            return
        resp = requests.get("https://graph.facebook.com/v21.0/oauth/access_token", params={
            "grant_type": "fb_exchange_token",
            "client_id": META_APP_ID,
            "client_secret": META_APP_SECRET,
            "fb_exchange_token": current,
        }, timeout=10)
        if resp.status_code == 200 and "access_token" in resp.json():
            new_token = resp.json()["access_token"]
            set_token("meta", {"access_token": new_token, "refreshed_at": datetime.now().isoformat()})
            print("[Meta] 토큰 갱신 완료 (+60일)")
        else:
            print(f"[Meta] 갱신 실패: {resp.text[:200]}")
    except Exception as e:
        print(f"[Meta] 오류: {e}")


def refresh_cafe24_tokens():
    if not CAFE24_CLIENT_ID or not CAFE24_CLIENT_SECRET:
        return
    auth = base64.b64encode(
        f"{CAFE24_CLIENT_ID}:{CAFE24_CLIENT_SECRET}".encode()
    ).decode()
    for store_name, mall_id in CAFE24_MALLS.items():
        if not mall_id:
            continue
        svc_key = f"cafe24_{mall_id}"
        token_data = get_token(svc_key)
        if not token_data:
            print(f"[카페24] {store_name} 토큰 없음")
            continue
        try:
            expires_at = token_data.get("expires_at", "")
            if expires_at:
                expire_dt = datetime.fromisoformat(expires_at.replace(".000", ""))
                if expire_dt > datetime.now():
                    continue
            refresh_token = token_data.get("refresh_token")
            if not refresh_token:
                continue
            resp = requests.post(
                f"https://{mall_id}.cafe24api.com/api/v2/oauth/token",
                headers={
                    "Authorization": f"Basic {auth}",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                timeout=10,
            )
            if resp.status_code == 200:
                set_token(svc_key, resp.json())
                print(f"[카페24] {store_name} 토큰 갱신")
            else:
                print(f"[카페24] {store_name} 갱신 실패: {resp.text[:200]}")
        except Exception as e:
            print(f"[카페24] {store_name} 오류: {e}")


def check_and_refresh_all():
    print("토큰 상태 확인 중...")
    refresh_meta_token()
    refresh_cafe24_tokens()
    print("토큰 확인 완료")
