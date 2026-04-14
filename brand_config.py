"""
브랜드 분류 설정
- DB에서 매핑 정보를 읽어서 사용
- 대시보드 설정 페이지에서 매핑 관리 가능
"""
import os
import sqlite3

DB_PATH = os.path.join(os.path.dirname(__file__), "dashboard_data.db")


def _get_keyword_mappings() -> dict:
    """DB에서 키워드 → 브랜드 매핑 조회"""
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT 키워드, 브랜드 FROM brand_mapping WHERE 유형 = '키워드' OR 플랫폼 = '전체'"
        ).fetchall()
        conn.close()
        # 긴 키워드 먼저 매칭 (더 구체적인 것 우선)
        return {kw: brand for kw, brand in sorted(rows, key=lambda x: -len(x[0]))}
    except Exception:
        return {}


def _get_campaign_mappings() -> dict:
    """DB에서 네이버SA 캠페인ID → 브랜드 매핑 조회"""
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT 키워드, 브랜드 FROM brand_mapping WHERE 플랫폼 = 'Naver SA'"
        ).fetchall()
        conn.close()
        return {kw: brand for kw, brand in rows}
    except Exception:
        return {}


def detect_brand(text: str) -> str:
    """텍스트(상품명/캠페인명)에서 브랜드 감지"""
    mappings = _get_keyword_mappings()
    for kw, brand in mappings.items():
        if kw in text:
            return brand
    return "기타"


def get_brand_by_campaign_id(campaign_id: str) -> str:
    """네이버SA 캠페인 ID로 브랜드 조회"""
    mappings = _get_campaign_mappings()
    return mappings.get(campaign_id, "기타")
