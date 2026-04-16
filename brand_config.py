"""
브랜드 분류 설정
- DB에서 매핑 정보를 읽어서 사용
- 대시보드 설정 페이지에서 매핑 관리 가능
- 매핑은 프로세스 내 캐시 (TTL 5분) — 호출마다 DB 왕복 방지
"""
import os
import time
import db_compat as sqlite3

DB_PATH = os.path.join(os.path.dirname(__file__), "dashboard_data.db")

_CACHE = {"keyword": (0.0, None), "campaign": (0.0, None)}
_TTL = 300  # 5분


def _cached(kind: str, loader):
    ts, data = _CACHE.get(kind, (0.0, None))
    if data is not None and (time.time() - ts) < _TTL:
        return data
    data = loader()
    _CACHE[kind] = (time.time(), data)
    return data


def _load_keyword_mappings() -> dict:
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT 키워드, 브랜드 FROM brand_mapping WHERE 유형 = '키워드' OR 플랫폼 = '전체'"
        ).fetchall()
        conn.close()
        return {kw: brand for kw, brand in sorted(rows, key=lambda x: -len(x[0]))}
    except Exception:
        return {}


def _load_campaign_mappings() -> dict:
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT 키워드, 브랜드 FROM brand_mapping WHERE 플랫폼 = 'Naver SA'"
        ).fetchall()
        conn.close()
        return {kw: brand for kw, brand in rows}
    except Exception:
        return {}


def _get_keyword_mappings() -> dict:
    return _cached("keyword", _load_keyword_mappings)


def _get_campaign_mappings() -> dict:
    return _cached("campaign", _load_campaign_mappings)


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


def clear_cache():
    """매핑 변경 후 즉시 반영하고 싶을 때 호출"""
    _CACHE["keyword"] = (0.0, None)
    _CACHE["campaign"] = (0.0, None)
