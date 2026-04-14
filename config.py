"""
API 설정 관리 - .env 파일에서 인증 정보를 로드합니다.
"""
import os
from dotenv import load_dotenv

load_dotenv(interpolate=False)  # $가 포함된 값이 깨지지 않도록


# ── 카페24 ──
CAFE24_CLIENT_ID = os.getenv("CAFE24_CLIENT_ID", "")
CAFE24_CLIENT_SECRET = os.getenv("CAFE24_CLIENT_SECRET", "")
CAFE24_MALLS = {
    "아자차(카페24)": os.getenv("CAFE24_MALL_ID_AZACHA", ""),
    "반드럽(카페24)": os.getenv("CAFE24_MALL_ID_BANDREUP", ""),
    "웰바이오젠(카페24)": os.getenv("CAFE24_MALL_ID_WELBIOGEN", ""),
}

# ── 스마트스토어 ──
SMARTSTORE_CLIENT_ID = os.getenv("SMARTSTORE_CLIENT_ID", "")
SMARTSTORE_CLIENT_SECRET = os.getenv("SMARTSTORE_CLIENT_SECRET", "")

# ── 쿠팡 ──
COUPANG_ACCESS_KEY = os.getenv("COUPANG_ACCESS_KEY", "")
COUPANG_SECRET_KEY = os.getenv("COUPANG_SECRET_KEY", "")
COUPANG_VENDOR_ID = os.getenv("COUPANG_VENDOR_ID", "")

# ── Meta 광고 ──
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "")
META_AD_ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID", "")
META_APP_ID = os.getenv("META_APP_ID", "")
META_APP_SECRET = os.getenv("META_APP_SECRET", "")

# ── 네이버 검색광고 ──
NAVER_SA_API_KEY = os.getenv("NAVER_SA_API_KEY", "")
NAVER_SA_SECRET_KEY = os.getenv("NAVER_SA_SECRET_KEY", "")
NAVER_SA_CUSTOMER_ID = os.getenv("NAVER_SA_CUSTOMER_ID", "")

# ── 쿠팡 광고 ──
COUPANG_ADS_ACCESS_KEY = os.getenv("COUPANG_ADS_ACCESS_KEY", "")
COUPANG_ADS_SECRET_KEY = os.getenv("COUPANG_ADS_SECRET_KEY", "")
COUPANG_ADS_ADVERTISER_ID = os.getenv("COUPANG_ADS_ADVERTISER_ID", "")


def is_configured(service: str) -> bool:
    """해당 서비스의 API 키가 설정되어 있는지 확인"""
    checks = {
        "cafe24": bool(CAFE24_CLIENT_ID and CAFE24_CLIENT_SECRET),
        "smartstore": bool(SMARTSTORE_CLIENT_ID and SMARTSTORE_CLIENT_SECRET),
        "coupang": bool(COUPANG_ACCESS_KEY and COUPANG_SECRET_KEY),
        "meta": bool(META_ACCESS_TOKEN and META_AD_ACCOUNT_ID),
        "naver_sa": bool(NAVER_SA_API_KEY and NAVER_SA_SECRET_KEY and NAVER_SA_CUSTOMER_ID),
        "coupang_ads": bool(COUPANG_ADS_ACCESS_KEY and COUPANG_ADS_SECRET_KEY),
    }
    return checks.get(service, False)
