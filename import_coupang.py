"""쿠팡 광고비 임포트
- C:/Users/조현우/Desktop/쿠팡광고내역/ 폴더의 모든 xlsx 읽기
- 매출최적화*.xlsx: '광고비' 컬럼
- 신규구매고객*.xlsx: '집행 광고비' 컬럼
- 캠페인명 → 브랜드 매핑 후 일별/브랜드별 합산
- ads 테이블에 INSERT OR REPLACE (광고채널='쿠팡', 브랜드별)
"""
import os
import glob
import db_compat as sqlite3
import warnings
import pandas as pd
from datetime import datetime

warnings.filterwarnings('ignore')

DB_PATH = "dashboard_data.db"
SRC_DIR = "C:/Users/조현우/Desktop/쿠팡광고내역"


def map_brand(campaign_name):
    """캠페인명에서 브랜드 추출"""
    if not isinstance(campaign_name, str):
        return None
    name = campaign_name
    if '마르문' in name or '아자차' in name:
        return '아자차'
    if '풋쉐이버' in name or '반드럽' in name:
        return '반드럽'
    if '트라핀' in name or '웰바이오젠' in name:
        return '웰바이오젠'
    if '윈토르' in name:
        return '윈토르'
    return None


def parse_date(v):
    """20260101 → 2026-01-01"""
    s = str(v).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    try:
        return pd.to_datetime(v).date().isoformat()
    except:
        return None


def load_files():
    """폴더의 모든 xlsx 읽어서 (날짜, 브랜드, 광고비) 합산"""
    daily = {}  # (date, brand) -> ad_cost
    skipped = []
    for fpath in sorted(glob.glob(f"{SRC_DIR}/*.xlsx")):
        fname = os.path.basename(fpath)
        df = pd.read_excel(fpath, header=0)
        # 컬럼 정규화
        camp_col = '캠페인명' if '캠페인명' in df.columns else '캠페인 이름'
        ad_col = '광고비' if '광고비' in df.columns else '집행 광고비'
        date_col = '날짜'
        rows = 0
        for _, r in df.iterrows():
            d = parse_date(r[date_col])
            if not d:
                continue
            brand = map_brand(r[camp_col])
            if brand is None:
                skipped.append(r[camp_col])
                continue
            ad = r[ad_col]
            try:
                ad = int(float(ad))
            except:
                ad = 0
            if ad <= 0:
                continue
            key = (d, brand)
            daily[key] = daily.get(key, 0) + ad
            rows += 1
        print(f"  {fname}: {rows} 행 집계")
    if skipped:
        unique_skipped = sorted(set(str(s) for s in skipped if s))
        print(f"  스킵된 캠페인 (브랜드 매핑 안됨): {unique_skipped}")
    return daily


def main():
    print(f"[쿠팡 광고비 임포트]")
    daily = load_files()
    print(f"\n총 {len(daily)} (날짜, 브랜드) 키")

    conn = sqlite3.connect(DB_PATH)
    # 기존 쿠팡 데이터 삭제 (재임포트)
    conn.execute("DELETE FROM ads WHERE 광고채널='쿠팡'")
    inserted = 0
    for (d, brand), ad in daily.items():
        conn.execute(
            """INSERT OR REPLACE INTO ads
               (날짜, 광고채널, 광고비, 노출수, 클릭수, 전환수, 전환매출, 브랜드)
               VALUES (?,?,?,?,?,?,?,?)""",
            (d, "쿠팡", ad, 0, 0, 0, 0, brand)
        )
        inserted += 1
    conn.commit()
    print(f"\n적재 완료: {inserted}건")

    # 검증: 월별/브랜드별 합계
    print("\n=== 월별/브랜드별 쿠팡 광고비 ===")
    for row in conn.execute(
        "SELECT substr(날짜,1,7) m, 브랜드, SUM(광고비) FROM ads "
        "WHERE 광고채널='쿠팡' GROUP BY m, 브랜드 ORDER BY m, 브랜드"
    ):
        print(f"  {row[0]} {row[1]}: {row[2]:,}")

    # 월별 총합 (시트 비교용)
    print("\n=== 월별 쿠팡 총합 ===")
    for row in conn.execute(
        "SELECT substr(날짜,1,7) m, SUM(광고비) FROM ads "
        "WHERE 광고채널='쿠팡' GROUP BY m ORDER BY m"
    ):
        print(f"  {row[0]}: {row[1]:,}")

    conn.close()


if __name__ == "__main__":
    main()
