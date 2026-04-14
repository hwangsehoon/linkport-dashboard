"""
스프레드시트 → DB 보충 임포트
- 스프레드시트 브랜드별 일별 합계 추출
- API 데이터와 비교하여 부족한 부분만 보충
- 중복 없이 정확한 총액 맞춤
"""
import pandas as pd
import sqlite3
from datetime import date, datetime

DB_PATH = "dashboard_data.db"
BASE_DIR = "C:/Users/조현우/Desktop/광고일지"


def get_conn():
    return sqlite3.connect(DB_PATH)


def find_brand_sections(df):
    """시트에서 브랜드 섹션(합계 행) 위치 자동 감지"""
    sections = []
    for row_idx in range(min(200, len(df))):
        for col_idx in range(min(10, len(df.columns))):
            v = str(df.iloc[row_idx, col_idx]).strip()
            brand = None
            if '아자차' in v and '합계' in v:
                brand = '아자차'
            elif '반드럽 합계' in v:
                brand = '반드럽'
            elif '웰바이오젠 합계' in v:
                brand = '웰바이오젠'
            elif '윈토르 합계' in v:
                brand = '윈토르'
            elif '통 합' in v or '통합' in v:
                brand = '아자차'

            if brand:
                # 이미 같은 brand가 있으면 (아자차 자사몰/오픈마켓 분리된 경우) 합산용으로 추가
                sections.append({
                    'brand': brand,
                    'header_row': row_idx,
                    'col': col_idx,
                })
    return sections


def extract_daily_totals(df, section, year, month, next_section_row=None):
    """브랜드 섹션에서 일별 합계(광고비/매출) 추출"""
    header_row = section['header_row']
    col_row = header_row + 1

    # 컬럼 위치 찾기
    ad_col = None
    rev_col = None
    date_col = 2  # 기본

    if col_row < len(df):
        for c in range(min(10, len(df.columns))):
            h = str(df.iloc[col_row, c]).strip()
            if h == '광고비' and ad_col is None:
                ad_col = c
            if (h == '매출' or h == '실매출') and rev_col is None:
                rev_col = c
            if '날' in h:
                date_col = c

    data_start = header_row + 2
    data_end = next_section_row - 3 if next_section_row else data_start + 33

    rows = []
    for r in range(data_start, min(data_end, len(df))):
        dv = df.iloc[r, date_col]
        if pd.isna(dv):
            continue
        try:
            d = pd.to_datetime(dv).date()
        except:
            continue
        if d.year != year or d.month != month:
            continue

        rev = 0
        if rev_col is not None and rev_col < len(df.columns):
            rv = df.iloc[r, rev_col]
            try:
                rev = int(float(rv)) if pd.notna(rv) else 0
            except:
                rev = 0

        ad = 0
        if ad_col is not None and ad_col < len(df.columns):
            av = df.iloc[r, ad_col]
            try:
                ad = int(float(av)) if pd.notna(av) else 0
            except:
                ad = 0

        if rev > 0 or ad > 0:
            rows.append({'date': d.isoformat(), 'revenue': rev, 'ad_cost': ad})

    return rows


def process_all_sheets():
    """모든 시트 처리하여 브랜드별 일별 스프레드시트 합계 반환"""
    files = [
        ('링포 광고일지_2026.xlsx', 2026),
        ('링포 광고일지_2025.xlsx', 2025),
        ('링포 광고일지_2024.xlsx', 2024),
        ('링포 광고일지_2023.xlsx', 2023),
    ]

    all_data = {}  # {(date, brand): {revenue, ad_cost}}

    for fname, year in files:
        fpath = f"{BASE_DIR}/{fname}"
        xls = pd.ExcelFile(fpath)
        print(f"[{year}년]")

        for sheet in xls.sheet_names:
            if '월' not in sheet or '정산' in sheet or '목표' in sheet or '데이터' in sheet:
                continue
            try:
                month_num = int(sheet.replace('월', '').strip())
            except:
                continue

            print(f"  {month_num}월...", end=" ", flush=True)

            try:
                df = pd.read_excel(fpath, sheet_name=sheet, header=None)
            except:
                print("읽기 실패")
                continue

            sections = find_brand_sections(df)
            if not sections:
                print("섹션 없음")
                continue

            month_brands = {}
            for i, sec in enumerate(sections):
                next_row = sections[i + 1]['header_row'] if i + 1 < len(sections) else None
                rows = extract_daily_totals(df, sec, year, month_num, next_row)
                brand = sec['brand']

                for r in rows:
                    key = (r['date'], brand)
                    if key not in all_data:
                        all_data[key] = {'revenue': 0, 'ad_cost': 0}
                    # 아자차(자사몰)+아자차(오픈마켓) 합산
                    all_data[key]['revenue'] += r['revenue']
                    all_data[key]['ad_cost'] += r['ad_cost']

                    if brand not in month_brands:
                        month_brands[brand] = {'rev': 0, 'ad': 0}
                    month_brands[brand]['rev'] += r['revenue']
                    month_brands[brand]['ad'] += r['ad_cost']

            summary = ", ".join(f"{b}:{d['rev']:,}" for b, d in month_brands.items())
            print(summary)

    return all_data


def get_api_data():
    """DB에서 API 데이터 브랜드별 일별 합계 가져오기"""
    conn = get_conn()

    # 매출 - 브랜드 매핑
    sales = pd.read_sql_query(
        "SELECT 날짜, 스토어, 채널, 매출, 브랜드 FROM sales WHERE 채널 != '스프레드시트'",
        conn
    )

    # 스토어명 → 브랜드 매핑
    def map_brand(row):
        store = str(row.get('스토어', ''))
        brand = str(row.get('브랜드', ''))
        if brand and brand != '' and brand != 'nan':
            return brand
        if '아자차' in store or '마르문' in store:
            return '아자차'
        if '반드럽' in store:
            return '반드럽'
        if '웰바이오젠' in store:
            return '웰바이오젠'
        return '기타'

    sales['_brand'] = sales.apply(map_brand, axis=1)
    api_sales = sales.groupby(['날짜', '_brand'])['매출'].sum().to_dict()

    # 광고비
    ads = pd.read_sql_query(
        "SELECT 날짜, 광고채널, 광고비, 브랜드 FROM ads WHERE 광고채널 != '전체(스프레드시트)'",
        conn
    )
    if not ads.empty:
        ads['_brand'] = ads['브랜드'].fillna('기타').replace('', '기타')
        api_ads = ads.groupby(['날짜', '_brand'])['광고비'].sum().to_dict()
    else:
        api_ads = {}

    conn.close()
    return api_sales, api_ads


def main():
    conn = get_conn()

    # 기존 보충 데이터 삭제
    conn.execute("DELETE FROM sales WHERE 채널 = '스프레드시트'")
    conn.execute("DELETE FROM ads WHERE 광고채널 = '전체(스프레드시트)'")
    conn.commit()

    print("=== 스프레드시트 추출 ===\n")
    sheet_data = process_all_sheets()

    print("\n=== API 데이터 로드 ===")
    api_sales, api_ads = get_api_data()
    print(f"API 매출: {len(api_sales)}건, 광고비: {len(api_ads)}건")

    print("\n=== 차이 계산 및 보충 저장 ===")
    saved_sales = 0
    saved_ads = 0
    total_supplement_rev = 0
    total_supplement_ad = 0

    for (dt, brand), vals in sheet_data.items():
        sheet_rev = vals['revenue']
        sheet_ad = vals['ad_cost']

        # API에 있는 금액
        api_rev = api_sales.get((dt, brand), 0)
        api_ad = api_ads.get((dt, brand), 0)

        # 부족분 = 스프레드시트 - API
        gap_rev = sheet_rev - int(api_rev)
        gap_ad = sheet_ad - int(api_ad)

        # 부족분이 의미있는 경우만 저장 (최소 100원 이상)
        if gap_rev > 100:
            conn.execute(
                """INSERT OR REPLACE INTO sales
                   (날짜, 스토어, 채널, 주문건수, 매출, 객단가, 순방문자수, 전환율, 브랜드)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (dt, f"기타({brand})", "스프레드시트", 0, gap_rev, 0, 0, 0.0, brand)
            )
            saved_sales += 1
            total_supplement_rev += gap_rev

        if gap_ad > 100:
            conn.execute(
                """INSERT OR REPLACE INTO ads
                   (날짜, 광고채널, 광고비, 노출수, 클릭수, 전환수, 전환매출, 브랜드)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (dt, "전체(스프레드시트)", gap_ad, 0, 0, 0, 0, brand)
            )
            saved_ads += 1
            total_supplement_ad += gap_ad

    conn.commit()

    print(f"매출 보충: {saved_sales}건, {total_supplement_rev:,}원")
    print(f"광고비 보충: {saved_ads}건, {total_supplement_ad:,}원")

    # 최종 검증
    print("\n" + "=" * 70)
    print("=== 최종 검증: 스프레드시트 합계 vs DB 합계(API+보충) ===")
    print("=" * 70)

    files = [
        ('링포 광고일지_2026.xlsx', 2026),
        ('링포 광고일지_2025.xlsx', 2025),
        ('링포 광고일지_2024.xlsx', 2024),
        ('링포 광고일지_2023.xlsx', 2023),
    ]

    for fname, year in files:
        fpath = f"{BASE_DIR}/{fname}"
        xls = pd.ExcelFile(fpath)

        for sheet in xls.sheet_names:
            if '월' not in sheet or '정산' in sheet or '목표' in sheet or '데이터' in sheet:
                continue
            try:
                month_num = int(sheet.replace('월', '').strip())
            except:
                continue

            # 시트 행3의 전체 합계
            df = pd.read_excel(fpath, sheet_name=sheet, header=None, nrows=5)
            try:
                sheet_rev = int(float(df.iloc[3, 4])) if pd.notna(df.iloc[3, 4]) else 0
                sheet_ad = int(float(df.iloc[3, 3])) if pd.notna(df.iloc[3, 3]) else 0
            except:
                sheet_rev = 0
                sheet_ad = 0

            # DB 전체 합계 (API + 보충)
            month_key = f"{year}-{month_num:02d}"
            db_rev = conn.execute(
                "SELECT SUM(매출) FROM sales WHERE substr(날짜,1,7)=?", (month_key,)
            ).fetchone()[0] or 0
            db_ad = conn.execute(
                "SELECT SUM(광고비) FROM ads WHERE substr(날짜,1,7)=?", (month_key,)
            ).fetchone()[0] or 0

            rev_diff = int(db_rev) - sheet_rev
            rev_pct = (rev_diff / sheet_rev * 100) if sheet_rev else 0
            ad_diff = int(db_ad) - sheet_ad
            ad_pct = (ad_diff / sheet_ad * 100) if sheet_ad else 0

            rev_flag = "OK" if abs(rev_pct) <= 3 else "~" if abs(rev_pct) <= 10 else "!!"
            ad_flag = "OK" if abs(ad_pct) <= 3 else "~" if abs(ad_pct) <= 10 else "!!"

            print(f"{month_key} | 매출 시트:{sheet_rev:>12,} DB:{int(db_rev):>12,} ({rev_pct:+.1f}%){rev_flag} | 광고비 시트:{sheet_ad:>12,} DB:{int(db_ad):>12,} ({ad_pct:+.1f}%){ad_flag}")

    conn.close()
    print("\n=== 완료! ===")


if __name__ == "__main__":
    main()
