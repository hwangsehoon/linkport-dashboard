"""
전수 검증 스크립트
- 모든 년도, 모든 월, 모든 일자를 1개씩 전부 확인
- 스프레드시트 원본 vs DB 비교
- 결과를 파일로 저장
"""
import pandas as pd
import sqlite3
from datetime import date, datetime

DB_PATH = "dashboard_data.db"
BASE_DIR = "C:/Users/조현우/Desktop/광고일지"


def read_sheet_brand_daily(fpath, sheet_name, year, month):
    """시트에서 브랜드별 일별 합계 매출/광고비 정확하게 추출"""
    try:
        df = pd.read_excel(fpath, sheet_name=sheet_name, header=None)
    except:
        return {}

    results = {}  # {(date_str, brand): {'sheet_rev': x, 'sheet_ad': y}}

    # 브랜드 섹션 찾기
    sections = []
    for row_idx in range(min(200, len(df))):
        for col_idx in range(min(10, len(df.columns))):
            v = str(df.iloc[row_idx, col_idx]).strip()
            brand = None
            if '아자차' in v and ('합계' in v or '통 합' in v or '통합' in v):
                brand = '아자차'
            elif '반드럽 합계' in v:
                brand = '반드럽'
            elif '웰바이오젠 합계' in v:
                brand = '웰바이오젠'
            elif '윈토르 합계' in v:
                brand = '윈토르'

            if brand:
                sections.append({'brand': brand, 'row': row_idx, 'col': col_idx})

    if not sections:
        # 2023 초기: 합계 없이 전체가 아자차
        # row3에 전체 합계가 있는지 확인
        try:
            r3_val = df.iloc[3, 4]
            if pd.notna(r3_val) and float(r3_val) > 1000:
                sections = [{'brand': '아자차', 'row': 4, 'col': 3}]
        except:
            pass

    for i, sec in enumerate(sections):
        brand = sec['brand']
        header_row = sec['row']
        col_row = header_row + 1
        data_start = header_row + 2

        # 다음 섹션까지의 범위
        if i + 1 < len(sections):
            data_end = sections[i + 1]['row'] - 3
        else:
            data_end = data_start + 35

        # 컬럼 위치: 헤더 행에서 '광고비'와 '매출' 찾기
        ad_col = None
        rev_col = None
        date_col = 2

        if col_row < len(df):
            for c in range(min(10, len(df.columns))):
                h = str(df.iloc[col_row, c]).strip() if pd.notna(df.iloc[col_row, c]) else ''
                if '날' in h or 'date' in h.lower():
                    date_col = c
                # 첫 번째 '광고비' = 합계 광고비
                if h == '광고비' and ad_col is None:
                    ad_col = c
                # 첫 번째 '매출' 또는 '실매출' = 합계 매출
                if (h == '매출' or h == '실매출') and rev_col is None:
                    rev_col = c

        # 데이터 행 읽기
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

            key = (d.isoformat(), brand)
            if key not in results:
                results[key] = {'sheet_rev': 0, 'sheet_ad': 0}
            results[key]['sheet_rev'] += rev
            results[key]['sheet_ad'] += ad

    return results


def get_db_daily():
    """DB에서 브랜드별 일별 매출/광고비 전체"""
    conn = sqlite3.connect(DB_PATH)

    # 매출
    sales = pd.read_sql_query("SELECT 날짜, 스토어, 채널, 매출, 브랜드 FROM sales", conn)

    def map_brand(row):
        store = str(row.get('스토어', ''))
        brand = str(row.get('브랜드', ''))
        if brand and brand not in ('', 'nan', 'None'):
            return brand
        if '아자차' in store or '마르문' in store:
            return '아자차'
        if '반드럽' in store:
            return '반드럽'
        if '웰바이오젠' in store:
            return '웰바이오젠'
        if '윈토르' in store:
            return '윈토르'
        return '기타'

    sales['_brand'] = sales.apply(map_brand, axis=1)
    db_sales = sales.groupby(['날짜', '_brand'])['매출'].sum()

    # 광고비
    ads = pd.read_sql_query("SELECT 날짜, 광고채널, 광고비, 브랜드 FROM ads", conn)
    if not ads.empty:
        ads['_brand'] = ads['브랜드'].fillna('기타').replace('', '기타')
        db_ads = ads.groupby(['날짜', '_brand'])['광고비'].sum()
    else:
        db_ads = pd.Series(dtype=float)

    conn.close()
    return db_sales, db_ads


def main():
    files = [
        ('링포 광고일지_2026.xlsx', 2026),
        ('링포 광고일지_2025.xlsx', 2025),
        ('링포 광고일지_2024.xlsx', 2024),
        ('링포 광고일지_2023.xlsx', 2023),
    ]

    print("DB 데이터 로드 중...")
    db_sales, db_ads = get_db_daily()
    print(f"DB 매출: {len(db_sales)}건, 광고비: {len(db_ads)}건")

    output = []
    output.append("=" * 120)
    output.append("전수 검증 결과 - 스프레드시트 vs DB (브랜드별 일별)")
    output.append("=" * 120)

    error_count = 0
    ok_count = 0
    total_count = 0

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

            print(f"  검증: {year}년 {month_num}월...", flush=True)
            sheet_data = read_sheet_brand_daily(fpath, sheet, year, month_num)

            month_errors = []

            for (dt, brand), vals in sorted(sheet_data.items()):
                total_count += 1
                sheet_rev = vals['sheet_rev']
                sheet_ad = vals['sheet_ad']

                db_rev = int(db_sales.get((dt, brand), 0))
                db_ad = int(db_ads.get((dt, brand), 0))

                rev_diff = db_rev - sheet_rev
                ad_diff = db_ad - sheet_ad

                # 매출 오차 10% 초과 또는 100만원 이상 차이
                rev_pct = abs(rev_diff / sheet_rev * 100) if sheet_rev > 0 else 0
                ad_pct = abs(ad_diff / sheet_ad * 100) if sheet_ad > 0 else 0

                is_error = (sheet_rev > 10000 and rev_pct > 10) or abs(rev_diff) > 1000000

                if is_error:
                    error_count += 1
                    month_errors.append(
                        f"  !! {dt} {brand:8s} | 매출: 시트{sheet_rev:>10,} DB{db_rev:>10,} (차이{rev_diff:>+10,} {rev_pct:.0f}%) | 광고비: 시트{sheet_ad:>10,} DB{db_ad:>10,}"
                    )
                else:
                    ok_count += 1

            if month_errors:
                output.append(f"\n--- {year}년 {month_num}월 ({len(month_errors)}건 오차) ---")
                output.extend(month_errors)
            else:
                output.append(f"  {year}년 {month_num}월: OK ({len(sheet_data)}일)")

    output.append(f"\n{'=' * 120}")
    output.append(f"총 {total_count}건 검증 | OK: {ok_count}건 | 오차: {error_count}건")
    output.append(f"오차율: {error_count/max(1,total_count)*100:.1f}%")
    output.append(f"{'=' * 120}")

    # 파일 저장
    with open("verify_full_result.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(output))

    print(f"\n결과: OK {ok_count}건 / 오차 {error_count}건 / 총 {total_count}건")
    print("상세 결과: verify_full_result.txt")


if __name__ == "__main__":
    main()
