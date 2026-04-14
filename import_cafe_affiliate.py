"""카페제휴/바이럴 광고비·매출 시트 임포트 (2026~)
- 링포 광고일지_YYYY.xlsx 에서 각 브랜드 섹션의 '바이럴', '기타(일매출 포함x)' 컬럼만 추출
- 광고비 → ads 테이블 (광고채널='카페제휴', 브랜드별)
- 매출 → sales 테이블 (스토어='카페제휴', 채널='카페제휴', 브랜드별)
- 매월 시트 갱신 후 재실행 시 INSERT OR REPLACE로 덮어씀
"""
import os
import sys
import db_compat as sqlite3
import warnings
import pandas as pd

warnings.filterwarnings('ignore')

DB_PATH = "dashboard_data.db"
SHEET_DIR = "C:/Users/조현우/Desktop/광고일지"

# 시트 라벨 → DB 브랜드명
LABEL_TO_BRAND = {
    '아자차': '아자차',
    '반드럽': '반드럽',
    '웰바이오젠': '웰바이오젠',
    '윈토르': '윈토르',
    '트라핀': '웰바이오젠',
    '마르문': '아자차',
}

# 카페제휴/바이럴로 간주할 채널 키워드 (브랜드 섹션의 채널 헤더)
CAFE_KEYWORDS = ['바이럴', '기타(일매출 포함x)', '기 타(일매출 포함x)', '기 타', '카페']


def detect_brand_from_section(label):
    for k, v in LABEL_TO_BRAND.items():
        if k in label:
            return v
    return None


def extract_from_sheet(fpath, sheet_name, year, month):
    """한 월 시트에서 카페제휴/바이럴 데이터 추출"""
    df = pd.read_excel(fpath, sheet_name=sheet_name, header=None)
    rows = []  # [(date, brand, ad, rev), ...]
    # 모든 브랜드 합계 섹션 찾기
    for r in range(len(df)):
        for c in range(min(10, len(df.columns))):
            v = df.iloc[r, c]
            if pd.isna(v): continue
            s = str(v).strip()
            if '합계' not in s: continue
            brand = detect_brand_from_section(s)
            if brand is None: continue
            # 해당 섹션에서 카페/바이럴/기타 채널 찾기
            header_row = r
            col_row = r + 1
            data_start = r + 2
            data_end = data_start + 33
            date_col = 2

            # 카페제휴 관련 컬럼 (광고비/매출 쌍)
            cafe_cols = []  # [(ad_col, rev_col), ...]
            for ch_c in range(5, min(40, len(df.columns))):
                ch_v = df.iloc[header_row, ch_c]
                if pd.isna(ch_v): continue
                ch_name = str(ch_v).replace('\n', '').strip()
                is_cafe = any(kw in ch_name for kw in CAFE_KEYWORDS)
                if not is_cafe: continue
                # 채널 시작 컬럼부터 광고비/매출 헤더 찾기
                ad_col = rev_col = None
                for off in range(5):
                    cc = ch_c + off
                    if cc >= len(df.columns): break
                    if pd.notna(df.iloc[col_row, cc]):
                        h = str(df.iloc[col_row, cc]).strip()
                        if '광고비' in h and ad_col is None:
                            ad_col = cc
                        elif h == '매출' and rev_col is None:
                            rev_col = cc
                    if off > 0 and pd.notna(df.iloc[header_row, cc]):
                        break
                if ad_col is not None or rev_col is not None:
                    cafe_cols.append((ch_name, ad_col, rev_col))

            if not cafe_cols: continue

            # 일자별 추출
            for dr in range(data_start, min(data_end, len(df))):
                date_v = df.iloc[dr, date_col]
                if pd.isna(date_v): continue
                try:
                    d = pd.to_datetime(date_v).date()
                    if d.year != year or d.month != month: continue
                except:
                    continue
                for ch_name, ad_c, rev_c in cafe_cols:
                    ad = 0
                    rev = 0
                    if ad_c is not None and pd.notna(df.iloc[dr, ad_c]):
                        try: ad = max(0, int(float(df.iloc[dr, ad_c])))
                        except: pass
                    if rev_c is not None and pd.notna(df.iloc[dr, rev_c]):
                        try: rev = max(0, int(float(df.iloc[dr, rev_c])))
                        except: pass
                    if ad > 0 or rev > 0:
                        rows.append({
                            'date': d.isoformat(),
                            'brand': brand,
                            'channel_label': ch_name,
                            'ad': ad,
                            'rev': rev,
                        })
    return rows


def main():
    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2026
    fpath = f"{SHEET_DIR}/링포 광고일지_{year}.xlsx"
    if not os.path.exists(fpath):
        print(f"파일 없음: {fpath}")
        return

    xls = pd.ExcelFile(fpath)
    sheets = [s for s in xls.sheet_names if '월' in s and '정산' not in s and '목표' not in s]
    all_rows = []
    for sh in sheets:
        try:
            month = int(sh.replace('월', '').strip())
        except:
            continue
        rows = extract_from_sheet(fpath, sh, year, month)
        all_rows.extend(rows)
        if rows:
            ad_sum = sum(r['ad'] for r in rows)
            rev_sum = sum(r['rev'] for r in rows)
            print(f"  {sh}: {len(rows)}건 (광고비 {ad_sum:,}, 매출 {rev_sum:,})")

    print(f"\n총 {len(all_rows)}건 적재")

    conn = sqlite3.connect(DB_PATH)
    # 기존 카페제휴 데이터 (해당 연도) 클리어
    conn.execute(f"DELETE FROM ads WHERE 광고채널='카페제휴' AND substr(날짜,1,4)='{year}'")
    conn.execute(f"DELETE FROM sales WHERE 채널='카페제휴' AND substr(날짜,1,4)='{year}'")

    # 일자/브랜드 단위로 합산 (여러 채널 라벨이 같은 브랜드면 합침)
    agg = {}
    for r in all_rows:
        k = (r['date'], r['brand'])
        if k not in agg:
            agg[k] = {'ad': 0, 'rev': 0}
        agg[k]['ad'] += r['ad']
        agg[k]['rev'] += r['rev']

    for (d, brand), v in agg.items():
        if v['ad'] > 0:
            conn.execute(
                """INSERT OR REPLACE INTO ads
                   (날짜, 광고채널, 광고비, 노출수, 클릭수, 전환수, 전환매출, 브랜드)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (d, '카페제휴', v['ad'], 0, 0, 0, 0, brand)
            )
        if v['rev'] > 0:
            conn.execute(
                """INSERT OR REPLACE INTO sales
                   (날짜, 스토어, 채널, 주문건수, 매출, 객단가, 순방문자수, 전환율, 브랜드)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (d, f'{brand}(카페제휴)', '카페제휴', 0, v['rev'], 0, 0, 0.0, brand)
            )
    conn.commit()

    print("\n=== 월별 카페제휴 합계 ===")
    for row in conn.execute(
        f"SELECT substr(날짜,1,7) m, SUM(광고비) FROM ads "
        f"WHERE 광고채널='카페제휴' AND substr(날짜,1,4)='{year}' GROUP BY m ORDER BY m"
    ):
        print(f"  {row[0]} 광고비: {row[1]:,}")
    for row in conn.execute(
        f"SELECT substr(날짜,1,7) m, SUM(매출) FROM sales "
        f"WHERE 채널='카페제휴' AND substr(날짜,1,4)='{year}' GROUP BY m ORDER BY m"
    ):
        print(f"  {row[0]} 매출: {row[1]:,}")

    conn.close()


if __name__ == "__main__":
    main()
