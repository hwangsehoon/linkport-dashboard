"""
스프레드시트 → DB 정밀 임포트 v2
- 모든 월, 모든 브랜드, 모든 개별 채널을 1개씩 읽음
- API로 커버되는 채널은 스킵
- API에 없는 채널만 정확하게 DB에 저장
"""
import pandas as pd
import sqlite3
from datetime import date, datetime

DB_PATH = "dashboard_data.db"
BASE_DIR = "C:/Users/조현우/Desktop/광고일지"

# API로 커버되는 매출 채널 (스킵)
API_SALES_CHANNELS = {
    "자사몰", "마르문(CAFE24)", "마르문",
    "스스", "스마트스토어", "스스(마르문)", "스스(유산균)",
    "쿠팡", "쿠팡Wing", "쿠팡 Wing", "쿠팡(마르문)", "쿠팡(유산균)",
}

# API로 커버되는 광고비 채널 (스킵)
# 네이버SA는 2025-04 이후만 API에 있으므로 날짜 체크 필요
API_AD_CHANNELS = {
    "Meta", "메타", "FB", "FB/IS", "FB(링.포)",
}

# 네이버SA 관련 채널명 (2025-04 이후만 스킵)
NAVER_SA_NAMES = {
    "N - SA", "N - SA(파워링크)", "Naver SA", "네이버 SA", "N-SA", "N - SA ",
}

# 윈토르는 API 미연동이므로 모든 채널 가져와야 함
# (윈토르 브랜드인 경우 API_SALES_CHANNELS 무시)


def get_conn():
    return sqlite3.connect(DB_PATH)


def is_api_sales_channel(channel_name, brand):
    """이 매출 채널이 API로 커버되는지 판단"""
    if brand == "윈토르":
        return False  # 윈토르는 전부 스프레드시트에서 가져와야 함
    clean = channel_name.strip()
    for api_ch in API_SALES_CHANNELS:
        if api_ch in clean:
            return True
    return False


def is_api_ad_channel(channel_name, brand, dt):
    """이 광고비 채널이 API로 커버되는지 판단"""
    if brand == "윈토르":
        return False  # 윈토르는 전부 스프레드시트에서 가져와야 함
    clean = channel_name.strip()

    # Meta 계열
    for api_ch in API_AD_CHANNELS:
        if api_ch in clean or clean in api_ch:
            return True

    # 네이버SA - 2025-04 이후만 API 있음
    for nsa in NAVER_SA_NAMES:
        if nsa in clean or clean in nsa:
            if dt >= "2025-04-01":
                return True
            else:
                return False

    return False


def parse_brand_section(df, header_row, brand, year, month, next_section_row=None):
    """한 브랜드 섹션에서 개별 채널의 일별 데이터를 추출"""
    col_row = header_row + 1
    data_start = header_row + 2
    data_end = next_section_row - 3 if next_section_row else data_start + 33

    # 날짜 열 찾기
    date_col = 2
    if col_row < len(df):
        for c in range(5):
            h = str(df.iloc[col_row, c]).strip() if c < len(df.columns) and pd.notna(df.iloc[col_row, c]) else ''
            if '날' in h or 'date' in h.lower():
                date_col = c
                break

    # 채널 목록 추출 (header_row에서)
    channels = []  # [(channel_name, start_col)]
    for c in range(len(df.columns)):
        v = df.iloc[header_row, c]
        if pd.notna(v):
            name = str(v).replace('\n', '').strip()
            # 브랜드명 자체나 합계는 스킵 (개별 채널만)
            if '합계' in name or name == brand or name in ['아자차', '반드럽', '웰바이오젠', '윈토르']:
                continue
            # "ROAS", "비고" 등 비채널 스킵
            if name in ['ROAS', 'B.ROAS', '비고', '실매출']:
                continue
            # 환불, 마진율 등 스킵
            if '환불' in name or '마진' in name or 'raw' in name or '주문건수' in name:
                continue
            # 총 ROAS 스킵
            if '총 ROAS' in name:
                continue
            channels.append((name, c))

    # 각 채널의 광고비/매출 열 위치 결정
    # 채널 시작 열에서 컬럼 행을 읽어 광고비와 매출 위치를 찾음
    channel_configs = []
    for ch_name, ch_start in channels:
        # 이 채널 영역의 컬럼 헤더를 읽음
        # 보통 4열 단위 (광고비, 매출, ROAS, 비고) 또는
        # 2열 단위 (매출, 비고) - 매출만 있는 채널
        ad_col = None
        rev_col = None

        # 다음 채널 시작 전까지의 열을 검사
        next_ch_start = None
        for other_name, other_start in channels:
            if other_start > ch_start:
                if next_ch_start is None or other_start < next_ch_start:
                    next_ch_start = other_start

        end_col = next_ch_start if next_ch_start else ch_start + 6

        for c in range(ch_start, min(end_col, len(df.columns))):
            if col_row < len(df) and pd.notna(df.iloc[col_row, c]):
                h = str(df.iloc[col_row, c]).strip()
                if ('광고비' in h) and ad_col is None:
                    ad_col = c
                elif h == '매출' and rev_col is None:
                    rev_col = c
                elif h == '실매출' and rev_col is None:
                    rev_col = c

        if ad_col is not None or rev_col is not None:
            channel_configs.append({
                'name': ch_name,
                'ad_col': ad_col,
                'rev_col': rev_col,
            })

    # 일별 데이터 읽기
    results = []
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

        dt_str = d.isoformat()

        for cfg in channel_configs:
            ch_name = cfg['name']

            # 매출 읽기
            rev = 0
            if cfg['rev_col'] is not None and cfg['rev_col'] < len(df.columns):
                rv = df.iloc[r, cfg['rev_col']]
                try:
                    rev = int(float(rv)) if pd.notna(rv) else 0
                except:
                    rev = 0

            # 광고비 읽기
            ad = 0
            if cfg['ad_col'] is not None and cfg['ad_col'] < len(df.columns):
                av = df.iloc[r, cfg['ad_col']]
                try:
                    ad = int(float(av)) if pd.notna(av) else 0
                except:
                    ad = 0

            if rev <= 0 and ad <= 0:
                continue

            # API 커버 여부 판단
            skip_sales = is_api_sales_channel(ch_name, brand)
            skip_ads = is_api_ad_channel(ch_name, brand, dt_str)

            # 매출 저장 (API에 없는 채널만)
            if rev > 0 and not skip_sales:
                results.append({
                    'type': 'sales',
                    'date': dt_str,
                    'brand': brand,
                    'channel': ch_name,
                    'revenue': rev,
                })

            # 광고비 저장 (API에 없는 채널만)
            if ad > 0 and not skip_ads:
                results.append({
                    'type': 'ads',
                    'date': dt_str,
                    'brand': brand,
                    'channel': ch_name,
                    'ad_cost': ad,
                })

    return results


def find_brand_sections(df):
    """시트에서 브랜드 섹션 찾기"""
    sections = []
    brand_map = {
        '아자차': '아자차', '반드럽': '반드럽', '웰바이오젠': '웰바이오젠', '윈토르': '윈토르',
        '통 합': '아자차', '통합': '아자차',
    }

    for row_idx in range(min(200, len(df))):
        for col_idx in range(min(5, len(df.columns))):
            v = str(df.iloc[row_idx, col_idx]).strip()
            if '합계' in v or '통 합' in v or '통합' in v:
                for key, brand in brand_map.items():
                    if key in v:
                        sections.append({'brand': brand, 'row': row_idx, 'label': v})
                        break
    return sections


def main():
    conn = get_conn()

    # 기존 스프레드시트 보충 데이터 삭제
    conn.execute("DELETE FROM sales WHERE 채널 = '스프레드시트'")
    conn.execute("DELETE FROM ads WHERE 광고채널 LIKE '%스프레드시트%'")
    conn.commit()

    files = [
        ('링포 광고일지_2026.xlsx', 2026),
        ('링포 광고일지_2025.xlsx', 2025),
        ('링포 광고일지_2024.xlsx', 2024),
        ('링포 광고일지_2023.xlsx', 2023),
    ]

    total_sales_saved = 0
    total_ads_saved = 0
    output = []

    for fname, year in files:
        fpath = f"{BASE_DIR}/{fname}"
        xls = pd.ExcelFile(fpath)
        output.append(f"\n[{year}년]")

        for sheet in xls.sheet_names:
            if '월' not in sheet or '정산' in sheet or '목표' in sheet or '데이터' in sheet:
                continue
            try:
                month_num = int(sheet.replace('월', '').strip())
            except:
                continue

            df = pd.read_excel(fpath, sheet_name=sheet, header=None)
            sections = find_brand_sections(df)

            month_sales = 0
            month_ads = 0
            month_details = []

            for i, sec in enumerate(sections):
                next_row = sections[i + 1]['row'] if i + 1 < len(sections) else None
                results = parse_brand_section(df, sec['row'], sec['brand'], year, month_num, next_row)

                for r in results:
                    if r['type'] == 'sales':
                        conn.execute(
                            """INSERT OR IGNORE INTO sales
                               (날짜, 스토어, 채널, 주문건수, 매출, 객단가, 순방문자수, 전환율, 브랜드)
                               VALUES (?,?,?,?,?,?,?,?,?)""",
                            (r['date'], f"{r['channel']}({r['brand']})", "스프레드시트",
                             0, r['revenue'], 0, 0, 0.0, r['brand'])
                        )
                        month_sales += r['revenue']
                        total_sales_saved += 1
                        month_details.append(f"    매출 {r['date']} {r['brand']} {r['channel']}: {r['revenue']:,}")

                    elif r['type'] == 'ads':
                        conn.execute(
                            """INSERT OR IGNORE INTO ads
                               (날짜, 광고채널, 광고비, 노출수, 클릭수, 전환수, 전환매출, 브랜드)
                               VALUES (?,?,?,?,?,?,?,?)""",
                            (r['date'], f"{r['channel']}(스프레드시트)", r['ad_cost'],
                             0, 0, 0, 0, r['brand'])
                        )
                        month_ads += r['ad_cost']
                        total_ads_saved += 1

            conn.commit()
            output.append(f"  {month_num}월: 매출보충 {month_sales:,}원 / 광고비보충 {month_ads:,}원")

    # 결과 저장
    output.append(f"\n총 매출 보충: {total_sales_saved}건")
    output.append(f"총 광고비 보충: {total_ads_saved}건")

    with open("import_v2_result.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(output))

    conn.close()
    print(f"완료: 매출 {total_sales_saved}건 / 광고비 {total_ads_saved}건")
    print("상세: import_v2_result.txt")


if __name__ == "__main__":
    main()
