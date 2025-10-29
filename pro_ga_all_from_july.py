import os
from datetime import datetime, timedelta
import re

from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, DateRange, Dimension, Metric,
    FilterExpression, FilterExpressionList, Filter
)
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ====== 🔧 설정값 (네 환경 반영 완료) ======
PROPERTY_ID = "464149233"  # GA4 속성 ID
SPREADSHEET_ID = "1BvNaXrnuau729-2yJiX-ztRv4GaK-7OCn3Bh9_guaRQ"  # 스프레드시트 ID
SHEET_NAME = "GA_All_From_July"  # 7월 1일부터 전체 데이터

SERVICE_ACCOUNT_JSON = "cafe24promkt-21a0fd3e7f24.json"

# (선택) 특정 소스/매체/캠페인 필터 — 필요 없으면 빈 문자열 유지
FILTER_SOURCE = ""      # 예: "google"
FILTER_MEDIUM = ""      # 예: "cpc"
FILTER_CAMPAIGN = "bx"  # bx 캠페인만 필터링

# 조회 주 수 (오늘 기준 지난 N주)
WEEKS = 12
# =========================================


def get_credentials(json_path: str):
    scopes = [
        "https://www.googleapis.com/auth/analytics.readonly",
        "https://www.googleapis.com/auth/spreadsheets"
    ]
    return service_account.Credentials.from_service_account_file(json_path, scopes=scopes)


def get_week_range(year_week: str):
    """yearWeek 형식(예: 202537)을 주차 기간(예: 2025.09.08~09.14)으로 변환 (ISO 주차 기준)"""
    try:
        if len(year_week) != 6:
            return year_week
        
        year = int(year_week[:4])
        week = int(year_week[4:])
        
        # ISO 주차 기준: 1월 4일이 포함된 주가 첫 번째 주
        jan_4 = datetime(year, 1, 4)
        jan_4_weekday = jan_4.weekday()  # 월요일=0, 일요일=6
        
        # 해당 연도의 첫 번째 ISO 주의 월요일
        first_monday = jan_4 - timedelta(days=jan_4_weekday)
        
        # 해당 주의 월요일 계산
        week_monday = first_monday + timedelta(weeks=week-1)
        week_sunday = week_monday + timedelta(days=6)
        
        # 형식: MM.DD~MM.DD
        return f"{week_monday.strftime('%m.%d')}~{week_sunday.strftime('%m.%d')}"
    
    except (ValueError, IndexError):
        return year_week


def convert_to_number(value):
    """문자열을 숫자로 변환 (정수 우선, 실패시 실수, 실패시 0)"""
    try:
        # 빈 문자열이나 None 처리
        if not value or value == '':
            return 0
        
        # 문자열인 경우 숫자로 변환 시도
        if isinstance(value, str):
            # 소수점이 있는지 확인
            if '.' in value:
                return float(value)
            else:
                return int(value)
        
        # 이미 숫자인 경우 그대로 반환
        return value
    except (ValueError, TypeError):
        return 0


def calculate_weekly_summary(rows):
    """주차별 매체별 합계를 계산"""
    from collections import defaultdict
    
    # 주차별, 매체별로 데이터를 그룹핑
    weekly_summary = defaultdict(lambda: defaultdict(lambda: {
        'campaigns': set(),
        'sessions': 0,
        'activeUsers': 0,
        'eventCount': 0,
        'conversions': 0
    }))
    
    # 헤더를 제외한 데이터 행들을 처리
    for row in rows[1:]:  # 첫 번째 행은 헤더
        week_range = row[2]  # weekRange
        session_medium = row[4]  # sessionMedium
        campaign_name = row[6]  # sessionCampaignName
        sessions = int(row[7]) if row[7].isdigit() else 0
        active_users = int(row[8]) if row[8].isdigit() else 0
        event_count = int(row[9]) if row[9].isdigit() else 0
        conversions = int(row[10]) if row[10].isdigit() else 0
        
        # 매체 타입 결정
        media_type = "YouTube" if session_medium == "paid_youtube" else "Reels"
        
        # 합계 계산
        weekly_summary[week_range][media_type]['campaigns'].add(campaign_name)
        weekly_summary[week_range][media_type]['sessions'] += sessions
        weekly_summary[week_range][media_type]['activeUsers'] += active_users
        weekly_summary[week_range][media_type]['eventCount'] += event_count
        weekly_summary[week_range][media_type]['conversions'] += conversions
    
    # 결과 생성
    summary_rows = []
    summary_header = [
        "weekRange", "mediaType", "campaignNames", "sessions", "activeUsers", "eventCount", "conversions"
    ]
    summary_rows.append(summary_header)
    
    # 주차별로 정렬하여 결과 생성
    for week_range in sorted(weekly_summary.keys()):
        for media_type in ["YouTube", "Reels"]:
            if media_type in weekly_summary[week_range]:
                data = weekly_summary[week_range][media_type]
                campaign_names = ", ".join(sorted(data['campaigns']))
                summary_rows.append([
                    week_range,
                    media_type,
                    campaign_names,
                    data['sessions'],
                    data['activeUsers'],
                    data['eventCount'],
                    data['conversions']
                ])
    
    return summary_rows


def ensure_sheet_exists_and_clear(sheets_service, spreadsheet_id: str, sheet_name: str):
    # 스프레드시트의 시트 목록 조회
    meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = meta.get("sheets", [])
    existing_names = {s["properties"]["title"] for s in sheets}

    requests = []

    if sheet_name not in existing_names:
        # 시트가 없으면 생성
        requests.append({"addSheet": {"properties": {"title": sheet_name}}})
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()
    else:
        # 있으면 전체 지우기
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A:Z"
        ).execute()


def build_dimension_filter():
    and_filters = []
    
    # 캠페인 필터
    if FILTER_CAMPAIGN:
        and_filters.append(Filter(
            field_name="sessionCampaignName",
            string_filter=Filter.StringFilter(
                value=FILTER_CAMPAIGN,
                match_type=Filter.StringFilter.MatchType.EXACT
            )
        ))
    
    # 소스 필터
    if FILTER_SOURCE:
        and_filters.append(Filter(
            field_name="sessionSource",
            string_filter=Filter.StringFilter(
                value=FILTER_SOURCE,
                match_type=Filter.StringFilter.MatchType.EXACT
            )
        ))
    
    # 매체 필터
    if FILTER_MEDIUM:
        and_filters.append(Filter(
            field_name="sessionMedium",
            string_filter=Filter.StringFilter(
                value=FILTER_MEDIUM,
                match_type=Filter.StringFilter.MatchType.EXACT
            )
        ))

    if not and_filters:
        return None

    # FilterExpression과 Filter 객체를 구분해서 처리
    filter_expressions = []
    for f in and_filters:
        if isinstance(f, FilterExpression):
            filter_expressions.append(f)
        else:
            filter_expressions.append(FilterExpression(filter=f))

    return FilterExpression(
        and_group=FilterExpressionList(expressions=filter_expressions)
    )


def main():
    creds = get_credentials(SERVICE_ACCOUNT_JSON)

    # GA4 클라이언트
    ga_client = BetaAnalyticsDataClient(credentials=creds)

    # 날짜 범위 (2025년 7월 1일부터 오늘까지)
    end_date = datetime.today().date()
    start_date = datetime(2025, 7, 1).date()
    date_range = DateRange(start_date=str(start_date), end_date=str(end_date))

    # 차원/지표
    dimensions = [
        Dimension(name="date"),                  # 일자별 데이터
        Dimension(name="yearWeek"),              # 주차별 데이터 (예: 202535)
        Dimension(name="sessionSource"),         # 세션 소스
        Dimension(name="sessionMedium"),         # 세션 매체
        Dimension(name="sessionCampaignName"),   # 캠페인명
    ]
    metrics = [
        Metric(name="sessions"),                 # 세션수
        Metric(name="activeUsers"),              # 활성 사용자 수
        Metric(name="eventCount"),               # 이벤트수
        Metric(name="conversions"),              # 전환수
    ]

    # 필터
    dimension_filter = build_dimension_filter()

    # 리포트 요청
    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        dimensions=dimensions,
        metrics=metrics,
        date_ranges=[date_range],
        dimension_filter=dimension_filter,
        limit=100000,
        order_bys=[{"dimension": {"dimension_name": "date"}}],
    )

    response = ga_client.run_report(request)

    # 결과 변환
    rows = []
    header = [
        "date", "weekRange", "sessionSource", "sessionMedium", "mediaType", "sessionCampaignName",
        "sessions", "activeUsers", "eventCount", "conversions"
    ]
    rows.append(header)

    for r in response.rows:
        d = [d.value for d in r.dimension_values]
        m = [m.value for m in r.metric_values]
        
        # 주차기간 계산
        year_week = d[1]  # yearWeek는 두 번째 차원
        week_range = get_week_range(year_week)
        
        # 매체 타입 구분
        session_medium = d[3]  # sessionMedium은 네 번째 차원 (0: date, 1: yearWeek, 2: sessionSource, 3: sessionMedium, 4: sessionCampaignName)
        if session_medium == "paid_youtube":
            media_type = "YouTube"
        elif session_medium == "paid_reels":
            media_type = "Reels"
        elif session_medium == "cpc":
            media_type = "Google Ads"
        elif session_medium == "organic":
            media_type = "Organic"
        elif session_medium == "referral":
            media_type = "Referral"
        elif session_medium == "direct":
            media_type = "Direct"
        else:
            media_type = session_medium
        
        # 메트릭 데이터를 숫자로 변환
        converted_metrics = [convert_to_number(metric) for metric in m]
        
        # 데이터 재구성: date, weekRange, sessionSource, sessionMedium, mediaType, sessionCampaignName
        new_row = [
            d[0],  # date
            week_range,  # weekRange
            d[2],  # sessionSource
            d[3],  # sessionMedium
            media_type,  # mediaType
            d[4],  # sessionCampaignName
        ] + converted_metrics  # 숫자로 변환된 메트릭
        
        rows.append(new_row)

    # Google Sheets 쓰기
    sheets_service = build("sheets", "v4", credentials=creds)

    # 상세 데이터 시트
    ensure_sheet_exists_and_clear(sheets_service, SPREADSHEET_ID, SHEET_NAME)
    sheets_service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A1",
        valueInputOption="USER_ENTERED",  # 숫자 자동 인식
        body={"values": rows}
    ).execute()

    print(f"✅ Done. {len(rows)-1} rows written to '{SHEET_NAME}' (from 2025-07-01)")
    print(f"✅ Spreadsheet: {SPREADSHEET_ID}")


if __name__ == "__main__":
    try:
        main()
    except HttpError as e:
        print("❌ Google API Error:", e)
        raise
    except Exception as e:
        print("❌ Error:", e)
        raise
