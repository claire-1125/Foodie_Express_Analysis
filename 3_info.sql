-- 테이블 스키마
SELECT 
 TO_JSON_STRING(
    ARRAY_AGG(STRUCT( 
      IF(is_nullable = 'YES', 'NULLABLE', 'REQUIRED') AS mode,
      column_name AS name,
      data_type AS type)
    ORDER BY ordinal_position), TRUE
  ) AS schema
FROM advanced.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'app_logs_cleaned_target'

-- 테이블 미리보기
SELECT
  *
FROM advanced.app_logs_cleaned_target
LIMIT 10;

-- 테이블 레코드 수 확인: 728085건
SELECT
  COUNT(*) AS records,
FROM advanced.app_logs_cleaned_target;

-- 데이터 기간 확인: 22.08.01~23.01.20 (172일; 약 6개월)
SELECT
  MIN(event_date) AS start_date,
  MAX(event_date) AS end_date,
  DATE_DIFF(MAX(event_date), MIN(event_date), DAY) AS day_diff,
FROM advanced.app_logs_cleaned_target;


-- 카테고리 가짓수 확인: 총 14가지
SELECT
  DISTINCT event_name AS event_list,
FROM advanced.app_logs_cleaned_target;

