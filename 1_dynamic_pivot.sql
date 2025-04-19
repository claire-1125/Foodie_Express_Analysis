CREATE OR REPLACE PROCEDURE `inflearn-bigquery-439112.advanced.app_logs_preprocess`()
BEGIN


DECLARE params_list STRING;
DECLARE d_query_str STRING;


-- 1. 파라미터 리스트 추출
-- COALESCE() 인자들의 자료형을 통일시켜주기 위해 int_value를 STRING 으로 변환해야 함.
SET params_list = (
  SELECT
    STRING_AGG( DISTINCT
      CONCAT(
        "MAX(IF(params.key='",
        params.key,
        "', COALESCE(params.value.string_value, CAST(params.value.int_value AS STRING)), NULL)) AS `",
        params.key,
      '`'
      )
    ) 
  FROM advanced.app_logs
  CROSS JOIN UNNEST(event_params) AS params
);

-- 2. 날릴 쿼리
-- DOW 시작일 변환 이슈: https://stackoverflow.com/questions/60106367/bigquery-day-of-week-number-where-monday-1/60106522#60106522

SET d_query_str = 
  """
  CREATE TEMP TABLE app_logs_temp AS
  SELECT
    DATETIME(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul') AS event_datetime,
    EXTRACT(DATE FROM DATETIME(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul')) AS event_date,
    EXTRACT(TIME FROM DATETIME(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul')) AS event_time,
    DATE_TRUNC(EXTRACT(DATE FROM DATETIME(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul')), WEEK(MONDAY)) AS event_week,
    MOD(EXTRACT(DAYOFWEEK FROM DATETIME(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul')) + 5, 7) + 1 AS event_dow,  -- ISO 기준 (월요일이 1)
    user_pseudo_id,
    user_id,
    event_name,""" 
    || params_list || 
  """
  FROM advanced.app_logs
  CROSS JOIN UNNEST(event_params) AS params
  GROUP BY ALL""";

  

-- 3. 임시 테이블 생성
EXECUTE IMMEDIATE d_query_str;


-- 4. 최종 테이블 생성 및 데이터 적재
EXECUTE IMMEDIATE FORMAT(
  """
  CREATE OR REPLACE TABLE advanced.app_logs_cleaned
  PARTITION BY event_date  -- 원본과 동일한 파티션 키
  AS
  SELECT * FROM app_logs_temp"""
);


-- -- 5. event_params 중 일부 컬럼 자료형 변경
-- -- restaurant_id, food_id, banner_id
-- ALTER TABLE advanced.app_logs_cleaned
-- ALTER COLUMN restaurant_id SET DATA TYPE INT64,
-- ALTER COLUMN food_id SET DATA TYPE INT64,
-- ALTER COLUMN banner_id SET DATA TYPE INT64;


END;