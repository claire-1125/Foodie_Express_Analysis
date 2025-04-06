CREATE OR REPLACE PROCEDURE `inflearn-bigquery-439112.advanced.app_logs_target_segment_visit_interval`()
BEGIN

CREATE OR REPLACE TABLE advanced.app_logs_target_visit_seg AS

/*
방문 주기 구분
- 하루만 방문한 사람 (재방문 X)
- 단기 재방문자 (7~14일 간격)
- 중기 재방문자 (15~30일 간격)
- 장기 재방문자 (30일 초과)
*/

WITH user_active_sequence AS (
  -- 1. 유저별 활동 일자 시퀀스: 유입 일자, 활동 일자, 직전 활동 일자
  -- 회원 49678명
  SELECT DISTINCT
    user_pseudo_id,
    event_date,
    LAG(event_date) OVER (PARTITION BY user_pseudo_id ORDER BY event_date) AS prev_event_date,
  FROM advanced.app_logs_cleaned_target
)
, user_only_1day AS (
  -- 2-1. 정합성 검증: 하루만 사용하고 이탈한 사람 13151명
  SELECT DISTINCT
    user_pseudo_id,
  FROM advanced.app_logs_cleaned_target
  WHERE user_pseudo_id IN (
    SELECT user_pseudo_id
    FROM user_active_sequence
    GROUP BY user_pseudo_id
    HAVING COUNT(DISTINCT event_date) = 1
  )
)
, user_visit_interval_calc AS (
  -- 2. 유저별 유입일 이후 각 방문 간격 계산 (36527명; 49678-13151)
  SELECT
    user_pseudo_id,
    event_date,
    prev_event_date,
    DATE_DIFF(event_date, prev_event_date, DAY) AS day_diff,
    ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_date DESC) AS visit_interval_order,  -- 방문 간격 순서 (맨 마지막을 1로 둠.)
    CASE
      WHEN DATE_DIFF(event_date, prev_event_date, DAY) <= 14 THEN 'short'
      WHEN DATE_DIFF(event_date, prev_event_date, DAY) BETWEEN 15 AND 30 THEN 'mid'
      ELSE 'long'
    END AS visit_interval_cat,
  FROM user_active_sequence
  WHERE 1=1
  AND user_pseudo_id NOT IN (SELECT * FROM user_only_1day)  -- 재방문한 유저만 남김
  AND prev_event_date IS NOT NULL  -- 유입일 이전 제외
  AND DATE_DIFF(event_date, prev_event_date, DAY) != 0
)
, visit_interval_count_per_user AS (
  -- 3. 유저별 방문 간격 유형별 카운팅
  SELECT
    user_pseudo_id,
    visit_interval_cat,
    COUNT(*) AS cnt,
  FROM user_visit_interval_calc
  GROUP BY user_pseudo_id, visit_interval_cat
)
, mode_visit_interval_cat_per_user AS (
  -- 4. 유저별 최빈 방문 간격 유형 추출 (공동 1위 존재함.)
  SELECT
    user_pseudo_id,
    visit_interval_cat,
    cnt,
    RANK() OVER (PARTITION BY user_pseudo_id ORDER BY cnt DESC) AS cat_cnt_order
  FROM visit_interval_count_per_user
  QUALIFY cat_cnt_order = 1
)


-- 한번만 방문한 유저 (13151명)
SELECT
  user_pseudo_id,
  'one_day' AS visit_interval_cat,
FROM user_only_1day

UNION DISTINCT

-- 최빈 방문 간격 유형이 하나인 경우: 바로 분류 (26370명)
SELECT
  user_pseudo_id,
  visit_interval_cat,
FROM mode_visit_interval_cat_per_user
WHERE user_pseudo_id IN (
  SELECT user_pseudo_id
  FROM mode_visit_interval_cat_per_user
  GROUP BY user_pseudo_id
  HAVING COUNT(*) = 1
)

UNION DISTINCT

-- 최빈 방문 간격 유형이 여러 개인 경우: 최신 방문 간격 유형으로 분류 (10157명)
SELECT
  user_pseudo_id,
  visit_interval_cat,
FROM user_visit_interval_calc
WHERE 1=1
AND user_pseudo_id NOT IN (
  SELECT user_pseudo_id
  FROM mode_visit_interval_cat_per_user
  GROUP BY user_pseudo_id
  HAVING COUNT(*) = 1
)
AND visit_interval_order = 1;


END;