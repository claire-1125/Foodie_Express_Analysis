WITH revisit_ordered_logs AS (
  -- 1) 재방문 유저 중 주문 내역 존재
  SELECT
    *
  FROM advanced.app_logs_cleaned_target
  WHERE 1=1
  AND user_pseudo_id NOT IN (
    SELECT user_pseudo_id
    FROM advanced.app_logs_target_visit_seg
    WHERE visit_interval_cat = 'one_day'
  )
  AND user_pseudo_id IN (
    SELECT user_pseudo_id  
    FROM advanced.app_logs_target_order_seg
    WHERE user_segment = 'ordered'
  )
)
, week_diff_ordered_users AS (
  -- 2) 사용자별 첫방문일, 방문일, 방문간격(주차) 추출
  SELECT
    user_pseudo_id,
    first_date,
    event_date,
    DATE_DIFF(event_date, first_date, WEEK) AS week_diff,
  FROM (
  SELECT DISTINCT
    user_pseudo_id,
    MIN(event_date) OVER (PARTITION BY user_pseudo_id) AS first_date,
    event_date,
  FROM revisit_ordered_logs
  )
)
, week_retain_ordered_users AS (
  -- 3) 주차별 유저 수 카운팅
  SELECT
    week_diff,
    COUNT(DISTINCT user_pseudo_id) AS retain_users
  FROM week_diff_ordered_users
  GROUP BY week_diff
)
, week0_retain_ordered_users AS (
  -- 4) 유입주차 유저 수 카운팅
  SELECT
    ANY_VALUE(week_diff) AS week_diff,
    COUNT(DISTINCT user_pseudo_id) AS first_users
  FROM week_diff_ordered_users
  WHERE week_diff = 0
)




, revisit_no_ordered_logs AS (
  -- 재방문 유저 중 주문 내역 없음
  SELECT
    *
  FROM advanced.app_logs_cleaned_target
  WHERE 1=1
  AND user_pseudo_id NOT IN (
    SELECT user_pseudo_id
    FROM advanced.app_logs_target_visit_seg
    WHERE visit_interval_cat = 'one_day'
  )
  AND user_pseudo_id IN (
    SELECT user_pseudo_id  
    FROM advanced.app_logs_target_order_seg
    WHERE user_segment = 'non_ordered'
  )
)
, week_diff_no_ordered_users AS (
  -- 2) 사용자별 첫방문일, 방문일, 방문간격(주차) 추출
  SELECT
    user_pseudo_id,
    first_date,
    event_date,
    DATE_DIFF(event_date, first_date, WEEK) AS week_diff,
  FROM (
  SELECT DISTINCT
    user_pseudo_id,
    MIN(event_date) OVER (PARTITION BY user_pseudo_id) AS first_date,
    event_date,
  FROM revisit_no_ordered_logs
  )
)
, week_retain_no_ordered_users AS (
  -- 3) 주차별 유저 수 카운팅
  SELECT
    week_diff,
    COUNT(DISTINCT user_pseudo_id) AS retain_users
  FROM week_diff_no_ordered_users
  GROUP BY week_diff
)
, week0_retain_no_ordered_users AS (
  -- 4) 유입주차 유저 수 카운팅
  SELECT
    ANY_VALUE(week_diff) AS week_diff,
    COUNT(DISTINCT user_pseudo_id) AS first_users
  FROM week_diff_no_ordered_users
  WHERE week_diff = 0
)


-- 주차별 리텐션 계산
SELECT
    'ordered' AS user_segment,
    w.week_diff,
    w.retain_users,
    f.first_users AS first_users,
    ROUND(SAFE_DIVIDE(w.retain_users, f.first_users)*100,3) AS retention_rate
FROM week_retain_ordered_users w
CROSS JOIN week0_retain_ordered_users f

UNION ALL

-- 주차별 리텐션 계산
SELECT
  'no_ordered' AS user_segment,
  w.week_diff,
  w.retain_users,
  f.first_users AS first_users,
  ROUND(SAFE_DIVIDE(w.retain_users, f.first_users)*100,3) AS retention_rate
FROM week_retain_no_ordered_users w
CROSS JOIN week0_retain_no_ordered_users f

ORDER BY user_segment, week_diff
