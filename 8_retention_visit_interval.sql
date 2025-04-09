/* 단기, 중기, 장기 유저별 리텐션 계산 */

/* 주차별 리텐션: 단기 재방문 유저 */

WITH short_user_logs AS (
  -- 1) 단기 재방문 유저의 로그
  -- 단기 재방문 유저 전체 인원인 9661명, 분석 대상 로그 수 190787건
  SELECT
    event_datetime,
    event_date,
    event_time,
    event_week,
    event_dow,
    user_pseudo_id,
    user_id,
    firebase_screen,
    event_name,
  FROM advanced.app_logs_cleaned_target
  WHERE 1=1
  AND user_pseudo_id IN (
    SELECT user_pseudo_id
    FROM advanced.app_logs_target_visit_seg
    WHERE visit_interval_cat = 'short'
  )
)
, week_diff_short_users AS (
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
    FROM short_user_logs
    )
)
, week_retain_short_users AS (
    -- 3) 주차별 유저 수 카운팅
    SELECT
        week_diff,
        COUNT(DISTINCT user_pseudo_id) AS retain_users
    FROM week_diff_short_users
    GROUP BY week_diff
)
, week0_retain_short_users AS (
    -- 4) 유입주차 유저 수 카운팅
    SELECT
        ANY_VALUE(week_diff) AS week_diff,
        COUNT(DISTINCT user_pseudo_id) AS first_users
    FROM week_diff_short_users
    WHERE week_diff = 0
)


-- 주차별 리텐션 계산
SELECT
    'short' AS user_segment,
    w.week_diff,
    w.retain_users,
    f.first_users AS first_users,
    ROUND(SAFE_DIVIDE(w.retain_users, f.first_users)*100,3) AS retention_rate
FROM week_retain_short_users w
CROSS JOIN week0_retain_short_users f
ORDER BY week_diff



/* 주문 퍼널: 중기 재방문 유저 */

WITH mid_user_logs AS (
  -- 1) 중기 재방문 유저의 로그
  -- 중기 재방문 유저 전체 인원인 명, 분석 대상 로그 수 건
  SELECT
    event_datetime,
    event_date,
    event_time,
    event_week,
    event_dow,
    user_pseudo_id,
    user_id,
    firebase_screen,
    event_name,
  FROM advanced.app_logs_cleaned_target
  WHERE 1=1
  AND user_pseudo_id IN (
    SELECT user_pseudo_id
    FROM advanced.app_logs_target_visit_seg
    WHERE visit_interval_cat = 'mid'
  )
)
, week_diff_mid_users AS (
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
    FROM mid_user_logs
    )
)
, week_retain_mid_users AS (
    -- 3) 주차별 유저 수 카운팅
    SELECT
        week_diff,
        COUNT(DISTINCT user_pseudo_id) AS retain_users
    FROM week_diff_mid_users
    GROUP BY week_diff
)
, week0_retain_mid_users AS (
    -- 4) 유입주차 유저 수 카운팅
    SELECT
        ANY_VALUE(week_diff) AS week_diff,
        COUNT(DISTINCT user_pseudo_id) AS first_users
    FROM week_diff_mid_users
    WHERE week_diff = 0
)


-- 주차별 리텐션 계산
SELECT
    'mid' AS user_segment,
    w.week_diff,
    w.retain_users,
    f.first_users AS first_users,
    ROUND(SAFE_DIVIDE(w.retain_users, f.first_users)*100,3) AS retention_rate
FROM week_retain_mid_users w
CROSS JOIN week0_retain_mid_users f
ORDER BY week_diff



/* 주문 퍼널: 장기 재방문 유저 */

WITH long_user_logs AS (
  -- 1) 장기 재방문 유저의 로그
  -- 장기 재방문 유저 전체 인원인 명, 분석 대상 로그 수 건
  SELECT
    event_datetime,
    event_date,
    event_time,
    event_week,
    event_dow,
    user_pseudo_id,
    user_id,
    firebase_screen,
    event_name,
  FROM advanced.app_logs_cleaned_target
  WHERE 1=1
  AND user_pseudo_id IN (
    SELECT user_pseudo_id
    FROM advanced.app_logs_target_visit_seg
    WHERE visit_interval_cat = 'long'
  )
)
, week_diff_long_users AS (
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
    FROM long_user_logs
    )
)
, week_retain_long_users AS (
    -- 3) 주차별 유저 수 카운팅
    SELECT
        week_diff,
        COUNT(DISTINCT user_pseudo_id) AS retain_users
    FROM week_diff_long_users
    GROUP BY week_diff
)
, week0_retain_long_users AS (
    -- 4) 유입주차 유저 수 카운팅
    SELECT
        ANY_VALUE(week_diff) AS week_diff,
        COUNT(DISTINCT user_pseudo_id) AS first_users
    FROM week_diff_long_users
    WHERE week_diff = 0
)


-- 주차별 리텐션 계산
SELECT
    'long' AS user_segment,
    w.week_diff,
    w.retain_users,
    f.first_users AS first_users,
    ROUND(SAFE_DIVIDE(w.retain_users, f.first_users)*100,3) AS retention_rate
FROM week_retain_long_users w
CROSS JOIN week0_retain_long_users f
ORDER BY week_diff


