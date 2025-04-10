/* 재방문 유저의 코호트 리텐션 계산 */

WITH revisit_user_logs AS (
  -- 1) 재방문 유저의 로그
  -- 재방문 유저 전체 인원인 36527명, 분석 대상 로그 수 273441건
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
        CONCAT(firebase_screen, ": ", event_name) AS screen_event,
    FROM advanced.app_logs_cleaned_target
    WHERE 1=1
    AND user_pseudo_id NOT IN (
        SELECT user_pseudo_id
        FROM advanced.app_logs_target_visit_seg
        WHERE visit_interval_cat = 'one_day'
    )
)
, week_diff_revisit AS (
    -- 2) 사용자별 유입주차, 방문주차, 방문간격(주차) 추출
    SELECT
    user_pseudo_id,
    first_week,
    event_week,
    DATE_DIFF(event_week, first_week, WEEK) AS week_diff,
    FROM (
    SELECT DISTINCT
        user_pseudo_id,
        MIN(event_week) OVER (PARTITION BY user_pseudo_id) AS first_week,
        event_week,
    FROM revisit_user_logs
    )
)
, week_retain_revisit AS (
    -- 3) 유입주차, 방문간격(주차)별 유저 수 카운팅
    SELECT
        first_week,
        week_diff,
        COUNT(DISTINCT user_pseudo_id) AS retain_users
    FROM week_diff_revisit
    GROUP BY first_week, week_diff
)


-- 코호트 리텐션 계산
SELECT
    first_week,
    week_diff,
    retain_users,
    first_users,
    ROUND(SAFE_DIVIDE(retain_users,first_users),3) AS retention_rate
FROM (
    SELECT
        first_week,
        week_diff,
        retain_users,
        FIRST_VALUE(retain_users) OVER (PARTITION BY first_week ORDER BY week_diff) AS first_users
    FROM week_retain_revisit
)



/* 단기 재방문 유저의 코호트 리텐션 계산 */

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
        CONCAT(firebase_screen, ": ", event_name) AS screen_event,
    FROM advanced.app_logs_cleaned_target
    WHERE 1=1
    AND user_pseudo_id IN (
        SELECT user_pseudo_id
        FROM advanced.app_logs_target_visit_seg
        WHERE visit_interval_cat = 'short'
    )
)
, week_diff_short AS (
    -- 2) 사용자별 유입주차, 방문주차, 방문간격(주차) 추출
    SELECT
    user_pseudo_id,
    first_week,
    event_week,
    DATE_DIFF(event_week, first_week, WEEK) AS week_diff,
    FROM (
    SELECT DISTINCT
        user_pseudo_id,
        MIN(event_week) OVER (PARTITION BY user_pseudo_id) AS first_week,
        event_week,
    FROM short_user_logs
    )
)
, week_retain_short AS (
    -- 3) 유입주차, 방문간격(주차)별 유저 수 카운팅
    SELECT
        first_week,
        week_diff,
        COUNT(DISTINCT user_pseudo_id) AS retain_users
    FROM week_diff_short
    GROUP BY first_week, week_diff
)


-- 코호트 리텐션 계산
SELECT
    first_week,
    week_diff,
    retain_users,
    first_users,
    ROUND(SAFE_DIVIDE(retain_users,first_users),3) AS retention_rate
FROM (
    SELECT
        first_week,
        week_diff,
        retain_users,
        FIRST_VALUE(retain_users) OVER (PARTITION BY first_week ORDER BY week_diff) AS first_users
    FROM week_retain_short
)



/* 중기 재방문 유저의 코호트 리텐션 계산 */

WITH mid_user_logs AS (
  -- 1) 중기 재방문 유저의 로그
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
, week_diff_mid AS (
    -- 2) 사용자별 유입주차, 방문주차, 방문간격(주차) 추출
    SELECT
    user_pseudo_id,
    first_week,
    event_week,
    DATE_DIFF(event_week, first_week, WEEK) AS week_diff,
    FROM (
    SELECT DISTINCT
        user_pseudo_id,
        MIN(event_week) OVER (PARTITION BY user_pseudo_id) AS first_week,
        event_week,
    FROM mid_user_logs
    )
)
, week_retain_mid AS (
    -- 3) 유입주차, 방문간격(주차)별 유저 수 카운팅
    SELECT
        first_week,
        week_diff,
        COUNT(DISTINCT user_pseudo_id) AS retain_users
    FROM week_diff_mid
    GROUP BY first_week, week_diff
)


-- 코호트 리텐션 계산
SELECT
    first_week,
    week_diff,
    retain_users,
    first_users,
    ROUND(SAFE_DIVIDE(retain_users,first_users),3) AS retention_rate
FROM (
    SELECT
        first_week,
        week_diff,
        retain_users,
        FIRST_VALUE(retain_users) OVER (PARTITION BY first_week ORDER BY week_diff) AS first_users
    FROM week_retain_mid
)



/* 장기 재방문 유저의 코호트 리텐션 계산 */

WITH long_user_logs AS (
  -- 1) 장기 재방문 유저의 로그
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
, week_diff_long AS (
    -- 2) 사용자별 유입주차, 방문주차, 방문간격(주차) 추출
    SELECT
        user_pseudo_id,
        first_week,
        event_week,
        DATE_DIFF(event_week, first_week, WEEK) AS week_diff,
    FROM (
    SELECT DISTINCT
        user_pseudo_id,
        MIN(event_week) OVER (PARTITION BY user_pseudo_id) AS first_week,
        event_week,
    FROM long_user_logs
    )
)
, week_retain_long AS (
    -- 3) 유입주차, 방문간격(주차)별 유저 수 카운팅
    SELECT
        first_week,
        week_diff,
        COUNT(DISTINCT user_pseudo_id) AS retain_users
    FROM week_diff_long
    GROUP BY first_week, week_diff
)


-- 코호트 리텐션 계산
SELECT
    first_week,
    week_diff,
    retain_users,
    first_users,
    ROUND(SAFE_DIVIDE(retain_users,first_users),3) AS retention_rate
FROM (
    SELECT
        first_week,
        week_diff,
        retain_users,
        FIRST_VALUE(retain_users) OVER (PARTITION BY first_week ORDER BY week_diff) AS first_users
    FROM week_retain_long
)

