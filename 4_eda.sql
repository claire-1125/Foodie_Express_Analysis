/*===========================사용자 수==================================*/
WITH dau_list AS (
  -- 단순 DAU: 날짜별 사용자수 (전제조건: 사용자수 = 활성 사용자수)
  SELECT
    event_date,
    COUNT(DISTINCT user_pseudo_id) AS dau,
  FROM advanced.app_logs_cleaned_target
  GROUP BY event_date
)
, dau_avg_median AS (
  -- DAU의 평균값, 중앙값: 703.838명 / 815.0명
  SELECT
    ROUND(AVG(dau) OVER (),3) AS avg_dau,
    PERCENTILE_CONT(dau,0.5) OVER () AS median_dau,
  FROM dau_list
  LIMIT 1
)
, wau_list AS (
  -- 단순 WAU: 주차별 사용자수 (전제조건: 사용자수 = 활성 사용자수)
  SELECT
    event_week,
    COUNT(DISTINCT user_pseudo_id) AS wau,
  FROM advanced.app_logs_cleaned_target
  GROUP BY event_week
  -- ORDER BY event_week
)
, wau_avg_median AS (
  -- WAU의 평균값, 중앙값: 4669.44명 / 5544.0명
  SELECT
    ROUND(AVG(wau) OVER (),3) AS avg_wau,
    PERCENTILE_CONT(wau,0.5) OVER () AS median_wau,
  FROM wau_list
  LIMIT 1
)
, mau_list AS (
  -- 단순 MAU: 월별 사용자수 (전제조건: 사용자수 = 활성 사용자수)
  SELECT
    DATE_TRUNC(event_date, MONTH) AS event_month,
    COUNT(DISTINCT user_pseudo_id) AS mau,
  FROM advanced.app_logs_cleaned_target
  GROUP BY DATE_TRUNC(event_date, MONTH)
  -- ORDER BY event_month
)
, mau_avg_median AS (
  -- MAU의 평균값, 중앙값: 16748.333명 / 17994.5명
  SELECT
    ROUND(AVG(mau) OVER (),3) AS avg_mau,
    PERCENTILE_CONT(mau,0.5) OVER () AS median_mau,
  FROM mau_list
  LIMIT 1
)


/*===========================사용주기==================================*/


-- stickiness 계산: dau/mau
-- 일간 방문자 대비 한 달 안에 재방문하는 비율이 얼마나 될까?
SELECT
  d.event_date,
  -- m.event_month,
  d.dau,
  m.mau,
  ROUND(SAFE_DIVIDE(d.dau, m.mau)*100,4) AS stickiness,
  SUM(d.dau) OVER (PARTITION BY m.event_month) AS sum_of_dau,
FROM mau_list m
CROSS JOIN dau_list d
WHERE DATE_TRUNC(d.event_date, MONTH) = m.event_month
ORDER BY m.event_month, d.event_date;


-- stickiness 계산: dau/wau
-- 일간 방문자 대비 일주일 안에 재방문하는 비율이 얼마나 될까?
SELECT
  d.event_date,
  -- w.event_week,
  w.wau,
  d.dau,
  ROUND(SAFE_DIVIDE(d.dau, w.wau)*100,4) AS stickiness,
  SUM(d.dau) OVER (PARTITION BY w.event_week) AS sum_of_dau,
FROM wau_list w
CROSS JOIN dau_list d
WHERE DATE_TRUNC(d.event_date, WEEK(MONDAY)) = w.event_week
ORDER BY w.event_week, d.event_date;


WITH user_active_sequence AS (
  -- 1. 유저별 활동 일자 시퀀스: 유입 일자, 활동 일자, 직전 활동 일자
  SELECT DISTINCT
    user_pseudo_id,
    event_date,
    LAG(event_date) OVER (PARTITION BY user_pseudo_id ORDER BY event_date) AS prev_event_date,
  FROM advanced.app_logs_cleaned_target
)


-- 2. 유저별 유입일 이후 각 방문 간격 계산
SELECT
  *
FROM (
  SELECT
    user_pseudo_id,
    event_date,
    prev_event_date,
    IFNULL(DATE_DIFF(event_date, prev_event_date, DAY),0) AS day_diff
  FROM user_active_sequence
  WHERE prev_event_date IS NOT NULL  -- 유입일 이전 제외
)
WHERE day_diff != 0 
ORDER BY user_pseudo_id, event_date


-- 주차별 리텐션

-- 2) 사용자별 첫방문일 기준 주 차이 계산
WITH week_diff_per_user AS (
  SELECT
    user_pseudo_id,
    first_week,
    event_week,
    DATE_DIFF(event_week, first_week, WEEK) AS week_diff
  FROM (
    -- 1) 사용자별 첫방문일, 방문일 리스트 추출
    SELECT DISTINCT
      user_pseudo_id,
      MIN(event_week) OVER(PARTITION BY user_pseudo_id) AS first_week,
      event_week
    FROM advanced.app_logs_cleaned_target
  )
)
, week_retain AS (
  -- 3) 주 차이별 이용자수 계산
  SELECT
    week_diff,
    COUNT(DISTINCT user_pseudo_id) AS retain_user
  FROM week_diff_per_user
  GROUP BY week_diff
)
-- 4) 주 차이별 리텐션 비율 계산
, first_week_retain AS (
  SELECT
    COUNT(DISTINCT user_pseudo_id) AS first_week_retain_user
  FROM week_diff_per_user
  WHERE 1=1
  AND week_diff=0
)

SELECT
  week_.week_diff,
  week_.retain_user,
  first_week_.first_week_retain_user,
  ROUND(SAFE_DIVIDE(week_.retain_user, first_week_.first_week_retain_user)*100, 3) AS retention_ratio
FROM week_retain AS week_
CROSS JOIN first_week_retain AS first_week_
ORDER BY week_.week_diff ASC



/*===========================요일, 시간대 분포==================================*/


-- 전체 유저 기준 주로 어느 시간대에 접속했는가?
SELECT
  EXTRACT(HOUR FROM event_time) AS event_hour,
  COUNT(DISTINCT user_pseudo_id) AS user_cnt
FROM advanced.app_logs_cleaned_target
GROUP BY event_hour
ORDER BY event_hour;


-- 전체 유저 기준 주로 어느 요일에 접속했는가?
SELECT
  CASE 
    WHEN event_dow = 1 THEN 'Mon'
    WHEN event_dow = 2 THEN 'Tue'
    WHEN event_dow = 3 THEN 'Wed'
    WHEN event_dow = 4 THEN 'Thu'
    WHEN event_dow = 5 THEN 'Fri'
    WHEN event_dow = 6 THEN 'Sat'
    ELSE 'Sun'
  END AS event_dow_str,
  event_dow,  -- 요일이 숫자로 표시됨. (정렬용)
  COUNT(DISTINCT user_pseudo_id) AS user_cnt
FROM advanced.app_logs_cleaned_target
GROUP BY event_dow
ORDER BY event_dow;


-- 전체 유저 기준 주로 어느 요일+시간에 접속했는가?
SELECT DISTINCT
  FORMAT_DATETIME("%a %Hh", event_datetime) AS event_dow_hour,  -- 요일 + 시간 출력
  RANK() OVER (ORDER BY event_dow, event_hour) AS order_num,
  COUNT(DISTINCT user_pseudo_id) OVER (PARTITION BY event_dow, event_hour) AS user_cnt
FROM (
  SELECT
    event_datetime,
    event_dow,
    EXTRACT(HOUR FROM event_datetime) AS event_hour,
    user_pseudo_id
  FROM advanced.app_logs_cleaned_target
)
ORDER BY order_num


/*===========================세션, 체류시간==================================*/


-- 하루에 사용자들이 평균적으로 몇 번 방문하는가?
-- 일별 유저당 평균 세션 수
SELECT
  event_date,
  ROUND(AVG(session_cnt), 2) AS avg_sessions_per_user
FROM (
  -- 일별 유저별 세션 수
  SELECT
    event_date,
    user_pseudo_id,
    COUNT(DISTINCT session_id) AS session_cnt,
  FROM `advanced.app_logs_cleaned_target`
  GROUP BY event_date, user_pseudo_id
)
GROUP BY event_date
ORDER BY event_date



-- 하루에 한 번 방문할 때 몇 개의 화면을 보는가?
-- 일별 세션당 평균 스크린뷰, 유니크뷰
SELECT
  event_date,
  ROUND(AVG(screen_view_cnt), 2) AS avg_screen_view_per_sess,
  ROUND(AVG(unique_view_cnt), 2) AS avg_unique_view_per_sess,
FROM (
  -- 일별 세션별 스크린뷰, 유니크뷰
  SELECT
    event_date,
    user_pseudo_id,
    session_id,
    COUNT(*) AS screen_view_cnt,
    COUNT(DISTINCT firebase_screen) AS unique_view_cnt,
  FROM `advanced.app_logs_cleaned_target`
  WHERE event_name='screen_view'
  GROUP BY event_date, user_pseudo_id, session_id
)
GROUP BY event_date
ORDER BY event_date



-- 하루에 한 번 방문할 때 화면당 얼마나 머무르는가?
-- 일별 세션당 화면당 평균 체류시간 (firebase_screen별 체류시간)
SELECT
  event_date,
  ROUND(AVG(duration_time),2) AS avg_duration_time_per_screen,
FROM (
  -- 일별 세션당 화면당 체류시간
  SELECT
    event_date,
    user_pseudo_id,
    session_id,
    firebase_screen,
    DATETIME_DIFF(MAX(event_datetime), MIN(event_datetime), SECOND) AS duration_time,
  FROM advanced.app_logs_cleaned_target
  GROUP BY event_date, user_pseudo_id, session_id, firebase_screen
)
GROUP BY event_date
ORDER BY event_date


/*===========================주문==================================*/


WITH order_cnt_list_d AS (
  -- 일일 주문수, 주문 유저 수
  SELECT
    event_date,
    COUNT(*) AS order_cnt,
    COUNT(DISTINCT user_id) AS order_users_cnt
  FROM advanced.app_logs_cleaned_target
  WHERE event_name = 'click_payment'
  GROUP BY event_date
  -- HAVING order_cnt != order_users_cnt  -- 하루에 한 사람이 여러 번 주문한 경우 (거의 한 건 차이)
  ORDER BY event_date
)
-- , order_cnt_list_w AS (
--   -- 주차별 주문 유저 수
--   SELECT
--     event_week,
--     COUNT(DISTINCT user_id) AS order_users_cnt
--   FROM advanced.app_logs_cleaned_target
--   WHERE event_name = 'click_payment'
--   GROUP BY event_week
--   -- ORDER BY event_week
-- )
-- , order_cnt_list_m AS (
--   -- 월별 주문 건수
--   SELECT
--     DATE_TRUNC(event_date, MONTH) AS event_month,
--     COUNT(DISTINCT user_id) AS order_users_cnt
--   FROM advanced.app_logs_cleaned_target
--   WHERE event_name = 'click_payment'
--   GROUP BY DATE_TRUNC(event_date, MONTH)
--   -- ORDER BY event_month
-- )


-- DAU와 일간 주문 수 비교
-- 접속한 사람에 비해 주문까지 한 사람은 얼마나 될까?
SELECT
  d.event_date,
  d.dau,
  o.order_users_cnt,
  ROUND(SAFE_DIVIDE(o.order_users_cnt, d.dau) * 100, 3) AS order_ratio
FROM dau_list d
INNER JOIN order_cnt_list_d o ON d.event_date = o.event_date
ORDER BY d.event_date ASC;


-- -- WAU와 주간 주문 수 비교
-- -- 접속한 사람에 비해 주문까지 한 사람은 얼마나 될까?
-- SELECT
--   w.event_week,
--   w.wau,
--   o.order_users_cnt,
--   ROUND(SAFE_DIVIDE(o.order_users_cnt, w.wau) * 100, 3) AS order_ratio  -- 주문율
-- FROM wau_list w
-- INNER JOIN order_cnt_list_w o ON w.event_week = o.event_week
-- ORDER BY w.event_week ASC




/*===========================주문 퍼널==================================*/

-- /* 
--   < 일별 주문 퍼널 구하기 >

--   1. 퍼널 정의
--   - 접속~결제하기 내 여러 가지 갈래 중 '카테고리 메뉴 타고 들어오는 경우'를 고려해봄
--   - 간단하게 알아보기 위해 먼저 스크린뷰 집계를 진행함.
  

--   2. 전제조건
--   - 오픈 퍼널 사용
--   - user_pseudo_id로 집계 
--   - 각 이벤트를 하나의 퍼널로 본다.
--  */


-- WITH base AS (
--   -- 전체 세션 리스트 
--   SELECT
--     EXTRACT(DATE FROM TIMESTAMP_MICROS(event_timestamp)) AS event_date,
--     TIMESTAMP_MICROS(event_timestamp) AS event_datetime,
--     user_pseudo_id,
--     user_id,
--     event_name, 
--     MAX(IF(params.key='firebase_screen', params.value.string_value, NULL)) AS `firebase_screen`, 
--     MAX(IF(params.key='session_id', params.value.string_value, NULL)) AS `session_id`,
--     -- platform,
--   FROM advanced.app_logs
--   CROSS JOIN UNNEST(event_params) AS params
--   GROUP BY ALL
-- )
-- , base_filtered AS (
--   SELECT
--     event_date,
--     event_datetime,
--     user_pseudo_id,
--     CONCAT(firebase_screen, ': ', event_name) AS screen_event
--   FROM base
--   WHERE 1=1
--   AND event_name IN ('click_payment','screen_view')
--   AND firebase_screen IN ('welcome','home','food_category','restaurant','food_detail','cart')
-- )
-- , funnel_origin AS (
--   SELECT
--     event_date,
--     screen_event,
--     CASE WHEN screen_event='welcome: screen_view' THEN 1
--         WHEN screen_event='home: screen_view' THEN 2
--         WHEN screen_event='food_category: screen_view' THEN 3
--         WHEN screen_event='restaurant: screen_view' THEN 4
--         WHEN screen_event='food_detail: screen_view' THEN 5
--         WHEN screen_event='cart: screen_view' THEN 6
--         WHEN screen_event='cart: click_payment' THEN 7
--         ELSE 0
--     END AS funnel_step,
--     COUNT(DISTINCT user_pseudo_id) AS user_cnt
--   FROM base_filtered
--   GROUP BY ALL
-- )
-- , dau_list AS (
--   -- 일별로 퍼널과 DAU를 대조하기 위함.
--   SELECT
--     EXTRACT(DATE FROM TIMESTAMP_MICROS(event_timestamp)) AS event_date,
--     COUNT(DISTINCT user_pseudo_id) AS dau,
--   FROM advanced.app_logs
--   GROUP BY EXTRACT(DATE FROM TIMESTAMP_MICROS(event_timestamp))
-- )


-- -- 일별 주문 퍼널 전환율/이탈율 계산
-- -- 22-07-31의 경우 click_payment 이벤트가 발생하지 않음.
-- SELECT
--   f.event_date,
--   f.screen_event,
--   f.funnel_step,
--   f.user_cnt,
--   d.dau,
--   ROUND(SAFE_DIVIDE(f.user_cnt, d.dau)*100, 3) AS convertion_rate,  -- 전환율
--   ROUND((1-SAFE_DIVIDE(f.user_cnt, d.dau))*100, 3) AS churn_rate  -- 이탈율 (=1-전환률)
-- FROM funnel_origin f
-- CROSS JOIN dau_list d
-- WHERE 1=1
-- AND f.event_date = d.event_date
-- AND f.event_date != '2022-07-31'
-- ORDER BY event_date, funnel_step


/*===========================================================*/

/* 
  < 주차별 주문 퍼널 구하기 >

  1. 퍼널 정의
  - 접속~결제하기 내 여러 가지 갈래 중 '카테고리 메뉴 타고 들어오는 경우'를 고려해봄
  - 간단하게 알아보기 위해 먼저 스크린뷰 집계를 진행함.
  

  2. 전제조건
  - 오픈 퍼널 사용
  - user_pseudo_id로 집계 
  - 각 이벤트를 하나의 퍼널로 본다.
 */


WITH base AS (
  -- 전체 세션 리스트 
  SELECT
    EXTRACT(DATE FROM TIMESTAMP_MICROS(event_timestamp)) AS event_date,
    TIMESTAMP_MICROS(event_timestamp) AS event_datetime,
    user_pseudo_id,
    user_id,
    event_name, 
    MAX(IF(params.key='firebase_screen', params.value.string_value, NULL)) AS `firebase_screen`, 
    MAX(IF(params.key='session_id', params.value.string_value, NULL)) AS `session_id`,
    -- platform,
  FROM advanced.app_logs
  CROSS JOIN UNNEST(event_params) AS params
  GROUP BY ALL
)
, base_filtered AS (
  SELECT
    DATE_TRUNC(event_date, WEEK(MONDAY)) AS event_week,
    event_date,
    event_datetime,
    user_pseudo_id,
    CONCAT(firebase_screen, ': ', event_name) AS screen_event
  FROM base
  WHERE 1=1
  AND event_name IN ('click_payment','screen_view')
  AND firebase_screen IN ('welcome','home','food_category','restaurant','food_detail','cart')
)
, funnel_origin AS (
  SELECT
    event_week,
    screen_event,
    CASE WHEN screen_event='welcome: screen_view' THEN 1
        WHEN screen_event='home: screen_view' THEN 2
        WHEN screen_event='food_category: screen_view' THEN 3
        WHEN screen_event='restaurant: screen_view' THEN 4
        WHEN screen_event='food_detail: screen_view' THEN 5
        WHEN screen_event='cart: screen_view' THEN 6
        WHEN screen_event='cart: click_payment' THEN 7
        ELSE 0
    END AS funnel_step,
    COUNT(DISTINCT user_pseudo_id) AS user_cnt
  FROM base_filtered
  GROUP BY ALL
)
, wau_list AS (
  -- 주차별로 퍼널과 WAU를 대조하기 위함.
  SELECT
    DATE_TRUNC(EXTRACT(DATE FROM TIMESTAMP_MICROS(event_timestamp)), WEEK(MONDAY)) AS event_week,
    COUNT(DISTINCT user_pseudo_id) AS wau,
  FROM advanced.app_logs
  GROUP BY DATE_TRUNC(EXTRACT(DATE FROM TIMESTAMP_MICROS(event_timestamp)), WEEK(MONDAY))
)


-- 주차별 주문 퍼널 전환율/이탈율 계산
-- 22-07-25 주차의 경우 click_payment 이벤트가 발생하지 않음.
SELECT
  f.event_week,
  f.screen_event,
  f.funnel_step,
  f.user_cnt,
  w.wau,
  ROUND(SAFE_DIVIDE(f.user_cnt, w.wau)*100, 3) AS convertion_rate,  -- 전환율
  ROUND((1-SAFE_DIVIDE(f.user_cnt, w.wau))*100, 3) AS churn_rate  -- 이탈율 (=1-전환률)
FROM funnel_origin f
CROSS JOIN wau_list w
WHERE 1=1
AND f.event_week = w.event_week
AND f.event_week != '2022-07-25'
ORDER BY event_week, funnel_step
