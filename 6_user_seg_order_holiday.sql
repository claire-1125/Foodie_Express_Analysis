CREATE OR REPLACE PROCEDURE `inflearn-bigquery-439112.advanced.app_logs_target_segment_order_holiday`()

BEGIN

CREATE OR REPLACE TABLE advanced.app_logs_target_order_seg AS

/* DAU + 주문 유저 수 */

WITH dau_list AS (
  SELECT
    event_date,
    COUNT(DISTINCT user_pseudo_id) AS dau,
  FROM advanced.app_logs_cleaned_target
  GROUP BY event_date
)
, order_cnt_list_d AS (
  -- 일별 주문 유저 수
  SELECT
    event_date,
    COUNT(DISTINCT user_id) AS order_users_cnt
  FROM advanced.app_logs_cleaned_target
  WHERE event_name = 'click_payment'
  GROUP BY event_date
  ORDER BY event_date
)
, dau_vs_order_user AS (
  -- DAU와 일별 주문 유저 수 비교
  -- 주문율 = 일일 방문자 수 대비 일일 주문자 수
  SELECT
    d.event_date,
    d.dau,
    o.order_users_cnt,
    ROUND(SAFE_DIVIDE(o.order_users_cnt, d.dau) * 100, 3) AS order_ratio  -- 주문율
  FROM dau_list d
  INNER JOIN order_cnt_list_d o ON d.event_date = o.event_date
)
, holiday AS (
  -- 연휴 정의: 주문율 30% 초과
  SELECT
    event_date
  FROM dau_vs_order_user
  WHERE order_ratio > 30
)


/* 
전체 유저를 주문 여부에 따라 구분하고 다음을 계산했다. 

- 방문일
- 주문일 (= 비연휴 주문일 + 연휴 주문일)
- 비연휴 주문일: 전체 주문일 중 비연휴에 주문한 일자 수
- 연휴 주문일: 전체 주문일 중 연휴에 주문한 일자 수
*/


-- 주문한 적 있는 유저 11467명
SELECT
  'ordered'AS user_segment,
  user_pseudo_id, 
  COUNT(DISTINCT event_date) AS visit_day_cnt,
  COUNT(DISTINCT IF(event_name='click_payment', event_date, NULL)) AS order_day_cnt,
  COUNT(DISTINCT IF((event_name='click_payment') AND (event_date NOT IN (SELECT * FROM holiday)), event_date, NULL)) AS order_normal_cnt,
  COUNT(DISTINCT IF((event_name='click_payment') AND (event_date IN (SELECT * FROM holiday)), event_date, NULL)) AS order_holiday_cnt,
FROM advanced.app_logs_cleaned_target
WHERE user_pseudo_id IN (
  SELECT DISTINCT
    user_pseudo_id  
  FROM advanced.app_logs_cleaned_target
  WHERE event_name = 'click_payment'
)
GROUP BY user_pseudo_id

UNION DISTINCT

-- 주문한 적 없는 유저 38211명
SELECT
  'non_ordered'AS user_segment,
  user_pseudo_id,
  COUNT(DISTINCT event_date) AS visit_day_cnt,
  0 AS order_day_cnt,
  0 AS order_normal_cnt,
  0 AS order_holiday_cnt,
FROM advanced.app_logs_cleaned_target
WHERE user_pseudo_id NOT IN (
  SELECT DISTINCT
    user_pseudo_id  
  FROM advanced.app_logs_cleaned_target
  WHERE event_name = 'click_payment'
)
GROUP BY user_pseudo_id;


END