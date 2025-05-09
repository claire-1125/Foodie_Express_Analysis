/* 
[ 주문 퍼널 분석 ] 

1. 퍼널 분석을 적용할 그룹 단위

- 그룹 1: 일회성 유저 vs. 재방문 유저
- 그룹 2: 단기 재방문 vs. 중기 재방문 vs. 장기 재방문
- 그룹 3: 재방문 유저 내 연휴 유입 vs. 연휴 외 유입


2. 전제조건
- '오픈 퍼널' 사용 (= 모든 유입 경로 고려)
- 이에 따라 전환율을 '첫 단계' 대비 잔존율로 정의한다.
- 유저 집계 단위: user_pseudo_id
- 매출 데이터는 없으므로 click_payment 이벤트를 주문으로 정의힌다.


3. 정의한 퍼널
1) 방문
- home: screen_view 
- 분석 대상이 모두 '회원'이므로 바로 home: screen_view부터 봐도 무관
- 정합성 검증 결과 실제로 welcome: screen_view, home: screen_view 간의 유저 data leakage 없음 확인 완료.

2) 탐색
- 이 단계에서는 오히려 screen_view 이벤트를 고려하지 않았다! (접속 행위 자체가 탐색이라고 보긴 어려우므로)

2-1) 카테고리 메뉴 타고 들어오는 경우
- home: click_food_category → food_category: screen_view → food_category: click_restaurant → restaurant: screen_view → restaurant: click_food → food_detail: screen_view
2-2) 홈에서 추천 메뉴 클릭해서 들어오는 경우
- home: click_recommend_food → restaurant: screen_view → restaurant: click_food → food_detail: screen_view
2-3) 홈에서 근처 식당 클릭해서 들어오는 경우
- home: click_restaurant_nearby → restaurant: screen_view → restaurant: click_food → food_detail: screen_view
2-4) 키워드 검색해서 들어오는 경우
- home: click_search → search: screen_view → search: request_search → search_result: screen_view → search_result: click_restaurant → restaurant: screen_view → restaurant: click_food → food_detail: screen_view
2-5) 배너 클릭해서 들어오는 경우
- home: click_banner → restaurant: screen_view → restaurant: click_food → food_detail: screen_view


3) 장바구니
- food_detail: click_cart, cart: click_recommend_extra_food

4) 결제
- cart: click_payment

*/


/* 주문 퍼널: 일회성 유저 */

WITH one_day_user_logs AS (
  -- 1) 일회성 유저의 로그
  -- 일회성 유저 전체 인원인 13151명, 분석 대상 로그 수 39114건
  SELECT
    *
  FROM (
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
      WHERE visit_interval_cat = 'one_day'
    )
  )
  WHERE 1=1
  AND screen_event IN (
    'home: screen_view','home: click_food_category', 
    'home: click_recommend_food','home: click_restaurant_nearby','home: click_search', 
    'home: click_banner','food_category: click_restaurant','search: request_search', 
    'search_result: click_restaurant','restaurant: click_food','food_detail: click_cart', 
    'cart: click_recommend_extra_food','cart: click_payment'
  )  -- 퍼널에 사용할 'firebase_screen: event_name' 조합만 추출
)

, one_day_funnel_annot AS (
  -- 2) 퍼널 단계 표시
  -- 분석 대상 로그 수 39114건
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
    screen_event,
    CASE
      WHEN screen_event IN ('home: screen_view') THEN 1  -- 방문
      WHEN screen_event IN ('home: click_food_category','home: click_recommend_food','home: click_restaurant_nearby',
                          'home: click_search','home: click_banner','food_category: click_restaurant',
                          'search: request_search','search_result: click_restaurant','restaurant: click_food') THEN 2  -- 탐색
      WHEN screen_event IN ('food_detail: click_cart','cart: click_recommend_extra_food') THEN 3  -- 장바구니
      WHEN screen_event IN ('cart: click_payment') THEN 4  -- 결제
      ELSE 0  -- 이상치 처리 (해당 케이스 없음)
    END AS funnel_step,
  FROM one_day_user_logs
)
, one_day_tot AS (
  -- 3-1) 전체 인원 따로 계산 (전환율 계산용)
  SELECT COUNT(DISTINCT user_pseudo_id) AS tot_users
  FROM one_day_funnel_annot
)
, one_day_funnel_cnt AS (
  -- 3-2) 각 퍼널 단계 인원 계산
  SELECT
    funnel_step,
    COUNT(DISTINCT user_pseudo_id) AS funnel_users,
  FROM one_day_funnel_annot
  WHERE funnel_step != 0
  GROUP BY funnel_step
  ORDER BY funnel_step
)


-- 4) 전환율 및 최종 결과 추출
-- 주문 퍼널 전환율 (이탈율) 계산: 전체 기간
-- 오픈 퍼널이므로 '첫 단계 대비 전환율' 계산 (↔ 이전 단계 대비 전환율)
SELECT
  'one_day' AS user_segment,
  funnel_step,
  funnel_users,
  tot_users,
  ROUND(SAFE_DIVIDE(funnel_users, tot_users),3) AS conversion_rate
FROM one_day_funnel_cnt
CROSS JOIN one_day_tot
ORDER BY funnel_step ASC




/* 주문 퍼널: 재방문 유저 */

WITH revisit_user_logs AS (
  -- 1) 재방문 유저의 로그
  -- 재방문 유저 전체 인원인 36527명, 분석 대상 로그 수 273441건
  SELECT
    *
  FROM (
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
  WHERE 1=1
  AND screen_event IN (
    'home: screen_view','home: click_food_category', 
    'home: click_recommend_food','home: click_restaurant_nearby','home: click_search', 
    'home: click_banner','food_category: click_restaurant','search: request_search', 
    'search_result: click_restaurant','restaurant: click_food','food_detail: click_cart', 
    'cart: click_recommend_extra_food','cart: click_payment'
  )  -- 퍼널에 사용할 'firebase_screen: event_name' 조합만 추출
)

, revisit_funnel_annot AS (
  -- 2) 퍼널 단계 표시
  -- 분석 대상 로그 수 273441건
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
    screen_event,
    CASE
      WHEN screen_event IN ('home: screen_view') THEN 1  -- 방문
      WHEN screen_event IN ('home: click_food_category','home: click_recommend_food','home: click_restaurant_nearby',
                          'home: click_search','home: click_banner','food_category: click_restaurant',
                          'search: request_search','search_result: click_restaurant','restaurant: click_food') THEN 2  -- 탐색
      WHEN screen_event IN ('food_detail: click_cart','cart: click_recommend_extra_food') THEN 3  -- 장바구니
      WHEN screen_event IN ('cart: click_payment') THEN 4  -- 결제
      ELSE 0  -- 이상치 처리 (해당 케이스 없음)
    END AS funnel_step,
  FROM revisit_user_logs
)
, revisit_tot AS (
  -- 3-1) 전체 인원 따로 계산 (전환율 계산용)
  SELECT COUNT(DISTINCT user_pseudo_id) AS tot_users
  FROM revisit_funnel_annot
)
, revisit_funnel_cnt AS (
  -- 3-2) 각 퍼널 단계 인원 계산
  SELECT
    funnel_step,
    COUNT(DISTINCT user_pseudo_id) AS funnel_users,
  FROM revisit_funnel_annot
  WHERE funnel_step != 0
  GROUP BY funnel_step
  ORDER BY funnel_step
)


-- 4) 전환율 및 최종 결과 추출
-- 주문 퍼널 전환율 (이탈율) 계산: 전체 기간
-- 오픈 퍼널이므로 '첫 단계 대비 전환율' 계산 (↔ 이전 단계 대비 전환율)
SELECT
  'revisit' AS user_segment,
  funnel_step,
  funnel_users,
  tot_users,
  ROUND(SAFE_DIVIDE(funnel_users, tot_users),3) AS conversion_rate
FROM revisit_funnel_cnt
CROSS JOIN revisit_tot
ORDER BY funnel_step ASC




/* 주문 퍼널: 단기 재방문 유저 */

WITH short_user_logs AS (
  -- 1) 단기 재방문 유저의 로그
  -- 단기 재방문 유저 전체 인원인 명, 분석 대상 로그 수 건
  SELECT
    *
  FROM (
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
  WHERE 1=1
  AND screen_event IN (
    'home: screen_view','home: click_food_category', 
    'home: click_recommend_food','home: click_restaurant_nearby','home: click_search', 
    'home: click_banner','food_category: click_restaurant','search: request_search', 
    'search_result: click_restaurant','restaurant: click_food','food_detail: click_cart', 
    'cart: click_recommend_extra_food','cart: click_payment'
  )  -- 퍼널에 사용할 'firebase_screen: event_name' 조합만 추출
)

, short_funnel_annot AS (
  -- 2) 퍼널 단계 표시
  -- 분석 대상 로그 수 건
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
    screen_event,
    CASE
      WHEN screen_event IN ('home: screen_view') THEN 1  -- 방문
      WHEN screen_event IN ('home: click_food_category','home: click_recommend_food','home: click_restaurant_nearby',
                          'home: click_search','home: click_banner','food_category: click_restaurant',
                          'search: request_search','search_result: click_restaurant','restaurant: click_food') THEN 2  -- 탐색
      WHEN screen_event IN ('food_detail: click_cart','cart: click_recommend_extra_food') THEN 3  -- 장바구니
      WHEN screen_event IN ('cart: click_payment') THEN 4  -- 결제
      ELSE 0  -- 이상치 처리 (해당 케이스 없음)
    END AS funnel_step,
  FROM short_user_logs
)
, short_tot AS (
  -- 3-1) 전체 인원 따로 계산 (전환율 계산용)
  SELECT COUNT(DISTINCT user_pseudo_id) AS tot_users
  FROM short_funnel_annot
)
, short_funnel_cnt AS (
  -- 3-2) 각 퍼널 단계 인원 계산
  SELECT
    funnel_step,
    COUNT(DISTINCT user_pseudo_id) AS funnel_users,
  FROM short_funnel_annot
  WHERE funnel_step != 0
  GROUP BY funnel_step
  ORDER BY funnel_step
)


-- 4) 전환율 및 최종 결과 추출
-- 주문 퍼널 전환율 (이탈율) 계산: 전체 기간
-- 오픈 퍼널이므로 '첫 단계 대비 전환율' 계산 (↔ 이전 단계 대비 전환율)
SELECT
  'short' AS user_segment,
  funnel_step,
  funnel_users,
  tot_users,
  ROUND(SAFE_DIVIDE(funnel_users, tot_users),3) AS conversion_rate
FROM short_funnel_cnt
CROSS JOIN short_tot
ORDER BY funnel_step ASC


/* 주문 퍼널: 중기 재방문 유저 */

WITH mid_user_logs AS (
  -- 1) 중기 재방문 유저의 로그
  -- 중기 재방문 유저 전체 인원인 명, 분석 대상 로그 수 건
  SELECT
    *
  FROM (
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
      WHERE visit_interval_cat = 'mid'
    )
  )
  WHERE 1=1
  AND screen_event IN (
    'home: screen_view','home: click_food_category', 
    'home: click_recommend_food','home: click_restaurant_nearby','home: click_search', 
    'home: click_banner','food_category: click_restaurant','search: request_search', 
    'search_result: click_restaurant','restaurant: click_food','food_detail: click_cart', 
    'cart: click_recommend_extra_food','cart: click_payment'
  )  -- 퍼널에 사용할 'firebase_screen: event_name' 조합만 추출
)

, mid_funnel_annot AS (
  -- 2) 퍼널 단계 표시
  -- 분석 대상 로그 수 건
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
    screen_event,
    CASE
      WHEN screen_event IN ('home: screen_view') THEN 1  -- 방문
      WHEN screen_event IN ('home: click_food_category','home: click_recommend_food','home: click_restaurant_nearby',
                          'home: click_search','home: click_banner','food_category: click_restaurant',
                          'search: request_search','search_result: click_restaurant','restaurant: click_food') THEN 2  -- 탐색
      WHEN screen_event IN ('food_detail: click_cart','cart: click_recommend_extra_food') THEN 3  -- 장바구니
      WHEN screen_event IN ('cart: click_payment') THEN 4  -- 결제
      ELSE 0  -- 이상치 처리 (해당 케이스 없음)
    END AS funnel_step,
  FROM mid_user_logs
)
, mid_tot AS (
  -- 3-1) 전체 인원 따로 계산 (전환율 계산용)
  SELECT COUNT(DISTINCT user_pseudo_id) AS tot_users
  FROM mid_funnel_annot
)
, mid_funnel_cnt AS (
  -- 3-2) 각 퍼널 단계 인원 계산
  SELECT
    funnel_step,
    COUNT(DISTINCT user_pseudo_id) AS funnel_users,
  FROM mid_funnel_annot
  WHERE funnel_step != 0
  GROUP BY funnel_step
  ORDER BY funnel_step
)


-- 4) 전환율 및 최종 결과 추출
-- 주문 퍼널 전환율 (이탈율) 계산: 전체 기간
-- 오픈 퍼널이므로 '첫 단계 대비 전환율' 계산 (↔ 이전 단계 대비 전환율)
SELECT
  'mid' AS user_segment,
  funnel_step,
  funnel_users,
  tot_users,
  ROUND(SAFE_DIVIDE(funnel_users, tot_users),3) AS conversion_rate
FROM mid_funnel_cnt
CROSS JOIN mid_tot
ORDER BY funnel_step ASC



/* 주문 퍼널: 장기 재방문 유저 */

WITH long_user_logs AS (
  -- 1) 장기 재방문 유저의 로그
  -- 장기 재방문 유저 전체 인원인 명, 분석 대상 로그 수 건
  SELECT
    *
  FROM (
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
      WHERE visit_interval_cat = 'long'
    )
  )
  WHERE 1=1
  AND screen_event IN (
    'home: screen_view','home: click_food_category', 
    'home: click_recommend_food','home: click_restaurant_nearby','home: click_search', 
    'home: click_banner','food_category: click_restaurant','search: request_search', 
    'search_result: click_restaurant','restaurant: click_food','food_detail: click_cart', 
    'cart: click_recommend_extra_food','cart: click_payment'
  )  -- 퍼널에 사용할 'firebase_screen: event_name' 조합만 추출
)

, long_funnel_annot AS (
  -- 2) 퍼널 단계 표시
  -- 분석 대상 로그 수 건
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
    screen_event,
    CASE
      WHEN screen_event IN ('home: screen_view') THEN 1  -- 방문
      WHEN screen_event IN ('home: click_food_category','home: click_recommend_food','home: click_restaurant_nearby',
                          'home: click_search','home: click_banner','food_category: click_restaurant',
                          'search: request_search','search_result: click_restaurant','restaurant: click_food') THEN 2  -- 탐색
      WHEN screen_event IN ('food_detail: click_cart','cart: click_recommend_extra_food') THEN 3  -- 장바구니
      WHEN screen_event IN ('cart: click_payment') THEN 4  -- 결제
      ELSE 0  -- 이상치 처리 (해당 케이스 없음)
    END AS funnel_step,
  FROM long_user_logs
)
, long_tot AS (
  -- 3-1) 전체 인원 따로 계산 (전환율 계산용)
  SELECT COUNT(DISTINCT user_pseudo_id) AS tot_users
  FROM long_funnel_annot
)
, long_funnel_cnt AS (
  -- 3-2) 각 퍼널 단계 인원 계산
  SELECT
    funnel_step,
    COUNT(DISTINCT user_pseudo_id) AS funnel_users,
  FROM long_funnel_annot
  WHERE funnel_step != 0
  GROUP BY funnel_step
  ORDER BY funnel_step
)


-- 4) 전환율 및 최종 결과 추출
-- 주문 퍼널 전환율 (이탈율) 계산: 전체 기간
-- 오픈 퍼널이므로 '첫 단계 대비 전환율' 계산 (↔ 이전 단계 대비 전환율)
SELECT
  'long' AS user_segment,
  funnel_step,
  funnel_users,
  tot_users,
  ROUND(SAFE_DIVIDE(funnel_users, tot_users),3) AS conversion_rate
FROM long_funnel_cnt
CROSS JOIN long_tot
ORDER BY funnel_step ASC




/* 주문 퍼널: 재방문 + 연휴 유입 vs. 재방문 + 연휴 외 유입 */

WITH dau_list AS (
  SELECT
    event_date,
    COUNT(DISTINCT user_pseudo_id) AS dau,
  FROM advanced.app_logs_cleaned_target
  GROUP BY event_date
)
, order_cnt_list_d AS (
  -- 일일 주문 유저 수
  SELECT
    event_date,
    COUNT(DISTINCT user_id) AS order_users_cnt
  FROM advanced.app_logs_cleaned_target
  WHERE event_name = 'click_payment'
  GROUP BY event_date
)
, dau_vs_order_user AS (
  -- DAU와 일별 주문 유저 수 비교
  SELECT
    d.event_date,
    d.dau,
    o.order_users_cnt,
    ROUND(SAFE_DIVIDE(o.order_users_cnt, d.dau) * 100, 3) AS order_ratio
  FROM dau_list d
  INNER JOIN order_cnt_list_d o ON d.event_date = o.event_date
)
, holiday AS (
  -- 연휴 정의
  SELECT
    event_date AS holiday_date,
    DATE_TRUNC(event_date, WEEK(MONDAY)) AS holiday_week
  FROM dau_vs_order_user
  WHERE order_ratio > 30
)



, revisit_user_logs AS (
  -- 1) 재방문 유저 로그
  SELECT
    *
  FROM advanced.app_logs_cleaned_target
  WHERE 1=1
  AND user_pseudo_id NOT IN (
    SELECT user_pseudo_id
    FROM advanced.app_logs_target_visit_seg
    WHERE visit_interval_cat = 'one_day'
  )
)
, week_diff_revisit_users AS (
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
  FROM revisit_user_logs
  )
)

, revisit_holiday_logs AS (
  -- 재방문 + 연휴 유입 유저 로그
  SELECT
    *
  FROM (
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
    WHERE user_pseudo_id IN (
      SELECT DISTINCT user_pseudo_id
      FROM week_diff_revisit_users
      WHERE first_date IN (SELECT holiday_date FROM holiday)
    )
  )
  WHERE screen_event IN (
    'home: screen_view','home: click_food_category', 
    'home: click_recommend_food','home: click_restaurant_nearby','home: click_search', 
    'home: click_banner','food_category: click_restaurant','search: request_search', 
    'search_result: click_restaurant','restaurant: click_food','food_detail: click_cart', 
    'cart: click_recommend_extra_food','cart: click_payment'
  )  -- 퍼널에 사용할 'firebase_screen: event_name' 조합만 추출
)
, revisit_holiday_funnel_annot AS (
  -- 2) 퍼널 단계 표시
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
    screen_event,
    CASE
      WHEN screen_event IN ('home: screen_view') THEN 1  -- 방문
      WHEN screen_event IN ('home: click_food_category','home: click_recommend_food','home: click_restaurant_nearby',
                          'home: click_search','home: click_banner','food_category: click_restaurant',
                          'search: request_search','search_result: click_restaurant','restaurant: click_food') THEN 2  -- 탐색
      WHEN screen_event IN ('food_detail: click_cart','cart: click_recommend_extra_food') THEN 3  -- 장바구니
      WHEN screen_event IN ('cart: click_payment') THEN 4  -- 결제
      ELSE 0  -- 이상치 처리 (해당 케이스 없음)
    END AS funnel_step,
  FROM revisit_holiday_logs
)
, revisit_holiday_tot AS (
  -- 3-1) 전체 인원 따로 계산 (전환율 계산용)
  SELECT COUNT(DISTINCT user_pseudo_id) AS tot_users
  FROM revisit_holiday_funnel_annot
)
, revisit_holiday_funnel_cnt AS (
  -- 3-2) 각 퍼널 단계 인원 계산
  SELECT
    funnel_step,
    COUNT(DISTINCT user_pseudo_id) AS funnel_users,
  FROM revisit_holiday_funnel_annot
  WHERE funnel_step != 0
  GROUP BY funnel_step
  ORDER BY funnel_step
)

, revisit_normal_day_logs AS (
  -- 재방문 + 연휴 외 유입 유저 로그
  SELECT
    *
  FROM (
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
    WHERE user_pseudo_id IN (
      SELECT DISTINCT user_pseudo_id
      FROM week_diff_revisit_users
      WHERE first_date NOT IN (SELECT holiday_date FROM holiday)
    )
  )
  WHERE screen_event IN (
    'home: screen_view','home: click_food_category', 
    'home: click_recommend_food','home: click_restaurant_nearby','home: click_search', 
    'home: click_banner','food_category: click_restaurant','search: request_search', 
    'search_result: click_restaurant','restaurant: click_food','food_detail: click_cart', 
    'cart: click_recommend_extra_food','cart: click_payment'
  )  -- 퍼널에 사용할 'firebase_screen: event_name' 조합만 추출
)
, revisit_normal_day_funnel_annot AS (
  -- 2) 퍼널 단계 표시
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
    screen_event,
    CASE
      WHEN screen_event IN ('home: screen_view') THEN 1  -- 방문
      WHEN screen_event IN ('home: click_food_category','home: click_recommend_food','home: click_restaurant_nearby',
                          'home: click_search','home: click_banner','food_category: click_restaurant',
                          'search: request_search','search_result: click_restaurant','restaurant: click_food') THEN 2  -- 탐색
      WHEN screen_event IN ('food_detail: click_cart','cart: click_recommend_extra_food') THEN 3  -- 장바구니
      WHEN screen_event IN ('cart: click_payment') THEN 4  -- 결제
      ELSE 0  -- 이상치 처리 (해당 케이스 없음)
    END AS funnel_step,
  FROM revisit_normal_day_logs
)
, revisit_normal_day_tot AS (
  -- 3-1) 전체 인원 따로 계산 (전환율 계산용)
  SELECT COUNT(DISTINCT user_pseudo_id) AS tot_users
  FROM revisit_normal_day_funnel_annot
)
, revisit_normal_day_funnel_cnt AS (
  -- 3-2) 각 퍼널 단계 인원 계산
  SELECT
    funnel_step,
    COUNT(DISTINCT user_pseudo_id) AS funnel_users,
  FROM revisit_normal_day_funnel_annot
  WHERE funnel_step != 0
  GROUP BY funnel_step
  ORDER BY funnel_step
)



-- 4) 전환율 및 최종 결과 추출
-- 주문 퍼널 전환율 (이탈율) 계산: 전체 기간
-- 오픈 퍼널이므로 '첫 단계 대비 전환율' 계산 (↔ 이전 단계 대비 전환율)
SELECT
  'holiday' AS user_segment,
  funnel_step,
  funnel_users,
  tot_users,
  SAFE_DIVIDE(funnel_users, tot_users) AS conversion_rate
FROM revisit_holiday_funnel_cnt
CROSS JOIN revisit_holiday_tot

UNION ALL

SELECT
  'normal_day' AS user_segment,
  funnel_step,
  funnel_users,
  tot_users,
  SAFE_DIVIDE(funnel_users, tot_users) AS conversion_rate
FROM revisit_normal_day_funnel_cnt
CROSS JOIN revisit_normal_day_tot

ORDER BY user_segment, funnel_step

