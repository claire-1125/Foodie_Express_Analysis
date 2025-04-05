CREATE OR REPLACE PROCEDURE `inflearn-bigquery-439112.advanced.app_logs_analysis_target`()
BEGIN

CREATE OR REPLACE TABLE advanced.app_logs_cleaned_target 
PARTITION BY event_date
AS

  WITH user_mapping_list AS (
    SELECT DISTINCT
      user_id,
      user_pseudo_id
    FROM advanced.app_logs_cleaned
  )
  , user_mapping_annot AS (
    SELECT 
      user_id,
      user_pseudo_id,
      CASE 
        WHEN user_id IS NULL THEN 'anonymous_user'
        ELSE 'identified_user' 
      END AS user_type,
    FROM user_mapping_list
  )
  , identified_device AS (
    SELECT
      DISTINCT user_pseudo_id
    FROM user_mapping_annot
    WHERE user_type = 'identified_user'
  )

  , anonymous_device AS (
    SELECT
      DISTINCT user_pseudo_id
    FROM user_mapping_annot
    WHERE user_type = 'anonymous_user'
  )

  , i_a_both AS (
    -- 49678건
    SELECT
      user_pseudo_id
    FROM anonymous_device
    WHERE user_pseudo_id IN (SELECT user_pseudo_id FROM identified_device)
  )
  , i_only AS (
    -- NOT IN vs. NOT EXISTS vs. EXCEPT DISTINCT
    -- https://williamwibowo.medium.com/bigquery-not-in-vs-not-exists-vs-except-distinct-understand-the-differences-in-3-minutes-e66d2159f744
    -- 0건
    SELECT
      user_pseudo_id
    FROM identified_device
    WHERE user_pseudo_id NOT IN (SELECT user_pseudo_id FROM anonymous_device)
  )
  , a_only AS (
    -- 비회원 3145건
    SELECT
      user_pseudo_id
    FROM anonymous_device
    WHERE user_pseudo_id NOT IN (SELECT user_pseudo_id FROM identified_device)
  )


  -- -- 정합성 검증
  -- SELECT DISTINCT
  --   user_id,
  --   user_pseudo_id
  -- FROM advanced.app_logs_cleaned
  -- WHERE user_pseudo_id NOT IN (
  --   SELECT user_pseudo_id
  --   FROM i_a_both

  --   UNION DISTINCT

  --   SELECT user_pseudo_id
  --   FROM i_only

  --   UNION DISTINCT

  --   SELECT user_pseudo_id
  --   FROM a_only
  -- )



  -- -- 비회원의 행동?
  -- SELECT DISTINCT
  --   firebase_screen,
  --   event_name
  -- FROM `advanced.app_logs_cleaned`
  -- WHERE user_pseudo_id IN (SELECT user_pseudo_id FROM a_only)


  -- 비회원 제외하고 분석
  SELECT
    *
  FROM `advanced.app_logs_cleaned`
  WHERE user_pseudo_id NOT IN (SELECT user_pseudo_id FROM a_only);
  -- PARTITION BY event_date


END;