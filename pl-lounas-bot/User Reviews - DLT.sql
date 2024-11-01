-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Lounasbotin käyttäjien lounasarvioiden DLT-pipeline

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### USER_REVIEWS_BRONZE_CLEAN
-- MAGIC - **Source:** user_reviews_bronze
-- MAGIC
-- MAGIC Siivoa ja validoi bronze-taulun lounasarviot.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE user_reviews_bronze_clean
(
  CONSTRAINT valid_id EXPECT (
    id IS NOT NULL
  ) ON VIOLATION DROP ROW
  ,CONSTRAINT valid_info EXPECT (
    user_id IS NOT NULL 
    AND message_id IS NOT NULL
    AND created_at IS NOT NULL
    AND restaurant_name IS NOT NULL
  ) ON VIOLATION DROP ROW
  ,CONSTRAINT valid_review EXPECT(
    reaction_type IN('neutral','like','dislike')
  ) ON VIOLATION DROP ROW
) AS
SELECT 
  id
  ,user_id
  ,message_id
  ,reaction_type
  ,created_at
  ,restaurant_name
FROM STREAM(pl_lounas_bot.user_reviews.user_reviews_bronze);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### USER_REVIEWS_SILVER
-- MAGIC
-- MAGIC - **Source**: user_reviews_bronze_clean
-- MAGIC - **SCD:** Type 1
-- MAGIC
-- MAGIC Hae tiedot user_reviews_bronze_clean -taulusta ja poimii sieltä ainoastaan viimeisimmällä timestampilla olevat tietueet per arvio-id. 

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE user_reviews_silver;

APPLY CHANGES INTO LIVE.user_reviews_silver
  FROM STREAM(LIVE.user_reviews_bronze_clean)
  KEYS (id)
  APPLY AS DELETE WHEN 1=2
  SEQUENCE BY created_at

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### USER_REVIEWS_GOLD
-- MAGIC - **Source:** USER_REVIEWS_SILVER
-- MAGIC
-- MAGIC Käytännössä samat tiedot kuin USER_REVIEWS_SILVER, mutta gold-tauluna mahdollisia tulevia tarpeita varten.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE user_reviews_gold AS
SELECT
  id
  ,user_id
  ,message_id
  ,reaction_type
  ,created_at
  ,restaurant_name
FROM
  LIVE.user_reviews_silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### USER_REVIEWS_STATS_DAILY_GOLD
-- MAGIC - **Source:** user_reviews_silver
-- MAGIC
-- MAGIC Ravintoloiden arvioiden statistiikkaa per päivä. Ravintola yksilöitynä.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE user_reviews_stats_daily_gold AS
SELECT
  restaurant_name
  ,to_date(created_at) AS date
  ,year(created_at) AS year
  ,month(created_at) AS month
  ,day(created_at) AS day
  ,count_if(reaction_type LIKE 'like') as positive_reviews
  ,count_if(reaction_type LIKE 'neutral') as neutral_reviews
  ,count_if(reaction_type LIKE 'dislike') as negative_reviews
  ,count(reaction_type) AS total_reviews
FROM LIVE.user_reviews_silver
GROUP BY
  restaurant_name
  ,date

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### USER_REVIEWS_RESTAURANT_STATS_MONTHLY_GOLD
-- MAGIC - **Source:** user_reviews_silver
-- MAGIC
-- MAGIC Ravintoloiden edistyneempää statistiikkaa per kuukausi. Ravintola yksilöitynä.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE user_reviews_restaurant_stats_monthly_gold AS
SELECT
  restaurant_name
  ,year(created_at) AS year
  ,month(created_at) AS month
  ,count_if(reaction_type LIKE 'like') as positive_reviews
  ,count_if(reaction_type LIKE 'neutral') as neutral_reviews
  ,count_if(reaction_type LIKE 'dislike') as negative_reviews
  ,count(reaction_type) AS total_reviews
  ,ROUND((positive_reviews / total_reviews) * 100, 3) AS positives_of_total
  ,ROUND((neutral_reviews / total_reviews) * 100, 3) AS neutrals_of_total
  ,ROUND((negative_reviews / total_reviews) * 100, 3) AS negatives_of_total
FROM LIVE.user_reviews_silver silver
GROUP BY
  restaurant_name
  ,year
  ,month
ORDER BY
  year
  ,month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### USER_REVIEWS_RESTAURANT_STATS_YEARLY_GOLD
-- MAGIC - **Source:** user_reviews_silver
-- MAGIC
-- MAGIC Ravintoloiden edistyneempää statistiikkaa per vuosi. Ravintola yksilöitynä.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE user_reviews_restaurant_stats_yearly_gold AS
SELECT
  restaurant_name
  ,year(created_at) AS year
  ,count_if(reaction_type LIKE 'like') as positive_reviews
  ,count_if(reaction_type LIKE 'neutral') as neutral_reviews
  ,count_if(reaction_type LIKE 'dislike') as negative_reviews
  ,count(reaction_type) AS total_reviews
  ,ROUND((positive_reviews / total_reviews) * 100, 3) AS positives_of_total
  ,ROUND((neutral_reviews / total_reviews) * 100, 3) AS neutrals_of_total
  ,ROUND((negative_reviews / total_reviews) * 100, 3) AS negatives_of_total
FROM LIVE.user_reviews_silver silver
GROUP BY
  restaurant_name
  ,year
ORDER BY
  year

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### USER_REVIEWS_TOTAL_MONTHLY_GOLD
-- MAGIC - **Source:** user_reviews_silver
-- MAGIC
-- MAGIC Ravintoloiden edistyneempää statistiikkaa per kuukausi. Kaikki ravintolat yhdessä.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE user_reviews_total_monthly_gold AS
SELECT
  year(created_at) AS year
  ,month(created_at) AS month
  ,count_if(reaction_type LIKE 'like') as positive_reviews
  ,count_if(reaction_type LIKE 'neutral') as neutral_reviews
  ,count_if(reaction_type LIKE 'dislike') as negative_reviews
  ,count(reaction_type) AS total_reviews
  ,ROUND((positive_reviews / total_reviews) * 100, 3) AS positives_of_total
  ,ROUND((neutral_reviews / total_reviews) * 100, 3) AS neutrals_of_total
  ,ROUND((negative_reviews / total_reviews) * 100, 3) AS negatives_of_total
FROM LIVE.user_reviews_silver
GROUP BY
  year
  ,month
ORDER BY
  year
  ,month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### USER_REVIEWS_TOTAL_YEARLY_GOLD
-- MAGIC - **Source:** user_reviews_silver
-- MAGIC
-- MAGIC Ravintoloiden edistyneempää statistiikkaa per vuosi. Kaikki ravintolat yhdessä.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE user_reviews_total_yearly_gold AS
SELECT
  year(created_at) AS year
  ,count_if(reaction_type LIKE 'like') as positive_reviews
  ,count_if(reaction_type LIKE 'neutral') as neutral_reviews
  ,count_if(reaction_type LIKE 'dislike') as negative_reviews
  ,count(reaction_type) AS total_reviews
  ,ROUND((positive_reviews / total_reviews * 100), 3) AS positives_of_total
  ,ROUND((neutral_reviews / total_reviews) * 100, 3) AS neutrals_of_total
  ,ROUND((negative_reviews / total_reviews) * 100, 3) AS negatives_of_total
FROM LIVE.user_reviews_silver
GROUP BY
  year
ORDER BY
  year;
