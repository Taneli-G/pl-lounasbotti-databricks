-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Lounasbotin käyttäjien lounasarvioiden DLT-pipeline

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### USER_REVIEWS_BRONZE_CLEAN
-- MAGIC Siivoa bronze-taulun lounasarviot ja luo DLT-streaming taulu.
-- MAGIC **user_reviews_bronze_clean** sisältää ainoastaan valideja käyttäjäarvioita.

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
SELECT * FROM STREAM(pl_lounas_bot.user_reviews.user_reviews_bronze);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### USER_REVIEWS_SILVER
-- MAGIC #### SCD Type 1
-- MAGIC Seuraava streaming-DLT taulu saa tietonsa user_reviews_bronze_clean -taulusta ja poimii sieltä ainoastaan viimeisimmällä timestampilla olevat tietueet per arvio-id. 

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE user_reviews_silver;

APPLY CHANGES INTO LIVE.user_reviews_silver
  FROM STREAM(LIVE.user_reviews_bronze_clean)
  KEYS (id)
  APPLY AS DELETE WHEN 1=2
  SEQUENCE BY created_at
