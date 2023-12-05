# Databricks notebook source
# MAGIC %md
# MAGIC # Hae lounasbotin käyttäjien arviot MiniPossusta

# COMMAND ----------

# MAGIC %md
# MAGIC ### JDBC connection string MiniPossuun
# MAGIC
# MAGIC MiniPossun ylläpitäjä: Juuso Paakkunainen (juuso.paakkunainen@productivityleap.com) (5.12.2023)

# COMMAND ----------

driver = "org.postgresql.Driver"

database_host = "ec2-18-203-205-71.eu-west-1.compute.amazonaws.com"
database_port = "5432"
database_name = "d9j5rds7v76as3"
table = "public.user_reactions"
user = "cezsfrpwjbrrxo"
password = "107c5f821bb4f1701e3d6d76c8cba3eca000ab6fee6d4f68df562e418528bd09"

url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Apumuuttujia
# MAGIC - **source_table_name:** Väliaikaisen näkymän nimi, näkymä lähdejärjestelmän taulusta.
# MAGIC - **target_Table_name:** Kohdetaulun catalog.skeema.taulu määrittely. Haettu data tallennetaan tällä nimellä.

# COMMAND ----------

source_table_name = "user_reviews"
target_table_name = "pl_lounas_bot.user_reviews.user_reviews_bronze"
spark.conf.set("user_reviews.target_table_name", target_table_name)
spark.conf.set("user_reviews.source_table_name", source_table_name)

# COMMAND ----------

remote_table = (spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("dbtable", table)
    .option("user", user)
    .option("password", password)
    .load()
)

# Luo väliaikainen näkymä lähdejärjestelmän user_reviews taulusta
remote_table.createOrReplaceTempView('user_reviews_src')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Suorita lähdejärjestelmän tietojen merge Bronze-tauluun.
# MAGIC Seuraava SQL
# MAGIC 1. muodostaa väliaikaisen näkymän lähdejärjestelmän taulusta,
# MAGIC 2. luo Bronze-taulun mikäli sitä ei ole olemassa ja
# MAGIC 3. Suorittaa mergen lähdejärjestelmän näkymästä Bronze-tauluun, tallettaen *jokaisen* uuden ja muuttuneen lounasarvion **uutena rivinä**.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW ${user_reviews.source_table_name} AS 
# MAGIC SELECT * FROM user_reviews_src;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${user_reviews.target_table_name} (
# MAGIC   id INT
# MAGIC   ,user_id BIGINT
# MAGIC   ,message_id BIGINT
# MAGIC   ,reaction_type STRING
# MAGIC   ,created_at TIMESTAMP
# MAGIC   ,restaurant_name STRING
# MAGIC );
# MAGIC
# MAGIC MERGE INTO ${user_reviews.target_table_name} trg
# MAGIC USING ${user_reviews.source_table_name} src
# MAGIC ON src.id = trg.id AND src.created_at = trg.created_at
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT *;
# MAGIC
# MAGIC

# COMMAND ----------

spark.conf.unset("user_reviews.target_table_name")
spark.conf.unset("user_reviews.source_table_name")
