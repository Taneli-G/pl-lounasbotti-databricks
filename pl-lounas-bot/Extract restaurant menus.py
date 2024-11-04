# Databricks notebook source
# MAGIC %md
# MAGIC # Hae lounasbotin lounaslistat MiniPossusta

# COMMAND ----------

# MAGIC %md
# MAGIC ### JDBC connection string MiniPossuun
# MAGIC
# MAGIC MiniPossun ylläpitäjä: Juuso Paakkunainen (juuso.paakkunainen@productivityleap.com) (29.1.2024)

# COMMAND ----------

driver = "org.postgresql.Driver"

database_host = dbutils.widgets.get("database_host")
database_port = dbutils.widgets.get("database_port")
database_name = dbutils.widgets.get("database_name")
table = "public.menus"
user = dbutils.widgets.get("user")
password = dbutils.widgets.get("password")

url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"

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
remote_table.createOrReplaceTempView('menus_src')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apumuuttujia
# MAGIC - **source_table_name:** Väliaikaisen näkymän nimi, näkymä lähdejärjestelmän taulusta.
# MAGIC - **target_Table_name:** Kohdetaulun catalog.skeema.taulu määrittely. Haettu data tallennetaan tällä nimellä.

# COMMAND ----------

#source_table_name = "menus"
#target_table_name = "pl_lounas_bot.menus.menus_bronze"

dbutils.widgets.text("menus.source_table_name", "menus")
dbutils.widgets.text("menus.target_table_name", "pl_lounas_bot.menus.menus_bronze")
#spark.conf.set("menus.target_table_name", target_table_name)
#spark.conf.set("menus.source_table_name", source_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Suorita lähdejärjestelmän tietojen merge Bronze-tauluun.
# MAGIC Seuraava SQL
# MAGIC 1. muodostaa väliaikaisen näkymän lähdejärjestelmän taulusta,
# MAGIC 2. luo Bronze-taulun mikäli sitä ei ole olemassa ja
# MAGIC 3. Suorittaa mergen lähdejärjestelmän näkymästä Bronze-tauluun, tallettaen *jokaisen* uuden ja muuttuneen ravintolan ruokalistan **uutena rivinä**.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW IDENTIFIER(:menus.source_table_name) AS 
# MAGIC SELECT * FROM menus_src;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS IDENTIFIER(:menus.target_table_name) (
# MAGIC   id INT
# MAGIC   ,date DATE
# MAGIC   ,menu_raw STRING
# MAGIC   ,restaurant_name STRING
# MAGIC   ,timestamp TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC MERGE INTO IDENTIFIER(:menus.target_table_name) trg
# MAGIC USING IDENTIFIER(:menus.source_table_name) src
# MAGIC ON src.id = trg.id AND src.timestamp = trg.timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *;

# COMMAND ----------

dbutils.widgets.removeAll()
#spark.conf.unset("user_reviews.target_table_name")
#spark.conf.unset("user_reviews.source_table_name")
