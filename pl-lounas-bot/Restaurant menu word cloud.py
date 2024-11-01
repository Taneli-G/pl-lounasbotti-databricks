# Databricks notebook source
# MAGIC %md
# MAGIC # Ruokalistojen sanapilvi

# COMMAND ----------

# imports, should be installed cluster-level

import pandas as pd
import numpy as np
import pyspark.pandas as ps
import spacy
import os
import re
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from ctypes import CDLL
from libvoikko import Voikko

# COMMAND ----------

DIRECTORY = "/Workspace/Repos/taneli.grohn@productivityleap.com/pl-lounasbotti-databricks/pl-lounas-bot/voikko/"

CDLL("/Workspace/Repos/taneli.grohn@productivityleap.com/pl-lounasbotti-databricks/pl-lounas-bot/voikko/libarchive.so.13")
CDLL("/Workspace/Repos/taneli.grohn@productivityleap.com/pl-lounasbotti-databricks/pl-lounas-bot/voikko/libhfstospell.so.11")

# Setup libvoikko to load native libvoikko.so.1 from voikko directory
Voikko.setLibrarySearchPath(
    DIRECTORY
)
# Setup libvoikko1.so.1 to load dictionary files from voikko
os.environ["VOIKKO_DICTIONARY_PATH"] = (
    "/Workspace/Repos/taneli.grohn@productivityleap.com/pl-lounasbotti-databricks/pl-lounas-bot/voikko/"
)



# COMMAND ----------

# test that Voikko is working

nlp = spacy.load('spacy_fi_experimental_web_md')

doc = nlp('Hän ajoi punaisella autolla.')
for t in doc:
    print(f'{t.lemma_}\t{t.pos_}')

# COMMAND ----------

# MAGIC %md
# MAGIC Valitaan lounaslistoilta vain listat, jotka sisältävät dataa ja jolloin ravintola ei ole ollut suljettu.

# COMMAND ----------

# all menus 

menus_df = spark.sql("""
    SELECT * 
    FROM pl_lounas_bot.menus.menus_bronze 
    WHERE menu_raw IS NOT NULL 
    AND menu_raw NOT LIKE '%Lounaslistaa ei löytynyt tälle päivälle%' 
    AND menu_raw NOT LIKE '%SULJETTU%'
    AND menu_raw NOT LIKE ''
""").toPandas()
menus_df

# COMMAND ----------

# all user reviews

reviews_df = spark.sql("SELECT * FROM pl_lounas_bot.menus.menus_bronze m LEFT JOIN pl_lounas_bot.user_reviews.user_reviews_bronze r ON DATE(r.created_at) = m.`date` WHERE r.reaction_type is not null ORDER BY timestamp DESC").toPandas()
reviews_df

# COMMAND ----------

SULO_STOP_WORDS = ['sulo', 'brgr', 'burger', 'salaattipöytä']

def clean_words(words):
    """
    Cleans a list of words from non-alphabetic characters
    """
    pattern = r"[^a-zA-ZäÄöÖåÅ\d]"
    cleaned_words = [
        re.sub(pattern, "", word)
        for word in words
        if word.strip() and word.strip() != ","
    ]
    cleaned_words = [word.removesuffix(",") for word in cleaned_words if word.strip()]
    return cleaned_words


def array_to_string(array):
    """
    Converts an array to a comma-separated string
    """
    return ",".join(array)


def tokenize_and_lemmatize(nlp, sentence):
    """
    Tokenizes and lemmatizes a sentence
    """
    doc = nlp(sentence)
    lemma_array = [t.lemma_.lower() for t in doc if 
                   not t.is_stop and 
                   not t.is_punct and 
                   not t.is_space and 
                   not t.is_digit]
    return lemma_array

# COMMAND ----------

# drop empty rows
menus_df.dropna(inplace=True)
menus_df.info()

# COMMAND ----------

# remove links
x = menus_df["menu_raw"]
x_clnd_link = [re.sub(r"http\S+", "", text) for text in x]
print(x_clnd_link[0])

# COMMAND ----------

# remove special characters and numbers

pattern = r"\s*\([^)]*\)"
x_cleaned = [re.sub(pattern," ",text) for text in x_clnd_link]
pattern = "[^a-zA-ZäÄöÖåÅ\d]"
x_cleaned = [re.sub(pattern," ",text) for text in x_cleaned]
print(x_cleaned[0])


# COMMAND ----------

# all text to lower case
x_lowered = [text.lower() for text in x_cleaned]
print(x_lowered[0])

# COMMAND ----------

# tokenize and lemmatize the text
x_tokenized = [tokenize_and_lemmatize(nlp, text) for text in x_lowered]
print(x_tokenized[0])

# COMMAND ----------

# remove empty words

cleaned_x_tokenized = [[word for word in sentence if word.strip()] for sentence in x_tokenized]
print(cleaned_x_tokenized[0])

# COMMAND ----------

# remove sulo stopwords

cleaned_x_tokenized = [[word for word in sentence if word not in SULO_STOP_WORDS] for sentence in x_tokenized]

print(cleaned_x_tokenized[0])

# COMMAND ----------

# count on unique words in the data
len(np.unique([word for text in cleaned_x_tokenized for word in text]))

# COMMAND ----------

# Create a new DataFrame from the cleaned_x_tokenized list
tokenized_df = pd.DataFrame({'menus_tokenized': cleaned_x_tokenized})

# Ensure the index matches with reviews_df if not already
tokenized_df.index = menus_df.index

# Join the new DataFrame column to the existing DataFrame by row number
menus_df = menus_df.join(tokenized_df)

# COMMAND ----------

menus_df

# COMMAND ----------

grouped = menus_df.groupby('restaurant_name')

for name, group in grouped:
    # Collect all words across menus, preserving frequencies
    all_words = [word for menu in group['menus_tokenized'] for word in menu]
    
    # Join all words into a single string for the word cloud
    text = ' '.join(all_words)
    
    # Generate the word cloud, which automatically scales words by frequency
    wordcloud = WordCloud(width=1920, height=1080, background_color='black', colormap='Pastel1').generate(text)
    
    # Plotting
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud)
    plt.title(f'Ravintola {name.capitalize()}')
    plt.axis("off")
    plt.show()

# COMMAND ----------

for name, group in grouped:
    # Flatten the list of tokenized words for each restaurant
    all_words = [word for menu in group['menus_tokenized'] for word in menu]
    
    # Manually count word frequencies
    word_counts = {}
    for word in all_words:
        if word in word_counts:
            word_counts[word] += 1
        else:
            word_counts[word] = 1

    # Sort words by frequency and get the top 10 most common words
    sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:50]
    
    # Separate words and counts for plotting
    words, counts = zip(*sorted_words)
    
    # Plot histogram of the most frequent words
    plt.figure(figsize=(10, 5))
    plt.bar(words, counts)
    plt.title(f'Most Frequent Words in Ravintola {name.capitalize()}')
    plt.xlabel('Words')
    plt.ylabel('Frequency')
    plt.xticks(rotation=90)
    plt.show()
