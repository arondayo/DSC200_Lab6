import pandas as pd
import zipfile

with zipfile.ZipFile('data/archive.zip', 'r') as zip_ref:
    zip_ref.extractall('data/archive')

# Identify and fix issues in a provided dataset

# Write a data cleaning function that
#   Cleans: corrects issues:
#       Duplicate rows
#       Inconsistent / missing data in fields
#       Merge the files meaningfully, provide reasoning + rational
#   Outputs: Saves cleaned version

# Console output: prints number of features and observations before and after cleaning

df_movies = pd.read_csv('data/archive/rotten_tomatoes_movies.csv') # , index_col='rotten_tomatoes_link'
df_reviews = pd.read_csv('data/archive/rotten_tomatoes_critic_reviews.csv')

print("---------------- movies shape: ----------------")
print(df_movies.shape)
print("---------------- movies info: ----------------")
print(df_movies.info())
print("---------------- movies unique: ----------------")
print(df_movies.nunique())
print(df_movies)
print("\n=================================================\n")

print("--------------df_reviews shape: ----------------")
print(df_reviews.shape)
print("--------------df_reviews info: ----------------")
print(df_reviews.info())
print("--------------df_reviews unique: ----------------")
print(df_reviews.nunique())

# todo: what meaningful merges can we do here
# the only col that relates the two is 'rotten_tomatoes_link', [movies 1:M reviews]