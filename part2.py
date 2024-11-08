from heapq import merge

import pandas as pd
import zipfile

def display_dataframe_info(df, name: str):
    print(f"---------------- {name} shape: ----------------")
    print(df.shape)
    print(f"---------------- {name} info: ----------------")
    print(df.info())
    print(f"---------------- {name} unique: ----------------")
    print(df.nunique())
    print(f"---------------- {name} null: ----------------")
    print(df.isnull().sum())
    print(f"---------------- {name}: ----------------")
    print(df)
    print("\n======================================================\n")

# Identify and fix issues in a provided dataset

# Write a data cleaning function that
#   Cleans: corrects issues:
#       Duplicate rows
#       Inconsistent / missing data in fields
#       Merge the files meaningfully, provide reasoning + rational
#   Outputs: Saves cleaned version

# Console output: prints number of features and observations before and after cleaning

# loading from zip error handling
try:
    df_movies = pd.read_csv('data/archive/rotten_tomatoes_movies.csv')  # , index_col='rotten_tomatoes_link'
    df_reviews = pd.read_csv('data/archive/rotten_tomatoes_critic_reviews.csv')
except FileNotFoundError:
    print("CSV files not found, attempting to extract from zip file...")
    try:
        with zipfile.ZipFile('data/archive.zip', 'r') as zip_ref:
            zip_ref.extractall('data/archive')
    except FileNotFoundError:
        print("Please place the archive.zip file in the folder")
        exit()
    finally:
        df_movies = pd.read_csv('data/archive/rotten_tomatoes_movies.csv')  # , index_col='rotten_tomatoes_link'
        df_reviews = pd.read_csv('data/archive/rotten_tomatoes_critic_reviews.csv')


display_dataframe_info(df_movies, "Movies")
display_dataframe_info(df_reviews, "Reviews")

# todo: what meaningful merges can we do here
# the only col that relates the two is 'rotten_tomatoes_link', [movies 1:M reviews]

temp_movies_df = df_movies.dropna().drop_duplicates(subset=['rotten_tomatoes_link'])
# display_dataframe_info(temp_movies_df, "TempMovies")

temp_reviews_df = df_reviews.dropna().drop_duplicates(subset=['rotten_tomatoes_link'])
# display_dataframe_info(temp_reviews_df, "TempReviews")

merged_df = pd.merge(temp_movies_df, temp_reviews_df, on='rotten_tomatoes_link')

output = merged_df[
    ['rotten_tomatoes_link',
     'movie_title',
     'movie_info',
     'critics_consensus',
     'tomatometer_rating',
     'audience_rating',
     'critic_name',
     'review_content']]

output.set_index('rotten_tomatoes_link', drop=True, inplace=True)
display_dataframe_info(output, "output")

sample_score_high = output.loc[output['tomatometer_rating']>=90, :].sample(20)
sample_score_low = output.loc[output['tomatometer_rating']<=10, :].sample(20)

try:
    output.to_csv('data/lab6_group1_cleaned.csv')

    sample_score_high.to_csv('data/sample_score_high.csv')
    sample_score_low.to_csv('data/sample_score_low.csv')
except PermissionError:
    print("Unable to write to currently open csv files")
    print("Please close any open instances of the csv files before running the program")
