import pandas as pd
import zipfile

# Menu
while True:
    print("========== Data cleaning assignment ==========")
    print("1:\tPart 1\n2:\tPart 2\nE:\tExit")
    user_input = input()
    if user_input == '1':
        # Part 1
        print("========== Part 1 ==========\n")

        """
        Steven Weil Aaron Anderson
        11/10/2024
        Part 1
        The purpose of this code is to load in 3 datasets, and merge them into one file for further analysis later.
        The research question we are looking to solve is does sports team performance affect the local economy performance?
        To answer this question we need to look at each season of Reds and Bengals data and compare
        that to the annual GDP of the Kentucky Ohio area.
        """
        try:
            # Load the three datasets into their respective dataframes
            # df1 is the dataframe filled with the annual data from each bengals season
            df1 = pd.read_csv("data/data/bengals_data.csv")
            # df2 is the dataframe filled with the annual data from each reds season.
            df2 = pd.read_csv("data/data/reds_data.csv")
            # df3 is the data frame that holds data on the GDP (gross domestic product) of each state based on different regions, industries, and years.
            df3 = pd.read_csv("data/data/gsp_naics_all.csv")
        except FileNotFoundError:
            print("CSV files not found, attempting to extract from zip file...")
            try:
                with zipfile.ZipFile('./data/data.zip', 'r') as zip_ref:
                    zip_ref.extractall('./data')
            except FileNotFoundError:
                print("Please place the data.zip file in the folder")
                break
            finally:
                df1 = pd.read_csv("data/data/bengals_data.csv")
                df2 = pd.read_csv("data/data/reds_data.csv")
                df3 = pd.read_csv("data/data/gsp_naics_all.csv")

        # filter the data
        # We are only focused on how the teams performed overall so we are dropping all the columns except for year, (which will be the index),
        # Team, wins, losses, did they make the playoffs, and how they finished in their division.
        df1 = df1[['Year', 'Tm', 'W', 'L', 'Playoffs', 'Div. Finish']]
        df2 = df2[['Year', 'Tm', 'W', 'L', 'Playoffs', 'Finish']]

        """
        The third dataset requires alot of cleaning and adjusting before it is ready to go. The years are stored by column
         so we need to match the granularity of the other two datasets before we can merge. Also. there is tons of extra rows and columns
        that are unnecessary so we will need to drop all of them without losing any important data. Finally, the dataset only had the
        years 1997 - 2015, so those are the years we will be looking at. 
        """
        # Remove all rows where GeoName (State) is not Kentucky or Ohio
        df3 = df3[df3['GeoName'].isin(['Kentucky', 'Ohio'])]
        # Remove all rows where the Description (Industry) is not the spectator sports/ local entertainment
        df3 = df3[df3['Description'].isin(['    Performing arts, spectator sports, museums, and related activities'])]

        # We are only interested in the GDP value, so drop all unnecessary columns
        df3 = df3.drop(
            columns=['GeoFIPS', 'Region', 'ComponentId', 'ComponentName', 'IndustryId', 'IndustryClassification'])

        # In order to get the years to be the index we need to change them from columns to one singular column. For this
        # we used the melt function.
        df_melted = pd.melt(df3, var_name='Year', value_name='GDP Value', id_vars=['GeoName', 'Description'])

        # Change the GDP Value and Year columns to numeric data so they can be merged with the reds and bengals data
        df_melted['GDP Value'] = pd.to_numeric(df_melted['GDP Value'], errors='coerce')
        df_melted['Year'] = pd.to_numeric(df_melted['Year'], errors='coerce')

        # Group by the year to sum all regions of Kentucky and Ohio into one GDP total for each year.
        df_grouped = df_melted.groupby('Year', as_index=False).agg(
            {'GeoName': 'first', 'Description': 'first', 'GDP Value': 'sum'})

        # Change the row values for better clarity.
        df_grouped['GeoName'] = 'Kentucky and Ohio'
        df_grouped['Description'] = 'Sports and Entertainment GDP'

        # Change the column names for better clarity
        df_grouped.rename(columns={'Description': 'Industry', 'GeoName': 'States'}, inplace=True)

        # Merge the datasets using Inner joins on the year column. This will output one row for each year that contains
        # data about each teams season for that year and the GDP of kentucky and ohio that year.
        merged_df = pd.merge(df1, df2, how='inner', on='Year')  # Adjust 'how' and 'on' as necessary
        merged_df = pd.merge(merged_df, df_grouped, how='inner', on='Year')  # Adjust 'how' and 'on' as necessary

        # Save the new merged data into the output.csv file
        merged_df.to_csv("data/lab6_group1.csv", index=False)

        # Output the successful merge indicator
        print("Merged dataset saved to data/lab6_group1.csv\n")

        break

    elif user_input == '2':
        # Part 2
        print("========== Part 2 ==========\n")
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

        print('Before:')
        print(f'\tMovies: {df_movies.shape}')
        print(f'\tReviews: {df_reviews.shape}\n')

        # Make a meaningful merge...
        # idea: merge the reviews -> movies so each of the movies has 1 review each
        # the only col that relates the two is 'rotten_tomatoes_link', [movies 1:M reviews]

        # Removing rows with missing data and/or duplicates of both dataframes
        temp_movies_df = df_movies.dropna().drop_duplicates(subset=['rotten_tomatoes_link'])
        temp_reviews_df = df_reviews.dropna().drop_duplicates(subset=['rotten_tomatoes_link'])

        # merging on 'rotten_tomatoes_link' with inner merge so that the resulting dataframe will have a review for each movie
        merged_df = pd.merge(temp_movies_df, temp_reviews_df, on='rotten_tomatoes_link')

        # ====== Extra output idea ======
        # while merging the two datasets like this is good I had an idea of producing a random sample of 20 crappy movie reviews
        # creating the target subset of cols relevant to this idea
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
        # pulling 20 random rows with a tomatometer-rating 10 and below (bad movies)
        sample_score_low = output.loc[output['tomatometer_rating'] <= 10, :].sample(20)

        try:
            merged_df.to_csv('data/lab6_group1_cleaned.csv')
            print('After:')
            print(f'\tMerged: {merged_df.shape}\n')

            sample_score_low.to_csv('data/sample_score_low.csv')
            print('After (20 Crappy Movies):')
            print(f'\t20 Crappy Movies: {sample_score_low.shape}\n')
        except PermissionError:
            print("Something went wrong, unable to write to currently open csv files")
            print("Please close any open instances of the csv files before running the program")

        break
    else:
        # Exit
        print("Exiting...")
        break