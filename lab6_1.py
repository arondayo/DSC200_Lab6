"""
Steven Weil Aaron Anderson
11/10/2024
Part 1
The purpose of this code is to load in 3 datasets, and merge them into one file for further analysis later.
The research question we are looking to solve is does sports team performance affect the local economy performance?
To answer this question we need to look at each season of Reds and Bengals data and compare
that to the annual GDP of the Kentucky Ohio area.
"""


#import the pandas library
import pandas as pd

#Load the three datasets into their respective dataframes
#df1 is the dataframe filled with the annual data from each bengals season
df1 = pd.read_csv("data/bengals_data.csv")
#df2 is the dataframe filled with the annual data from each reds season.
df2 = pd.read_csv("data/reds_data.csv")
#df3 is the data frame that holds data on the GDP (gross domestic product) of each state based on different regions, industries, and years.
df3 = pd.read_csv("data/gsp_naics_all.csv")

#filter the data
#We are only focused on how the teams performed overall so we are dropping all the columns except for year, (which will be the index),
#Team, wins, losses, did they make the playoffs, and how they finished in their division.
df1 = df1[['Year', 'Tm', 'W', 'L', 'Playoffs', 'Div. Finish']]
df2 = df2[['Year', 'Tm', 'W', 'L', 'Playoffs', 'Finish']]


"""
The third dataset requires alot of cleaning and adjusting before it is ready to go. The years are stored by column
 so we need to match the granularity of the other two datasets before we can merge. Also. there is tons of extra rows and columns
that are unnecessary so we will need to drop all of them without losing any important data. Finally, the dataset only had the
years 1997 - 2015, so those are the years we will be looking at. 
"""
#Remove all rows where GeoName (State) is not Kentucky or Ohio
df3 = df3[df3['GeoName'].isin(['Kentucky', 'Ohio'])]
#Remove all rows where the Description (Industry) is not the spectator sports/ local entertainment
df3 = df3[df3['Description'].isin(['    Performing arts, spectator sports, museums, and related activities'])]

#We are only interested in the GDP value, so drop all unnecessary columns
df3 = df3.drop(columns=['GeoFIPS', 'Region', 'ComponentId', 'ComponentName', 'IndustryId', 'IndustryClassification'])

#In order to get the years to be the index we need to change them from columns to one singular column. For this
#we used the melt function.
df_melted = pd.melt(df3, var_name='Year', value_name='GDP Value', id_vars=['GeoName', 'Description'])

#Change the GDP Value and Year columns to numeric data so they can be merged with the reds and bengals data
df_melted['GDP Value'] = pd.to_numeric(df_melted['GDP Value'], errors='coerce')
df_melted['Year'] = pd.to_numeric(df_melted['Year'], errors='coerce')

#Group by the year to sum all regions of Kentucky and Ohio into one GDP total for each year.
df_grouped = df_melted.groupby('Year', as_index=False).agg({'GeoName': 'first', 'Description': 'first', 'GDP Value': 'sum'})

#Change the row values for better clarity.
df_grouped['GeoName'] = 'Kentucky and Ohio'
df_grouped['Description'] = 'Sports and Entertainment GDP'

#Change the column names for better clarity
df_grouped.rename(columns={'Description': 'Industry', 'GeoName': 'States'}, inplace=True)

#Merge the datasets using Inner joins on the year column. This will output one row for each year that contains
#data about each teams season for that year and the GDP of kentucky and ohio that year.
merged_df = pd.merge(df1, df2, how='inner', on='Year')  # Adjust 'how' and 'on' as necessary
merged_df = pd.merge(merged_df, df_grouped, how='inner', on='Year')  # Adjust 'how' and 'on' as necessary

#Save the new merged data into the output.csv file
merged_df.to_csv("data/output.csv", index=False)

#Output the successful merge indicator
print("Merged dataset saved to data/output.csv")




