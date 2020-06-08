# Movies-ETL

# Project Overview
The purpose of the project was to create an automated extraction, transformation, and loading (ETL) pipeline for new movie data. Datasets would be downloaded from multiple sources (Wikipedia (Wiki), Kaggle.com, and MovieLens), undergo automated ETL, and would be loaded into a PostgreSQL database. See <[challenge.py](challenge.py)> for the automated ETL script. 

# Resources
Data: <[wikipedia.movies.json](Data/wikipedia.movies.json)>, movies_metadata.csv, and ratings.csv. (The last two files are available <[here](https://www.kaggle.com/rounakbanik/the-movies-dataset/download)>.)

Software: json 2.0.9, numpy 1.18.1, pandas 1.0.3, re 2.2.1, sqlalchemy 1.3.17, jupyter notebook 6.0.3, and python 3.8.2

# Discussion
To create an automated ETL pipeline, assumptions were made about the datasets.

Some assumptions were that...
1) Kaggle data would continue to have less outliers, be more consistent in content and data types within a column, and have less null values than Wiki data. The implications are that, even though Kaggle and Wiki share some column headers, only Kaggle data would be used to fill the columns 'title', 'release_date', 'original_language', and 'production_companies'; Kaggle data would be the main source for the columns 'runtime', 'budget', and 'revenue' and null values would be replaced with Wiki data; finally, Wiki data would supply the remaining columns (their data being only available from Wiki).

2) The raw ratings.csv file would continue to have the same table structure and the same data type for each column. This assumption must be true for ratings.csv data to be loaded into the PostgreSQL database since the raw file would be loaded into a pre-existing empty table where columns have pre-assigned data types. 

3) Wiki data for 'Box office', 'Budget', and 'Running time' would continue to be string data type or would have list structure. It is based on this assumption that the ETL script converts lists to strings and then, strings to float data type.

4) The majority of 'Box office' and 'Budget' Wiki data would be able to be parsed into certain forms using regular expressions (e.g. $###.# million, $###.# billion, or $###,###,###). These forms limit extraction of data to North American currencies. Thus, it is also assumed that the majority of 'Box office' and 'Budget' Wiki data would continue to be in USD. 

5) Names of important columns would not change in Wiki and Kaggle raw data. The ELT script references names of important columns for data extraction, data type conversions, and column reordering and renaming. Thus, this assumption needs to hold true for the script to successfully run and load files. For Wiki data, important columns are, in general, those with < 90% null rows (since otherwise, columns were discarded). A few examples of names of important columns are 'imdb_link', 'Box office', 'Budget', and 'Running time' in Wiki data and 'id', 'release_date', 'revenue', 'budget', and 'runtime' in Kaggle data. 