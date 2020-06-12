import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
from config import db_password
import time

# arguments for movies_ETL function
# replace file paths with your unique file paths
wiki_path = '/Users/patricialan/Desktop/Module8ETL/Movies-ETL/Data/wikipedia.movies.json'
kaggle_path = '/Users/patricialan/Desktop/Module8ETL/Movies-ETL/Data/movies_metadata.csv'
ratings_path = '/Users/patricialan/Desktop/Module8ETL/Movies-ETL/Data/ratings.csv'

# movies_ETL function
def movies_ETL(wiki_path, kaggle_path, ratings_path):  
    
    try:
        # read files
        with open(f'{wiki_path}', mode='r') as file:
            wiki = json.load(file)
        kaggle = pd.read_csv(f'{kaggle_path}', low_memory = False)
        ratings = pd.read_csv(f'{ratings_path}')
    except:
        print("Unable to read files. Ensure correct file paths.")
    
    # ------------------wiki----------------------
    
    # movies must have director, imdb link, and not be TV series
    wiki = [movie for movie in wiki if ('Director' in movie or 'Directed by' in movie) \
        and 'imdb_link' in movie and 'No. of episodes' not in movie]

    # change column names
    def change_column_name(old_name, new_name):         
        for movie in wiki:
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
             
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name('Directed by', 'Director')
    change_column_name('Distributed by', 'Distributor')
    change_column_name('Edited by', 'Editor(s)')
    change_column_name('Length', 'Running time')
    change_column_name('Original release', 'Release date')
    change_column_name('Music by', 'Composer(s)')
    change_column_name('Produced by', 'Producer(s)')
    change_column_name('Producer', 'Producer(s)')
    change_column_name('Productioncompanies ', 'Production company(s)')
    change_column_name('Productioncompany ', 'Production company(s)')
    change_column_name('Released', 'Release Date')
    change_column_name('Release Date', 'Release date')
    change_column_name('Screen story by', 'Writer(s)')
    change_column_name('Screenplay by', 'Writer(s)')
    change_column_name('Story by', 'Writer(s)')
    change_column_name('Theme music composer', 'Composer(s)')
    change_column_name('Written by', 'Writer(s)')

    # create dataframe
    wiki_df = pd.DataFrame(wiki)
    
    # new 'imdb_id' column
    wiki_df['imdb_id'] = wiki_df['imdb_link'].str.extract(r'(tt\d{7})')
    
    # drop duplicate imdb_ids
    wiki_df.drop_duplicates(subset='imdb_id', inplace=True)
    
    # keep columns where < 90% of rows are null
    wiki_columns_to_keep = [column for column in wiki_df.columns if wiki_df[column].isnull().sum() < len(wiki_df)*0.9]
    wiki_df = wiki_df[wiki_columns_to_keep]
    
    # clean columns 'Box office', 'Budget', & 'Running time'
    ## join lists
    box_office = wiki_df['Box office'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    budget = wiki_df['Budget'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    running_time = wiki_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    ## format amounts in ranges
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    
    ## format amounts with citation references
    budget = budget.str.replace(r'\[\d+\]\s*', '')
    
    ## box office & budget: forms to extract
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
    
    ## function converts strings to floating-point numbers
    def parse_dollars(s):
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
            # remove dollar sign and " million"
            s = re.sub('\$|\s|[a-zA-Z]','', s)
            # convert to float and multiply by a million
            value = float(s) * 10**6
            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
            # remove dollar sign and " billion"
            s = re.sub('\$|\s|[a-zA-Z]','', s)
            # convert to float and multiply by a billion
            value = float(s) * 10**9
            # return value
            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
            # remove dollar sign and commas
            s = re.sub('\$|,','', s)
            # convert to float
            value = float(s)
            # return value
            return value

        # otherwise, return NaN
        else:
            return np.nan
    
    ## box office & budget: extract forms & apply parse_dollars function
    wiki_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

    ## running time:
    ## extract rows with hours & min
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    ## convert strings to numbers; coerce empties to NaN, then turn into zero
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    ## if pure minutes is zero, then convert hours+min to min
    wiki_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    
    # drop old columns
    wiki_df.drop(['Box office', 'Budget', 'Running time'], axis=1, inplace=True)
    
    # ------------------kaggle----------------------
    
    # keep rows where adult = False (also drops 3 bad data rows) & drop 'adult' column
    # drop 'video' column because contains 1 unique value "False"
    kaggle = kaggle[kaggle['adult'] == 'False'].drop(['adult','video'],axis='columns')
    
    try: 
        # convert columns to numeric dtype
        kaggle['budget'] = kaggle['budget'].astype(int, errors='raise')
    except: 
        print("Kaggle 'budget' could not be converted to integer values. Continuing...")
    try:
        kaggle['id'] = pd.to_numeric(kaggle['id'], errors='raise')
    except: 
        print("Kaggle 'id' could not be converted to numeric values. Continuing...")
    try:
        kaggle['popularity'] = pd.to_numeric(kaggle['popularity'], errors='raise')
    except: 
        print("Kaggle 'popularity' could not be converted to numeric values. Continuing...")
        # convert release date to datetime
    try:
        kaggle['release_date'] = pd.to_datetime(kaggle['release_date'], errors='raise') 
    except: 
        print("Kaggle 'release_date' could not be converted to datetime. Continuing...")

    # --------------wiki-kaggle merge---------------
    
    # merge & rename redundant columns
    movies_df = pd.merge(wiki_df, kaggle, on='imdb_id', suffixes=['_wiki','_kaggle'])
    
    # drop redundant columns: title_wiki, Release date (wiki), Language (wiki), and Production company(s) (wiki)
    movies_df.drop(columns=['title_wiki', 'Release date', 'Language', 'Production company(s)'], inplace=True)
    
    try:
        # drop a known outlier: tt00457939 titled 'From Here To Eternity' released 1953 
        # got mixed in same row as 'The Holiday' released 2006
        movies_df = movies_df.drop(movies_df[(movies_df['url'] == 'https://en.wikipedia.org/wiki/The_Holiday') & (movies_df['imdb_id'] == 'tt0045793')].index)
    except:
        print('Corrupt entry for "tt00457939" is no longer present.')
    
    # function fills in missing data for a wiki-kaggle column pair, then drops the redundant column
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)
        df.drop(columns=wiki_column, inplace=True)
        
    # kaggle columns 'runtime', 'budget_kaggle', & 'revenue': fill with wiki, then drop wiki columns
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    
    # reorder columns
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection',
        'url','imdb_link','runtime','budget_kaggle','revenue','release_date','popularity','vote_average','vote_count',
        'genres','original_language','overview','spoken_languages','Country','production_companies',
        'production_countries','Distributor','Producer(s)','Director','Starring','Cinematography','Editor(s)',
        'Writer(s)','Composer(s)','Based on']]
    
    # rename columns
    movies_df.rename({'id':'kaggle_id',
        'title_kaggle':'title',
        'url':'wikipedia_url',
        'budget_kaggle':'budget',
        'Country':'country',
        'Distributor':'distributor',
        'Producer(s)':'producers',
        'Director':'director',
        'Starring':'starring',
        'Cinematography':'cinematography',
        'Editor(s)':'editors',
        'Writer(s)':'writers',
        'Composer(s)':'composers',
        'Based on':'based_on'}, axis='columns', inplace=True)
    
    # --------------ratings---------------
    
    # create pivot table: index = movieId, columns = rating values, values = counts of users for each rating value
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
        .rename({'userId':'count'}, axis=1) \
        .pivot(index='movieId',columns='rating', values='count')
    
    # rename column headers
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    
    # --------------wiki-kaggle-ratings merge---------------
    
    # merge on kaggle column 'kaggle_id' and ratings index 'movieId'
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    
    # fill in no ratings with zero
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    
    #----------load files into PostgreSQL database 'movie_data'---------
    
    # create database engine for PostgreSQL to connect
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    engine = create_engine(db_string)
    
    try:
        # load movies_df to PostgreSQL
        movies_with_ratings_df.to_sql(name='movies', con=engine, if_exists='append')
    except:
        print("Unable to load 'movies_with_ratings_df'. Continuing with loading of 'ratings.csv'.")
    
    try:
        # load raw ratings data to PostgreSQL     
        ## create a variable for the number of rows imported
        rows_imported = 0

        ## get the start time from time.time()
        start_time = time.time()

        for data in pd.read_csv(f'{ratings_path}', chunksize=1000000):

            ## print out the range of rows that are being imported
            print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
            data.to_sql(name='ratings', con=engine, if_exists='append')

            ## increment the number of rows imported by the chunksize
            rows_imported += len(data)

            ## print that the rows have finished importing
            ## add elapsed time until this print out
            print(f'Done. {time.time() - start_time} total seconds elapsed')
    except:
        print("Unable to load 'ratings.csv'.")
              
    return

# call on movies_ETL function
movies_ETL(wiki_path, kaggle_path, ratings_path)