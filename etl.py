import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Inserts Record into Song Table and Artist Table
    - Select columns for artist ID, name, location, latitude, and longitude
    - Use df.values to select just the values from the dataframe
    - Index to select the first (only) record in the dataframe
    - Convert the array to a list and set it to song_data and artist_data
    
    Parameters:
            cur     : Allows Python code to execute PostgreSQL command in a database session. (created by the connection.cursor())
            filepath: The location of the data to be read.
    '''
     # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert artist record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.flatten())
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Inserts records by processing from log_data
    
    - Filter records by NextSong action
    - Convert the ts timestamp column to datetime
    - Extract the timestamp, hour, day, week of year, month, year, and weekday from the ts column and set time_data to a list containing these values in order
    - Specify labels for these columns and set to column_labels
    - Create a dataframe, time_df, containing the time data for this file by combining column_labels and time_data into a dictionary and converting this into a dataframe
    
    Parameters:
            cur     : Allows Python code to execute PostgreSQL command in a database session. (created by the connection.cursor())
            filepath: The location of the data to be read.
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']== 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user recordsrow
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Get all files matching extension from directory to processes the data in each function you have created
    
    Parameters:
            cur     : Allows Python code to execute PostgreSQL command in a database session. (created by the connection.cursor())
            conn    : Handles the connection to a PostgreSQL database instance.
            filepath: The location of the data to be read.
            func    : The function that you want to process
    '''
    # get all files matching extension from directory
    
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
    - Establishes connection with the sparkify database and getscursor to it.  
    - Iterate over files and process with song_data.  
    - Iterate over files and process with log_data.  
    - Finally, closes the connection. 
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
