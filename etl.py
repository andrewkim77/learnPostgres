import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """ 
    read song data files and insert data to song_table, artist_table 
    
    Parameters: 
        cur(database object) : database cursor
        filepath(string) : file path to deal with
    
    """
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_data = pd.DataFrame( df.values[:,(7,8,0,9,5)] )
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = pd.DataFrame( df.values[:,(0,4,2,1,3)] )
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ 
    read log data files and insert time_table, user_table, songplay_table
    
    Parameters: 
        cur(database object) : database cursor
        filepath(string) : file path to deal with
    
    """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df["start_time"] = pd.to_datetime(df["ts"])  
    df["hour"] = df["start_time"].dt.hour
    df["day"] = df["start_time"].dt.day
    df["week"] = df["start_time"].dt.week
    df["month"] = df["start_time"].dt.month
    df["year"] = df["start_time"].dt.year
    df["weekday"] = df["start_time"].dt.weekday
    
    # insert time data records
    time_df = df[["start_time","hour","day","week","month","year","weekday"]]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.DataFrame( df.values[:,(17,2,5,3,7)] )

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    log_files = get_files(filepath)
    
    for filepath in log_files:
        df = pd.read_json(filepath, lines=True)
        df = df[df["page"]=="NextSong"]
        df["start_time"] = pd.to_datetime(df["ts"])
        for index, row in df.iterrows():

            # get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()

            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None

            # insert songplay record
            songplay_data = (row.start_time, row.userId, row.level, songid, artistid, \
                             row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """ 
    find files in the filepath and call process_song_file, process_log_file function 
    
    Parameters:
        cur(database object) : database cursor
        conn(database object) : database connection
        filepath(string) : file path to deal with
        func(string) : function name to call
    
    """
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
    """ connect database and process_data """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb   \
                            user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()