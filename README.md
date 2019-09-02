Project explanation
===================
This project result consists of files "etl.py, create_tables.py sql_queries.py"

1)sql_queries.py explanation
----------------------------

collection of query scripts for "drop table, create table, insert record, find song"

### script - drop table example
    songplay_table_drop = "drop table if exists songplays"

### table schemas and sample data
    songplays ( 
    songplay_id serial PRIMARY KEY, 
    start_time timestamp NOT NULL, 
    user_id varchar NOT NULL, 
    level varchar NOT NULL, 
    song_id varchar, 
    artist_id varchar, 
    session_id varchar NOT NULL, 
    location varchar NOT NULL,
    user_agent varchar NOT NULL)
    ![songplays data](./images/songplays_data.jpg)
    
    users (
    user_id varchar PRIMARY KEY, 
    first_name  varchar NOT NULL, 
    last_name  varchar NOT NULL, 
    gender  varchar NOT NULL, 
    level varchar NOT NULL)
    ![users data](./images/users_data.jpg)
    
    songs (
    song_id varchar PRIMARY KEY, 
    title varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    year int , 
    duration float NOT NULL)
    ![songs data](./images/songs_data.jpg)
    
    artists (
    artist_id varchar PRIMARY KEY, 
    name varchar NOT NULL, 
    location varchar , 
    latitude float , 
    longitude float )
    ![artists data](./images/artists_data.jpg)
    
    time (
    start_time timestamp PRIMARY KEY, 
    hour int NOT NULL, 
    day int NOT NULL, 
    week int NOT NULL, 
    month int NOT NULL, 
    year int NOT NULL, 
    weekday int NOT NULL )
    ![time data](./images/time_data.jpg)
    
### duplication data
###### songs table
    
    * there are some duplication data, total 7 duplication data.
    * duplication data example : songId = SOUDSGM12AC9618304, SOFCHDR12AB01866EF etc
###### artist table
    
    * there are some duplication data, total 11 duplication data.
    * duplication data example : aristId = ARNTLGG11E2835DDB9, AREVWGE1187B9B890A etc
###### time table
    
    * there are some duplication data, total 7 duplication data.
    * duplication data example : start_time = 1970-01-01 00:25:42.171216796, 1970-01-01 00:25:42.984111796 etc
###### user table
    
    * there are some duplication data, total 6683 duplication data.
    * duplication data example : userId = 95, 97 etc
###### songplay table
    
    * table key is serial number. so there is no duplication data.
    

2)create_tables.py explanation
------------------------------

drop and creat tables using drop_table_queries, create_table_queries


3)etl.py explanation
--------------------

There are two main functions, process_song_file and process_log_file

### process_song_file
read song data from json files and insert into database with song, artist data

###### code - open songfile 
    
    df = pd.read_json(filepath,lines=True)
    
###### code - get song data example
    
    song_data = pd.DataFrame( df.values[:,(7,8,0,9,5)] )

### process_log_file
read log data files and insert time_table, user_table, songplay_table

###### code - filter by NextSong action for getting useful data
    
    df = df.loc[df['page'] == 'NextSong']
    
###### code - convert timestamp column to datetime and add colums
    
    df["start_time"] = pd.to_datetime(df["ts"])  
    df["hour"] = df["start_time"].dt.hour
    df["day"] = df["start_time"].dt.day
    df["week"] = df["start_time"].dt.week
    df["month"] = df["start_time"].dt.month
    df["year"] = df["start_time"].dt.year
    df["weekday"] = df["start_time"].dt.weekday
    
###### code - load user table
    
    user_df = pd.DataFrame( df.values[:,(17,2,5,3,7)] )

###### code - read log data, getting songid artistid information and insert songplay table
    
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

