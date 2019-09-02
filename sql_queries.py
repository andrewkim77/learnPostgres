# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = ("""
  create table if not exists songplays ( 
    songplay_id serial PRIMARY KEY, 
    start_time timestamp NOT NULL, 
    user_id varchar NOT NULL, 
    level varchar NOT NULL, 
    song_id varchar, 
    artist_id varchar, 
    session_id varchar NOT NULL, 
    location varchar NOT NULL,
    user_agent varchar NOT NULL)
""")

user_table_create = ("""
  create table if not exists  users (
    user_id varchar PRIMARY KEY, 
    first_name  varchar NOT NULL, 
    last_name  varchar NOT NULL, 
    gender  varchar NOT NULL, 
    level varchar NOT NULL
  )
""")

song_table_create = ("""
  create table if not exists  songs (
    song_id varchar PRIMARY KEY, 
    title varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    year int , 
    duration float NOT NULL
  )
""")

artist_table_create = ("""
  create table if not exists  artists (
    artist_id varchar PRIMARY KEY, 
    name varchar NOT NULL, 
    location varchar , 
    latitude float , 
    longitude float
  )
""")

time_table_create = ("""
  create table if not exists  time (
    start_time timestamp PRIMARY KEY, 
    hour int NOT NULL, 
    day int NOT NULL, 
    week int NOT NULL, 
    month int NOT NULL, 
    year int NOT NULL, 
    weekday int NOT NULL
  )
""")

# INSERT RECORDS

songplay_table_insert = ("""
insert into songplays (  start_time , user_id , level , song_id , artist_id , session_id , location ,
    user_agent  )
    values
        ( %s, %s, %s, %s, %s, %s, %s, %s ) ;
""")

user_table_insert = ("""
insert into users ( user_id , first_name  , last_name  , gender  , level )
    values
        ( %s, %s, %s, %s, %s )
    ON CONFLICT(user_id) DO UPDATE SET level = excluded.level ;
""")

song_table_insert = ("""
insert into songs (     song_id , title , artist_id , year , duration  )
    values
    ( %s, %s, %s, %s, %s )
    ON CONFLICT( song_id ) DO NOTHING
    ;
""")

artist_table_insert = ("""
insert into artists (     artist_id ,     name ,     location ,     latitude ,     longitude  )
    values
    ( %s, %s, %s, %s, %s )
    ON CONFLICT( artist_id ) DO NOTHING
    ;
""")


time_table_insert = ("""
insert into time ( start_time , hour ,  day ,  week , month , year , weekday  )
    values
        ( %s, %s, %s, %s, %s, %s, %s )
        ON CONFLICT( start_time ) DO NOTHING
        ;
""")

# FIND SONGS

song_select = ("""
SELECT songs.song_id, artists.artist_id
    FROM songs
    INNER JOIN artists
    ON songs.artist_id = artists.artist_id
    WHERE songs.title = %s 
    AND artists.name = %s 
    AND songs.duration = %s ;
""")



# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]