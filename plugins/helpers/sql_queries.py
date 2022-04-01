class SqlQueries:
    # SQL queries for truncate insert
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    
    # SQL queries for append insert
    songplay_table_insert_append = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time AS start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
            WHERE events.page = 'NextSong'
                AND NOT EXISTS (SELECT start_time FROM songplays WHERE start_time = songplays.start_time)
    """)
    
    user_table_insert_append = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events AS se
        WHERE page='NextSong' AND NOT EXISTS( SELECT user_id FROM users WHERE se.userid = users.userid) 
    """)

    song_table_insert_append = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs AS se WHERE NOT EXISTS( SELECT song_id FROM songs WHERE se.song_id = songs.songid) 
    """)

    artist_table_insert_append = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs AS ss WHERE NOT EXISTS ( SELECT artist_id FROM artists WHERE ss.artist_id = artists.artistid)
    """)

    time_table_insert_append = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    
    # DQ Check SQL queries
    songplays_null_check = ("""
        SELECT COUNT(*) from songplays WHERE playid IS NULL
    """)
    
    users_null_check = ("""
        SELECT COUNT(*) from users WHERE userid IS NULL
    """)
    
    songs_null_check = ("""
        SELECT COUNT(*) from songs WHERE songid IS NULL
    """)
    
    artists_null_check = ("""
        SELECT COUNT(*) from artists WHERE artistid IS NULL
    """)
    
    time_null_check = ("""
        SELECT COUNT(*) from time WHERE start_time IS NULL
    """)
    
    # Count
    songplays_count_check = ("""
        SELECT COUNT(*) from songplays
    """)
    
    users_count_check = ("""
        SELECT COUNT(*) from users
    """)
    
    songs_count_check = ("""
        SELECT COUNT(*) from songs
    """)
    
    artists_count_check = ("""
        SELECT COUNT(*) from artists
    """)
    
    time_count_check = ("""
        SELECT COUNT(*) from time
    """)
    
    
    