import configparser

# CREATE SCHEMAS
fact_schema = ("CREATE SCHEMA IF NOT EXISTS fact_tables")
dimension_schema = ("CREATE SCHEMA IF NOT EXISTS dimension_tables")
staging_schema = ("CREATE SCHEMA IF NOT EXISTS staging_tables")

# SET SCHEMAS
set_fact_schema = ("SET search_path TO fact_tables")
set_dimension_schema = ("SET search_path TO dimension_tables")
set_staging_schema = ("SET search_path TO staging_tables")

# DROP SCHEMAS
fact_schema_drop = ("DROP SCHEMA IF EXISTS fact_tables CASCADE")
dimension_schema_drop = ("DROP SCHEMA IF EXISTS dimension_tables CASCADE")
staging_schema_drop = ("DROP SCHEMA IF EXISTS staging_tables CASCADE")

create_schemas_queries = [fact_schema, dimension_schema, staging_schema]
drop_schemas_queries = [fact_schema_drop, dimension_schema_drop, staging_schema_drop]

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSON_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
ARN = config.get("IAM_ROLE", "ARN")
REGION = config.get('GEO', 'REGION')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    event_id        bigint      IDENTITY(0,1),
    artist          varchar,
    auth            varchar,
    first_name      varchar,
    gender          varchar,
    item_in_session int,
    last_name       varchar,
    length          decimal,
    level           varchar,
    location        varchar,
    method          varchar,
    page            varchar,
    registration    varchar,
    session_id      varchar          SORTKEY DISTKEY,
    song            varchar,
    status          int,
    ts              bigint,
    user_agent      varchar,
    user_id         int
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           int,
    artist_id           varchar SORTKEY DISTKEY,
    artist_latitude     decimal,
    artist_longitude    decimal,
    artist_location     varchar,
    artist_name         varchar,
    song_id             varchar,
    title               varchar,
    duration            decimal,
    year                int
);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id    int        PRIMARY KEY IDENTITY (0,1),
        start_time     timestamp  SORTKEY, 
        user_id        varchar    NOT NULL      DISTKEY, 
        level          varchar, 
        song_id        varchar    NOT NULL,
        artist_id      varchar    ,
        session_id     varchar,
        location       varchar,
        user_agent     varchar);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id     int         NOT NULL PRIMARY KEY DISTKEY SORTKEY,
    first_name  varchar,
    last_name   varchar,
    gender      varchar,
    level       varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id     varchar     PRIMARY KEY SORTKEY,
    title       varchar     NOT NULL,
    artist_id   varchar     NOT NULL,
    year        int,
    duration    decimal
) diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id   varchar PRIMARY KEY SORTKEY,
    name        varchar     NOT NULL,
    location    varchar,
    latitude    decimal,
    longitude   decimal
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time  timestamp  PRIMARY KEY SORTKEY,
    hour        int       NOT NULL,
    day         int       NOT NULL,
    week        int       NOT NULL,
    month       int       NOT NULL,
    year        int       NOT NULL,
    weekday     int
) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
        from {}
        iam_role {}
        format as json {}
        region {};
""").format(LOG_DATA, ARN, LOG_JSON_PATH,REGION)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
        iam_role {}
        format as json 'auto'
        region {};
""").format(SONG_DATA, ARN,REGION)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT 
        TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time,
        se.user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.session_id,
        se.location,
        se.user_agent
    FROM staging_tables.staging_events se 
        INNER JOIN staging_tables.staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name
    WHERE se.page = 'NextSong';    
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
        se.user_id,
        se.first_name,
        se.last_name,
        se.gender,
        se.level
    FROM staging_tables.staging_events se
    WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration) 
    SELECT DISTINCT 
        ss.song_id, 
        ss.title, 
        ss.artist_id, 
        ss.year, 
        ss.duration
    FROM staging_tables.staging_songs ss;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT 
        ss.artist_id, 
        ss.artist_name, 
        ss.artist_location,
        ss.artist_latitude,
        ss.artist_longitude
    FROM staging_tables.staging_songs ss;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT 
        TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time,
        EXTRACT(hour from start_time),
        EXTRACT(day from start_time),
        EXTRACT(week from start_time),
        EXTRACT(month from start_time),
        EXTRACT(year from start_time),
        EXTRACT(weekday from start_time)
    FROM staging_tables.staging_events se
    WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [(set_staging_schema, staging_events_table_create), 
                        (set_staging_schema, staging_songs_table_create), 
                        (set_dimension_schema, user_table_create), 
                        (set_dimension_schema, song_table_create),
                        (set_dimension_schema, artist_table_create), 
                        (set_dimension_schema, time_table_create), 
                        (set_fact_schema, songplay_table_create)]

drop_table_queries = [staging_events_table_drop, 
                      staging_songs_table_drop, 
                      songplay_table_drop, 
                      user_table_drop, 
                      song_table_drop, 
                      artist_table_drop, 
                      time_table_drop]

copy_table_queries = [(set_staging_schema, staging_events_copy), 
                      (set_staging_schema, staging_songs_copy)]

insert_table_queries = [(set_dimension_schema, user_table_insert),
                        (set_dimension_schema, song_table_insert),
                        (set_dimension_schema, artist_table_insert),
                        (set_dimension_schema, time_table_insert),
                        (set_fact_schema, songplay_table_insert)]

