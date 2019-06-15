stage_songs = ("""
    copy staging_songs from '{}' 
    access_key_id '{}' secret_access_key '{}'
    json 'auto';
""")

load_songs = ("""
    insert into songs
    (song_id,song_title,artist_id,year,duration)
        select distinct song_id,title, artist_id,year,duration
        from staging_songs
        where song_id is not null
""")

load_artists = ("""
    insert into artists
    (artist_id,artist_name,artist_location,artist_latitude, artist_longitude)
        select distinct artist_id, artist_name,artist_location, 
            artist_latitude, artist_longitude
        from staging_songs
        where artist_id is not null 
""")


init_tables = ("""
    begin;
    drop table if exists staging_songs;
    drop table if exists songs;
    drop table if exists artists;

    create table staging_songs(
        num_songs                       int,
        artist_id                       varchar,
        artist_name                     varchar,
        artist_longitude                numeric(11,3),  
        artist_latitude                 numeric(11,3),
        artist_location                 varchar,
        song_id                         varchar,
        title                           varchar,
        duration                        numeric(12,6),
        year                            int
    );

    create table songs(
    song_id                           varchar not null          sortkey,
    song_title                        varchar not null,
    artist_id                         varchar not null,
    year                              int not null,
    duration                          numeric(12,6) not null
    ) diststyle all;

    create table artists(
    artist_id                        varchar not null           sortkey,
    artist_name                      varchar not null,
    artist_location                  varchar,
    artist_longitude                 numeric(11,8),
    artist_latitude                  numeric(11,8)
    ) diststyle all;

    commit;

""")

def main():
    pass

if __name__ == "__main__":
    main()