# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users cascade"
song_table_drop = "drop table if exists songs cascade"
artist_table_drop = "drop table if exists artists cascade"
time_table_drop = "drop table if exists time cascade"

# CREATE TABLES

songplay_table_create = ("""
create table if not exists songplays(
    songplay_id serial primary key,
    start_time timestamptz references time(start_time),
    user_id varchar(18) references users(user_id),
    level varchar(5) default 'free',
    song_id varchar(18) references songs(song_id),
    artist_id varchar(18) references artists(artist_id),
    session_id integer,
    location varchar(80),
    user_agent text
);
""")

user_table_create = ("""
create table if not exists users(
    user_id varchar(18) primary key,
    first_name varchar(50) not null,
    last_name varchar(50) not null,
    gender varchar(1) not null,
    level varchar(5) default 'free'
);
""")

song_table_create = ("""
create table if not exists songs(
    song_id varchar(18) primary key,
    title varchar(100) NOT NULL,
    artist_id varchar(18) NOT NULL,
    year smallint check(year>=0),
    duration numeric(9,5) NOT NULL
);
""")

artist_table_create = ("""
create table if not exists artists(
    artist_id varchar(18) primary key,
    name varchar(100) NOT NULL,
    location varchar(80) NOT NULL,
    latitude float8,
    longitude float8
);
""")

time_table_create = ("""
create table if not exists time(
    start_time timestamptz primary key,
    hour smallint check(hour>=0 and hour<24) not null,
    day smallint check(day>0 and day<=31) not null,
    week smallint check(week>0 and week<53) not null,
    month smallint check(month>0 and month <=12) not null,
    year smallint check(year>0) not null,
    weekday smallint check(weekday>=0 and weekday<7) not null
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
values (%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
insert into users(user_id, first_name, last_name, gender, level)
values (%s, %s, %s, %s, %s)
on conflict (user_id) do update
    set level = excluded.level;
""")

song_table_insert = ("""
insert into songs(song_id, title, artist_id, year, duration)
values (%s, %s, %s, %s, %s);
""")

artist_table_insert = ("""
insert into artists(artist_id, name, location, latitude, longitude)
values (%s, %s, %s, %s, %s)
on conflict do nothing;
""")


time_table_insert = ("""
insert into time(start_time, hour, day, week, month, year, weekday)
values (%s, %s, %s, %s, %s, %s, %s);
""")

# FIND SONGS

song_select = ("""
select s.song_id, s.artist_id
from songs s
join artists a on s.artist_id = a.artist_id
where s.title = %s
    and a.name = %s
    and s.duration = %s
""")

# QUERY LISTS

create_table_queries = [
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create
    ]
drop_table_queries = [
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
    songplay_table_drop
    ]