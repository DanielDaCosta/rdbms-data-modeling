import os
import glob
import psycopg2
import pandas as pd
from config import config
from sql_queries import *
import tempfile
params = config()


def insert_many_from_dataframe(conn, df, table):
    """
    Insert many into table from Dataframe
    """
    params_insert = ["%s" for i in range(len(df.columns))]
    params_insert = ','.join(params_insert)
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    query  = f"INSERT INTO {table}({cols}) VALUES({params_insert}) on conflict do nothing;"
    cur = conn.cursor()
    try:
        cur.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        cur.close()
        raise error
    print(f'Successfully insert many to table {table}')
    cur.close()


def copy_from_dataframe(conn, df, table):
    """
    Copy data from DataFrame and save it to
    sql table using copy_from() from psycopg2
    """
    with tempfile.NamedTemporaryFile() as temp:
        df.to_csv(temp.name, index=False, header=False)
        cur = conn.cursor()
        try:
            cur.copy_from(temp, table, sep=',')
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            conn.rollback()
            cur.close()
            raise error
    print(f'Successfully copied to table {table}')
    cur.close()


def insert_from_dataframe(conn, df, query):
    cur = conn.cursor()
    try:
        for i, row in df.iterrows():
            cur.execute(query, list(row))
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        cur.close()
        raise error
    print('Successfully inserted to table')
    cur.close()


def process_song_file(conn, all_files):
    print('Processing song files')
    df = pd.DataFrame()
    for file in all_files:
        df_one = pd.read_json(file, lines=True)
        df = df.append(df_one, ignore_index=True)
    copy_from_dataframe(
        conn,
        df[['song_id', 'title', 'artist_id', 'year', 'duration']],
        'songs'
    )
    df.rename(columns={
        'artist_name': 'name',
        'artist_location': 'location',
        'artist_latitude': 'latitude',
        'artist_longitude': 'longitude'
    }, inplace=True)

    insert_from_dataframe(
        conn,
        df[['artist_id', 'name', 'location', 'latitude', 'longitude']],
        artist_table_insert
    )


def process_log_file(conn, all_files):
    print('Processing log files')
    df = pd.DataFrame()
    for file in all_files:
        df_one = pd.read_json(file, lines=True)
        df = df.append(df_one, ignore_index=True)
    df = df[df['page'] == 'NextSong']

    # # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    df['start_time'] = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    new_df_dict = {i:j for i, j in zip(column_labels, time_data)}
    time_df = pd.DataFrame.from_dict(new_df_dict)

    insert_many_from_dataframe(
        conn,
        time_df,
        'time'
    )

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df.rename(columns={
        'userId': 'user_id',
        'firstName': 'first_name',
        'lastName': 'last_name',
    }, inplace=True)

    insert_many_from_dataframe(
        conn,
        user_df,
        'users'
    )

    # insert songplay records
    print('Insert into song plays tables')
    all_song_play_data = []
    for index, row in df.iterrows():
        cur = conn.cursor()
        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        conn.commit()
        songid, artistid = results if results else None, None

        # insert songplay record
        songplay_data = (
            row.start_time, row.userId, row.level, songid,
            artistid, row.sessionId, row.location, row.userAgent
        )
        all_song_play_data.append(songplay_data)

    df_song_play = pd.DataFrame(
        all_song_play_data,
        columns=['start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent']
    )
    insert_many_from_dataframe(
        conn,
        df_song_play,
        'songplays'
    )
    print('Successfully inserted to song plays')


def process_data(conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    func(conn, all_files)


def truncate_all_tables(conn):
    """
    Truncate all tables
    """
    cur = conn.cursor()
    try:
        for query in truncate_table_queries:
            cur.execute(query)
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        cur.close()
        raise error


def main():
    conn = psycopg2.connect(**params)
    conn.set_session(autocommit=True)

    truncate_all_tables(conn)

    process_data(conn, filepath='data/song_data', func=process_song_file)
    process_data(conn, filepath='data/log_data', func=process_log_file)

    print('ALL TABLE WERE POPULATED')


if __name__ == "__main__":
    main()