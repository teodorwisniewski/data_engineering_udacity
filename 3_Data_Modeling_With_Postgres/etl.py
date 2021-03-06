import os
import glob
import psycopg2
import pandas as pd
import json
from sql_queries import song_table_insert, artist_table_insert, time_table_insert,\
                        user_table_insert, song_select, songplay_table_insert


def process_song_file(cur, filepath):
    # open song file
    with open(filepath) as f:
        file_dict = json.load(f)
        data = {col: [val] for col, val in file_dict.items()}
    df = pd.DataFrame.from_dict(data)

    # insert song record
    cols = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = df[cols].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    cols = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artist_data = df[cols].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    data = []
    with open(filepath) as f:
        for line in f:
            data.append(json.loads(line))
    df = pd.DataFrame(data)

    # filter by NextSong action
    cond = df.page == "NextSong"
    df = df.loc[cond]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    data = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(data)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    cols = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = df[cols].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for (index, row), start_time_t in zip(df.iterrows(),t):
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record

        songplay_data = (index, start_time_t, row.userId, row.level, songid, artistid, row.sessionId, row.location,
                         row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()