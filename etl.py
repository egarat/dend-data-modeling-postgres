import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Processes given song files, extracts the data and write them to the songs and artists table. This function can only handle JSON files containing single records.
    
        Parameters:
            cur (object): Cursor of the database connection to execute queries
            filepath (string): Absolute path of the file to be ingested
    """
    # open song file
    # JSON file needs to opened as a Series and then converted to DataFrame as it only contains scalar values
    df = pd.DataFrame([pd.read_json(filepath, typ="series")])

    # insert song record
    # select song ID, title, artist ID, year, and duration from the DataFrame and assign to song_data as a list
    song_data = [df.values[0][6], df.values[0][7], df.values[0][1], df.values[0][9], df.values[0][8]]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    # select artist ID, name, location, latitude, and longitude from the DataFrame and assign to artist_data as a list
    artist_data = [df.values[0][1], df.values[0][5], df.values[0][4], df.values[0][2], df.values[0][3]]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Processes given log file, extracts and transforms the data and write them to the time, users, and songplays table. This function can only handle JSON files containing multiple records separated by line breaks.
    NOTE: This function needs to run AFTER process_song_file() as it will need to look up for song_id and artist_id from the songs and artists table.
    
        Parameters:
            cur (object): Cursor of the database connection to execute queries
            filepath (string): Absolute path of the file to be ingested
    """
    # open log file
    # JSON files contains multiple objects separated by a linebreak and must be opened using the option lines=True
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    # timestamp are in miliseconds and need to be converted using the option unit="ms"
    # t will hold the converted column as a pandas Series
    t = pd.to_datetime(df["ts"], origin='unix', unit="ms")
    
    # insert time data records
    # time_data will store the DataFrame containing the datetime and the derived information
    time_data = pd.concat([t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.dayofweek], axis=1).values.tolist()
    column_labels = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
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
        # row.ts is a timestamp and must be converted to datetime again, to match the time table
        songplay_data = [pd.to_datetime(row.ts, unit="ms"), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Retrieves a file list from a given path and pass each file to the corresponding function.
    
        Parameters:
            cur (object): Cursor of the database connection
            conn (object): Object of the database connection
            filepath (string): File path to extract files that will be passed to func
            func (function): Function that will perform ETL tasks for the given file
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
    """
    Entry function, which creates a connection object to the target database and a cursor to perform queries on it. This function will also invoke the process_data() function to perform the main ETL tasks.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()