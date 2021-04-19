import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb")
cur = conn.cursor()

def process_song_file(cur, filepath):
    '''
    Process the song files and inserts the data into the Postgres DB.
    Accepts 2 paramaters from the process_data()
    cur: database cursor reference
    filepath: directory for the files
    '''
    # OPEN SONG FILE
    df = pd.read_json(filepath, lines=True)
    df.head()
    
    # INSERT SONG RECORD
    [num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name,
    song_id, title, duration, year] = df.values[0]

    song_data = [song_id, title, artist_id, year, duration]
    
    cur.execute(song_table_insert, song_data)
    conn.commit()
    
    # INSERT ARTIST RECORD
    artist_data = [artist_id, artist_name, artist_location, artist_longitude, artist_latitude]
    cur.execute(artist_table_insert, artist_data)
    conn.commit()

def process_log_file(cur, filepath):
    '''
    Process the log files and inserts the data into the Postgres DB.
    Accepts 2 paramaters from the process_data()
    cur: database cursor reference
    filepath: directory for the files
    '''
    # OPEN LOG FILE
    df = pd.read_json(filepath, lines=True)

    # FILTER BY NEXTSONG ACTION
    df = df[df['page']=='NextSong']

    # CONVERT TIMESTAMP COLUMN TO DATETIME
    t = pd.to_datetime(df['ts'], unit='ms') 
    
    # INSERT TIME DATA RECORD
    time_data = []
    for line in t:
        time_data.append([line, line.hour, line.day, line.week, line.month, line.year,
                          line.day_name()])
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_records(time_data, columns=column_labels)
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        conn.commit()

    # LOAD USER TABLE
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # INSERT USER ROWS
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # INSERT SONGPLAY RECORDS
    for index, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
            print(results)
        else:
            songid, artistid = None, None

        # INSERT SONGPLAY RECORD
        songplay_data = [pd.to_datetime(row.ts, unit='ms'), row.userId, row.level,
                         songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)
        conn.commit()

def process_data(cur, conn, filepath, func):
    '''
    The process_data() is used to load song and log file data to the Postgres database.
    It accepts 4 parameters when called from the main().
    cur: database cursor reference
    conn: database connection reference
    filepath: directory for the files
    func: funcntion call 
    '''
    # GET ALL FILES MATCHING EXTENSION FROM DIRECTORY
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # GET TOTAL NUMBER OF FILES FOUND
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # ITERATE OVER FILES AND PROCESS
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    ''' The driver function for loading data and calling the process functions. '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()