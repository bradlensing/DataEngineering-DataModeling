## Data Modeling with Postgres

### Overview

In this project we will process data (ETL) from a dataset of JSON objects. Using Python along with the psycopg2 package to interact with a PostgreSQL DB.

### Build / Run Process

1. Open a terminal in folder location.
1. Run python create_tables.py
1. Run python etl.py
1. Use the test.ipynb to verify data processed satisfactorily.

### Data

- Song datasets: all json files are nested in subdirectories under /data/song_data. A sample of this files is:
  - {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
- Log datasets: all json files are nested in subdirectories under /data/log_data. A sample of a single row of each files is:
  - {"artist":"Slipknot","auth":"Logged In", "firstName":"Aiden", "gender":"M", "itemInSession":0, "lastName":"Ramirez", "length":192.57424, "level":"paid", "location":"New York-Newark-Jersey City, NY-NJ-PA", "method":"PUT", "page":"NextSong", "registration":1540283578796.0, "sessionId":19, "song":"Opium Of The People (Album Version)", "status":200, "ts":1541639510796, "userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}

### Schema for Song Play Analysis

Using the song and log datasets I optimized the queries for song play analysis using a star schema described below:

- #### Fact Table

  - songplays : Records in log data associated with song plays records with NextSong.
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

- #### Dimension Tables
  1. users - users in the app
     - user_id, first_name, last_name, gender, level
  1. songs - songs in music database
     - song_id, title, artist_id, year, duration
  1. artists - artists in music database
     - artist_id, name, location, latitude, longitude
  1. time - timestamps of records in songplays broken down into specific units
     - start_time, hour, day, week, month, year, weekday

### ETL Pipeline

- create_tables.py > Will create the database and the tables.
- elt.py > Will process the files from the datasets and insert into the DB
- test.ipynb > Can be used to verify the data is correctly inserted into the correct tables.
- etl.ipynb > Used in the development phase to code step by step processes.
