import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
from datetime import timedelta
import sqlite3
import time
def check_validate(df):
    # Checking wether the user has listened to songs or not
    if df.empty:
        print("No songs listened")
        return False
    
    # To check if the data does not have duplicated values
    if df['played_at'].is_unique:
        pass
    else:
        raise Exception("Primary key check")
        
        
    # To check wether data has null values or not
    if df.isnull().values.any():
        raise Exception("Dataset contains null values")
        
    # (Optional)
    # To check for the songs listened within 48 hours
    """
        
    yesterday = datetime.now() - timedelta(days =2)
    yesterday = yesterday.replace(hour =0, minute=0, second=0,microsecond =0 )
    print(yesterday)
    DataFrame['timestamps']
    timestamps = DataFrame['timestamps'].tolist()
    for i in timestamps:
        if datetime.strptime(i,'%Y-%m-%d')<yesterday:
            raise Exception("There are song that were played before 24 hrs")
    """
    
    return True
def run_spotify_etl():
    DATABASE_LOCATION =  "sqlite:///played_song.sqlite"
# Spotify UserID
    USER_ID = ""
# Spotify Token
    TOKEN = ""
    headers ={
        "Content-type" :"application/json",
        "Authorization" : f"Bearer {TOKEN}"  #.format(token = TOKEN)
    }
    
    
    # Converting time to Unix timestamp in miliseconds
    today = datetime.now()
    time_req = today - timedelta(days =60)
    unix_timestamp = int(time_req.timestamp())*1000
    
    req = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time = unix_timestamp),headers = headers)
    
    data = req.json()
    print(data)
    
    song=[]
    artist_name=[]
    played_at_list=[]
    timestamps=[]
    
    # Extracting relevant data from the json file
    for i in data['items']:
        song.append(i['track']['name'])
        artist_name.append(i['track']['album']['artists'][0]['name'])
        played_at_list.append(i['played_at'])
        timestamps.append(i["played_at"][0:10])
    
    
    song_dict ={
    'song' : song,
    'artist' : artist_name,
    'played_at' : played_at_list,
    'timestamps' : timestamps
    }
    
    
    DataFrame = pd.DataFrame(song_dict, columns = ["song","artist","played_at","timestamps"])
    print(DataFrame)
    DataFrame['song'].nunique()
    
    # Transform phase of ETL pipeline
    #check_validate(DataFrame)
    if check_validate(DataFrame):
        print("Data is valid")
    
    # Load phase of ETL pipeline
    # Creating Database connection
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    connection = sqlite3.connect('played_song.sqlite')
    cursor = connection.cursor()
    
    sql_query ="""
    CREATE TABLE IF NOT EXISTS song_played(
    song VARCHAR(200),
    atrist VARCHAR(200),
    played_at VARCHAR(200),
    timestamps VARCHAR(200),
    CONSTRAINT PK_tracks PRIMARY KEY (played_at)
    )"""
    
    cursor.execute(sql_query)
    print("Database successfully created")
    
    connection.commit()
    DataFrame.to_sql('song_played',connection,if_exists='replace', index = False)

    print(cursor.execute("select * from song_played").fetchall())
    
    # Closing Database connection
    connection.close()
    print("Database closed successfully")
        
