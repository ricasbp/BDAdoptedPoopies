import pandas as pd
import sqlite3
from datetime import datetime

dc = pd.read_csv('disney-characters.csv')
dd = pd.read_csv('disney-director.csv')
dva = pd.read_csv('disney-voice-actors.csv')

#-------------------------SQLITE-------------------------

conn = sqlite3.connect('open_disney.db')
cursor = conn.cursor()

cursor.execute('''DROP TABLE IF EXISTS characters;''')
cursor.execute('''DROP TABLE IF EXISTS director;''')
cursor.execute('''DROP TABLE IF EXISTS voice_actors;''')

cursor.execute('''CREATE TABLE characters (
    movie_title TEXT(100) PRIMARY KEY,
    release_date DATE NOT NULL,
    hero TEXT(100),
    villain TEXT(100),
    song TEXT(100));''')

cursor.execute('''CREATE TABLE director (
    director TEXT(100) PRIMARY KEY,
    name TEXT(100),
    FOREIGN KEY(name) REFERENCES characters(movie_title));''')

cursor.execute('''CREATE TABLE voice_actors (
    voice_actor TEXT(100) PRIMARY KEY,
    movie TEXT(100),
    character TEXT(100) NOT NULL,
    FOREIGN KEY(movie) REFERENCES characters(movie_title));''')

ins_qry_dc = "insert into characters (movie_title, release_date, hero, villain, song) values (?,?,?,?,?);"
for c in range(len(dc)):
    movie_title = dc.loc[c][1]
    release_date = datetime.strptime(dc.loc[c][2], '%B %d, %Y')
    hero = dc.loc[c][3]
    villain = dc.loc[c][4]
    song = dc.loc[c][5]
    try:
        cursor.execute(ins_qry_dc, (movie_title,release_date,hero,villain,song))
        conn.commit()
    except:
        print("error in operation")
        conn.rollback()

ins_qry_dd = "insert into director (director, name) values (?,?);"
for d in range(len(dd)):
    director = dd.loc[d][1]
    name = dd.loc[c][2]
    try:
        cursor.execute(ins_qry_dd, (director,name))
        conn.commit()
    except:
        print("error in operation")
        conn.rollback()

ins_qry_dva = "insert into voice_actors (voice_actor, movie, character) values (?,?,?);"
for va in range(len(dva)):
    voice_actor = dva.loc[d][1]
    movie = dva.loc[c][2]
    character = dva.loc[c][3]
    # try:
    cursor.execute(ins_qry_dva, (voice_actor,movie, character))
    conn.commit()
    # except:
    #     print("error in operation")
    #     conn.rollback()