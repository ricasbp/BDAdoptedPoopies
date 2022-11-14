import pandas as pd
import sqlite3

dc = pd.read_csv('disney-characters.csv')
dd = pd.read_csv('disney-director.csv')
dva = pd.read_csv('disney-voice-actors.csv')

#-------------------------SQLITE-------------------------

conn = sqlite3.connect('open_disney.db')
c = conn.cursor()

c.execute('''DROP TABLE IF EXISTS characters;''')
c.execute('''DROP TABLE IF EXISTS director;''')
c.execute('''DROP TABLE IF EXISTS voice_actors;''')

c.execute('''CREATE TABLE characters (
    movie_title TEXT(100) PRIMARY KEY,
    release_date DATE NOT NULL,
    hero TEXT(50),
    villain TEXT(50),
    song TEXT(50));''')

c.execute('''CREATE TABLE director (
    director TEXT(50) PRIMARY KEY,
    name TEXT(50),
    FOREIGN KEY(name) REFERENCES characters(movie_title));''')

c.execute('''CREATE TABLE voice_actors (
    voice_actor TEXT(50) PRIMARY KEY,
    movie TEXT(100),
    character TEXT(50) NOT NULL,
    FOREIGN KEY(movie) REFERENCES characters(movie_title));''')

ins_qry_dc = "insert into characters (movie_title, release_date, hero, villain, song) values (?,?,?,?,?);"
for c in range(len(dc)):
    print(dc.loc[c][1])
    movie_title = dc.loc[c][1]
    release_date = dc.loc[c][2]
    hero = dc.loc[c][3]
    villain = dc.loc[c][4]
    song = dc.loc[c][5]
    try:
        c.execute(ins_qry_dc, (movie_title,release_date,hero,villain,song))
        conn.commit()
    except:
        print("error in operation")
        conn.rollback()
