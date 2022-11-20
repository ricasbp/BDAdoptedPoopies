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
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    voice_actor TEXT(100),
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
    name = dd.loc[d][2]
    try:
        cursor.execute(ins_qry_dd, (director,name))
        conn.commit()
    except:
        print("error in operation")
        conn.rollback()

ins_qry_dva = "insert into voice_actors (voice_actor, movie, character) values (?,?,?);"
for va in range(len(dva)):
    voice_actor = dva.loc[va][2]
    vaArray = voice_actor.split("; ")
    for i in range(len(vaArray)):
        if vaArray[i] == "None":
            vaArray[i] = None #TODO
    movie = dva.loc[va][3]
    character = dva.loc[va][1]
    try:
        if voice_actor.__contains__(";"):
            cursor.execute(ins_qry_dva, (vaArray[1],movie, character))
        cursor.execute(ins_qry_dva, (vaArray[0],movie, character))
        conn.commit()
    except:
        print("error in operation")
        conn.rollback()

sel_1_1 = "select voice_actor from voice_actors where movie = 'The Little Mermaid';"
cursor.execute(sel_1_1)

rows_1_1 = cursor.fetchall()

#for r in rows_1_1:
#    print(r)

sel_1_2 = "select character from voice_actors group by character having count(character) > 1;"
cursor.execute(sel_1_2)

rows_1_2 = cursor.fetchall()

#for r in rows_1_2:
#    print(r)

sel_2_1 = "select "