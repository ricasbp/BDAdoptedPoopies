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
cursor.execute('''DROP TABLE IF EXISTS directors;''')
cursor.execute('''DROP TABLE IF EXISTS voice_actors;''')

cursor.execute('''CREATE TABLE characters (
    movie_title TEXT(100) PRIMARY KEY,
    release_date DATE NOT NULL,
    hero TEXT(100),
    villain TEXT(100),
    song TEXT(100));''')

cursor.execute('''CREATE TABLE directors (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    director TEXT(100),
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
    if villain is None:   
#TODO isto nao funciona
        villain = "Nao existe" 
    song = dc.loc[c][5]
    try:
        cursor.execute(ins_qry_dc, (movie_title,release_date,hero,villain,song))
        conn.commit()
    except:
        print("error in operation")
        conn.rollback()

ins_qry_dd = "insert into directors (director, name) values (?,?);"
for d in range(len(dd)):
    director = dd.loc[d][2]
    name = dd.loc[d][1]
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
            vaArray[i] = None 
#TODO verificar no sqlbrowse e mongo compass
    movie = dva.loc[va][3]
    character = dva.loc[va][1]
    try:
        if voice_actor.__contains__(";"):
            vaArray = voice_actor.split("; ")
            for j in range(len(vaArray)):
                cursor.execute(ins_qry_dva, (vaArray[j],movie, character))
        cursor.execute(ins_qry_dva, (vaArray[0],movie, character))
        conn.commit()
    except:
        print("error in operation")
        conn.rollback()

#voice actors from the movie The Little Mermaid
sel_a_1 = "select voice_actor from voice_actors where movie = 'The Little Mermaid';"
cursor.execute(sel_a_1)

rows_a_1 = cursor.fetchall()

#for r in rows_a_1:
#    print(r)

#characters who have more than one voice_actor
sel_a_2 = "select character from voice_actors group by character having count(character) > 1;"
cursor.execute(sel_a_2)

rows_a_2 = cursor.fetchall()

#for r in rows_a_2:
#    print(r)

# heroi do filme em que o nome do diretor começa com a letra B e tem mais de 5 atores
#TODO ta bugado, nao aparece mais heros
sel_b_1 = '''select hero 
from characters
inner join directors on directors.name = characters.movie_title and directors.director like 'B%'
inner join voice_actors on voice_actors.movie = characters.movie_title group by voice_actors.movie having count(voice_actors.movie) > 5;'''

cursor.execute(sel_b_1)

rows_b_1 = cursor.fetchall()

#for r in rows_b_1:
#    print(r)

# Todos os vilões que têm um voice_actor que não trabalhou com o Diretor "Byron Howard" TODO:Testar melhor   
sel_b_2 = '''select villain
from characters 
inner join voice_actors on characters.movie_title = voice_actors.movie and characters.villain = voice_actors.character 
left join directors on characters.movie_title = directors.name and directors.director == "Byron Howard";'''
cursor.execute(sel_b_2)

rows_b_2 = cursor.fetchall()

#for r in rows_b_2:
#    print(r)

#insert into directors "Stephen Hillenburg" with the movie name "Spongebob Squarepants"
ins_c_1 = '''insert into directors (director, name) values ("Stephen Hillenburg","Spongebob Squarepants");'''
cursor.execute(ins_c_1)

#update the jungle book Villain to balu
up_c_2 = '''update characters
            set villain = "Balu"
            where movie_title = "The Jungle Book";'''
cursor.execute(up_c_2)
