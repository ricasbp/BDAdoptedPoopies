import time
import sqlite3
from restructdata import *

dc = structCharacters()
dd = structDirectors()
dva = structVoiceActors()

#-------------------------SQLITE-------------------------

#Connection to DB
conn = sqlite3.connect('open_disney.db')
cursor = conn.cursor()


#Drop tables if they're created
cursor.execute('''DROP TABLE IF EXISTS characters;''')
cursor.execute('''DROP TABLE IF EXISTS directors;''')
cursor.execute('''DROP TABLE IF EXISTS voice_actors;''')

#Drop indexes if they're created
cursor.execute('''DROP INDEX IF EXISTS index_characters;''')
cursor.execute("DROP INDEX IF EXISTS index_VA")

#Creates tables
cursor.execute('''CREATE TABLE characters (
    movie_title TEXT(100) PRIMARY KEY,
    release_date DATE NOT NULL,
    hero TEXT(100),
    villain TEXT(100),
    song TEXT(100));''')

cursor.execute('''CREATE TABLE directors (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    director TEXT(100) NOT NULL,
    name TEXT(100),
    FOREIGN KEY(name) REFERENCES characters(movie_title));''')

cursor.execute('''CREATE TABLE voice_actors (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    voice_actor1 TEXT(100),
    voice_actor2 TEXT(100),
    movie TEXT(100),
    character TEXT(100) NOT NULL,
    FOREIGN KEY(movie) REFERENCES characters(movie_title));''')

#Insert data into characters table
ins_qry_dc = "insert into characters (movie_title, release_date, hero, villain, song) values (?,?,?,?,?);"
for c in range(len(dc)):
    movie_title = dc.loc[c][1]
    release_date = dc.loc[c][2]
    hero = dc.loc[c][3]
    villain = dc.loc[c][4]
    song = dc.loc[c][5]
    try:
        cursor.execute(ins_qry_dc, (movie_title,release_date,hero,villain,song))
        conn.commit()
    except:
        print("error in operation")
        conn.rollback()

#Creates index for faster selects
createSecondaryIndex = "CREATE INDEX index_characters ON characters(movie_title,hero,villain);"
cursor.execute(createSecondaryIndex)

#Insert data into directors table
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

#Insert data into voice_actors table
ins_qry_dva = "insert into voice_actors (voice_actor1, voice_actor2, movie, character) values (?,?,?,?);"
for va in range(len(dva)):
    voice_actor = dva.loc[va][2]
    voice_actor2 = dva.loc[va][4]
    movie = dva.loc[va][3]
    character = dva.loc[va][1]
    try:
        cursor.execute(ins_qry_dva, (voice_actor, voice_actor2, movie, character))
        conn.commit()
    except:
        print("error in operation")
        conn.rollback()

#Creates index for faster selects
cursor.execute("CREATE INDEX index_VA ON voice_actors(movie,character);")

#SELECT voice actors from the movie The Little Mermaid
sel_a_1 = "select voice_actor1 from voice_actors where movie = 'The Little Mermaid';"

time_i = time.time()

cursor.execute(sel_a_1)
rows_a_1 = cursor.fetchall()

time_f = time.time()
print('sel_a_1 time with no index: ', time_f-time_i)
#for r in rows_a_1:
#    print(r)

#SELECT characters who have more than one voice_actor
sel_a_2 = "select character from voice_actors where voice_actor2 is not null;"

time_i = time.time()

cursor.execute(sel_a_2)
rows_a_2 = cursor.fetchall()

time_f = time.time()
print('sel_a_2 time with no index: ', time_f-time_i)
#for r in rows_a_2:
#    print(r)

#SELECT heros from the movie which the directors name starts with "B" and it has more than 12 voice actors
sel_b_1 = '''select hero 
from characters
inner join directors on directors.name = characters.movie_title and directors.director like 'B%'
inner join voice_actors on voice_actors.movie = characters.movie_title group by voice_actors.movie having count(voice_actors.movie) > 12;'''

time_i = time.time()

cursor.execute(sel_b_1)
rows_b_1 = cursor.fetchall()

time_f = time.time()
print('sel_b_1 time with no index: ', time_f-time_i)
#for r in rows_b_1:
#    print(r)

#SELECT villains from the movie where the voice actor of the villain didn't work with the Director "Ron Clements"
sel_b_2 = '''select villain
from characters
inner join voice_actors on characters.movie_title = voice_actors.movie and characters.villain = voice_actors.character 
left join directors on characters.movie_title = directors.name and directors.director == "Ron Clements" where directors.director is null;'''

time_i = time.time()

cursor.execute(sel_b_2)
rows_b_2 = cursor.fetchall()

time_f = time.time()
print('sel_b_2 time with no index: ', time_f-time_i)
#for r in rows_b_2:
#    print(r)

#insert into directors "Stephen Hillenburg" with the movie name "Spongebob Squarepants"
ins_c_1 = '''insert into directors (director, name) values ("Stephen Hillenburg","Spongebob Squarepants");'''

time_i = time.time()

cursor.execute(ins_c_1)
time_f = time.time()
print('ins_c_1 time with no index: ', time_f-time_i)

#update the jungle book Villain to balu
up_c_2 = '''update characters
            set villain = "Baloo Bear"
            where movie_title = "The Jungle Book";'''

time_i = time.time()

cursor.execute(up_c_2)
cursor.fetchall()

time_f = time.time()
print('up_c_2 time with no index: ', time_f-time_i)