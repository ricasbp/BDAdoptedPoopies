import pandas as pd
from pymongo import MongoClient
from restructdata import *
import time

dc = structCharacters()
dd = structDirectors()
dva = structVoiceActors()

#-------------------------MONGODB-------------------------

client = MongoClient()

db = client.open_disney

disneyC = db.disneyC
disneyD = db.disneyD
disneyVA = db.disneyVA

data_dc = dc.to_dict(orient = "records")
data_dd = dd.to_dict(orient = "records")
data_dva = dva.to_dict(orient = "records")

disneyC.drop()
disneyD.drop()
disneyVA.drop()

disneyC.insert_many(data_dc)
disneyD.insert_many(data_dd)
disneyVA.insert_many(data_dva)

#select voice actors from the movie The Little Mermaid
select1 = disneyVA.find({'movie':"The Little Mermaid"}, { 'voice-actor': 1})
#for s in select1:
#    print (s.get('voice-actor'))

#select the characters that have more than one voice actor
select2 = disneyVA.find({"voice_actor2" : { "$ne" : None}}, {"character" : 1})

# for s1 in select2:
#    print (s1)

#insert into directors "Stephen Hillenburg" para movie "Spongebob Squarepants"
ins1 = disneyD.insert_one({'director':'Stephen Hillenburg','movie':'Spongebob Squarepants'})

#update in characters in "The Jungle Book" villain from current to Balu
upd = disneyC.update_one({"movie_title": "The Jungle Book"},{"$set":{"villain":"Baloo Bear"}})

#complex:
#complexa1: heroi do filme em que nome diretor começa com letra B e tem mais de cinco atores

#from disneyD where name: /^t/
#insc = disneyD.find({'name': /^B/},{'director':1})
#insc1 = disneyVA.aggregate([{"$group":{_id:"$movie", count:{"$sum":1}}},
                              #{"$match":{"count":{"$gt":5}}}])

sel_comp1 = disneyC.aggregate([
    {
    # Join with director table
    "$lookup": {
        "from": "disneyD",       # other table name
        "localField": "movie_title",   # name of disneyD table field
        "foreignField": "name", # name of userinfo table field
        "as": "disney_director",        # alias for userinfo table
    }
    },
    {
    "$unwind" : "$disney_director"
    }, 
    {
    "$match": {
        "disney_director.director": { "$regex": "^B" }  
        }
    },
    {
    "$lookup":{
        "from": "disneyVA", 
        "localField": "movie_title", 
        "foreignField": "movie",
        "as": "disney_voiceactor"
        },
    },
    {
    "$unwind" : "$disney_voiceactor"
    },
    {
    "$group":{"_id":"$disney_voiceactor.movie", 
        "count":{"$sum":1},
        "hero" : {"$first" : "$hero"}
        }
    },
    {   
    "$match":{"count":{"$gt":12}}
    },
    {
    "$project" : {
        "hero" : 1
        }
    }
])

# for s1 in sel_comp1:
#     print (s1.get("hero"))

# Todos os vilões que têm um voice_actor que não trabalhou com o Diretor "Ron Clements"
'''select villain
from characters
inner join voice_actors on characters.movie_title = voice_actors.movie and characters.villain = voice_actors.character 
left join directors on characters.movie_title = directors.name and directors.director == "Ron Clements" where directors.director is null;'''

sel_comp2 = disneyC.aggregate([
    {
    "$lookup": {
        "from": "disneyVA", 
        "localField": "movie_title", 
        "foreignField": "movie",
        "as": "disney_voiceactor"
        },
    },
    {
    "$unwind" : "$disney_voiceactor"
    },
    {
    "$match" : { 
        "$expr" : {
            "$eq" : ["$disney_voiceactor.character","$villain"]}
        }
    },
    {
    "$lookup":{
        "from": "disneyD", 
        "localField": "movie_title", 
        "foreignField": "name",
        "as": "disney_director"
        },
    },
    {
    "$unwind" : "$disney_director"
    }, 
    {
    "$match": {
        "disney_director.director": {"$ne": "Ron Clements"}
        }
    },
    {
    "$project" :
        {
            "villain" : 1
        }
    },
])


# for s2 in sel_comp2:
#     print(s2)


#
# 4. Indexes
# 

# connection to mongo
client = MongoClient()

# creation of the database, for indexing next
db = client.open_disney_index

# creation of the collections
disneyC = db.disneyC
disneyD = db.disneyD
disneyVA = db.disneyVA

data_dc = dc.to_dict(orient = "records")
data_dd = dd.to_dict(orient = "records")
data_dva = dva.to_dict(orient = "records")

disneyC.drop()
disneyD.drop()
disneyVA.drop()

disneyC.insert_many(data_dc)
disneyD.insert_many(data_dd)
disneyVA.insert_many(data_dva)


disneyVA.create_index("sortmovieNameZtoA")
disneyD.create_index("sortDirectorNameZtoA")

# create an index in descending order, and return a result
resp = disneyVA.create_index([("movie", -1)])
print("index response:", resp)

# create an index in ascending order, and return a result
resp = disneyD.create_index([("director", 1)])
print("index response:", resp)

client.close() 


def performance(collection, query):
    timeTotal = 0
    for x in range(1500):
        time_i = time.time()
        mydoc2 = collection.find(query)
        time_f = time.time()
        timeTotal = timeTotal + (time_f - time_i)
    print('Average Time of 1500 runs: ', timeTotal/1500)

# connection to mongo without indexes
client = MongoClient()
db = client.open_disney
disneyC = db.disneyC
disneyD = db.disneyD
disneyVA = db.disneyVA

# connection to mongo with indexes
client2 = MongoClient()
db2 = client2.open_disney_index
disneyC_i = db2.disneyC
disneyD_i = db2.disneyD
disneyVA_i = db2.disneyVA


#select voice actors from the movie The Little Mermaid
select1query = { "movie":"The Little Mermaid",  "voice-actor": 1 }

#select the characters that have more than one voice actor
select2query = { "voice_actor2" : { "$ne" : None}, "character" : 1 }


print("select1query")
performance(disneyVA, select1query)
performance(disneyVA_i, select1query)

# Ainda nao consegui fazer para o aggregate
def performanceAggregate(collection, query):
    timeTotal = 0
    for x in range(1500):
        time_i = time.time()
        mydoc2 = collection.aggregate(query)
        time_f = time.time()
        timeTotal = timeTotal + (time_f - time_i)
    print('Average Time of 1500 runs: ', timeTotal/1500)

# sel_comp1query = [{
    
#     # Join with director table
#     "$lookup": {
#         "from": "disneyD",       # other table name
#         "localField": "movie_title",   # name of disneyD table field
#         "foreignField": "name", # name of userinfo table field
#         "as": "disney_director",        # alias for userinfo table
#     }
#     , 
#     "$unwind" : "$disney_director"
#     , 
#     "$match": {
#         "disney_director.director": { "$regex": "^B" }  
#         }
#     ,
#     "$lookup":{
#         "from": "disneyVA", 
#         "localField": "movie_title", 
#         "foreignField": "movie",
#         "as": "disney_voiceactor"
#         }
#     ,
#     "$unwind" : "$disney_voiceactor"
#     ,
#     "$group":{"_id":"$disney_voiceactor.movie", 
#         "count":{"$sum":1},
#         "hero" : {"$first" : "$hero"}
#         }
#     ,
#     "$match":{"count":{"$gt":12}}
#     ,
#     "$project" : {
#         "hero" : 1
#         }
    
# }]


#print("sel_comp1")
#performanceAggregate(disneyD, sel_comp1query)
#performanceAggregate(disneyD_i, sel_comp1query)