import pandas as pd
from pymongo import *
from restructdata import *
import time

dc = structCharacters()
dd = structDirectors()
dva = structVoiceActors()

#-------------------------MONGODB-------------------------

#Connection to DB
client = MongoClient()
db = client.open_disney

#Creates collections
disneyC = db.disneyC
disneyD = db.disneyD
disneyVA = db.disneyVA

data_dc = dc.to_dict(orient = "records")
data_dd = dd.to_dict(orient = "records")
data_dva = dva.to_dict(orient = "records")

#Drop documents if they're created
disneyC.drop()
disneyD.drop()
disneyVA.drop()

disneyD.drop_indexes()
disneyVA.drop_indexes()
disneyC.drop_indexes()

#Insert data into collections
disneyC.insert_many(data_dc)
disneyD.insert_many(data_dd)
disneyVA.insert_many(data_dva)

#SELECT voice actors from the movie The Little Mermaid
select1 = disneyVA.find({'movie':"The Little Mermaid"}, { 'voice-actor': 1})
#for s in select1:
#    print (s.get('voice-actor'))

#SELECT characters who have more than one voice_actor
select2 = disneyVA.find({"voice_actor2" : { "$ne" : None}}, {"character" : 1})

# for s1 in select2:
#    print (s1)

#insert into directors "Stephen Hillenburg" para movie "Spongebob Squarepants"
ins1 = disneyD.insert_one({'director':'Stephen Hillenburg','movie':'Spongebob Squarepants'})

#update in characters in "The Jungle Book" villain from current to Balu
upd = disneyC.update_one({"movie_title": "The Jungle Book"},{"$set":{"villain":"Baloo Bear"}})

#complex:

#SELECT heros from the movie which the directors name starts with "B" and it has more than 12 voice actors
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

#SELECT villains from the movie where the voice actor of the villain didn't work with the Director "Ron Clements"
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

# creation of the database, for indexing next
db_i = client.open_disney_index

# creation of the collections
disneyC_i = db_i.disneyC_i
disneyD_i = db_i.disneyD_i
disneyVA_i = db_i.disneyVA_i

disneyC_i.drop()
disneyD_i.drop()
disneyVA_i.drop()

disneyC_i.insert_many(data_dc)
disneyD_i.insert_many(data_dd)
disneyVA_i.insert_many(data_dva)

# Delete index if it exists one to correctly run the query

disneyD_i.drop_indexes()
disneyVA_i.drop_indexes()
disneyC_i.drop_indexes()

disneyVA_i.create_index( [("movie" , TEXT), ("character", ASCENDING)])

disneyD_i.create_index( [("director" , ASCENDING), ("name" , TEXT)])

disneyC_i.create_index( [("movie_title", TEXT), ("villain", 1), ("hero", 1), ("disney_voiceactor",1)])

def performance(collection, query):
    time_i = time.time()
    mydoc2 = collection.find(query)
    time_f = time.time()
    print('Time:  ', time_f - time_i)

#select voice actors from the movie The Little Mermaid
select1query = { "movie":"The Little Mermaid",  "voice-actor": 1 }

#select the characters that have more than one voice actor
select2query = { "voice_actor2" : { "$ne" : None}, "character" : 1 }

print("select1query")
performance(disneyVA, select1query)
performance(disneyVA_i, select1query)

print("select2query")
performance(disneyVA, select2query)
performance(disneyVA_i, select2query)

def performanceAggregate(collection, query):
    time_i = time.time()
    mydoc2 = collection.aggregate(query)
    time_f = time.time()
    print('Time: ', time_f - time_i)

sel_comp1query = [
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
    "$match":{"count":{"$gt":12},
            "disney_director.director": { "$regex": "^B" }  
        }
    },
    {
    "$project" : {
        "hero" : 1
        }
    }
]

#complexa1: heroi do filme em que nome diretor começa com letra B e tem mais de cinco atores

print("sel_comp1")
performanceAggregate(disneyC, sel_comp1query)
performanceAggregate(disneyC_i, sel_comp1query)

sel_comp2query = [
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
]

print("sel_comp2")
performanceAggregate(disneyC, sel_comp2query)
performanceAggregate(disneyC_i, sel_comp2query)

client.close()