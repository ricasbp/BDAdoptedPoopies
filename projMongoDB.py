import pandas as pd
from pymongo import MongoClient
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

# Delete index if it exists one to correctly run the query

disneyD.drop_indexes()
disneyVA.drop_indexes()
disneyC.drop_indexes()



# create an index in descending order, and return a result
resp = disneyVA.create_index( [("movie", -1)])
print("index response:", resp)


# create an index in ascending order, and return a result
resp = disneyC.create_index( [("movie_title", "hashed")] )
print("index response:", resp)


client.close() 



def performance(collection, query):
    #for x in range(1500):
    time_i = time.time()
    mydoc2 = collection.find(query)
    time_f = time.time()
    print('Time:  ', time_f - time_i)

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
]

#complexa1: heroi do filme em que nome diretor começa com letra B e tem mais de cinco atores

print("sel_comp1")
performanceAggregate(disneyC, sel_comp1query)
performanceAggregate(disneyC_i, sel_comp1query)