import pandas as pd
from pymongo import MongoClient
from restructdata import *

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

#select the characters that have more than one voice actor TODO reformular consoante as mudanças feitas
select2 = disneyVA.aggregate([{"$group":{"_id":"$character", "count":{"$sum":1}}},
                              {"$match":{"count":{"$gt":1}}}])

#for s1 in select2:
#    print (s1.get('voice_actor'), s1.get('character'))

#insert into directors "Stephen Hillenburg" para movie "Spongebob Squarepants"
ins1 = disneyD.insert_one({'director':'Stephen Hillenburg','movie':'Spongebob Squarepants'})

#update in characters in "The Jungle Book" villain from current to Balu
upd = disneyC.update_one({'movie_title': 'The Jungle Book'},{"$set":{'villain':'Balu'}})


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
    "$project" :
        {
            "villain" : 1
        }
    },
])


for s2 in sel_comp2:
    print (s2)
