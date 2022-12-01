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


ins_comp = disneyC.aggregate([

    # Join with director table
    {
        "$lookup":
        {
            "from": "disneyD",       # other table name
            #"localField": "movie_title",   # name of disneyD table field
            #"foreignField": "name", # name of userinfo table field
            "let": { "disneyC_title": "$name" },
            "pipeline": [
                { "$match":
                    { "$expr":
                        { "$and":
                            [
                                { "$regexMatch": {"input":"$director", "regex": 'B%'}},
                                { "$eq":[ "$movie_title", "$$disneyC_title"] }
                            ]
                        }
                    }
                }
            ],
            "as": "disney_director"         # alias for userinfo table
        },
    },
    # Join with voice_actor table
    # define which fields you want to fetch
    {   
        "$project":{
            "hero" : 1
        } 
    }
])

for s1 in ins_comp:
    print (s1.get('hero'), s1.get('voice_actor'))


"""
sel_b_1 = '''select hero 
from characters
inner join directors on directors.name = characters.movie_title and directors.director like 'B%'
inner join voice_actors on voice_actors.movie = characters.movie_title group by voice_actors.movie having count(voice_actors.movie) > 5;'''
"""

"""
    {
        "$lookup":{
            "from": "disneyVA", 
            "localField": "movie_title", 
            "foreignField": "movie",
            "as": "disney_voiceactor"
        },
    },
    {
        "$group":{"_id":"$movie", "count":{"$sum":1}}
    },
                    
    {
        "$match":{"count":{"$gt":12}}
    },"""