import pandas as pd
from pymongo import MongoClient

dc = pd.read_csv('disney-characters.csv')
dd = pd.read_csv('disney-director.csv')
dva = pd.read_csv('disney-voice-actors.csv')

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

select1 = disneyVA.find({'movie':{'$eq':"The Little Mermaid"}})
for s in select1:
    print (s.get('voice_actor'), s.get('movie'))

select2 = disneyVA.aggregate([{"$group":{_id:"$character", count:{"$sum":1}}},
                              {"$match":{"count":{"$gt":1}}}])

for s1 in select2:
    print (s1.get('voice_actor'), s1.get('character'))

