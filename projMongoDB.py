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



