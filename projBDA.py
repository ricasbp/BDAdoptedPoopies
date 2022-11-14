import pandas as pd
import sqlite3
from pymongo import MongoClient

dc = pd.read_csv('disney-characters.csv')
dd = pd.read_csv('disney-director.csv')
dva = pd.read_csv('disney-voice-actors.csv')

#-------------------------MONGODB-------------------------

client = MongoClient()

db = client.open_disney#TODO
disney = db.disney#TODO

data_dc = dc.to_dict(orient = "records")
data_dd = dd.to_dict(orient = "records")
data_dva = dva.to_dict(orient = "records")

disney.insert_many(data_dc)
disney.insert_many(data_dd)
disney.insert_many(data_dva)



#-------------------------SQLITE-------------------------

conn = sqlite3.connect('open_disney.db')#TODO
c = conn.cursor()
dc.to_sql('disney-characters', conn)#TODO
dd.to_sql('disney-director', conn)#TODO
dva.to_sql('disney-voice-actors', conn)#TODO



