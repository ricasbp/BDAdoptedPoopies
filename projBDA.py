import pandas as pd
import sqlite3
from pymongo import MongoClient

dc = pd.read_csv('disney-characters.csv')
dd = pd.read_csv('disney-director.csv')
dva = pd.read_csv('disney-voice-actors.csv')

#-------------------------MONGODB-------------------------

client = MongoClient()

db = client.open_clusters_no_index#TODO
cluster = db.cluster#TODO

data_dc = dc.to_dict(orient = "records")
data_dd = dd.to_dict(orient = "records")
data_dva = dva.to_dict(orient = "records")

cluster.insert_many(data_dc)
cluster.insert_many(data_dd)
cluster.insert_many(data_dva)



#-------------------------SQLITE-------------------------

conn = sqlite3.connect('open_clusters_no_index.db')#TODO
c = conn.cursor()
df.to_sql('clusters', conn)#TODO





