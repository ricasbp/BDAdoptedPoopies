import pandas as pd
import sqlite3
from pymongo import MongoClient

dc = pd.read_csv('disney-characters.csv')
dd = pd.read_csv('disney-director.csv')
dva = pd.read_csv('disney-voice-actors.csv')


