import pandas as pd
from datetime import datetime

def structCharacters():
    dc = pd.read_csv('disney-characters.csv')
    for c in range(len(dc)):
        movie_title = dc.loc[c][1].replace('\n','')
        dc["movie_title"] = dc["movie_title"].replace([dc.loc[c][1]],movie_title)

        release_date = datetime.strptime(dc.loc[c][2], '%B %d, %Y')
        dc["release_date"] = dc["release_date"].replace([dc.loc[c][2]], release_date)

    return dc


def structDirectors():
    dd = pd.read_csv('disney-director.csv')
    return dd

def structVoiceActors():
    dva = pd.read_csv('disney-voice-actors.csv')

    voice_actor2 = []

    for va in dva:
        voice_actor2.append(None)

    #TODO criar nova coluna no dataframe
    dva = dva.assign( voice_actor2 = voice_actor2)

    for va in range(len(dva)):
        voice_actor = dva.loc[va][2]
        vaArray = voice_actor.split("; ")

        dva['voice-actor'] = dva['voice-actor'].replace([dva.loc[va][2]],vaArray[0])
        dva['voice_actor2'] = dva['voice_actor2'].replace([dva.loc[va][4]],vaArray[1])
    return dva