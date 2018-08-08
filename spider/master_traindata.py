import re
import json
from datetime import datetime, timedelta

from Config_new import get_noau_config

_, db, r = get_noau_config()

def get_location(Location):
    Locations = Location.split(',')
    Locations = [i.strip() for i in Locations]
    return list(set(Locations))

def get_disaster(Disaster):
    Disasters = Disaster.split(',')
    Disasters = [i.strip() for i in Disasters]
    return list(set(Disasters))

def get_query_str(event):
    year = event['event']['year']
    disaster = get_disaster(event['event']['disaster'])
    article = event['event']['main_article']
    loc = get_location(event['event']['location'])
    return '(' + year + ')' + '(' + ' OR '.join(disaster) + ')' + '(' + article + ')' + '(' + ' OR '.join(loc) + ')'

def get_task():
    events = db.event_list.find({})
    for event in events:
        q = get_query_str(event)
        message = {'q': q, 'f': ['&f=news', '', '&f=tweets'], 'num': 100, 'event_id': event['_id']}
        print(message)
        r.rpush('train_data', json.dumps(message))

if __name__ == '__main__':
    get_task()


