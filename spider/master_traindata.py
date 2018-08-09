import json
from Config_new import get_noau_config

_, db, r = get_noau_config()

def get_location(Location):
    Location = re.sub('[^a-zA-Z ,]', '', Location)
    Locations = Location.split(',')
    Locations = [i.strip() for i in Locations]
    Locations.append(Location)
    Locations = ['"' + i + '"' for i in Locations if ' ' in i] + [i for i in Locations if ' ' not in i]
    return list(set(Locations))

def get_year(Year):
    try:
        Year_begin = Year.split('-')[0]
        Year_end = Year.split('-')[1]
    except IndexError:
        Year_begin = Year
        Year_end = Year
    return Year_begin, Year_end

def get_types(Type):
    Type = re.sub('[^a-zA-Z ,]', '', Type)
    Types = Type.split(',')
    Types = [i.strip() for i in Types]
    Types.append(Type)
    Types = ['"' + i + '"' for i in Types if ' ' in i] + [i for i in Types if ' ' not in i]
    return list(set(Types))


# def get_location(Location):
#     Locations = Location.split(',')
#     Locations = [i.strip() for i in Locations]
#     return list(set(Locations))
#
# def get_disaster(Disaster):
#     Disasters = Disaster.split(',')
#     Disasters = [i.strip() for i in Disasters]
#     return list(set(Disasters))

def get_query_str(event):
    loc = get_location(event['event']['location'])
    trigger = get_types(event['event']['disaster'])
    year_begin, year_end = get_year(event['event']['year'])
    date_since = year_begin + '-01-01'
    date_until = year_end + '-12-31'
    return '(' + ' OR '.join(loc) + ')' + ' ' + '(' + ' OR '.join(trigger) + ')' +\
           ' ' + 'since:' + date_since + ' ' + 'until:' + date_until

def get_task():
    events = db.event_list.find({}, {'_id': 1, 'event.location': 1, 'event.disaster': 1, 'event.year': 1})[:20]
    for event in events:
        q = get_query_str(event)
        message = {'q': q, 'f': ['&f=news', '', '&f=tweets'], 'num': 100, 'event_id': event['_id']}
        print(message)
        r.rpush('train_data', json.dumps(message))

if __name__ == '__main__':
    get_task()




