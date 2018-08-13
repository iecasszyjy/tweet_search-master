# -*- coding: utf-8 -*
# 获得爬取推文所需信息
import json
from Config_new import get_noau_config

_, db, r = get_noau_config()  # 数据库配置

def get_location(Location):  # 获取地点集合
    Locations = Location.split(',')
    Locations = [i.strip() for i in Locations]
    return list(set(Locations))

def get_year(Year):  # 获取起始年份
    if len(Year) > 5:
        Year_begin = '2013'
        Year_end = '2014'
    else:
        Year_begin = Year
        Year_end = Year
    return Year_begin, Year_end

def get_types(Disaster, Article):  # 获取事件触发词集合
    Type = Disaster + ',' + Article
    Types = Type.split(',')
    Types = [i.strip() for i in Types]
    return list(set(Types))

def get_query_str(event):  # 获取Twitter查询信息
    loc = get_location(event['event']['location'])
    trigger = get_types(event['event']['disaster'], event['event']['main_article'])
    year_begin, year_end = get_year(event['event']['year'])
    date_since = year_begin + '-01-01'
    date_until = year_end + '-12-31'
    # 注意查询格式必须形如(xxx OR xxx) (xxx OR xxx) since:xxxx-xx-xx until:xxxx-xx-xx
    return '(' + ' OR '.join(loc) + ')' + ' ' + '(' + ' OR '.join(trigger) + ')' +\
           ' ' + 'since:' + date_since + ' ' + 'until:' + date_until

def get_task():
    # 获取数据库中事件列表的前20条数据（got文件只能解析2012年以后的Twitter信息）
    events = db.event_list.find({})[:20]
    for event in events:
        q = get_query_str(event)
        message = {'q': q, 'f': ['&f=news', '', '&f=tweets'], 'num': 4000, 'event_id': event['_id']}
        print(message)
        # 把获取推文所需信息放入Redis数据库
        r.rpush('temp2', json.dumps(message))

if __name__ == '__main__':
    get_task()



