# -*- coding: utf-8 -*
# 获得爬取“宗教”推文所需信息
import json
from Config_Religion_od import get_noau_config
from datetime import datetime, timedelta

_, db, r = get_noau_config()  # 数据库配置


def get_year(Year):  # 获取起始年份
    if len(Year) > 5:
        Year_begin = '2013'
        Year_end = '2014'
    else:
        Year_begin = Year
        Year_end = Year
    return Year_begin, Year_end


def get_date(date):
    # 获取日期集合
    dates = date.split('\n')
    date_list = []
    for i in dates:
        if not len(i.strip()) == 0:
            date_list.append(i.strip())
    return list(set(date_list))


def get_location(location):
    # 获取地点集合
    locations = location.split('\n')
    location_list = []
    for i in locations:
        if not len(i.strip()) == 0:
            location_list.append(i.strip())
    return list(set(location_list))


def get_triggers(trigger):
    # 获取事件触发词集合
    triggers = trigger.split('\n')
    trigger_list = []
    for i in triggers:
        if not len(i.strip()) == 0:
            trigger_list.append(i.strip())
    return list(set(trigger_list))


def get_religions(religion):
    # 获取宗教名称集合
    religions = religion.split('\n')
    religion_list = []
    for i in religions:
        if not len(i.strip()) == 0:
            religion_list.append(i.strip())
    return list(set(religion_list))


def get_query_str(event):
    # 获取Twitter查询信息
    # loc = get_location(event['event']['loc'])
    trigger = get_triggers(event['event']['trigger'])
    religion = get_religions(event['event']['religion'])
    date_since = (temp - timedelta(days=7)).strftime('%Y-%m-%d')
    date_until = (temp + timedelta(days=7)).strftime('%Y-%m-%d')
    # year_begin, year_end = get_year(event['event']['year'])
    # date_since = '2012-01-01'
    # date_until = '2018-10-28'
    # 注意查询格式必须形如(xxx OR xxx) (xxx OR xxx) since:xxxx-xx-xx until:xxxx-xx-xx   # 暂时不加地点
    # return '(' + ' OR '.join(loc) + ')' + ' ' + '(' + ' OR '.join(trigger) + ')' + '(' + ' OR '.join(religion) + ')'\
    #        ' ' + 'since:' + date_since + ' ' + 'until:' + date_until
    return '(' + ' OR '.join(trigger) + ')' + ' ' + 'since:' + date_since + ' ' + 'until:' + date_until


def get_task():
    events = db.event_list.find({})
    for event in events:
        q = get_query_str(event)
        message = {'q': q, 'f': ['&f=news', '', '&f=tweets'], 'num': 10000, 'event_id': event['_id']}
        print(message)
        # 把获取推文所需信息放入Redis数据库
        r.rpush('Religion_od', json.dumps(message))
    print('master_Religion_od done!')



if __name__ == '__main__':
    get_task()


