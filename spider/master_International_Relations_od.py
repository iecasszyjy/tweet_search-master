# -*- coding: utf-8 -*
# 获得爬取“宗教”推文所需信息
import json
from Config_International_Relations_od import get_noau_config
from datetime import datetime, timedelta

_, db, r = get_noau_config()  # 数据库配置


def get_date(date):
    # 获取日期集合
    dates = date.split('\n')
    date_list = []
    for i in dates:
        if not len(i.strip()) == 0:
            date_list.append(i.strip())
    return list(set(date_list))


def get_location(location, gpe):
    # 获取地点集合
    locations = location.split('\n')
    gpes = gpe.split('\n')
    location_list = []
    for i in locations:
        if not len(i.strip()) == 0:
            location_list.append(i.strip())
    for j in gpes:
        if not len(j.strip()) == 0:
            if j.strip not in location_list:
                location_list.append(j.strip())
    return list(set(location_list))


def get_gpe(gpe):
    # 获取GPE集合
    gpes = gpe.split('\n')
    gpe_list = []
    for i in gpes:
        if not len(i.strip()) == 0:
            gpe_list.append(i.strip())
    return list(set(gpe_list))


def get_person(person):
    # 获取person集合
    persons = person.split('\n')
    person_list = []
    for i in persons:
        if not len(i.strip()) == 0:
            person_list.append(i.strip())
    return list(set(person_list))


def get_triggers(trigger):
    # 获取事件触发词集合
    triggers = trigger.split('\n')
    trigger_list = []
    for i in triggers:
        if not len(i.strip()) == 0:
            trigger_list.append(i.strip())
    return list(set(trigger_list))


def get_query_str(event):
    # 获取Twitter查询信息
    loc = get_location(event['event']['loc'], event['event']['gpe'])
    per = get_person(event['event']['per'])
    trigger = get_triggers(event['event']['trigger'])
    date = event['event']['date']
    date = date.strip()
    temp = datetime.strptime(date, "%Y-%m-%d")
    date_since = (temp - timedelta(days=7)).strftime('%Y-%m-%d')
    date_until = (temp + timedelta(days=7)).strftime('%Y-%m-%d')
    # 注意查询格式必须形如(xxx OR xxx) (xxx OR xxx) since:xxxx-xx-xx until:xxxx-xx-xx   # 暂时不加地点
    return '(' + ' OR '.join(loc) + ')' + ' ' + '(' + ' OR '.join(per) + ')' + ' ' \
           + '(' + ' OR '.join(trigger) + ')' + 'since:' + date_since + ' ' + 'until:' + date_until


# def get_query_list(evs):
#     loc_list = []
#     per_list = []
#     trigger_list = []
#     for ev in evs:
#         loc = get_location(ev['event']['loc'], ev['event']['gpe'])
#         per = get_person(ev['event']['per'])
#         trigger = get_triggers(ev['event']['trigger'])
#         loc_list.append(loc)
#         per_list.append(per)
#         trigger_list.append(trigger)
#     return loc_list, per_list, trigger_list


# def get_query_str(event, loc, per, trigger):
#     # 获取Twitter查询信息
#     date = event['event']['date']
#     date = date.strip()
#     temp = datetime.strptime(date, "%Y-%m-%d")
#     date_since = (temp - timedelta(days=7)).strftime('%Y-%m-%d')
#     date_until = (temp + timedelta(days=7)).strftime('%Y-%m-%d')
#     # 注意查询格式必须形如(xxx OR xxx) (xxx OR xxx) since:xxxx-xx-xx until:xxxx-xx-xx   # 暂时不加地点
#     return '(' + ' OR '.join(loc) + ')' + ' ' + '(' + ' OR '.join(per) + ')' + ' '\
#            + '(' + ' OR '.join(trigger) + ')' + 'since:' + date_since + ' ' + 'until:' + date_until
#     # return '(' + ' OR '.join(trigger) + ')' + ' ' + 'since:' + date_since + ' ' + 'until:' + date_until


def get_task():
    events = db.event_list.find({})
    # _loc, _per, _trigger = get_query_list(events)
    for event in events:
        q = get_query_str(event)
        message = {'q': q, 'f': ['&f=news', '', '&f=tweets'], 'num': 10000, 'event_id': event['_id']}
        print(message)
        # 把获取推文所需信息放入Redis数据库
        r.rpush('International_Relations_od', json.dumps(message))
    print('master_International_Relations_od done!')


if __name__ == '__main__':
    get_task()


