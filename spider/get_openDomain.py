# -*- coding:utf-8 -*-
# '开放域'事件获取
import pymongo
from pymongo import InsertOne
from pymongo.errors import BulkWriteError

import random
from pprint import pprint
from tqdm import tqdm

client = pymongo.MongoClient('54.161.160.206:27017')  # 与服务器上的MongoDB数据库建立连接
db_auth = client.admin
db_auth.authenticate(name='iecas', password='szyjy')
db = client.openDomain  #开放域数据库名称


def get_events():  # 事件较少，不使用解析网页方法，直接列出关键词放入数据库
    events = []
    # 事件日期列表
    dates = ['2018-03-25',
             '2018-04-27',
             '2018-05-07',
             '2018-05-26',
             '2018-06-12',
             '2018-06-19']

    locs = ['Beijing OR China OR first',
            '(Peace House) OR DMZ OR Panmunjom OR first',
            'Dalian OR China OR second OR 2nd',
            'DMZ OR Panmunjom OR second OR 2nd',
            'Singapore OR Sentosa',
            'Beijing OR China OR third OR 3rd']

    partners = ['Xi',
              'Moon',
              'Xi',
              'Moon',
              'Trump',
              'Xi']

    for i in range(6):
        events.append({
            'date': dates[i],
            'loc': locs[i],
            'partner': partners[i]
        })
    return events

if __name__ == '__main__':
    events = get_events()
    # 事件信息放入MongoDB数据库
    requests_ = [InsertOne({'_id': hash(i['date'] + i['loc'] + i['partner'] + str(random.randint(0, 100))), 'event': i}) for i in tqdm(events)]
    try:
        result = db.event_list.bulk_write(requests_)
        pprint(result.bulk_api_result)
    except BulkWriteError as bwe:
        pprint(bwe.details)
    client.close()

