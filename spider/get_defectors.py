# -*- coding:utf-8 -*-
# '脱北者'事件获取
import pymongo
from pymongo import InsertOne
from pymongo.errors import BulkWriteError

import random
from pprint import pprint
from tqdm import tqdm

client = pymongo.MongoClient('54.161.160.206:27017')  # 与服务器上的MongoDB数据库建立连接
db = client.nk_defect

def get_events():  # 事件较少，不使用解析网页方法，直接列出关键词放入数据库
    events = []
    # 事件日期列表
    dates = ['2016-04-11',
             '2016-08-17',
             '2016-08-17',
             '2017-11-13',
             '2017-12-20']
    # '脱北者'事件关键词列表
    types = ['intelligence OR colonel',
             'restaurant',
             'diplomat',
             'soldier',
             'warning shots']

    for i in range(5):
        events.append({
            'date': dates[i],
            'type': types[i],
        })
    return events

if __name__ == '__main__':
    events = get_events()
    # 事件信息放入MongoDB数据库
    requests_ = [InsertOne({'_id': hash(i['date'] + i['type'] + str(random.randint(0, 100))), 'event': i}) for i in tqdm(events)]
    try:
        result = db.event_list.bulk_write(requests_)
        pprint(result.bulk_api_result)
    except BulkWriteError as bwe:
        pprint(bwe.details)
    client.close()

