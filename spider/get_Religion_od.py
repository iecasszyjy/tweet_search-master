# -*- coding:utf-8 -*-
# '开放域'Religion事件查询列表获取，并存入数据库
# 查询列表构成{日期+地点+宗教名称+宗教触发词}

import pymongo
from pymongo import InsertOne
from pymongo.errors import BulkWriteError

import random
from pprint import pprint
from tqdm import tqdm

client = pymongo.MongoClient('54.161.160.206:27017')  # 与服务器上的MongoDB数据库建立连接
db_auth = client.admin
db_auth.authenticate(name='iecas', password='szyjy')
db_w = client.Religion_od  # 写入的数据库名称
db_r = client.currentEvent  # 读出的数据库

def get_events():
    # 从current event中读出需要的信息，然后加上自己总结的触发词，最后存入数据库
    religion = 'Taoism\nShinto\nFalun Gong\nSikhism\nJudaism\nKorean shamanism\nCaodaism\nTenriism\nJainism\n' \
               'Cheondoism\nHoahaoism\nChristianity\nIslam\nHinduism\nBuddhism\nFolk religion'  # 世界宗教名
    events = []
    query = db_r.Religion.find({})
    for i in query:
        if not len(i['information']['LOCATION']) == 0:
            triggers = 'religion\nreligions\nCardinals\nChurch\nbishop\nArchbishop\nPope\nFrancis\nMuslims\nCatholic\n'\
                       'Vatican\nPapal\nconclave\nArchdiocese\nSynod\nconclave\nSistine\nSaint\n'    # Religion事件触发词
            date = i['information']['DATE']
            locs = i['information']['LOCATION']
            if not len(i['information']['EVENT']) == 0:
                triggers += '\n' + i['information']['EVENT']
            events.append({'date': date, 'loc': locs, 'religion': religion,  'trigger': triggers})
    return events


if __name__ == '__main__':
    events = get_events()
    # 事件信息放入MongoDB数据库
    requests_ = [InsertOne({'_id': hash(i['date'] + i['loc'] + i['religion'] + i['trigger'] + str(random.randint(0, 100))), 'event': i})
                 for i in tqdm(events)]
    try:
        result = db_w.event_list.bulk_write(requests_)
        pprint(result.bulk_api_result)
    except BulkWriteError as bwe:
        pprint(bwe.details)
    client.close()

