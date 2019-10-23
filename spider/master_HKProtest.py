# -*- coding: utf-8 -*
# 获得爬取推文所需信息
# import json
from Config_2019 import getMongoClient, closeMongoClient

def get_task():
    event_list = list(db.event_list.find())
    stime, etime, locations, triggers = '','','',''
    print('2019HongKong_protest event_list num:' + str(len(event_list)))
    client = getMongoClient()
    db = client['2019HongKong_protest']
    searchInfo = db.searchInfo
    for event in event_list:
        print(event)
        # print(type(event))
        stime = event['event']['stime']
        etime = event['event']['etime']
        location = event['event']['location']
        triggers = event['event']['triggers']
        topics = event['event']['topics']
        q = '(' + location + ')' + ' ' + triggers + ' ' + topics + ' ' + 'since:' + stime + ' ' + 'until:' + etime
        actionId = event['id']
        message = {'q': q, 'f': ['&f=news', '', '&f=tweets'], 'num': 50000, 'action_id': actionId}
        print(message)
        # 把获取推文所需的特定格式信息存入mongo
        if searchInfo.find_one({'id': actionId}) is None:
            searchInfo.insert_one({'id': actionId, 'q': q, 'f': ['&f=news', '', '&f=tweets'], 'num': 50000})
    print('推文查询语句存储到mongo/2019HongKong_protest/searchInfo完成')
    closeMongoClient(client)

if __name__ == '__main__':
    get_task()


