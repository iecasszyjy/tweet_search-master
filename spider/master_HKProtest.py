# -*- coding: utf-8 -*
# 获得爬取推文所需信息
# import json
from Config_2019 import get_noau_config
# from datetime import datetime, timedelta

_, db = get_noau_config()  # 数据库配置

def get_task():
    conditions = db.conditions.find({})
    stime, etime, locations, triggers = '','','',''
    for condition in conditions:
        if condition['stime']:
            stime = condition['stime']
        elif condition['etime']:
            etime = condition['etime']
        elif condition['locations']:
            locations = condition['locations']
        elif condition['triggers']:
            triggers = condition['triggers']
    q = '(' + locations + ')' + ' ' + '(' + triggers + ')' + ' ' + 'since:' + stime + ' ' + 'until:' + etime
    actionId = hash(stime + etime + locations + triggers)
    message = {'q': q, 'f': ['&f=news', '', '&f=tweets'], 'num': 50000, 'action_id': actionId}
    print('推文查询语句组织完成')
    print(message)
    # # 把获取推文所需信息放入Redis数据库
    # r.rpush('2019HongKong_protest', json.dumps(message))
    # 把获取推文所需的特定格式信息存入mongo
    searchInfo = db.searchInfo
    if searchInfo.find_one({'id': actionId}) is None:
        searchInfo.insert_one({'id': actionId, 'q': q, 'f': ['&f=news', '', '&f=tweets'], 'num': 50000})
        print('推文查询语句存储到mongo/2019HongKong_protest/searchInfo完成')

if __name__ == '__main__':
    get_task()


