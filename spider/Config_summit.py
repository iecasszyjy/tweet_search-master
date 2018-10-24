# -*- coding:utf-8 -*-
# got文件、MongoDB数据库和Redis数据库配置文件
import os
import sys

import pymongo
import redis

def get_noau_config():
    # got文件
    if sys.version_info[0] < 3:
        import got
    else:
        import got3 as got

    # MongoDB数据库
    client = pymongo.MongoClient(os.environ['MONGOHOST'], 27017)
    db = client.summit
    db.authenticate(name='iecas', password='szyjy')

    # Redis数据库
    r = redis.StrictRedis(host=os.environ['REDISHOST'], port=6379, db=0)

    return got, db, r
