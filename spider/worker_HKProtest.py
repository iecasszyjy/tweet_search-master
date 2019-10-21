# -*- coding:utf-8 -*-
# 爬取推文
# import time
import json

import multiprocessing
from multiprocessing import Pool

from Config_2019 import get_noau_config

got, db = get_noau_config()  # 文件和数据库配置

def advance_search_dataset(q, f, num, actionId):  # 获取推文，放入MongoDB数据库
    collection = db.dataset
    tweetCriteria = got.manager.TweetCriteria().setQuerySearch(q).setTweetType(f).setMaxTweets(num)
    tweets = got.manager.TweetManager.getTweets(tweetCriteria)
    for tweet in tweets:
        if collection.find_one({'id': tweet['id']}) is None:  # 查看推文是否已存在，若不存在则放入数据库
            collection.insert_one({'id': tweet['id'], 'tweet': tweet, 'actionId': actionId, 'f': f, 'q': q})

def run_dataset_task(actionId, q, f, num):
    try:
        if type(f) != list:
            advance_search_dataset(q, f, num, actionId)
        else:
            # 多进程获取推文
            pool = Pool(processes=multiprocessing.cpu_count())
            # python官方建议：废弃apply，使用apply_async（使用apply在这里会出错）
            [pool.apply_async(advance_search_dataset, (q, fi, num, actionId)) for fi in f]
            # [pool.apply(advance_search_dataset, (q, f, num, event_id)) for f in message_data['f']]
            pool.close()
            pool.join()
        return True
    except:
        return False


if __name__ == '__main__':
    print '2019HongKong_protest craw_worker start!'
    searchInfo = db.searchInfo
    searchInfoResult = searchInfo.find()
    for item in searchInfoResult:
        print '2019HongKong_protest craw_worker process!'
        actionId = item['id']
        q = item['q']
        f = item['f']
        num = item['num']
        craw = run_dataset_task(actionId, q, f, num)  # 获取推文
        if craw:
            # 添加查询日志
            db.dataset_log.insert_one({'action': json.loads(item), 'status': 1})
        else:
            db.dataset_log.insert_one({'action': json.loads(item), 'status': 0})
    print '2019HongKong_protest craw_worker finish!'
