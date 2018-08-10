# -*- coding:utf-8 -*-
# 爬取推文
import time
import json

import multiprocessing
from multiprocessing import Pool

from Config_new import get_noau_config

got, db, r = get_noau_config()  # 文件和数据库配置

def advance_search_dataset(q, f, num, event_id):  # 获取推文，放入MongoDB数据库
    collection = db.dataset
    tweetCriteria = got.manager.TweetCriteria().setQuerySearch(q).setTweetType(f).setMaxTweets(num)
    tweets = got.manager.TweetManager.getTweets(tweetCriteria)
    for tweet in tweets:
        if collection.find_one({'_id': tweet['id']}) is None:  # 查看推文是否已存在，若不存在则放入数据库
            collection.insert_one({'_id': tweet['id'], 'tweet': tweet, 'event_id': event_id, 'f': f, 'q': q})


def run_dataset_task(message_data):
    try:
        q = message_data['q']
        num = message_data['num']
        event_id = message_data['event_id']
        if type(message_data['f']) != list:
            advance_search_dataset(q, message_data['f'], num, event_id)
        else:
            # 多进程获取推文
            pool = Pool(processes=multiprocessing.cpu_count())
            # python官方建议：废弃apply，使用apply_async（使用apply在这里会出错）
            [pool.apply_async(advance_search_dataset,(q,f,num,event_id)) for f in message_data['f']]
            # [pool.apply(advance_search_dataset, (q, f, num, event_id)) for f in message_data['f']]
            pool.close()
            pool.join()
        return True
    except:
        return False


if __name__ == '__main__':
    print 'craw_worker start!'
    while True:
        # 从Redis数据库中取出获取推文所需信息
        queue = r.lpop('temp1')
        if queue:
            print 'craw_worker process!'
            craw = run_dataset_task(json.loads(queue))  # 获取推文
            if craw:
                # 添加查询日志
                db.dataset_log.insert_one({'message': json.loads(queue), 'status': 1})
            else:
                db.dataset_log.insert_one({'message': json.loads(queue), 'status': 0})
        time.sleep(1)
        print 'craw_worker wait!'

       
