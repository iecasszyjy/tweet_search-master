# -*- coding: utf-8 -*
# 获得爬取推文所需信息
import json
from Config_defect import get_noau_config
from datetime import datetime, timedelta

_, db, r = get_noau_config()  # 数据库配置

def get_query_str(event):  # 获取Twitter查询信息
    loc_start = 'North Korea OR North'
    loc_end = 'South Korea OR South'
    trigger = "defectors OR defected OR defection"
    type = event['event']['type']
    date = event['event']['date']
    temp = datetime.strptime(date, "%Y-%m-%d")
    date_since = (temp - timedelta(days=1)).strftime('%Y-%m-%d')
    date_until = (temp + timedelta(days=10)).strftime('%Y-%m-%d')
    # 注意查询格式必须形如(xxx OR xxx) (xxx OR xxx) since:xxxx-xx-xx until:xxxx-xx-xx
    return '(' + loc_start + ')' + ' ' + '(' + loc_end + ')' + ' ' + '(' + type + ')' + ' ' + '(' + trigger + ')' +\
           ' ' + 'since:' + date_since + ' ' + 'until:' + date_until

def get_task():
    events = db.event_list.find({})
    for event in events:
        q = get_query_str(event)
        message = {'q': q, 'f': ['&f=news', '', '&f=tweets'], 'num': 10000, 'event_id': event['_id']}
        print(message)
        # 把获取推文所需信息放入Redis数据库
        r.rpush('defect', json.dumps(message))

if __name__ == '__main__':
    get_task()


