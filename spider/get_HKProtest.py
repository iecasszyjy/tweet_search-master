# -*- coding:utf-8 -*-
# '2019香港'事件获取
import pymongo
from pymongo import InsertOne
from pymongo.errors import BulkWriteError

import random
from pprint import pprint
from tqdm import tqdm

client = pymongo.MongoClient('mongodb://3.220.111.222:27017/')
client.admin.authenticate("aircas", "aircas@2018", mechanism='SCRAM-SHA-1')
db = client['2019HongKong_protest']

def set_conditions():  # 事件较少，不使用解析网页方法，直接列出关键词放入数据库
    # 设置起始、终止时间
    stime = '2019-03-01'
    etime = '2019-10-20'
    
    # 设置香港时间地点名词
    locations = 'Hong Kong OR ' + 'Hong Kong Island OR Central and Western District OR Eastern District OR Southern District OR Wan Chai District OR ' + 'Kowloon OR Kowloon City District OR Kwun Tong District OR Sham Shui Po District OR Wong Tai Sin District OR Yau Tsim Mong District OR ' + 'New Territories OR Island District OR Kwai Tsing District OR North District OR Sai Kung District OR Sha Tin District OR Tai Po District OR Tsuen Wan District OR Tuen Mun District OR Yuen Long District OR ' + "Kowloon Reservoir OR Kowloon City OR Kowloon Tong OR Kowloon Bay OR Pat Sin Leng OR Sheung Shui OR Sheung Wan OR To Kwa Wan OR Tai Shui Hang OR Tate's Cairn OR Tai Hang OR Tai Mei Tuk OR Tai Kok Tsui OR Tai Tung Shan OR Sunset Peak OR Tai Po Industrial Estate OR Tai Po OR Tai Po Kau OR Tai Po Market OR " +  "Tai Long Wan OR Tai Wai OR Tai Mo Shan OR Tai Wo Hau OR Tai Mong Tsai OR Tai Tam Reservoirs OR Tai Tam Bay OR Tai O OR Lantau Island OR Tai Pang Wan OR Mirs Bay OR Tai Lam Chung OR Tai Lam Chung Reservoir OR Siu Sai Wan OR Siu Lam OR Central and Western OR Central OR Tseng Lan Shue OR Yuen Long OR Fan Lau OR " + "Tin Shui Wai OR Tin Hau OR Prince Edward OR Tai Koo OR Tai Wo OR Tuen Mun OR Fo Tan OR Ngau Chi Wan OR Ngau Mei Hoi OR Port Shelter OR Ngau Tau Kok OR North Point OR North OR Pak Tam Chung OR Ta Kwu Ling OR Ting Kau OR Shek Mun OR Shek Kong OR Shek Kip Mei OR Shek Tong Tsui OR Shek Pik OR Shek Pik Reservoir OR " + "Shek O OR Kei Ling Ha Hoi OR Three Fathoms Cove OR Siu Hong OR Crooked Island OR Tolo Harbour OR Tsim Sha Tsui OR East Tsim Sha Tsui OR Tsim Bei Tsui OR Sai Kung Hoi OR Inner Port Shelter OR Sai Kung OR Sai Ying Pun OR Sai Wan Ho OR Ho Man Tin OR Jordan OR Hang Hau OR Heng Fa Chuen OR Sha Tin Hoi OR Sha Tin OR " + "Sha Tin Wai OR Sha Tau Kok OR Pui O OR Tolo Channel OR Stanley OR Chek Lap Kok OR King's Park OR Wo Hop Shek OR Peng Chau OR Mong Kok OR Ngong Ping OR Ngong Suen Chau OR Stonecutters Island OR Tung Ping Chau OR Tung Chung OR Eastern OR Tung Lung Chau OR Kwo Chau Kwan To OR Lam Tsuen OR Sunny Bay OR Ho Pui Reservoir OR " + "Yau Tsim Mong OR Yau Ma Tei OR Yau Tong OR Admiralty OR Cheung Sha Wan OR Cheung Chau OR Tsing Shan OR Castle Peak OR Tsing Yi OR Tsing Lung Tau"
    
    # 香港事件关键词
    triggers = 'protesters OR protests OR protest OR citizens OR marched OR march OR' + 'police OR government OR officers OR lam OR carrie OR political OR ' + 'force OR violence OR riot OR ' + 'mainland OR independent OR ' + 'lawmakers OR revolution'

    return stime, etime, locations, triggers

if __name__ == '__main__':
    stime, etime, locations, triggers = set_conditions()
    # 事件查询条件放入MongoDB数据库
    requests = list()
    requests.append(InsertOne({'_id': hash(stime), 'stime': stime}))
    requests.append(InsertOne({'_id': hash(etime), 'etime': etime}))
    requests.append(InsertOne({'_id': hash(locations), 'locations': locations}))
    requests.append(InsertOne({'_id': hash(triggers), 'triggers': triggers}))
    try:
        result = db.conditions.bulk_write(requests)
        pprint(result.bulk_api_result)
    except BulkWriteError as bwe:
        pprint(bwe.details)
    client.close()
