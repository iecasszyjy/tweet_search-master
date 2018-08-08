# -*- coding:utf-8 -*-
from bs4 import BeautifulSoup
import requests

import pymongo
from pymongo import InsertOne
from pymongo.errors import BulkWriteError

import random
from pprint import pprint
from tqdm import tqdm

client = pymongo.MongoClient('54.161.160.206:27017')  # 与MongoDB数据库建立连接
db = client.natural_disaster

url = "https://en.wikipedia.org/wiki/List_of_natural_disasters_in_the_United_States"  # 维基百科中列出的美国自然灾害
header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; ...) Gecko/20100101 Firefox/61.0',
}

def get_events_from_page(url):  # 从网页中提取事件信息
    res = requests.get(url, headers=header, verify=False)
    soup = BeautifulSoup(res.text, 'lxml')
    events = []  # 事件列表

    tbody = soup.find_all('tbody')[-1]  # 网页解析过程需根据具体网页源代码格式决定
    trs = tbody.find_all('tr')[1:]

    for tr in trs:
        tds = tr.find_all('td')
        year = tds[0].string.strip()

        disaster_as = tds[1].find_all('a')
        if disaster_as != []:
            disaster = ''
            for disaster_a in disaster_as:
                disaster = disaster + disaster_a.string + ','
            disaster = disaster.strip(',')
        else:
            disaster = tds[1].string.strip()

        article_a = tds[4].find('a')
        if article_a != None:
            article = article_a.string.strip()
        else:
            article = ''

        location_as = tds[5].find_all('a')
        if location_as != []:
            location = ''
            for location_a in location_as:
                location = location + location_a.string + ','
            location = location.strip(',')
        else:
            location = tds[5].string.strip()
            location = location.replace(' and ', ',')
            location = location.replace(';', ',')

        events.append({
            'year': year,
            'disaster': disaster,
            'main_article': article,
            'location': location,
        })
    return events

if __name__ == '__main__':
    events = get_events_from_page(url)
    # 事件信息编码放入MongoDB数据库
    requests_ = [InsertOne({'_id': hash(i['year'] + i['disaster'] + str(random.randint(0, 100))), 'event': i}) for i in tqdm(events)]
    try:
        result = db.event_list.bulk_write(requests_)
        pprint(result.bulk_api_result)
    except BulkWriteError as bwe:
        pprint(bwe.details)
    client.close()

