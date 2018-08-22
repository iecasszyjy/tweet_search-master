import re
import json
import fire
import time
from tqdm import tqdm
from datetime import datetime, timedelta
from collections import Counter
from Config import get_noau_config

_, db, r = get_noau_config()

# users = [i['tweet']['user']['screen_name'] for i in db.dataset_korea_m_1.find({}, {"tweet.user.screen_name": 1})]
# freq_users = [i[0] for i in Counter(users).most_common() if i[1] >= 5]


def get_query_str(loc, trigger):
    # start = (now - time_delta).strftime("%Y-%m-%d %H:%M:%S")
    # now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    return '(' + loc + ')' + ' ' + '(' + ' OR '.join(trigger) + ')'


def get_task():
    locs = ["American", "United States", "US"]
    triggers = ["Hurricane", "Wildfire", "Flood", "Blizzard", "Snow storm", "Tornado", "Mudflow", "Cold wave", "Blizzard"]
    now = datetime.now()
    WAIT_TIME = 15
    while True:
        for loc in locs:
            q = get_query_str(loc, triggers)
            message = {'q': q, 'f': ['&f=news', '', '&f=tweets'], 'num': -1,
                       "sinceTimeStamp": (now - timedelta(minutes=WAIT_TIME)).strftime("%Y-%m-%d %H:%M:%S"),
                       "untilTimeStamp": now.strftime("%Y-%m-%d %H:%M:%S")
                       }
            print(message)
            r.rpush("task:online_nd", json.dumps(message))
        # for user in freq_users:
        #     message = {'q': 'from:' + user, 'f': '&f=tweets', 'num': -1,
        #                "sinceTimeStamp": (now - timedelta(minutes=WAIT_TIME)).strftime("%Y-%m-%d %H:%M:%S"),
        #                "untilTimeStamp": now.strftime("%Y-%m-%d %H:%M:%S")
        #                }
        #     print(message)
        #     r.rpush('task:online_nd', json.dumps(message))
        time_gone = (datetime.now() - now).seconds
        if time_gone < 60 * WAIT_TIME:
            time.sleep(60 * WAIT_TIME - time_gone)
            now = datetime.now()
        else:
            now = now + timedelta(minutes=WAIT_TIME)


if __name__ == '__main__':
    get_task()



