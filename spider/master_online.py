import re
import json
import fire
import time
from tqdm import tqdm
from datetime import datetime, timedelta
from collections import Counter
from Config import get_noau_config

_, db, r = get_noau_config()

def get_query_str(partner, triggers, target):
    # start = (now - time_delta).strftime("%Y-%m-%d %H:%M:%S")
    # now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    return '(' + loc + ')' + ' ' + '(' + ' OR '.join(triggers) + ')' + ' ' + '(' + target + ')'


def get_task():
    partner = ["Kim"]
    triggers = ["meet", "trip", "summit", "talk", "meeting"]
    targets = ["Xi", "Moon", "Trump"]
    now = datetime.now()
    WAIT_TIME = 15
    while True:
        for par in partner:
            for target in targets:
                q = get_query_str(par, triggers, target)
                message = {'q': q, 'f': ['&f=news', '', '&f=tweets'], 'num': -1,
                           "sinceTimeStamp": (now - timedelta(minutes=WAIT_TIME)).strftime("%Y-%m-%d %H:%M:%S"),
                           "untilTimeStamp": now.strftime("%Y-%m-%d %H:%M:%S")
                           }
                print(message)
                r.rpush("task:online", json.dumps(message))

        time_gone = (datetime.now() - now).seconds
        if time_gone < 60 * WAIT_TIME:
            time.sleep(60 * WAIT_TIME - time_gone)
            now = datetime.now()
        else:
            now = now + timedelta(minutes=WAIT_TIME)


if __name__ == '__main__':
    get_task()
