import time
import json

import multiprocessing
from multiprocessing import Pool

from Config_new import get_noau_config

got, db, r = get_noau_config()

def advance_search_dataset(q, f, num, event_id):
    print "flag2"
    collection = db.dataset
    #collection.insert_one({"abc":"serbbbr"})
    print "flag3"
    tweetCriteria = got.manager.TweetCriteria().setQuerySearch(q).setTweetType(f).setMaxTweets(num)
    print "flag4"
    tweets = got.manager.TweetManager.getTweets(tweetCriteria)
    print tweets[0]
    for tweet in tweets:
        print 'tweet'
        if collection.find_one({'_id': tweet.id}) is None:
            print 'add'
            collection.insert_one({'_id': tweet.id, 'tweet': tweet.__dict__, 'event_id': event_id, 'f': f, 'q': q})


def run_dataset_task(message_data):
    # message_data['f'] = #'' '&f=tweets' '&f=news' '&f=broadcasts' '&f=videos' '&f=images' '&f=users'
    # message_data['q']  = #''
    # message_data['num'] =10
    try:
        q = message_data['q']
        num = message_data['num']
        event_id = message_data['event_id']
        print "flag1"
        if type(message_data['f']) != list:
            print "flagif"
            advance_search_dataset(q, message_data['f'], num, event_id)
        else:
            print "flagelse"
            pool = Pool(processes=multiprocessing.cpu_count())
            [pool.apply_async(advance_search_dataset,(q,f,num,event_id)) for f in message_data['f']]
            # [pool.apply(advance_search_dataset, (q, f, num, event_id)) for f in message_data['f']]
            pool.close()
            pool.join()
        print "flag6"
        return True
    except:
        return False


if __name__ == '__main__':
    print 'craw_worker start!'
    while True:
        queue = r.lpop('task:traindata')
        if queue:
            print 'craw_worker process!'
            craw = run_dataset_task(json.loads(queue))
            if craw:
                db.dataset_log.insert_one({'message': json.loads(queue), 'status': 1})
            else:
                db.dataset_log.insert_one({'message': json.loads(queue), 'status': 0})
        time.sleep(1)
        print 'craw_worker wait!'

        
        
        
