import os
import re
import codecs
import random
import multiprocessing
from multiprocessing import Pool

from tqdm import tqdm
import pandas as pd
import spacy

import json    
from bson import json_util
import redis

r = redis.StrictRedis(host='localhost',port=6379, db=0)

import pymongo

client = pymongo.MongoClient('localhost:27017')
db = client.tweet


if not os.path.exists('./stories/'): os.mkdir('./stories/')
if not os.path.exists('./story_fnames/'): os.mkdir('./story_fnames/')

nlp = spacy.load('en_core_web_md')

events = [e for e in db.current_event.find({},{'_id':1,'abstracts':1,'type':1})]

def get_dataset(event):
    abst_list = [i['abstract'] for i in event['abstracts']]
    abst = ''.join(abst_list)
    doc_reference = nlp(abst)
    tweets = []
    filter_dict = {'event_id':event['_id'],'tweet.lang':'en','tweet.media.card_url':None}
    query_dict = {'tweet.standard_text':1}
    records  = [i for i in db.pos.find(filter_dict,query_dict)]+[i for i in db.paper.find(filter_dict,query_dict)]
    for tweet in records:
        tweet_id = tweet['_id']
        tweet_text = re.sub('https?:\/\/\w+\.\w+\/\w+','',tweet['tweet']['standard_text']) 
        doc_tweet = nlp(tweet_text)
        tweets.append((tweet_id,doc_reference.similarity(doc_tweet),tweet_text))
    tweets = sorted(tweets,key=lambda x:x[1],reverse=True)
    df_candidate = pd.DataFrame.from_records(tweets,index=range(len(tweets)),columns=['id','simi','text'])
    df_candidate = df_candidate.drop_duplicates(['simi'])
    return abst_list,df_candidate[df_candidate['simi'] > 0.75][:100].sort_values(by='id')['text'].tolist()

def wite_tokenized_story(queue):
	event_id = json.loads(queue['event_id'],object_hook=json_util.object_hook)
	event = db.current_event.find_one({'_id':event_id},{'_id':1,'abstracts':1,'type':1})
	story_file_name = str(event['_id'])+'_'+str(event['type'])+'.story'
	if not os.path.exists('./stories/'+story_file_name):
		print(story_file_name)
		abst,cadi = get_dataset(event)
		with codecs.open('./stories/'+story_file_name,'a',encoding='utf-8') as f:
			for i in cadi:
				doc = nlp(i)
				f.write(' '.join([token.text for token in doc])+'\n\n')
			for i in abst:
				doc = nlp(i)
				f.write('@highlight\n\n')
				f.write(' '.join([token.text for token in doc])+'\n\n')
		
if __name__ == '__main__':
    print('data_worker start!')
    while True:
        queue = r.lpop('task:data')
        if queue:
            print('data_worker process!')
            craw = wite_tokenized_story(json.loads(queue))