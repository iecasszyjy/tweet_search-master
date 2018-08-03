import re
import json    
from bson import json_util
from datetime import datetime,timedelta

from tqdm import tqdm

from Config import get_spider_config
_,db,r = get_spider_config()


def get_task():
	for item in tqdm(db.current_event.find({'type':{'$gte':1},'event.date':{'$gte':'2016-01-01'}},{'_id':1,'queries':1})):
		q = item['queries']
		for i in q:
			message = {'q':i,'f':'&f=tweets','num':20,'event_id':json.dumps(item['_id'],default=json_util.default)}
			print message
			r.rpush('task:paper',json.dumps(message))
	db.close()

if __name__ == '__main__':
	get_task()

# s = '{"_id": {"$oid": "4edebd262ae5e93b41000000"}}'
# u = json.loads(s, object_hook=json_util.object_hook)

# print u  # Result:  {u'_id': ObjectId('4edebd262ae5e93b41000000')}

# s = json.dumps(u, default=json_util.default)

# print s  # Result:  {"_id": {"$oid": "4edebd262ae5e93b41000000"}}