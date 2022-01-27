##########jessica_dbpedia_es_search.py##########
'''
/home/jessica/elasticsearch-5.6.1_dbpedia_arabic_entity/bin/elasticsearch &
http://0.0.0.0:8865/_cat/indices?v
http://0.0.0.0:8865/dbpedia_arabic_entity/_search?pretty
'''

import re
import os
from elasticsearch import Elasticsearch 

os.system(u"""
	elasticsearch-5.6.1_dbpedia_arabic_entity/bin/elasticsearch &
	""")

es = Elasticsearch([{'host':'0.0.0.0','port':8865}])

while True:
	try:
		res= es.search(
			index='dbpedia_arabic_entity',
			body={'query':{'match':{"entity_name" : "test"}}})
		print("es started")
		break
	except:
		pass

#######

def search_es_entity(entity_name,
	return_entity_max_number = 1,
	return_entity_min_score = 5.0):
	try:
		res= es.search(
			index='dbpedia_arabic_entity',
			body={'query':{'match':{"entity_name" : entity_name}}})
		output = [r['_source'] for r in res['hits']['hits'] if r['_score'] >= return_entity_min_score]
		return output[0:return_entity_max_number]
	except:
		return None


##########jessica_dbpedia_es_search.py##########