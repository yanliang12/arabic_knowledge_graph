######jessica_arabic_knowledge_linking.py#######

import itertools
from jessica_dbpedia_query import *
from yan_ner_arabic import arabic_ner
from jessica_dbpedia_es_search import search_es_entity

from jessica_neo4j import *

start_neo4j(http_port = "5987", bolt_port = "4522")
neo4j_session = create_neo4j_session(bolt_port = "4522")

####

def entity_to_knowledge_triplets(entities,
	related_subject_num = 5,
	related_object_num = 5,
	common_object_number = 5,
	common_subject_number = 5,
	connected_entity_linkage_number = 5):
	triplets = []
	#####
	entity_ids = list(set([e['entity_dbpedia_id'] for e in entities]))
	for e in entity_ids:
		triplets += find_related_entities(e,
		related_subject_num = related_subject_num,
		related_object_num = related_object_num)
	####
	for r1, r2 in itertools.combinations(entity_ids,2):
		triplets += find_linking_entities(
			r1, r2, 
			common_object_number = common_object_number,
			common_subject_number = common_subject_number)
		triplets += find_entity_pair_relation(
			r1, r2, 
			relation_1_to_2_number = connected_entity_linkage_number,
			relation_2_to_1_number = connected_entity_linkage_number)
	#####
	triplets, entity_type_lookup, entity_name_lookup, relation_name_lookup = attach_triplet_type_and_name(triplets)
	return triplets

def arabic_text_knowledge_linking(text):
	entities = []
	for e in arabic_ner(text):
		est = search_es_entity(e['text'])
		try:
			entities.append({"entity_dbpedia_id":est[0]["entity_id"]})
		except:
			pass
	triplets = entity_to_knowledge_triplets(entities)
	ingest_knowledge_triplets_to_neo4j(triplets, neo4j_session)
	return triplets

######jessica_arabic_knowledge_linking.py#######