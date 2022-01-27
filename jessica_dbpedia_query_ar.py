#######jessica_dbpedia_query.py#######
import re
import time
import rdflib

g_relation = rdflib.Graph()
g_type = rdflib.Graph()

print('loading the DBpedia knowledge base')
start_time = time.time()
instance_types_ar = g_relation.parse(
	"instance_types_ar.ttl", 
	format='ttl')
mappingbased_objects_ar = g_type.parse(
	"mappingbased_objects_ar_ontology.ttl", 
	format='ttl')
print('knolwedge based loading completed. loading time %f seconds'%(time.time() - start_time))

#######

def id_to_name(entity_id):
	try:
		output = re.search(r'(resource)\/(?P<name>.*)', entity_id).group('name')
		output = re.sub(u"[^A-z\u0600-\u06FF]+", r' ', output)
		output = re.sub(u"^[^A-z\u0600-\u06FF]+|[^A-z\u0600-\u06FF]+$", r'', output)	
		output = re.sub(u"_", r' ', output)	
		return output
	except:
		return None

'''
id_to_name(u"http://ar.dbpedia.org/resource/كوغا_(إيباراكي)")
'''

def relation_processing(renaltion_name):
	output = re.search(r'(ontology)\/(?P<name>.*)', renaltion_name).group('name')
	output = re.sub(r'[^A-z]+', r'_', output)
	output = re.sub(r'^[^A-z]+|[^A-z]+$', r'', output)
	return output

'''
relation_processing(u"http://dbpedia.org/ontology/Language")
'''

def find_entity_type(entity_id):
	try:
		types = [t[0].toPython() for t in 
			instance_types_ar.query(u"""
			SELECT ?type WHERE {
			<%s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type .
			} LIMIT 1
			"""%(entity_id))]
		return relation_processing(types[0])
	except:
		return 'Other'

'''
find_entity_type(u"http://ar.dbpedia.org/resource/ابن_خلدون")
'''

'''
print(relation_processing("http://dbpedia.org/ontology/ground"))
'''

def find_related_entities(
	entity_id,
	related_subject_num = 4,
	related_object_num = 4):
	objects = []
	subjects = []
	#####
	try:
		if related_object_num > 0:
			objects = [{'relation': stmt[0].toPython(), 
				'object':stmt[1].toPython(),
				'subject':entity_id}
				for stmt 
				in mappingbased_objects_ar.query(u"""
				SELECT ?r ?o WHERE { <%s> ?r ?o . } LIMIT %d
				"""%(entity_id, related_object_num))]
	except:
		pass
	######
	try:
		if related_subject_num > 0:
			subjects = [{'relation': stmt[1].toPython(), 
				'subject':stmt[0].toPython(),
				'object':entity_id}
				for stmt 
				in mappingbased_objects_ar.query(u"""
				SELECT ?s ?r WHERE { ?s ?r <%s>. } LIMIT %d
				"""%(entity_id, related_subject_num))]
	except:
		pass
	######
	return subjects+objects

'''
find_related_entities(
	entity_id = "http://ar.dbpedia.org/resource/ميدياويكي",
	related_subject_num = 3,
	related_object_num = 3)
'''

def find_linking_entities(
	entity_id_1, 
	entity_id_2,
	common_object_number,
	common_subject_number):
	output = []
	######
	try:
		for stmt in mappingbased_objects_ar.query(u"""
			SELECT ?o ?r1 ?r2  WHERE { 
				<%s> ?r1 ?o . 
				<%s> ?r2 ?o . 
			} LIMIT %d
			"""%(entity_id_1, entity_id_2, common_object_number)):
			output.append({'subject':entity_id_1,
				'relation':stmt[1].toPython(),
				'object': stmt[0].toPython(), 
				})
			output.append({'subject':entity_id_2,
				'relation':stmt[2].toPython(),
				'object': stmt[0].toPython(), 
				})
	except:
		pass
	######
	try:
		for stmt in mappingbased_objects_ar.query(u"""
			SELECT ?s ?r1 ?r2  WHERE { 
				?s ?r1 <%s> . 
				?s ?r2 <%s> . 
			} LIMIT %d
			"""%(entity_id_1, entity_id_2, common_subject_number)):
			output.append({'object':entity_id_1,
				'relation':stmt[1].toPython(),
				'subject': stmt[0].toPython(), 
				})
			output.append({'object':entity_id_2,
				'relation':stmt[2].toPython(),
				'subject': stmt[0].toPython(), 
				})
	except:
		pass
	#####
	return output

'''
for t in find_linking_entities(
	entity_id_1 = "http://ar.dbpedia.org/resource/دبي",
	entity_id_2 = "http://ar.dbpedia.org/resource/دبي",
	common_object_number = 2,
	common_subject_number = 2):
	print(t)
'''

def find_entity_pair_relation(
	entity_id_1, 
	entity_id_2,
	relation_1_to_2_number,
	relation_2_to_1_number):
	output = []
	######
	try:
		for stmt in mappingbased_objects_ar.query(u"""
			SELECT ?r  WHERE { <%s> ?r <%s> . } LIMIT %d
			"""%(entity_id_1, entity_id_2, relation_1_to_2_number)):
			output.append({'subject':entity_id_1,
				'relation':stmt[0].toPython(),
				'object': entity_id_2, 
				})
	except:
		pass
	######
	try:
		for stmt in mappingbased_objects_ar.query(u"""
			SELECT ?r  WHERE { <%s> ?r <%s> . } LIMIT %d
			"""%(entity_id_2, entity_id_1, relation_2_to_1_number)):
			output.append({'subject':entity_id_2,
				'relation':stmt[0].toPython(),
				'object': entity_id_1, 
				})	
	except:
		pass
	#####
	return output

'''
for t in find_entity_pair_relation(
	entity_id_1 = "http://ar.dbpedia.org/resource/علي_العلياني",
	entity_id_2 = "http://ar.dbpedia.org/resource/دبي",
	relation_1_to_2_number = 1,
	relation_2_to_1_number = 2):
	print(t)
'''

def attach_triplet_type_and_name(input_triplets):
	entities = list(set([t['subject'] for t in input_triplets]+[t['object'] for t in input_triplets]))
	entity_type_lookup = {}
	entity_name_lookup = {}
	relation_name_lookup = {}
	outout_triplets = input_triplets
	for e in entities:
		entity_type_lookup[e] = find_entity_type(e)
		entity_name_lookup[e] = id_to_name(e)
	#######
	types = list(set([t['relation'] for t in input_triplets]))
	for t in types:
		relation_name_lookup[t] = relation_processing(t)
	######
	for t in outout_triplets:
		t['subject_type'] = entity_type_lookup[t['subject']]
		t['object_type'] = entity_type_lookup[t['object']]
		t['subject_name'] = entity_name_lookup[t['subject']]
		t['object_name'] = entity_name_lookup[t['object']]
		t['relation'] = relation_name_lookup[t['relation']]
	return outout_triplets, entity_type_lookup, entity_name_lookup, relation_name_lookup

'''
input_triplets = [
{'object': 'http://ar.dbpedia.org/resource/دبي', 'relation': 'http://dbpedia.org/ontology/deathPlace', 'subject': 'http://ar.dbpedia.org/resource/عبد_الرحمن_بن_حافظ'},
{'object': 'http://ar.dbpedia.org/resource/دبي', 'relation': 'http://dbpedia.org/ontology/deathPlace', 'subject': 'http://ar.dbpedia.org/resource/عبد_الرحمن_بن_حافظ'},
{'object': 'http://ar.dbpedia.org/resource/دبي', 'relation': 'http://dbpedia.org/ontology/deathPlace', 'subject': 'http://ar.dbpedia.org/resource/عبد_الرحمن_بن_حافظ'},
{'object': 'http://ar.dbpedia.org/resource/دبي', 'relation': 'http://dbpedia.org/ontology/birthPlace', 'subject': 'http://ar.dbpedia.org/resource/عبد_الرحمن_بن_حافظ'},
]

output_triplets, entity_type_lookup, entity_name_lookup, relation_name_lookup = attach_triplet_type_and_name(input_triplets)

for t in output_triplets:
	print(t)
'''
#######jessica_dbpedia_query.py#######