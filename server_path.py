#######server_path.py#######
import re
import time
import logging
import argsparser
from flask import *
from flask_restplus import *

from jessica_arabic_knowledge_linking import arabic_text_knowledge_linking

ns = Namespace('JessKnowLik', description='Jessica\'s Arabic Text to Knowledge Graph Engine. I am open for a DS/AI job, contact me by gaoyuanliang@outlook.com')
args = argsparser.prepare_args()

parser = ns.parser()
parser.add_argument('text', type=str, location='json')

#######Text2DBPedia_KnowledgeGraph
req_fields = {\
	'text': fields.String()\
	}
jessica_api_req = ns.model('JessKnowLik', req_fields)

rsp_fields = {\
	'status':fields.String,\
	'running_time':fields.Float\
	}
jessica_api_rsp = ns.model('JessKnowLik', rsp_fields)

@ns.route('/text')
class jessica_api(Resource):
	def __init__(self, *args, **kwargs):
		super(jessica_api, self).__init__(*args, **kwargs)
	@ns.marshal_with(jessica_api_rsp)
	@ns.expect(jessica_api_req)
	def post(self):		
		start = time.time()
		output = {}
		try:			
			args = parser.parse_args()	
			triplets = arabic_text_knowledge_linking(args['text'])
			output['status'] = "success"
			output['running_time'] = float(time.time()- start)
			return output, 200
		except Exception as e:
			output = {}
			output['status'] = 'error:'+str(e)
			output['running_time'] = float(time.time()- start)
			return output

#######server_path.py#######