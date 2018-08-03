# coding: utf-8
# ## nlp services
# `
# docker run -p 9000:9000 phiedulxp/lxp:corenlp
# docker run -p 8000:8000 allennlp/allennlp [python -m allennlp.run serve]
# `

example = '''A military helicopter surveying the damage, carrying the Governor of Oaxaca Alejandro Murat Hinojosa and Mexico's Secretary of the Interior Alfonso Navarrete Prida, crashes over Jamiltepec, killing 13 people on the ground. These deaths are the only known ones related to the earthquake reported so far. '''

import json, requests

model_ref = {
	'ner':'named-entity-recognition',
	'cr':'coreference-resolution',
	'srl':'semantic-role-labeling',
	'te':'textual-entailment',
	'mc':'machine-comprehension',
}
class AllenNLP:
	'''
	Wrapper for allennlp Restful API
	'ner':'named-entity-recognition', # {'sentence':}->{'logits':,'mask':,'tags':,'words':}
	'cr':'coreference-resolution',    # {'document':,}->{ "antecedent_indices":, "clusters":, "document": "predicted_antecedents"}
	'srl':'semantic-role-labeling',   # {'sentence':,}-{'tokens':,'verbs':,'words':}
	'te':'textual-entailment',        # {'hypothesis':,'.premise':}-{'label_logits':,'label_probs':,}
	'mc':'machine-comprehension',     # {'passage':,'question':}->  {"best_span":,"best_span_str":,
																																	 "passage_question_attention":,
																																	 "passage_tokens":,"question_tokens":,
																																	 "span_end_logits":,"span_end_probs":,
																																	 "span_start_logits":,"span_start_probs":,}
	'''
	def __init__(self, host='127.0.0.1', port='8000'):
		self.host = host
		self.port = port
	
	def annotate(self, data, model_name):
		self.model = model_ref[model_name]
		self.request_url = 'http://'+self.host+':'+self.port+'/'+'predict/'+self.model
		self.data = data
		try:
			res = requests.post(url=self.request_url,
													json=self.data,
													headers={'Connection': 'close'})
			return res.json()
		except Exception as e:
			print(e)


# In[47]:

class StanfordCoreNLP:
	'''
	Wrapper for Starford Corenlp Restful API
	annotators:"truecase,tokenize,ssplit,pos,lemma,ner,regexner,parse,depparse,openie,coref,kbp,sentiment"
	nlp = StanfordCoreNLP()
	output = nlp.annotate(text, properties={ 'annotators':,outputFormat': 'json',})
	'''

	def __init__(self, host='127.0.0.1', port='9000'):
		self.host = host
		self.port = port

	def annotate(self, data, properties=None, lang='en'):
		self.server_url = 'http://'+self.host+':'+self.port
		properties['outputFormat'] = 'json'
		try:
			res = requests.post(self.server_url,
													params={'properties': str(properties),
																	'pipelineLanguage':lang},
													data=data, 
													headers={'Connection': 'close'})
			return res.json()
		except Exception as e:
			print(e)

if __name__ == '__main__':
	anlp = AllenNLP()
	out1 = anlp.annotate({'sentence':example},'ner')
	snlp = StanfordCoreNLP()
	out2 = snlp.annotate(example,properties={'annotators':'ner'})

