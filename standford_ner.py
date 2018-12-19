import nltk
from nltk.tag import StanfordNERTagger
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from collections import Counter
import sql
import os

# Types of entities to store
labels = ['PERSON', 'LOCATION', 'ORGANIZATION']
stopset = set(stopwords.words('english'))

# Load Standord NER Java library
java_path = "C:\Program Files (x86)\Java\jre1.8.0_181\\bin\java.exe"
os.environ['JAVAHOME'] = java_path

st = StanfordNERTagger('./models/stanford-ner-2018-10-16/classifiers/english.all.3class.distsim.crf.ser.gz',
					   './models/stanford-ner-2018-10-16/stanford-ner.jar',
					   encoding='utf-8')
"""
	1. Tokenizes entire documents using NLTK, remove stopwords
	2. Named Entity Extraction using Stanford NER.
	3. Group named entity tokens into 1 entity
	4. Store docs and entities in SQL
	5. Returns for dev analysis
	
	Input: [(doc_id, text), ...]
"""
def stan_parse(docs):
	results = []
	top_tokens = []
	doc_ids = [x[0] for x in docs]
	tokenized_docs = [word_tokenize(doc[1]) for doc in docs]
	classified_docs = st.tag_sents([x for x in tokenized_docs])

	# Extract Named Entities [[(446, ('Charles', 'PERSON')), .. ], ... ])]
	entities = [[(x, y) for x, y in enumerate(classified_text) if y[1] in labels] for classified_text in classified_docs]

	# Iterate through each doc
	for idx, ent in enumerate(entities):
		# Pass if no entities found
		if len(ent) == 0:
			continue

		doc_id = doc_ids[idx]
		# Group neighboring entities if they have the same type
		groups = get_groups(ent)
		reduced_ents = [{**x, **{'doc_id': doc_id}} for x in reduce_groups(groups)]
		matched_names = match_names(reduced_ents)

		results += matched_names
		tokens = get_tokens(classified_docs[idx])
		token_counts = token_counter(classified_docs[idx])
		top = [x[0] for x in token_counts.most_common(10)]
		top_tokens += [{'doc_id': doc_ids[idx], 'tokens': top}]

		sql.insert_doc(doc_ids[idx], tokens)
		insert_entities(matched_names)

	return results, top_tokens

"""
Group neighboring named entities based on index and type per document
ie. [[(446, ('Charles', 'PERSON')), (447, ('R.', 'PERSON')), (448, ('Wall', 'PERSON'))], 
	[(516, ('Sarah', 'PERSON'))], [(654, ('Jim', 'PERSON'))]] 

return 
"""
def get_groups(ents):
	# TODO: need to figure out punctuation: ie 560 ORGANIZATION Bureau of Alcohol , Tobacco and Firearms vs. Sarah, Jim, and Andre
	groups = [[ents[0]]]
	for idx, ent in enumerate(ents[1:]):
		ent_idx = idx + 1
		type = ent[1][1]
		if ents[ent_idx][1][1] == type and ents[ent_idx][0] - ents[ent_idx - 1][0] == 1:
			for group in groups:
				if ents[ent_idx - 1] in group:
					group.append(ents[ent_idx])
					break
		else:
			groups.append([ents[ent_idx]])
	return groups

"""
Reduce groups into string name, count the number of times they appear in the document, and store start indices and length.
ie: [(446, ('Charles', 'PERSON')), (447, ('R.', 'PERSON')), (448, ('Wall', 'PERSON'))] => "Charles R. Wall"
"""
def reduce_groups(groups):
	names = []
	idxs = {}
	for group in groups:
		idx = group[0][0]
		type = group[0][1][1]
		name = ' '.join([y[1][0] for y in group])
		names.append((type, name))
		if name not in idxs:
			idxs[name] = {}
			idxs[name]['index'] = [idx]
			idxs[name]['length'] = len(group)
		else:
			idxs[name]['index'].append(idx)
	counts = [{'type': x[0], 'name': x[1], 'count': names.count(x), 'index': idxs[x[1]]['index'], 'length': idxs[x[1]]['length']} for x in set(names)]

	return counts
"""
	Experimental, need to see how accurate this is..
	If an entity fits completely into another entity as a substring, combine them to get accurate counts and reduce overall entities.
	
	Good: 'George' is in 'George W. Bush'
	Potentially Bad: If there are 2 people with the same first name in a doc..
"""
def match_names(ents):
	# match single names with full name
	reduced_ents = ents.copy()
	people = [x for x in reduced_ents if x['type'] == 'PERSON']
	for single in people:
		name = single['name'].lower()
		substrings = name.split(' ')
		# find matches where all substrings in another name
		possible_matches = [x for x in reduced_ents if all(item in x['name'].lower().split(' ') for item in substrings)
							and name != x['name'].lower()]

		if len(possible_matches) == 1:
			idx_single = reduced_ents.index(single)
			match = possible_matches[0]
			new_count = match['count'] + single['count']
			reduced_ents[reduced_ents.index(match)]['count'] = new_count
			reduced_ents.pop(idx_single)

	return reduced_ents
"""
	Remove stop words and named entities from document tokens to store in SQL, 
	potentially for richer search and matching.
	ie: "These words appear the most between these 2 entities"
"""
def get_tokens(tokens):
	tokens = [x for x in tokens if x[1] not in labels and x[0] not in stopset and (len(x[0]) > 1 or x[0].isalpha())]
	tokens = [x[0] for x in tokens]
	return " ".join(tokens).lower()

"""
	Generate token count per document
"""
def token_counter(tokens):
	tokens = [x for x in tokens if x[1] not in labels and x[0] not in stopset and (len(x[0]) > 1 or x[0].isalpha())]
	tokens = [x[0] for x in tokens]
	result = Counter()
	for token in set(tokens):
		result[token] = tokens.count(token)
	return result

"""
	Insert each entity into the SQL DB, insert relations between entity and document.
"""
def insert_entities(entities):
	for entity in entities:
		sql.insert_entity(entity['name'], entity['type'])
		sql.insert_foundin(entity['name'], entity['doc_id'], entity['count'])
	return
