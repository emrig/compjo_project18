import requests
import base64
import json
import pandas as pd

"""
    Get a document collection from Overview. Stores in csv for now, but wil bve integrated with SQL and be on-demand.
"""

files = 'https://www.overviewdocs.com/api/v1/files'
documentset_id = 'xxx'
encoded = base64.b64encode(b"XXXX:x-auth-token")
authHeader = "Basic " + encoded.decode("utf-8")
url = "https://www.overviewdocs.com/api/v1/document-sets/" + documentset_id + "/documents"
headers = { 'Authorization': authHeader }

r = requests.get(url, headers=headers)
docs = json.loads(r.text)
items = []

for doc in docs['items']:
    url = "https://www.overviewdocs.com/api/v1/document-sets/" + documentset_id + "/documents/" + str(doc['id'])
    r = requests.get(url, headers=headers)
    result = json.loads(r.text)
    items += [result]
    pass

table = pd.DataFrame(items)
table.to_csv('overview_docs.csv', index=False)

pass
