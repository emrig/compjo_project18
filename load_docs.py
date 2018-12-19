import pandas as pd
from standford_ner import stan_parse

"""
    Main. Loads docs from CSV (for now) and calls the Extraction and store pipeline
"""

file = './data/overview_docs.csv'
out_file = 'dev_output.csv'

# Extract and store into SQL
docs = pd.read_csv(file)[['id', 'text']]

# Save to CSV for dev analysis
counts, top_tokens = stan_parse(list(docs.values))
pd.DataFrame(counts).to_csv('stan_ner_overview_results_merged.csv', index=False)
pd.DataFrame(top_tokens).to_csv('stan_ner_overview_top_tokens.csv', index=False)
