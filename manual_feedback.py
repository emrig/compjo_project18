import sql

"""
    This is a dev utility to cycle through potential matches and resolve them on the command line.
"""


clusters = []
with open('distances.txt', 'r') as f:
    clusters = eval(f.read())
reduced = [x for x in clusters if len(x) < 5]
from collections import Counter
totals = Counter()
for group in reduced:
    totals[repr(group)] = sql.get_group_counts(group)
totals = totals.most_common(1000000)

types = ['PERSON', 'ORGANIZATION', 'LOCATION']

for entry in totals:
    print(entry)
    i = input("n or y\n")
    if i.lower() == 'y':
        name = input('type name\n')
        type = input('0. PERSON 1. ORGANIZATION 2. LOCATION\n')
        names = eval(entry[0])
        sql.merge_entity_group(name, names, types[int(type)])

