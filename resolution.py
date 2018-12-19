import sql
from jellyfish import jaro_distance

# Arbitrary threshold for Jaro distance. I will experiment with this number and
# different types of clustering overall for my Master's project
thresh = 0.8

"""
Creates a matrix of potential matches for each entity in the database if distance is above threshold

TODO: Can cut process time in half by storing scores for each word getting prepared, now n^2
"""
def get_clusters(words):
    num = len(words)
    result = [None]*num
    def distance(word):
        matches = []
        for i in range(num):
            score = jaro_distance(word, words[i])
            if score > thresh and words[i] != word:
                matches.append(i)
        return matches

    for i in range(num):
        matches = distance(words[i])
        if len(matches) > 0:
            result[i] = matches

    clusters = cluster(result)

    text_clusters = [[words[y] for y in x] for x in clusters]

    # Backup clusters since this is a long running operation, probably will store in SQL here
    with open('distances.txt', 'w') as f:
        f.write(str(text_clusters))

    return clusters

"""
Creates clusters by traversing every match of a particular word. This isn't the best approach
but yields pretty good results for now.
"""
def cluster(result):
    clusters = []

    def get_children(parent, cluster):
        for child in result[parent]:
            if child not in cluster:
                yield child
                for grandchild in get_children(child, cluster):
                    if grandchild not in cluster:
                        yield grandchild

    for parent, children in enumerate(result):
        if children:
            cluster = [parent]
            cluster += get_children(parent, cluster)
            cluster = set(cluster)
            if cluster not in clusters:
                clusters.append(set(cluster))

    return clusters
