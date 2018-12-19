import sql
import re
import matplotlib.pyplot as plt
from collections import Counter
from nltk.corpus import stopwords
from wordcloud import WordCloud

stopset = set(stopwords.words('english'))

"""
    Displays a work cloud from the documents a group of entities share
"""

def count_words(names):
    words_by_doc = [x[0] for x in sql.get_common_words(names)]
    all_words = " ".join(words_by_doc).split()
    regex = re.compile('.*[A-Za-z0-9].*')
    clean = [x for x in all_words if regex.match(x) and x not in stopset]
    counts = Counter(clean)
    return counts

def display_cloud(counts):
    word_cloud = WordCloud().generate_from_frequencies(counts)
    plt.imshow(word_cloud)
    plt.axis("off")
    plt.show()

if __name__ == '__main__':
    counts = count_words(['Kenneth Salazar'])
    display_cloud(counts)
    pass
