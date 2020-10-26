
from utils import parse_feedback
from core import Bucket, get_topics_tfidf
from sklearn.metrics import pairwise_distances
import numpy as np
import matplotlib.pyplot as plt

qtrain = parse_feedback("./qrels.train")
qtest = parse_feedback("./qrels.test")

collection = Bucket(debug=True)

X,y = get_topics_tfidf(collection, qtrain, smoothing = 0.01, field="name")

avg_dist = [ np.mean( np.tril( pairwise_distances( X[s:e,:]) ) ) for s,e in y.values() ]

plt.hist( avg_dist, bins=60, density=True,label="Intra Topic Distance")
plt.xlabel( "Euclidean distance")
plt.ylabel( "Relative frequency")

plt.title("Intra Topic Distance")
plt.show()


# print( { token.text for token in ana("I think this text is great")} )

#print(intra)
