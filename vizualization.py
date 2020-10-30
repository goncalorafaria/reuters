from utils import parse_feedback
from core import Bucket, get_topics_tfidf
from sklearn.metrics import pairwise_distances
import numpy as np
import matplotlib.pyplot as plt
from sklearn.manifold import TSNE,Isomap, MDS

# Instantiate the visualizer

qtrain = parse_feedback("./qrels.train")
qtest = parse_feedback("./qrels.test")

collection = Bucket(debug=True)

X,y = get_topics_tfidf(collection, qtrain, smoothing = 0, field="name")

c = [ j for j,( k,(s,e)) in zip(range(len(y)),y.items()) for i in range(s,e) ]

def plot_intra_topic_distance(X,y):

    avg_dist = [ np.mean( np.tril( pairwise_distances( X[s:e,:]) ) ) for s,e in y.values() ]

    plt.hist( avg_dist, bins=60, density=True,label="Intra Topic Distance")
    plt.xlabel( "Euclidean distance")
    plt.ylabel( "Relative frequency")

    plt.title("Intra Topic Distance")
    plt.show()

def plot_avg_distance(X,y):

    d = np.tril( pairwise_distances( X ) )
    l=[]

    for i in range(X.shape[0]) :
        for j in range(X.shape[1]) :
            if i > j :
                l.append(d[i,j])

    plt.hist( l, bins=60, density=True,label="Relevant documents Distance")
    plt.xlabel( "Euclidean distance")
    plt.ylabel( "Relative frequency")

    plt.title("Relevant documents Distance")
    plt.show()


def create_hypercounters(chunks=6):
    collection = Bucket(chunks=chunks,debug=True)

    hypercounter = get_hypercounter(collection, pos = False)
    pickle.dump( hypercounter, open( "pre.hypercounter", "wb" ) )

    hypercounter = get_hypercounter(collection, pos = True)
    pickle.dump( hypercounter, open( "pos.hypercounter", "wb" ) )

plot_intra_topic_distance(X,y)
    #topic_vec = [ np.mean( X[s:e,:], axis=0)for s,e in y.values() ]
    #None

#print(c)

def plot_dim_reduction(X):
    X_embedded = MDS(n_components=2).fit_transform(X)

    plt.scatter(X_embedded[:,0], X_embedded[:,1], c = c)
    plt.show()


# print( { token.text for token in ana("I think this text is great")} )

#print(intra)
