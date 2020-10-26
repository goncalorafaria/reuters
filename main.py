import glob
import os
from time import time

from core import BucketChunks, Bucket
from create import process_documents
from build import build_index

documents = [ i for a in os.listdir("../proj/rcv1/") for i in glob.glob("../proj/rcv1/" + a + "/*.xml") ]

def indexing(documents, SHARDS=5, dir =".", debug=False):
    """
    indexing(D,args):
        @input D and optional set of arguments on text preprocessing.
        @behavior preprocesses each document in D and builds an efficient inverted index.
        @output tuple with the inverted indexI, indexing time and space required.
    """

    start_time = time()
    #process_topics(path="./topics.txt")

    process_documents(
        documents = documents,
        worker_function = BucketChunks.worker_function, ## maps collection into invertedIndexes objects.
        NUM_WORKERS = SHARDS, ## number of threads executing in the first stage the worker function , and in the second stage the reduce function.
        QUEUE_SIZE = 40, ## number of documents in queue at every single point.
        NUM_READERS = 2,
        dir = dir) ## number of threads reading documents to the queue.

    timetaken1 = (time()-start_time)
    timetaken2 = build_index(SHARDS, sdir=dir)

    print("time taken in part1 : "+ str(timetaken1))
    print("time taken in part2 : "+ str(timetaken2))

    timetaken = timetaken1 + timetaken2

    collection = Bucket(debug=False)

    return collection, timetaken, None

def extract_topic_query(collection, qcode, k, extension=Bucket.Extension.Bo2):
    """
    extract_topic_query(q,I,k,args)
        @input topic q ∈ Q(identifer), inverted indexI, number of top terms for thetopic (k), and optional arguments on scoring
        @behavior selects the top-kinformative terms in q against I using parameterizable scoring
        @output list of k terms (a term can be either a word or phrase)
    """

    return collection.get_topics_terms(qcode, extension, limit=k)


def ranking(collection, qcode, limit, model = Bucket.Model.TF_IDF):
    """
    ranking(q,p,I,args)
        @input topic q ∈ Q (identifier), number of top documents to return (p), indexI,optional arguments on IR models
        @behavior uses directly the topic text (without extracting simpler queries) to rankdocuments in the indexed
            collection using the vector space model or pro-babilistic retrieval model
        @output ordered set of top-pdocuments, specically a list of pairs –(documentidentifier, scoring)– ordered in descending order of score

    """

    return collection.ranking(qcode, model, limit=limit)
