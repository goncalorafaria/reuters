import glob
import os
from time import time

from core import BucketChunks, Bucket
from create import process_documents
from build import build_index
from eval import eval

#documents = [ i for a in os.listdir("../proj/rcv1/") for i in glob.glob("../proj/rcv1/" + a + "/*.xml") ]

def indexing(documents, SHARDS=5, dir =".", debug=False):
    """
    indexing(D,args):
        @input      D and optional set of arguments on text preprocessing.
        @behavior   preprocesses each document in D and builds an efficient inverted index.
        @output     tuple with the inverted indexI, indexing time and space required.
    """

    start_time = time()
    #process_topics(path="./topics.txt")

    process_documents(
        documents = documents,
        worker_function = BucketChunks.worker_function, ## maps collection into invertedIndexes objects.
        NUM_WORKERS = SHARDS, ## number of threads executing in the first stage the worker function , and in the second stage the reduce function.
        QUEUE_SIZE = 40, ## number of documents in queue at every single point.
        NUM_READERS = 2,
        dir = dir,
        debug=debug) ## number of threads reading documents to the queue.

    timetaken1 = (time()-start_time)
    timetaken2 = build_index(SHARDS, sdir=dir,debug=debug)

    print("time taken in part1 : "+ str(timetaken1))
    print("time taken in part2 : "+ str(timetaken2))

    timetaken = timetaken1 + timetaken2

    collection = Bucket(chunks=SHARDS, debug=False)

    return collection, timetaken, None

def extract_topic_query(collection, qcode, k, model=Bucket.Model.TF_IDF):
    """
    extract_topic_query(q,I,k,args):
        @input      topic q ∈ Q(identifer), inverted indexI, number of top terms for thetopic (k), and optional arguments on scoring
        @behavior   selects the top-kinformative terms in q against I using parameterizable scoring
        @output     list of k terms (a term can be either a word or phrase)
    """

    return collection.get_topics_terms(qcode, model=model, limit=k)

def boolean_query(qcode, collection, k, model=Bucket.Model.TF_IDF):
    """
    boolean_query(q,k,I,args):
        @input      topicq(identifier), number of top terms k, and index I
        @behavior   maps  the  inpued  topic  into  a  simplified Boolean
                    query  using extract_topic_query and then search for matching* documents using the Boolean IR model
        @output     the filtered collection, specifically an ordered list of document identifiers

    *important: the Boolean querying should tolerate up toround(0.2×k)term mismatches
    """

    return collection.boolean_query( qcode, model=model, k=k)

def ranking(collection, qcode, limit, model = Bucket.Model.TF_IDF):
    """
    ranking(q,p,I,args):
        @input      topic q ∈ Q (identifier), number of top documents to return (p), indexI,optional arguments on IR models
        @behavior   uses directly the topic text (without extracting simpler queries) to rankdocuments in the indexed
                    collection using the vector space model or pro-babilistic retrieval model
        @output     ordered set of top-pdocuments, specically a list of pairs –(documentidentifier, scoring)– ordered in descending order of score

    """

    return collection.ranking(qcode, model, limit=limit)

def evaluation(collection, rtest, ranked=False, **kwargs):
    """
    evaluation(Qtest,Rtest,Dtest,args):
    @input          set of topics Qtest ⊆ Q, document collection Dtest, relevance feedback Rtest,
                    arguments on text processing and retrieval modes
    @behavior       uses the aforementioned functions of the target IR system to test simpleretrieval (Boolean querying)
                    tasks or ranking tasks for each topic q ∈ Qtest, and comprehensively evaluates the IR system against the
                    availablerelevance feedback
    @output         extensive evaluation statistics for the inpued queries, including recall-and-precision
                    curves at different output sizes, MAP, BPREF analysis, cu-mulative gains and efficiency.


    note: the input arguments are given in the form
            rtest =  topic -> [ ( doc.id, bool(relevance) ) ]
    """
    # rtest =  topic -> [ ( docids, bool ) ]
    # measure = "bref", "classic", "all"
    if "model" in kwargs:
        model = kwargs["model"]
    else:
        model = Bucket.Model.TF_IDF

    if ranked :

        if "limit" in kwargs:
            limit = kwargs["limit"]
        else:
            limit = 10

        pred = { qcode : set([ a for a,b in collection.ranking(qcode, model, limit=limit)]) for qcode in rtest.keys() }
        result, reduce_ps, reduce_rs, reduce_rb = eval(rtest, pred)

    else :

        if "k" in kwargs:
            k = kwargs["k"]
        else:
            k = 3

        if "extension" in kwargs:
            extension = kwargs["extension"]
        else:
            extension =  Bucket.Extension.Bo1

        pred = { qcode : set(collection.boolean_query(qcode, extension, model=model, k=k)) for qcode in rtest.keys() }
        result, reduce_ps, reduce_rs, reduce_rb = eval(rtest, pred)

    return result, reduce_ps, reduce_rs, reduce_rb



"""
 def myFun(**kwargs):
    for key, value in kwargs.items():
        print ("%s == %s" %(key, value))

# Driver code
myFun(first ='Geeks', mid ='for', last='Geeks')
"""
