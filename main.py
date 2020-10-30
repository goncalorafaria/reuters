import glob
import os
from time import time

from core import BucketChunks, Bucket, canonical_ranking, bool_query_worker_function, rank_query_worker_function, cononical_boolean_query
from create import process_documents
from build import build_index
from eval import eval
from utils import rrf
import concurrent.futures
from multiprocessing import Pool

import numpy as np

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

    #dbytes = sum([ os.path.getsize("bin/" + i) for i in os.listdir('bin')])
    igB = sum([ os.path.getsize(dir +"/indexdir/" + i) for i in os.listdir(dir + '/indexdir')])/float(1024*1024*1024)

    return collection, timetaken, igB

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

    return collection.boolean_query( qcode,  model=model, k=k)

def ranking(collection, qcode, limit, model = Bucket.Model.TF_IDF, fusion=rrf):
    """
    ranking(q,p,I,args):
        @input      topic q ∈ Q (identifier), number of top documents to return (p), indexI,optional arguments on IR models
        @behavior   uses directly the topic text (without extracting simpler queries) to rankdocuments in the indexed
                    collection using the vector space model or pro-babilistic retrieval model
        @output     ordered set of top-pdocuments, specically a list of pairs –(documentidentifier, scoring)– ordered in descending order of score

    """

    return collection.ranking(qcode, model, limit=limit, fusion=fusion)


def evaluation(qtest,rtest,dtest,ranked=True, model=[Bucket.Model.TF_IDF,Bucket.Model.BM25F], **kwargs):
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
    if isinstance(model,list):
        model = [ m.value for m in model ]
    else:
        model = model.value

    if "beta" in kwargs:
        beta= kwargs["beta"]
    else:
        beta=0.5

    if "WORKERS" in kwargs:
        workers = kwargs["WORKERS"]
        if workers > 1:
            #executor = concurrent.futures.ThreadPoolExecutor(max_workers=workers-1)
            if "pool" in kwargs:
                executor = kwargs["pool"]
            else:
                executor = Pool(workers-1)
    else:
        workers = 1

    if "collection" in kwargs:
        collection= kwargs["collection"]
    else:
        collection, timetaken, _ = indexing(dtest, SHARDS=5, dir =".", debug=False)
        print( ("time",timetaken) )

    ## split the work
    print("number workloads:" + str(len(qtest)))

    if ranked :

        if "limit" in kwargs:
            limit = kwargs["limit"]
        else:
            limit = 1000

        if "fusion" in kwargs:
            fusion = kwargs["fusion"]
        else:
            fusion = rrf

        wfunc = rank_query_worker_function

    else:

        if "k" in kwargs:
            limit = kwargs["k"]
        else:
            limit = 3

        fusion=None

        wfunc = bool_query_worker_function

    q_per_thread = len(qtest)//workers
    th_work = []
    work_q = list(qtest)
    for i in range(workers-1):
        pw = work_q[i*q_per_thread:(i+1)*q_per_thread]
        th_work.append(
            executor.apply_async( wfunc, (pw, model, limit, { q: collection.topics.docs[q] for q in pw}, fusion) )
        )

    pw =work_q[(workers-1)*q_per_thread:]
    p1 = wfunc( pw, model, limit, { q: collection.topics.docs[q] for q in pw} )

    results = p1 + [ e for f in th_work for e in f.get(timeout=None)]

    pred = { qcode: v for qcode,v in results }
    ### doing the work.

    result, measures = eval(rtest, pred, beta=beta)

    reduced = {}

    for k in measures:
        reduced[k] = []

    for _,v in result.items():
        for k, nv in v.items():
            reduced[k].append(nv)

    for k in measures:
        reduced[k] = ( np.mean(reduced[k]), np.std(reduced[k]) )

    return result, reduced
