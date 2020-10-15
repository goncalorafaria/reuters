import numpy as np

from scipy.sparse import csr_matrix
import threading
import concurrent.futures
from blist import sortedlist, blist

from spacy.lang.en import English
import spacy
import string

from queue import Queue

import random

from utils import AtomicBool
from core import InvertedIndex
from tqdm import tqdm

## InvertedIndex : Terms -> Postings
## Pstings: Term , [Posting]

def reader_function(args):
    docs, sharedqueue, batomic, clean_function = args
    print("number of docs:" + str(len(docs)))
    for d in tqdm(docs) :
        with open(d,"r") as  df:
            sharedqueue.put( (clean_function(df.read()),d) )

    batomic.set(False)


def process_documents(documents, worker_function,reduce_function, clean_function, NUM_WORKERS=3, QUEUE_SIZE=20, NUM_READERS=1):

    assert NUM_WORKERS > 0, " Must have  a postive number of workers"
    assert NUM_READERS > 0, " Must have  a postive number of readers"

    document_number= len(documents)

    workers = []

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS+NUM_READERS-1)

    docs_per_thread = document_number//NUM_READERS
    #reduces_per_thread = NUM_WORKERS//NUM_WORKERS

    # this queue will be in shared memory.
    # The workers read from here and readers put work here.
    sharedqueue = Queue(QUEUE_SIZE)
    ## atomic boolean implemented with RWLock
    # it says if the readers are or are not still working
    signal = AtomicBool(True)

    ## creates NUM_READERS reader tasks.
    for i in range(NUM_READERS):
        if i+1 == NUM_READERS:
            executor.submit(reader_function, (documents[i*docs_per_thread:],sharedqueue, signal,  clean_function) )
        else:
            executor.submit(reader_function, (documents[i*docs_per_thread :(i+1)*docs_per_thread],sharedqueue, signal,  clean_function) )


    ## puts the worker threads reading the and processing the documents.
    if NUM_WORKERS > 1 :
        iresults = executor.map(worker_function, [(sharedqueue,signal)]*(NUM_WORKERS-1))
    else:
        iresults = []

    ## each worker produced a single inverted index.
    indexes = [ worker_function((sharedqueue,signal)) ] + list(iresults)

    ## REDUCE PHASE
    if len(indexes)== 1:
        # if only had 1 worker we don't need to reduce.
        return indexes[0]
    else:
        # we need to merge the inverted indexes.
        ks = [ set(i.keys()) for i in indexes] ## lists of terms per index.
        d_per_set = [ i.count for i in indexes] ## counts of documents per index
        d_start = [0]+ list(np.cumsum(d_per_set)) ## essencially the comulative sum of document counts. (used to fix the doc ids)
        #outputkeys = blist(set.union(*ks)) ### all of the keys of the output inverted index.

        ## this queue will be used to send keys to the workers.
        sharedqueue = Queue()

        for k in set.union(*ks):
            sharedqueue.put(k)

        ## docs will be a big list of document names.
        docs = []
        for ind in indexes:
            docs += ind.docsnames

        ## we will divide the terms into pieces and each will be processed by a worker thread.
        tmp = blist([])

        ## starts the reducing threads.
        if NUM_WORKERS > 1 :
            results = executor.map(
                reduce_function,
                zip([indexes] * (NUM_WORKERS-1), [d_start] * (NUM_WORKERS-1), [sharedqueue]*(NUM_WORKERS-1))
            )
            tmp.extend(
                reduce_function(  (indexes,d_start, sharedqueue ) )
            )
        else:
            results = []
            tmp.extend(
                reduce_function(  (indexes,d_start, sharedqueue ) )
            )

        ## combines their results.
        for r in results:
            tmp.extend(r)

        ## creates the index.
        rindex = InvertedIndex(docs)

        ## adds the postings
        for k, v in tmp:
            rindex[k] = v

        return rindex
