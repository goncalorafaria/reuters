import threading
import concurrent.futures
from blist import blist
from os.path import join

from queue import Queue

from utils import AtomicBool
from core import BucketChunks
from tqdm import tqdm
import sys
import glob
import os
from time import time

## InvertedIndex : Terms -> Postings
## Pstings: Term , [Posting]

def reader_function(args):
    docs, sharedqueue, batomic , id, barrier, debug = args
    print("number of docs:" + str(len(docs)))

    if debug :
        docs = tqdm(docs)

    for d in docs:
        with open(d,"rb") as df:
            sharedqueue.put( (df.read(),d) )

    print("out of the reader.")

    barrier.wait()

    batomic.set(False)

def process_topics(path="./topics.txt",dir="."):
    docs = {}

    with open(path, "r") as xml:
        s = xml.read()
        for topic in s.split("<top>")[1:]:
            desc=[]
            narr=[]
            cache=None
            for i in topic.split("\n") :
                if i != "" :
                    reg = i.split(">")

                    if reg[0] == "<narr":
                        cache = narr
                    elif reg[0] == '</top':
                        None
                    elif reg[0] == '<num':
                        ss = reg[1].split(" ")
                        if len(ss[-1]) > 0 :
                            code = ss[-1]
                        else :
                            code = ss[-2]

                    elif reg[0] == '<title':
                        title = " ".join(reg[1].split(" ")[1:])
                    elif reg[0] == '<desc':
                        cache=desc
                    else:
                        cache.append(reg[0])

            entry = {"desc":" ".join(desc),"narr":" ".join(narr),"title":title}

            docs[code] = entry

    BucketChunks(docs, None, dir, index=False).dump(0, name="topicchunk")


def process_documents(documents, worker_function, NUM_WORKERS=3, QUEUE_SIZE=20, NUM_READERS=1,dir=".",debug=True):

    assert NUM_WORKERS > 0, " Must have  a postive number of workers"
    assert NUM_READERS > 0, " Must have  a postive number of readers"

    process_topics(path= join(dir,"topics.txt"))

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
    barrier = threading.Barrier(NUM_READERS, timeout=5000)
    ## creates NUM_READERS reader tasks.
    for i in range(NUM_READERS):
        if i+1 == NUM_READERS:
            executor.submit(reader_function, (documents[i*docs_per_thread:],sharedqueue, signal, i, barrier,debug) )
        else:
            executor.submit(reader_function, (documents[i*docs_per_thread :(i+1)*docs_per_thread],sharedqueue, signal, i, barrier,debug) )

    ## puts the worker threads reading the and processing the documents.
    if NUM_WORKERS > 1 :
        iresults = executor.map(worker_function, [(sharedqueue,signal)]*(NUM_WORKERS-1))
    else:
        iresults = []

    ## each worker produced a single inverted index.
    indexes = [ worker_function((sharedqueue,signal)) ] + list(iresults)

    ## REDUCE PHASE
    if len(indexes) == 1:
        # if only had 1 worker we don't need to reduce.
        indexes[0].sdir = dir
        return indexes[0]
    else:
        ndocs = sum([ i.count for i in indexes ])

        docs   = blist([ j for i in indexes for j in i.docs ])
        fnames = blist([ j for i in indexes for j in i.docind.keys() ])
        shards = ndocs//NUM_WORKERS

        for i in range(NUM_WORKERS-1):
            print(("chunk",i))
            bc = BucketChunks(
                        docs[i*shards:(i+1)*shards], fnames[i*shards:(i+1)*shards], dir)
            bc.dump( i )
            del bc

        i = NUM_WORKERS-1
        print(("chunk",i))
        bc = BucketChunks(
                docs[i*shards:], fnames[i*shards:], dir)
        bc.dump( i )


if __name__ == '__main__':
    assert len(sys.argv) > 2, "specify the number of workers and dir."
    assert int(sys.argv[1]) > 0, "number of shards must be a positive number."

    workers = int(sys.argv[1])
    rcvdir = sys.argv[2]

    coldir = join(rcvdir,"rcv1/")

    documents = [ i for a in os.listdir(coldir) for i in glob.glob( join(coldir,a)+"/*.xml" ) ]

    start_time = time()

    process_documents(
        documents = documents,
        worker_function = BucketChunks.worker_function, ## maps collection into invertedIndexes objects.
        NUM_WORKERS = workers , ## number of threads executing in the first stage the worker function , and in the second stage the reduce function.
        QUEUE_SIZE = 80, ## number of documents in queue at every single point.
        NUM_READERS = 2,
        dir = ".") ## number of threads reading documents to the queue.

    print("--- %s seconds ---" % (time() - start_time))
