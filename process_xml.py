import glob
import os
from time import time
from os.path import join

from core import BucketChunks
from create import process_documents, process_topics

#rcvdir = "../proj/"
if __name__ == '__main__':
    assert len(sys.argv) > 2, "specify the number of workers and dir."
    assert int(sys.argv[1]) > 0, "number of shards must be a positive number."

    workers = int(sys.argv[1])
    rcvdir = sys.argv[2]

    coldir = join(rcvdir,"rcv1/")

    documents = [ i for a in os.listdir(coldir) for i in glob.glob( join(join(coldir,a),"/*.xml") ) ]

    start_time = time()

    process_documents(
        documents = documents,
        worker_function = BucketChunks.worker_function, ## maps collection into invertedIndexes objects.
        NUM_WORKERS = workers , ## number of threads executing in the first stage the worker function , and in the second stage the reduce function.
        QUEUE_SIZE = 80, ## number of documents in queue at every single point.
        NUM_READERS = 2,
        dir = ".") ## number of threads reading documents to the queue.

    print("--- %s seconds ---" % (time() - start_time))
