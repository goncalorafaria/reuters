import glob
import os
from time import time

from core import BucketChunks
from create import process_documents, process_topics


#documents = glob.glob("exampletxt/*.txt")
documents = [ i for a in os.listdir("../proj/rcv1/") for i in glob.glob("../proj/rcv1/" + a + "/*.xml") ]

#documents =documents[:20000]

start_time = time()
process_topics(path="./topics.txt")


process_documents(
    documents = documents,
    worker_function = BucketChunks.worker_function, ## maps collection into invertedIndexes objects.
    NUM_WORKERS = 5, ## number of threads executing in the first stage the worker function , and in the second stage the reduce function.
    QUEUE_SIZE = 80, ## number of documents in queue at every single point.
    NUM_READERS = 2,
    dir = ".") ## number of threads reading documents to the queue.

print("--- %s seconds ---" % (time() - start_time))

#print(a)
