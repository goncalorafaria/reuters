import glob
import os
from time import time
import concurrent.futures

from core import InvertedIndex
from utils import cleanxml
from create import process_documents

documents = glob.glob("exampletxt/*.txt")
#print(documents)


start_time = time()

a = process_documents(
    documents = documents,
    worker_function = InvertedIndex.worker_function, ## maps collection into invertedIndexes objects.
    reduce_function = InvertedIndex.reduce_function, ## reduces a key of many inverded index objects.
    clean_function = cleanxml, ## text processing step after reading and before going to the worker function.
    NUM_WORKERS = 3, ## number of threads executing in the first stage the worker function , and in the second stage the reduce function.
    QUEUE_SIZE = 20, ## number of documents in queue at every single point.
    NUM_READERS = 1) ## number of threads reading documents to the queue.

print("--- %s seconds ---" % (time() - start_time))

#print(a.docs)
