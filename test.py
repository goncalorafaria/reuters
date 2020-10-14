import glob
import os
from time import time
import concurrent.futures

from core import InvertedIndex
from utils import cleanxml
from create import process_documents

docs = glob.glob("*.txt")[1:3]

a = process_documents(docs,worker_function = InvertedIndex.worker_function, reduce_function = InvertedIndex.reduce_function,
            clean_function=cleanxml, NUM_WORKERS=2, QUEUE_SIZE=500, NUM_READERS=2)

print(a.iindex.keys())
