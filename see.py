import glob
import os
from time import time
import concurrent.futures

from iindex import create_inverted_index
import spacy

docs = glob.glob("*.txt")[1:3]

executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
a = create_inverted_index(docs, 1, executor)

print(a.iindex.keys())
