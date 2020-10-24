from core import BucketChunks, Bucket
from sys import getsizeof, stderr
from itertools import chain
from collections import deque

collection = Bucket(debug=True)

########## checkinf for unique id




############





#ids = collection.ranking("R197", limit=20, model = Bucket.Model.TF_IDF,expantion = Bucket.Extension.Bo2)

ids = collection.boolean_query("R197", k=5, expantion = Bucket.Extension.Bo2)

docs = collection.get_documents(ids)

print( [ d["headline"] for d in docs ])
