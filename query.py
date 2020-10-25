from core import BucketChunks, Bucket
#from sys import getsizeof, stderr
#from itertools import chain
#from collections import deque

collection = Bucket(debug=True)

#def evaluate(q, relevance):


#########
r = collection.ranking("R197", model= Bucket.Model.TF_IDF, limit=2)

for id_, sc in r :
    print("####")
    print( sc )
    print( collection.get_document(id_)["headline"] )

#########

k = 5
leaveout = k//5

collection = Bucket(debug=True)

print(collection.topics.docs["R197"]["narr"])

ss = sum([ chunk.count for chunk in collection.chunks ])

print("searching over " + str(ss) + " documents.")

ids = collection.boolean_query("R197", k=5, expantion = Bucket.Extension.Bo2)

print(ids)
docs = collection.get_documents(ids)

print( [ d["headline"] for d in docs ])
