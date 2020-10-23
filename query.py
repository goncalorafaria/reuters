from whoosh import index
from whoosh.qparser import *
from core import BucketChunks

class Bucket():
    def __init__(
            self, sdir=".",
            chunks=5, indexdir="indexdir",
            indexname="usages"):

        self.topics = BucketChunks.load(sdir, 0, "topicchunk")
        self.ix = index.open_dir("indexdir", indexname="usages")
        self.qand = MultifieldParser(["headline","content"],self.ix.schema,group=AndGroup)
        self.qor = MultifieldParser(["headline","content"],self.ix.schema,group=OrGroup)
        self.chunks = [BucketChunks.load(sdir=sdir, id_=i) for i in range(chunks)]

    def query(self, q, limit=10):
        with self.ix.searcher() as searcher:
            results = searcher.search(q,  limit=limit)
            res_list = [r.values()[0] for r in results]

        return res_list

    def get_documents(self, res_list):

        adoc = set(res_list)
        docs = {}
        i=0
        for chunk in self.chunks:
            filtered = adoc.intersection(chunk.docind.keys())
            for t in filtered:
                docs[t] = chunk.docs[ chunk.docind[t] ]
                print("# :"+docs[t]["headline"] + " :: " + t)
                #print(docs[t])
            adoc = adoc - filtered
            i+=1
            #print("####")
            #print(set(res_list).intersection(chunk.docind.keys()))
        #print(adoc)

        return docs


o = Bucket()

query = "Portuguese australia immigration"

q = MultifieldParser(["headline","content"],o.ix.schema,group=AndGroup).parse(query)

ids = o.query(q, limit=10)
docs = o.get_documents(ids)

print(str(len(ids)) + " -> ids returned.")
print( ids )
print(str(len(docs)) + " -> documents returned.")
print(docs.keys())
