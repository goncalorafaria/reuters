from whoosh import index
from whoosh.qparser import *
from core import DocChunks

class Oracle():
    def __init__(
            self, sdir=".",
            chunks=5, indexdir="indexdir",
            indexname="usages"):

        self.ix = index.open_dir("indexdir", indexname="usages")
        self.qand = MultifieldParser(["headline","content"],self.ix.schema,group=AndGroup)
        self.qor = MultifieldParser(["headline","content"],self.ix.schema,group=OrGroup)
        self.chunks = [ DocChunks.load(sdir=sdir,id_=i) for i in range(chunks)]


    def query(self, q, limit=10):
        with self.ix.searcher() as searcher:
            results = searcher.search(q,  limit=limit)
            res_list = [r.values()[0] for r in results]
            adoc = set(res_list)

        docs = {}
        i=0
        for chunk in self.chunks:
            print("chunk number: " + str(i))
            filtered = adoc.intersection(chunk.docind.keys())
            for t in filtered:
                docs[t] =  chunk.docs[ chunk.docind[t] ]
                print("# :"+docs[t]["headline"] + " :: " + t)
                print(docs[t])
            adoc = adoc - filtered
            i+=1

        #return [ docs[r] for r in res_list ]




o = Oracle()

limit = 10

query = "Portuguese immigration Australia"

q = MultifieldParser(["headline","content"],o.ix.schema,group=AndGroup).parse(query)

o.query(q, limit=10)
