from time import time

import os, os.path
from tqdm import tqdm
from whoosh import index
from whoosh.fields import *
from whoosh.analysis import *

from core import BucketChunks

#assert len(sys.argv) > 1, "You need to specify the number of chunks."

def work(it, sdir):

    if not os.path.exists("indexdir"):
        os.mkdir("indexdir")

    #anah = CommaSeparatedTokenizer() |  IntraWordFilter(mergewords=False) | LowercaseFilter() | StopFilter(lang="en") | StemFilter(lang="en",cachesize=-1)
    ana = RegexTokenizer() |  IntraWordFilter(mergewords=True) | LowercaseFilter() | StopFilter(lang="en") | StemFilter(lang="en",cachesize=-1)

    schema = Schema(headline = KEYWORD(lowercase=True, field_boost=2.0,analyzer=ana),
                    content= TEXT(lang="en", phrase=False, chars=False, analyzer=ana,vector=True),
                    name=ID(unique=True, stored=True, sortable=True))

    ix = index.create_in("indexdir", schema=schema, indexname="usages")
    #ix = index.open_dir("indexdir", indexname="usages")
    writer = ix.writer(procs=4, limitmb=2048, batchsize=5000, multisegment=True)

    #for field in ["content","headline"]:
    #   stem_ana = writer.schema[field].format.analyzer
    #   stem_ana.cachesize = -1
    #   stem_ana.clear()
    j = 0
    for i in it:
        print( "Processing the chunk: " + str(i) )
        doc = BucketChunks.load(sdir, i)

        for d in tqdm(doc.docs):
            writer.add_document(name=d["itemid"],headline=d["headline"], content=d["text"])

        writer.commit()
        writer = ix.writer(procs=4, limitmb=2048, batchsize=5000, multisegment=True)

        del doc
        j+=1

def build_index(num_shards=5, sdir="."):
    #assert len(sys.argv) > 1, "specify the number of shards."
    #assert int(sys.argv[1]) > 0, "number of shards must be a positive number."
    #SHARDS = int(sys.argv[1])
    start_time = time()

    work(range(num_shards),sdir)

    return (time() - start_time)

if __name__ == '__main__':
    assert len(sys.argv) > 1, "specify the number of shards."
    assert int(sys.argv[1]) > 0, "number of shards must be a positive number."
    SHARDS = int(sys.argv[1])

    timetaken = build_index(num_shards=5)
    print("--- %s seconds ---" % (timetaken))
