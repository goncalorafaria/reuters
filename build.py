import glob
import sys
from time import time

import os, os.path
from tqdm import tqdm
from whoosh import index
from whoosh.fields import *
from whoosh.analysis import *

from core import InvertedIndex, DocChunks
from create import process_documents, process_topics
from lxml import etree as etree_lxml

from random import randrange

#assert len(sys.argv) > 1, "You need to specify the number of chunks."


"""
w = myindex.writer()
# Get the analyzer object from a text field
stem_ana = w.schema["content"].format.analyzer
# Set the cachesize to -1 to indicate unbounded caching
stem_ana.cachesize = -1
# Reset the analyzer to pick up the changed attribute
stem_ana.clear()
"""


def work(it, topics):

    if not os.path.exists("indexdir"):
        os.mkdir("indexdir")

    #anah = CommaSeparatedTokenizer() |  IntraWordFilter(mergewords=False) | LowercaseFilter() | StopFilter(lang="en") | StemFilter(lang="en",cachesize=-1)
    ana = RegexTokenizer() |  IntraWordFilter(mergewords=True) | LowercaseFilter() | StopFilter(lang="en") | StemFilter(lang="en",cachesize=-1)

    schema = Schema(headline = KEYWORD(lowercase=True, field_boost=2.0,analyzer=ana),
                    content= TEXT(lang="en" ,analyzer=ana),
                    name=ID(unique=True,stored=True))

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
        doc = DocChunks.load(".", i)

        for fname, d in tqdm(doc.items()):
            writer.add_document(name=fname,headline=d["headline"], content=d["text"])

        writer.commit()
        writer = ix.writer(procs=4, limitmb=2048, batchsize=5000, multisegment=True)

        del doc
        j+=1


if __name__ == '__main__':

    assert len(sys.argv) > 1, "specify the number of shards."
    assert int(sys.argv[1]) > 0, "number of shards must be a positive number."

    start_time = time()

    topics = DocChunks.load(".", 0,"topicchunk")

    SHARDS = int(sys.argv[1])

    work(range(SHARDS),topics)

    print("--- %s seconds ---" % (time() - start_time))
