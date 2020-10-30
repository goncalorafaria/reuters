from blist import blist
import pickle
import random
from lxml import etree as etree_lxml
from whoosh.qparser import *
from whoosh.classify import Bo1Model, Bo2Model, KLModel
from whoosh.scoring import *
from whoosh.analysis import *
from enum import Enum
from utils import parse_boolean_query, rrf
from time import time
from collections import Counter
import numpy as np
from math import log

from os.path import join
from whoosh import index

class BucketChunks():
    def __init__(self, docs, fnames, sdir, index=True):
        self.docs = docs
        self.count = len(docs)
        self.docind = {}

        #print( ":number of docs: "+ str(self.count) )

        if index :
            for i in range(self.count):
                self.docind[fnames[i]] = i

        self.sdir = sdir

    def dump(self,id_, name="chunk"):
        filename = join(self.sdir,"bin/"+ name + str(id_))
        with open(filename, 'wb') as filehandler:
            pickle.dump(self, filehandler)

    def load(sdir,id_, name="chunk"):
        filename = join(sdir,"bin/" + name + str(id_))
        with open(filename, 'rb') as filehandler:
            return pickle.load(filehandler)

    def add(self, chunk):
        for i in chunk.docs :
            self.docs.append(i)
            self.count += 1

    def items(self):
        return self.docs

    def worker_function(args):
        sharedqueue, batomic = args

        #docsnames = []
        #nlp = spacy.load("en", disable=[
        #    "parser", "ner","entity_linker","textcat",
    #        "entity_ruler","sentencizer","merge_noun_chunks",
    #        "merge_entities","merge_subtokens"])
        tmp = blist([])
        ktmp = blist([])
        placedate =None
        while batomic.get() or (not sharedqueue.empty()):
            try:
                xml_as_bytes, document = sharedqueue.get(False,500+random.randint(0, 1000))
                tree = etree_lxml.fromstring(xml_as_bytes)
                dt = []
                for text in tree.findall('./text', {}):
                    for line in text:
                         dt.append(line.text)

                #texts = [ line.text for line in text for text in tree.findall('./text', {})]
                headline = tree.find('./headline', {}).text
                itemid = tree.find('.', {}).attrib["itemid"]
                mdline =  tree.find('./dateline', {})

                del xml_as_bytes
                del tree

                if mdline is not None:
                    dateline = mdline.text
                    placedate = dateline.split(" ")
                else :
                    dateline = None
                    placedate = [" "]

                #stream.append(" ".join(dt))
                #stream.append(headline)
                #"text": " ".join([ token.lemma_ for token in nlp(" ".join(dt))]),
                #"headline": " ".join([ token.lemma_ for token in nlp(headline) if not (token.is_punct or token.is_stop) ]),
                fname = itemid
                #placedate = dateline.split(" ")


                doc = {
                    "text": " ".join(dt),#" ".join([ token.lemma_ for token in nlp(" ".join(dt)) if not (token.is_punct or token.is_stop) ]),
                    "headline": headline,#" ".join([ token.lemma_ for token in nlp(headline) if not (token.is_punct or token.is_stop) ]),
                    "itemid": itemid,
                    "dateline": (" ".join(placedate[:-1]),placedate[-1]),
                }

                tmp.append(doc)
                ktmp.append(fname)

            except Exception as e:
                #print(e)
                #print(type(e))
                #print(placedate)
                None

        return BucketChunks(tmp, ktmp, ".", index=True)

class Bucket():
    Model = Enum("Model"," ".join(['BM25F','DFREE','PL2','TF_IDF','FREQUENCY'] ))
    model_calls = [ BM25F(), DFree(), PL2(), TF_IDF(), Frequency()]
    Extension = Enum("Extension"," ".join(["Bo1","Bo2","KL"]))
    extension_calls = [Bo1Model, Bo2Model, KLModel]

    def __init__(
            self, sdir=".",
            chunks=5, indexdir="indexdir",
            indexname="usages",
            debug = False):

        self.debug = debug
        self.topics = BucketChunks.load(sdir, 0, "topicchunk")
        self.ix = index.open_dir("indexdir", indexname="usages")
        if debug :
            self.chunks = [BucketChunks.load(sdir=sdir, id_=i) for i in range(chunks)]
        self.weighting = TF_IDF()

    def set_weighting_method(self, weighting):
        #print(weighting.value)
        self.weighting = Bucket.model_calls[weighting.value-1]

    def boolean_query(self, qcode, model, k=5):

        return cononical_boolean_query(qcode, model= model.value, k=k, cache=self.topics.docs, ix = self.ix)

    def query(self, q, limit=10, sortedby="name", sort=False):

        with self.ix.searcher(weighting=self.weighting) as searcher:
            if not sort:
                results = searcher.search(q,  limit=limit)
            else :
                results = searcher.search(q,  limit=limit, sortedby=sortedby)

        return results

    def ranking(self, qcode, model, limit=20, fusion=rrf):
        if isinstance(model,list):
            model = [ m.value for m in model ]
        else:
            model = model.value

        return canonical_ranking(qcode, model=model.value, cache=self.topics.docs, limit=limit, fusion=rrf, ix = self.ix )


    def get_topics_terms(self,code, model, limit=5, cache=None, apart=False):
        return cononical_get_topics_terms(code, model=model.value, limit=limit, cache=self.topics.docs, ix = self.ix)

    def get_documents(self, res_list):

        assert self.debug, "You have to be in debug mode to retrieve docs."

        adoc = set(res_list)
        docs = {}
        i=0
        for chunk in self.chunks:
            filtered = adoc.intersection(chunk.docind.keys())
            for t in filtered:
                docs[t] = chunk.docs[ chunk.docind[t] ]
                #print("# :"+docs[t]["headline"] + " :: " + t)
                #print(docs[t])
            adoc = adoc - filtered
            i+=1
            #print("####")
            #print(set(res_list).intersection(chunk.docind.keys()))
        #print(adoc)

        return [docs[r] for r in res_list]

    def get_document(self, name):
        return self.get_documents([name])[0]


def get_topics_tfidf(collection, qtrain, smoothing = 0.01, field="name"):
    intra = {}
    for q, list in qtrain.items() :
        intra[q]=[]
        for id_, b in list :
            if b :
                intra[q].append(id_)

    ana = RegexTokenizer() |  IntraWordFilter(mergewords=True) | LowercaseFilter() | StopFilter(lang="en") | StemFilter(lang="en",cachesize=-1)

    uterms = []
    tfs = {}

    for q, ids_ in intra.items():
        docs = collection.get_documents(ids_)
        inventory = Counter()
        tfs[q] = [ Counter([ token.text for token in ana(d["text"])]) for d in docs ]
        bows = [ set(i.keys()) for i in tfs[q]]
        qbow = set.union(*bows)
        uterms.append(qbow)

    allterms = set.union(*uterms)

    with collection.ix.searcher(weighting=Bucket.Model.TF_IDF) as searcher:
        wmodel = TF_IDF()
        bigtable = { t: wmodel.idf( searcher, "content", t) for t in allterms }

    tidf = { q : [ { t: log(1+f)*bigtable[t] for t,f in d.items() } for d in dds ] for q, dds in tfs.items() }

    n = len(allterms)

    indcov = { t: i for t, i in zip(allterms,range(n)) }

    y = {}
    x = []
    i=0
    for q, dds in tfs.items():
        s = i
        for dd in dds:
            vi  = np.ones((n))*smoothing
            for k,value in dd.items():
                vi[ indcov[k] ] = value
            #y.append(q)
            x.append(vi)
        i+=len(dds)
        y[q] = (s,i)


    X = np.array(x)

    return X, y


def canonical_ranking(qcode, model, cache,limit=20, fusion= rrf ,ix = index.open_dir("indexdir", indexname="usages")):

    if isinstance(model,list):
        ranks = [ canonical_ranking(qcode, model=m, cache=cache, limit=limit, ix = ix ) for m in model ]
        l = rrf(ranks)
        if len(l) > limit :
            return l[:limit]
        else:
            return l


    wm = Bucket.model_calls[model-1]

    topic = cache[qcode]

    querysnip = topic["narr"]+"  "+topic["title"]

    txt = "".join(querysnip.split("\""))

    q = MultifieldParser(["headline","content"],
                ix.schema,group=OrGroup).parse(txt)

    with ix.searcher(weighting=wm) as searcher:
            results = searcher.search(q,  limit=limit)

            r = [ (searcher.stored_fields(id_)["name"], sc) for id_,sc in results.items() ]

    return r

def cononical_get_topics_terms(code, model, limit=5, cache=None, ix = index.open_dir("indexdir", indexname="usages")):

    wm = Bucket.model_calls[model-1]

    topic = cache[code]

    querysnip = topic["narr"]+"  "+topic["title"]
    txt = "".join(querysnip.split("\""))

    ana = RegexTokenizer() |  IntraWordFilter(mergewords=True) | LowercaseFilter() | StopFilter(lang="en") | StemFilter(lang="en",cachesize=-1)

    txt = " ".join([tk.text for tk in ana(txt)])

    with ix.searcher(weighting=wm) as searcher:

        tl = [ (token.text,wm.idf( searcher,"content",token.text)) for token in ana(txt) ]

    tl = [ (k[0],(v*k[1])) for k,v in Counter(tl).items() ]

    tl.sort(reverse=True,key= (lambda a : a[1]) )

    return set([ a for a,b in tl[:limit]])

def cononical_boolean_query(qcode, model, k=5, cache=None, ix = index.open_dir("indexdir", indexname="usages")):

    wm = Bucket.model_calls[model-1]

    tt = cononical_get_topics_terms(
                code=qcode, limit=k, model=model, cache=cache)

    query = parse_boolean_query(tt)

    with ix.searcher(weighting=wm) as searcher:
        results = searcher.search(query,  limit=None, sortedby="name")
        results = [ r.values()[0] for r in results]

    return results

def bool_query_worker_function( qtest, model, k,cache, fusion=None):

    pred = [ ( qcode,
        set(cononical_boolean_query(qcode, model=model, k=k, cache=cache))
        ) for qcode in qtest ]

    return pred

def rank_query_worker_function(qtest, model, limit, cache, fusion=rrf):

    pred = [ ( qcode,
        set([ a for a,b in canonical_ranking(qcode, model = model, limit=limit, cache=cache, fusion=fusion)])
        ) for qcode in qtest ]

    return pred
