from blist import blist
import pickle
import random
from lxml import etree as etree_lxml
from whoosh.qparser import *
from whoosh.classify import Bo1Model, Bo2Model, KLModel
from whoosh.scoring import *
from enum import Enum

from os.path import join
from whoosh import index

class BucketChunks():
    def __init__(self, docs, fnames, sdir, index=True):
        self.docs = docs
        self.count = len(docs)
        self.docind = {}

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
                else :
                    dateline = None

                #stream.append(" ".join(dt))
                #stream.append(headline)
                #"text": " ".join([ token.lemma_ for token in nlp(" ".join(dt))]),
                #"headline": " ".join([ token.lemma_ for token in nlp(headline) if not (token.is_punct or token.is_stop) ]),
                fname = itemid
                placedate = dateline.split(" ")

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

    def query(self, query, limit=10, terms=False, sortedby="name"):

        q = MultifieldParser(["headline","content"],self.ix.schema, group=OrGroup).parse(query)

        with self.ix.searcher(weighting=self.weighting) as searcher:
            if not terms:
                results = searcher.search(q,  limit=limit, terms=terms)
                results = [r.values()[0] for r in results]
            else :
                results = searcher.search(q,  limit=limit, terms=terms, sortedby=sortedby)

        return results

    def boolean_query(self, qcode, expantion, k=5):

        self.set_weighting_method(Bucket.Model.FREQUENCY)

        tt = self.get_topics_terms(
                    code=qcode, limit=k,
                    expantion=expantion)

        query = " ".join(tt)

        return self.query(query, limit=None, terms=True, sortedby="name")

    def ranking(self, qcode, expantion, model, limit=20):

        self.set_weighting_method(model)

        topic = self.topics.docs[qcode]

        return self.query(topic["narr"] + "  "+ topic["title"], limit=limit)

    def get_topics_terms(self,code, expantion, limit=5):

        topic = self.topics.docs[code]
        with self.ix.searcher(weighting=self.weighting) as searcher:
            r1 = searcher.key_terms_from_text("content", topic["narr"] +"  "+ topic["title"],
                    model= Bucket.extension_calls[expantion.value-1], numterms=limit)

        terms = set(list(zip(*r1))[0])
        """
        r = set()
        s = []
        for t,_ in ans:
            if t not in r:
                s.append(t)
                r.add(t)
            if len(r) == limit:
                break
        """
        return terms

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
