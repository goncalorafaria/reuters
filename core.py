from blist import blist
import pickle
import random
from lxml import etree as etree_lxml

from os.path import join


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
                fname = document
                placedate = dateline.split(" ")

                doc = {
                    "text": " ".join(dt),#" ".join([ token.lemma_ for token in nlp(" ".join(dt)) if not (token.is_punct or token.is_stop) ]),
                    "headline": headline,#" ".join([ token.lemma_ for token in nlp(headline) if not (token.is_punct or token.is_stop) ]),
                    "itemid": itemid,
                    "dateline": (" ".join(placedate[:-1]),placedate[-1]),
                    "fname": fname
                }

                tmp.append(doc)
                ktmp.append(fname)

            except Exception as e:
                #print(e)
                None

        return BucketChunks(tmp, ktmp, ".", index=True)
