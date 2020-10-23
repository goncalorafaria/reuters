from blist import sortedlist, blist
import pickle
import spacy
import random
import string
from lxml import etree as etree_lxml

from os.path import join, isfile
import spacy



class InvertedIndex():
    """
    This class represents a inverted index. essencially a dictionary that
    maps terms into postings.

    We can use normal dict operations.
    """
    def __init__(self, documents):
        self.count = len(documents)
        self.docsnames = documents
        self.docs = { i : documents[i] for i in range(self.count)}
        self.iindex = {}

    def add_doc(self, document):
        if document not in self.docs:
            self.docs[document] = self.count
        self.count +=1

    def __getitem__(self, x):
        return self.iindex[x]

    def __setitem__(self, index, value):
        self.iindex[index] = value

    def __contains__(self, item):

        return item in self.iindex

    def keys(self):
        return self.iindex.keys()

    def values(self):
        return self.iindex.values()

    def items(self):
        return self.iindex.items()

    def dump(self, filename):
        with open(filename, 'wb') as filehandler:
            pickle.dump(self, filehandler)

    def load(filename):
        with open(filename, 'rb') as filehandler:
            return pickle.load(filehandler)

    def worker_function(args):

        sharedqueue, batomic = args

        stop_words = spacy.lang.en.STOP_WORDS
        punctuations = string.punctuation

        nlp = spacy.load("en", disable=["parser","textcat"])

        i=0
        l=0
        docsnames = []
        while batomic.get() or (not sharedqueue.empty()):
            try:
                text, document = sharedqueue.get(False,500+random.randint(0, 1000))
                #print(document)
                doc = nlp(text)
                docsnames.append(document)
                i+=1
                ## 0. parse the xml
                ## 1. term -> (doc, freq)
                ## Posting(i, count[0])
                ## string.pontuation
                ## stop_words, lowercase , pontuation, symbols, nouns, apply lemmanizer  (all in the doc object spacy doc)
                ## do steaming
                ## 2. maybe build extra terms for entities doc.ents_
                ## 3. improve to Posting(i, count[0], positions) if they are required by the project statement.
                ## 4. make sure Postings are ordered by document id.
                ## 5. do document -> terms sparse matrix.

            except Exception as e:
                print(e)
                l+=1
                None

        ## inverted index sumarizer.
        ## obtain the term count.
        """
        countv = CountVectorizer(input="filename", tokenizer=LemmaTokenizer(),lowercase=True, stop_words=None)


        fmat = countv.fit_transform(documents)
        index = InvertedIndex(documents)

        for w, i in countv.vocabulary_.items():
            v = fmat[:,i]
            ix = v.nonzero()[0]
            plist = []
            for i,count in zip(ix,v[ix].toarray()):
                plist.append( Posting(i, count[0]) )

            index[w] = Postings(plist, csr_matrix.getnnz(v))


        """
        print("[Worker]End of one thread")
        index = InvertedIndex(docsnames)

        return index

    def reduce_function(args):
        indexes, d_start, sharedqueue = args
        tmp = blist([])

        while not sharedqueue.empty():
            try:
                k = sharedqueue.get(False,100+random.randint(0, 100))
            except:
                break
            s = 0
            j = 0
            indj = 0
            dlist = sortedlist([])
            for ind in indexes:
                if k in ind :
                    dlist.add(ind[k])

            for i in range(1,len(dlist)):
                dlist[i].dlist = d_start[i] + dlist[i].dlist

            while len(dlist) > 1:
                p1 = dlist[0]
                p2 = dlist[1]

                base = sortedlist([])
                base.update(p1.dlist)
                base.update(p2.dlist)

                r = Postings(base, p1.count + p2.count, False)

                dlist.discard(p1)
                dlist.discard(p2)

                dlist.add(r)

            tmp.append( (k,dlist[0]) )

        print("[Reduce]End of one thread")

        return tmp


class Postings():
    def __init__(self, dlist, count, cp=True):
        self.count = count
        if cp :
            self.dlist = sortedlist(dlist)
        else:
            self.dlist = dlist

    def __eq__(self, other):
        if not isinstance(other, Posting):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.count == other.count

    def __lt__(self, other):
        if not isinstance(other, Postings):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.count < other.count

    def __gt__(self, other):
        if not isinstance(other, Postings):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.count > other.count


class Posting():
    def __init__(self, id_, count):
        self.id_ = id_
        self.count = count

    def __eq__(self, other):
        if not isinstance(other, Posting):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.id_ == other.id_

    def __lt__(self, other):
        if not isinstance(other, Posting):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.id_ < other.id_

    def __gt__(self, other):
        if not isinstance(other, Posting):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.id_ > other.id_

    def __add__(self,value):
        self.id_+= value
        return self

    def __radd__(self,value):
        self.id_+= value
        return self

class DocChunks():
    def __init__(self, docs, sdir):
        self.docs = docs
        self.sdir = sdir
        self.count = len(docs)

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

    def worker_function(args):
        sharedqueue, batomic = args

        #docsnames = []
        nlp = spacy.load("en", disable=[
            "parser", "ner","entity_linker","textcat",
            "entity_ruler","sentencizer","merge_noun_chunks",
            "merge_entities","merge_subtokens"])
        tmp = blist([])
        stream = blist([])

        while batomic.get() or (not sharedqueue.empty()):
            try:
                xml_as_bytes, document = sharedqueue.get(False,500+random.randint(0, 1000))
                tree = etree_lxml.fromstring(xml_as_bytes)
                dt = blist([])
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

                stream.append(" ".join(dt))
                stream.append(headline)
                #"text": " ".join([ token.lemma_ for token in nlp(" ".join(dt))]),
                #"headline": " ".join([ token.lemma_ for token in nlp(headline) if not (token.is_punct or token.is_stop) ]),

                doc = {
                    "text": " ".join(dt),#" ".join([ token.lemma_ for token in nlp(" ".join(dt)) if not (token.is_punct or token.is_stop) ]),
                    "headline": headline,#" ".join([ token.lemma_ for token in nlp(headline) if not (token.is_punct or token.is_stop) ]),
                    "itemid": itemid,
                    "dateline": dateline,
                    "fname": document
                }

                tmp.append(doc)

            except Exception as e:
                #print(e)
                None

        return DocChunks(tmp, ".")
