import threading
from whoosh.query import Term, And,Or
from whoosh.analysis import *
from collections import Counter
from tqdm import tqdm

#from core import Bucket

def combSum(ranks):
    tmp_dict = {}
    for rank in ranks:
        for elementid,value in rank:
            if elementid in tmp_dict :
                tmp_dict[elementid] += value
            else:
                tmp_dict[elementid] = value

    l = [ (k,v) for k,v in tmp_dict.items()]
    l.sort(reverse=True, key= (lambda a : a[1]))

    return l

def combMnz(ranks):
    tmp_dict = {}
    count_dict = {}

    for rank in ranks:
        for elementid,value in rank:

            if value > 0:
                if elementid in count_dict :
                    count_dict[elementid] += 1
                else :
                    count_dict[elementid] = 1

            if elementid in tmp_dict :
                tmp_dict[elementid] += value
            else:
                tmp_dict[elementid] = value

    for k, v in tmp_dict.items():
        if k in count_dict:
            tmp_dict[k] = v * count_dict[k]
        else:
            tmp_dict[k] = 0

    l = [ (k,v) for k,v in tmp_dict.items()]
    l.sort(reverse=True, key= (lambda a : a[1]))

    return l

def rrf(ranks, mitigation=80):

    assert mitigation >= 0 , "Mitigation parameter must be greater than or equal to zero."

    tmp_dict = {}

    for i in range(0, len(ranks)):
        j=0
        for e,v in ranks[i]:
            if e in tmp_dict:
                tmp_dict[e] += 1.0/(mitigation + j)
            else:
                tmp_dict[e] = 1.0/(mitigation + j)

            j+=1

    l = [ (k,v) for k,v in tmp_dict.items()]
    l.sort(reverse=True, key= (lambda a : a[1]))

    return l



def get_hypercounter(collection, pos = True):

    assert collection.debug, " Collection must be in debug mode."
    redu = Counter()

    if pos:
        pipe= RegexTokenizer() |  IntraWordFilter(mergewords=True) | LowercaseFilter() | StopFilter(lang="en") | StemFilter(lang="en",cachesize=-1)
    else:
        pipe = RegexTokenizer()


    for chunk in tqdm(collection.chunks):
        for doc in tqdm(chunk.docs):
            redu.update([i.text for i in pipe(doc["text"])])

    #redu = Counter()

    print("reducing.")

    #for c in tqdm(corpus):
    #    redu = redu + c

    return redu

def parse_feedback(path="./qrels.train"):
    qset = {}
    with open(path, "r") as xml:
        s = xml.read()

        cols = s.split("\n")
        for cs in cols:
            entry = cs.split(" ")
            if len(entry) == 3:
                code = entry[0]
                doc = entry[1]
                relevant = (int(entry[2])==1)
                if code in qset:
                    qset[code].append( (doc, relevant) )
                else:
                    qset[code] = [ (doc, relevant) ]

    return qset

"""
This function returns the power set.
    eg. powerset( {1,2}, 2) = [ [], [1], [2], [1,2] ]

"""
def powerset( s, size):
    if size > 0:
        elem = s.pop()
        r = powerset(s, size-1)

        for ss in list(r):
            ns = list(ss[0])
            c = ss[1] + 1
            ns.append(elem)
            r.append( (ns,c) )
    else :
        r = []
        r.append( ([],0) )

    return r

def parse_boolean_query( terms ):
    def truncated_powerset_query(ps, keept, fields):
        q = []
        for s,count in ps :
            if count >= keept : ## if the sequence qualifies.
                terms = []
                for t in s: ## the sequence of words must be present.
                    frf = [] ## if the word is present in any of the fields.
                    for f in fields:
                        frf.append( Term(f, t) )
                    terms.append( Or(frf) )

                q.append( And(terms) )

        return q
    tc = len(terms)
    keept = tc - tc//5 # minimum sequence of correct elements.
    ps = powerset(terms, tc)
    query = Or(truncated_powerset_query( ps, keept, ["content","headline"]))

    return query

"""Simple reader-writer locks in Python
Many readers can hold the lock XOR one and only one writer"""

class RWLock:
    """ A lock object that allows many simultaneous "read locks", but
    only one "write lock." """

    def __init__(self):
        self._read_ready = threading.Condition(threading.Lock(  ))
        self._readers = 0

    def acquire_read(self):
        """ Acquire a read lock. Blocks only if a thread has
        acquired the write lock. """
        self._read_ready.acquire(  )
        try:
            self._readers += 1
        finally:
            self._read_ready.release(  )

    def release_read(self):
        """ Release a read lock. """
        self._read_ready.acquire(  )
        try:
            self._readers -= 1
            if not self._readers:
                self._read_ready.notifyAll(  )
        finally:
            self._read_ready.release(  )

    def acquire_write(self):
        """ Acquire a write lock. Blocks until there are no
        acquired read or write locks. """
        self._read_ready.acquire(  )
        while self._readers > 0:
            self._read_ready.wait(  )

    def release_write(self):
        """ Release a write lock. """
        self._read_ready.release(  )


class AtomicBool():
    def __init__(self, initialvalue=True):
        self.b=initialvalue
        self.rwl = RWLock()

    def get(self):
        try:
            self.rwl.acquire_read()
            b = self.b
        finally:
            self.rwl.release_read()

        return b

    def set(self,value):
        try:
            self.rwl.acquire_write()
            self.b= value
        finally:
            self.rwl.release_write()
