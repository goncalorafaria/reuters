import numpy as np

from scipy.sparse import csr_matrix
import threading
import concurrent.futures
from blist import sortedlist, blist

from spacy.tokenizer import Tokenizer
from spacy.lang.en import English
import spacy
import string

from queue import Queue
from utils import AtomicBool
import random

## InvertedIndex : Terms -> Postings
## Pstings: Term , [Posting]

def reader_function(args):
    docs, sharedqueue, batomic = args
    print("number of docs:" + str(len(docs)))
    for d in docs :
        with open(d,"r") as  df:
            sharedqueue.put( (df.read(),d) )

    batomic.set(False)

def worker_function(args):

    sharedqueue, batomic = args

    stop_words = spacy.lang.en.STOP_WORDS
    punctuations = string.punctuation

    index = InvertedIndex([])

    nlp = spacy.load("en", disable=["parser","textcat"])

    tokenizer = Tokenizer(nlp.vocab)
    i=0
    l=0
    while batomic.get() or (not sharedqueue.empty()):
        try:
            text, document = sharedqueue.get(False,500+random.randint(0, 1000))
            index.add_doc(document)
            doc = nlp(text)
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

        except :
            print("except")
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
    index = InvertedIndex([])

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


def process_documents(documents, worker_function,reduce_function, NUM_WORKERS, executor, QUEUE_SIZE=500, NUM_READERS=2):
    document_number= len(documents)

    workers = []

    docs_per_thread = document_number//NUM_READERS
    #reduces_per_thread = NUM_WORKERS//NUM_WORKERS

    # this queue will be in shared memory.
    # The workers read from here and readers put work here.
    sharedqueue = Queue(QUEUE_SIZE)
    ## atomic boolean implemented with RWLock
    # it says if the readers are or are not still working
    signal = AtomicBool(True)

    ## creates NUM_READERS reader tasks.
    for i in range(NUM_READERS):
        if i+1 == NUM_READERS:
            executor.submit(reader_function, (documents[i*docs_per_thread:],sharedqueue, signal) )
        else:
            executor.submit(reader_function, (documents[i*docs_per_thread :(i+1)*docs_per_thread],sharedqueue, signal) )


    ## puts the worker threads reading the and processing the documents.
    if NUM_WORKERS > 1 :
        iresults = executor.map(worker_function, [(sharedqueue,signal)]*(NUM_WORKERS-1))
    else:
        iresults = []

    ## each worker produced a single inverted index.
    indexes = [ worker_function((sharedqueue,signal)) ] + list(iresults)

    ## REDUCE PHASE
    if len(indexes)== 1:
        # if only had 1 worker we don't need to reduce.
        return indexes[0]
    else:
        # we need to merge the inverted indexes.
        ks = [ set(i.keys()) for i in indexes] ## lists of terms per index.
        d_per_set = [ i.count for i in indexes] ## counts of documents per index
        d_start = [0]+ list(np.cumsum(d_per_set)) ## essencially the comulative sum of document counts. (used to fix the doc ids)
        #outputkeys = blist(set.union(*ks)) ### all of the keys of the output inverted index.

        ## this queue will be used to send keys to the workers.
        sharedqueue = Queue()

        for k in set.union(*ks):
            sharedqueue.put(k)

        ## docs will be a big list of document names.
        docs = []
        for ind in indexes:
            docs += ind.docsnames

        ## we will divide the terms into pieces and each will be processed by a worker thread.
        tmp = blist([])

        ## starts the reducing threads.
        if NUM_WORKERS > 1 :
            results = executor.map(
                reduce_function,
                zip([indexes] * (NUM_WORKERS-1), [d_start] * (NUM_WORKERS-1), [sharedqueue]*(NUM_WORKERS-1))
            )
            tmp.extend(
                reduce_function(  (indexes,d_start, sharedqueue ) )
            )
        else:
            results = []
            tmp.extend(
                reduce_function(  (indexes,d_start, sharedqueue ) )
            )

        ## combines their results.
        for r in results:
            tmp.extend(r)

        ## creates the index.
        rindex = InvertedIndex(docs)

        ## adds the postings
        for k, v in tmp:
            rindex[k] = v

        return rindex

# In[11]:

create_inverted_index = lambda documents, NUM_WORKERS, executor : process_documents(documents, worker_function, reduce_function, NUM_WORKERS, executor)
#iindex = process_documents(documents, worker_function, reduce_function, NUM_WORKERS=6)


# In[ ]:
