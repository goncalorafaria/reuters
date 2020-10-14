from blist import sortedlist, blist
import pickle

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
