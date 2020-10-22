import threading
from lxml import etree

#etree.XPath("//text()")( etree.fromstring(text) )
#etree.fromstring(text)
# for element in root.iter("text"):

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
