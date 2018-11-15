from queue import Queue
from multiprocessing import Pool as ThreadPool

class ThreadPool:
    class Error(Exception):
        pass

    def __init__(self, workers, capacity=None):
        from threading import Thread
        self.workers = workers
        self.capacity = capacity if capacity else workers
        self.pool = {}
        self.queue = Queue(self.capacity)
        self.ready = False
        for i in range(self.workers):
            self.pool[i] = Thread(target=self.__wait_on_queue)

    def __enter__(self):
        self.ready = True
        #for i in range(self.workers):
        #    self.pool[i].start()
        for worker in self.pool.values():
            worker.start()
        return self

    def map(self, fn, data, async=False):
        length = len(data)                   \
            if hasattr(data, '__len__')      \
            else self.capacity

        result = Queue(length)

        for element in data:
            self.queue.put((fn, result, (element,), {}))

        if not async:
            return [result.get() for i in range(length)]
        else:
            return (result.get() for i in range(length))

    def __wait_on_queue(self):
        while self.ready:
            fn, result, args, kwargs = self.queue.get()
            if fn is None:
                continue
            result.put(fn(*args, **kwargs))

    def __exit__(self, ev, et, tb):
        self.ready = False
        for i in range(self.workers):
            self.queue.put((None, None, None, None))

    def submit(self, fn, args=(), kwargs={}):
        result = Queue(1)
        self.queue.put((fn, result, args, kwargs))
        return result
