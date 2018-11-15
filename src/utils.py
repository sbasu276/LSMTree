class Request:
    def __init__(self, op=None, key=None, value=None, fd=None):
        self.op = op
        self.key = key
        self.value = value
        self.fd = fd

def parse_req(request):
    print("UTIL ", request)
    req = request.strip('\n').split()
    print(req)
    request = None
    if len(req)>2:
        request = Request(req[0], req[1], req[2])
    else:
        request = Request(req[0], req[1])
    return request

def add_response(mapper, sock, response):
    if sock is not None:
        data = mapper[sock]
        data.resp = response
        mapper[sock] = data

def get(key, cache, persistent, lock):
    lock.acquire()
    val = cache.get(key)
    lock.release()
    if val is None:
        val = persistent.get(key)
        if val:
            lock.acquire()
            retkey, retval = cache.insert(key, val, dirty=False)
            lock.release()
            if retkey and retval:
                persistent.writeback(retkey, retval)
    if val:
        return val
    else:
        return "-1"

def put(key, value, cache, persistent, lock):
    lock.acquire()
    retval = cache.put(key, value)
    lock.release()
    if retval is None:
        if persistent.put(key, value):
            lock.acquire()
            retkey, retval = cache.insert(key, value, dirty=False)
            lock.release()
            if retkey and retval:
                persistent.writeback(retkey, retval)
            return "ACK"
        else:

            return "-1"
    return "ACK"

def insert(key, value, cache, persistent, lock):
    if persistent.insert(key, value):
        lock.acquire()
        retkey, retval = cache.insert(key, value)
        lock.release()
        if retkey and retval:
            persistent.writeback(retkey, retval)
        return "ACK"
    return "-1"

def delete(key, cache, persistent, lock):
    pdel = persistent.delete(key)
    lock.acquire()
    cdel = cache.delete(key)
    lock.release()
    if pdel is False and cdel is False:
        return "-1"
    return "ACK"

OP_FUNC_MAPPER = {
            'GET': get,
            'PUT': put,
            'INSERT': insert,
            'DELETE': delete
        }

def call_api(req, cache, persistent, lock):
    if OP_FUNC_MAPPER.get(req.op):
        if req.op in ['GET', 'DELETE']:
            return OP_FUNC_MAPPER[req.op](req.key, cache, persistent, lock)
        else:
            return OP_FUNC_MAPPER[req.op](req.key, req.value, cache, persistent, lock)
    else:
        return "-1"
