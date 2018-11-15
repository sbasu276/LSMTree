from polling import PollingSocket
from cache import Cache
from persistent import Persistent
from queue import Queue
from utils import Request, parse_req, add_response
import sys
from helper_threads import Thread, io_handler
import time
import pickle


class PollingServer:
    def __init__(self, host, port, cache_size, db_name, workers):
        self.host = host
        self.port = port
        self.cache = Cache(cache_size)
        self.persistent = Persistent(db_name)
        self.req_queue = Queue() # Unbounded queue
        self.resp_queue = Queue()
        self.pool = {}
        self.workers = workers #number of workers in thread pool
        self.polling = None
        self.pending_reqs = {} #dict to store pending requests on a key

    def init_helpers(self):
        for i in range(self.workers):
            self.pool[i] = Thread(target=io_handler, \
                                  args=(self.req_queue, \
                                        self.resp_queue,\
                                        self.persistent, ))
                                  
    def run_helpers(self):    
        for thread in self.pool.values():
            thread.start()

    def issue_request(self, req):
        if req.key not in self.pending_reqs:
            self.req_queue.put(req, block=False)
            self.pending_reqs[req.key] = [req]
        else:
            pend = self.pending_reqs[req.key]
            pend.append(req)
            self.pending_reqs[req.key] = pend

    def get_responses(self):
        responses = []
        for i in range(self.workers):
            response = self.resp_queue.get(block=False)
            if len(self.pending_reqs[response.key])==1:
                del self.pending_reqs[response.key]
            else:
                self.req_queue.put(self.pending_reqs[response.key][0], block=False)
                new_pending = self.pending_reqs[response.key]
                new_pending.pop(0)
                self.pending_reqs[response.key] = new_pending
            responses.append(response)
            if self.resp_queue.empty()==True:
                break
        return responses

    def __bring_to_cache(self, key, value, dirty):
        retkey, retval, succ = self.cache.insert(key, value, dirty)
        if retkey and retval:
            # Writeback dirty eviction
            wb_req = Request('WRITEBACK', retkey, retval)
            self.issue_request(wb_req)
        return succ

    def run_server(self):
        self.polling = PollingSocket(self.host, self.port)
        self.init_helpers()
        self.run_helpers()
        #num_req = 0
        #first = True
        #tot_time = 0

        while True:
            requests = self.polling.poll_connection()
            #start = time.time()
            #num_req += len(requests)
            #if num_req:
            #    if first:
            #        tot_time = 0
            #        first = False
            #    if tot_time >= 1:
            #        with open('ep.lg', 'a') as f:
            #            f.write(str(num_req)+" "+str(tot_time)+"\n")
            #        #print(str(num_req)+" "+str(tot_time)+"\n")
            #        num_req = 0
            #        tot_time = 0

            for request in requests:
                #Request is a tuple of sock, request_data
                data = request[1]
                #Process request here
                req = parse_req(data.decode('utf-8'))
                req.fd = request[0]

                if req.op == 'GET':
                    val = self.cache.get(req.key)
                    if val is None:
                        # Cache miss: non-blocking put to req_queue
                        # for helper_threads to consume 
                        self.issue_request(req)
                    else:
                        resp = val
                        add_response(self.polling.sock_data_map, request[0], resp)
                
                elif req.op == 'PUT':
                    retval = self.cache.put(req.key, req.value)    
                    if retval is None:
                        #Cache miss
                        self.issue_request(req)
                    else:
                        resp = "ACK"
                        add_response(self.polling.sock_data_map, request[0], resp)

                elif req.op in ['INSERT', 'DELETE']:
                    self.issue_request(req)

                else:
                    resp = "-1".encode('utf-8')
                    add_response(self.polling.sock_data_map, request[0], resp)

            # Consume from response queue (non-blocking)
            try:
                responses = self.get_responses()
                for resp in responses:
                    #Ignore for write-back requests
                    if resp.fd is None:
                        continue
                    
                    #resp.value is actual value for GET
                    #for PUT/INSERT resp.value is "ACK" message
                    #resp.value is "-1" for all errors
                    #Delete needs special check if nothing to 
                    #delete from both cache and persistent
                    if resp.op != 'DELETE':
                        add_response(self.polling.sock_data_map, resp.fd, resp.value)
                    
                    #Entry brought to cache line for GET/PUT miss and INSERT
                    if resp.op in ['GET', 'PUT', 'INSERT']:
                        if resp.value != "-1":
                            self.__bring_to_cache(resp.key, resp.value, False)
                    elif resp.op == 'DELETE':
                        result = self.cache.delete(req.key)
                        if resp.value == "-1" and result is False:
                            #Nothing to delete from cache/ persistent
                            add_response(self.polling.sock_data_map, resp.fd, "-1")
                        else:
                            add_response(self.polling.sock_data_map, resp.fd, "ACK")
                    else:
                        pass
                #self.cache.show()
            except:
                pass
            #tot_time = tot_time + time.time() - start

if __name__ == "__main__":
    host = sys.argv[1]
    port = int(sys.argv[2])
    cache_size = int(sys.argv[3])
    db_name = sys.argv[4]
    num_worker = int(sys.argv[5])
    server = PollingServer(host, port, cache_size, db_name, num_worker)
    server.run_server()
