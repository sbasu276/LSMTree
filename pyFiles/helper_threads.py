from queue import Queue
from threading import Thread, Lock

class OperationError(Exception):
    def __init__(self, op):
        self.op = op
    def __str__(self):
        return repr(self.op)

def io_handler(req_queue, resp_queue, persistent):
    while True:
        try:
            event = req_queue.get()
            if event.op == 'GET':
                value = persistent.get(event.key)
                event.value = value if value else "-1"
            elif event.op in 'PUT':
                try:
                    result = persistent.put(event.key, event.value)
                    if result is False:
                        event.value = "-1"
                except:
                    event.value = "-1"
            elif event.op in 'INSERT':
                try:
                    result = persistent.insert(event.key, event.value)
                    if result is False:
                        event.value = "-1"
                except:
                    event.value = "-1"
            elif event.op == 'DELETE':
                try:
                    result = persistent.delete(event.key)
                    if result:
                        event.value = "ACK"
                    else:
                        event.value = "-1"
                except:
                    event.value = "-1"
            elif event.op == 'WRITEBACK':
                #Internal operation. Not exposed.
                #print("WRITE BACK KEY: ", event.key, " VAL: ", event.value)
                persistent.writeback(event.key, event.value)
            else:
                pass
            resp_queue.put(event)
        except:
            pass
            
