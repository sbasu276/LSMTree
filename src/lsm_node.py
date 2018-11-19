import time

class LsmNode:
    def __init__(self, key, value, tombstone=False):
        self.key = key
        self.value = value
        self.timestamp = time.time()
        self.tombstone = tombstone
