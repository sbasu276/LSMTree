import logging

class WAL:
    def __init__(self, logfile):
        self.logger = logging.getLogger("lsm.LsmTree")
        self.fh = logging.FileHandler(logfile)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(self.fh)

    def txn(self, end=False):
        if end is False:
            self.logger.info("Txn_BEGIN")
        else:
            self.logger.info("Txn_END")

    def log(self, op, *args):
        msg = str(op)+str(args)
        self.logger.info(msg)
