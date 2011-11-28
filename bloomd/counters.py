"""
This module exports the Counters object which is a simple
class for managing various counters related to filters.
"""

class Counters(object):
    "Tracks opcounters"
    def __init__(self):
        self.set_hits = 0
        self.set_misses = 0
        self.check_hits = 0
        self.check_misses = 0
        self.page_outs = 0
        self.page_ins = 0

    @property
    def sets(self):
        return self.set_hits + self.set_misses

    @property
    def checks(self):
        return self.check_hits + self.check_misses

    def dict(self):
        return {"set_hits":self.set_hits,"set_misses":self.set_misses,
                "check_hits":self.check_hits,"check_misses":self.check_misses,
                "checks":self.checks,"sets":self.sets,"page_outs":self.page_outs,
                "page_ins":self.page_ins}

