#!/usr/bin/env python
"""
WorkQueue splitting by block

"""
__all__ = []



from WMCore.WorkQueue.Policy.Start.StartPolicyInterface import StartPolicyInterface
from copy import deepcopy
from math import ceil

class MonteCarlo(StartPolicyInterface):
    """Split elements into blocks"""
    def __init__(self, **args):
        StartPolicyInterface.__init__(self, **args)
        self.args.setdefault('SliceType', 'NumberOfEvents')
        self.args.setdefault('SliceSize', 1000000)


    def split(self):
        """Apply policy to spec"""
        total = int(self.initialTask.totalEvents())
        current = self.args['SliceSize']
        while total > 0:
            if total < current:
                current = total
            self.newQueueElement(Data = None,
                                 ParentData = [],
                                 WMSpec = self.wmspec,
                                 Jobs = ceil(current /
                                                float(self.args['SliceSize'])))
            total -= current


    def validate(self):
        """Check args and spec work with block splitting"""
        if not self.initialTask.totalEvents():
            raise RuntimeError, 'Invalid total events selection'

        if not self.initialTask.siteWhitelist():
            raise RuntimeError, "Site whitelist mandatory for MonteCarlo"
