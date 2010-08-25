#!/usr/bin/env python
"""
WorkQueue splitting by block

"""
__all__ = []
__revision__ = "$Id: MonteCarlo.py,v 1.2 2009/12/07 14:13:46 swakef Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WorkQueue.Policy.Start.StartPolicyInterface import StartPolicyInterface
from copy import deepcopy
from math import ceil

class MonteCarlo(StartPolicyInterface):
    """Split elements into blocks"""
    def __init__(self, **args):
        StartPolicyInterface.__init__(self, **args)
        self.args.setdefault('SliceType', 'NumEvents')
        self.args.setdefault('SliceSize', 1000)


    def split(self):
        """Apply policy to spec"""
        total = self.initialTask.totalEvents()
        current = self.args['SliceSize']
        while total > 0:
            if total < current:
                current = total
            # copy spec file restricting events
            spec = deepcopy(self.wmspec)
            spec.taskIterator().next().totalEvents = current
            self.newQueueElement(Data = None,
                                 ParentData = [],
                                 WMSpec = self.wmspec,
                                 Jobs = ceil(current /
                                                float(self.args['SliceSize'])))
            total -= current


    def validate(self):
        """Check args and spec work with block splitting"""
        pass
