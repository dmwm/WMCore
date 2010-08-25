#!/usr/bin/env python
"""
WorkQueue SplitPolicyInterface

"""
__all__ = []
__revision__ = "$Id: StartPolicyInterface.py,v 1.1 2009/12/02 13:52:43 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Policy.PolicyInterface import PolicyInterface
from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement

class StartPolicyInterface(PolicyInterface):
    """Interface for start policies"""
    def __init__(self, **args):
        PolicyInterface.__init__(self, **args)
        self.workQueueElements = []
        self.wmspec = None
        self.initialTask = None
        self.splitParams = None
        self.dbs_pool = {}

    def split(self):
        """Apply policy to spec"""
        raise NotImplementedError

    def validate(self):
        """Check params and spec are appropriate for the policy"""
        raise NotImplementedError

    def newQueueElement(self, **args):
        self.workQueueElements.append(WorkQueueElement(**args))

    def __call__(self, wmspec, dbs_pool = None):
        self.wmspec = wmspec
        self.initialTask = wmspec.taskIterator().next()
        self.splitParams = self.wmspec.data.policies.start
        self.dbs_pool.update(dbs_pool)
        self.split()
        return self.workQueueElements

    def dbs(self):
        """Get DBSReader"""
        from WMCore.Services.DBS.DBSReader import DBSReader
        dbs_url = self.initialTask.dbsUrl()
        if not self.dbs_pool.has_key(dbs_url):
            self.dbs_pool[dbs_url] = DBSReader(dbs_url)
        return self.dbs_pool[dbs_url]
