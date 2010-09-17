#!/usr/bin/env python
"""
WorkQueue SplitPolicyInterface

"""
__all__ = []

import types

from WMCore.WorkQueue.Policy.PolicyInterface import PolicyInterface
from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement
from WMCore.WMException import WMException

class EmptyWorkExcpetion(WMException):
    """
    Dummy exception class when spliting doesn't generate 
    any workqueue element

    TODO: Do something useful

    """

    pass

class StartPolicyInterface(PolicyInterface):
    """Interface for start policies"""
    def __init__(self, **args):
        PolicyInterface.__init__(self, **args)
        self.workQueueElements = []
        self.wmspec = None
        self.initialTask = None
        self.splitParams = None
        self.dbs_pool = {}
        self.data = None
        self.lumi = None

    def split(self):
        """Apply policy to spec"""
        raise NotImplementedError

    def validate(self):
        """Check params and spec are appropriate for the policy"""
        raise NotImplementedError

    def validateCommon(self):
        """Common validation stuff"""
        msg = 'WMSpec "%s" failed validation: ' % self.wmspec.name()

        if self.initialTask.siteWhitelist() and type(self.initialTask.siteWhitelist()) in types.StringTypes:
            raise RuntimeError, msg + 'Invalid site whitelist: Must be tuple/list but is %s' % type(self.initialTask.siteWhitelist())

        if self.initialTask.siteBlacklist() and type(self.initialTask.siteBlacklist()) in types.StringTypes:
            raise RuntimeError, msg + 'Invalid site blacklist: Must be tuple/list but is %s' % type(self.initialTask.siteWhitelist())

    def newQueueElement(self, **args):
        args.setdefault('WMSpec', self.wmspec)
        args.setdefault('Task', self.initialTask)
        self.workQueueElements.append(WorkQueueElement(**args))

    def __call__(self, wmspec, task, dbs_pool = None, data = None, mask = None):
        self.wmspec = wmspec
        self.splitParams = self.wmspec.data.policies.start
        self.initialTask = task
        if dbs_pool:
            self.dbs_pool.update(dbs_pool)
        self.data = data
        self.mask = mask
        self.validate()
        self.split()
        
        if len(self.workQueueElements) == 0:
            msg = """ No element is created from
                      wmspec: %s
                      task: %s
                      data: %s """ % (wmspec.name(), task.name(), data)
            raise EmptyWorkExcpetion(msg)
        return self.workQueueElements

    def dbs(self):
        """Get DBSReader"""
        from WMCore.Services.DBS.DBSReader import DBSReader
        dbs_url = self.initialTask.dbsUrl()
        if not self.dbs_pool.has_key(dbs_url):
            self.dbs_pool[dbs_url] = DBSReader(dbs_url)
        return self.dbs_pool[dbs_url]
