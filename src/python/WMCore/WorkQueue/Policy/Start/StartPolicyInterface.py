#!/usr/bin/env python
"""
WorkQueue SplitPolicyInterface

"""
__all__ = []



from WMCore.WorkQueue.Policy.PolicyInterface import PolicyInterface
from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement
from WMCore.WMException import WMException
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError, WorkQueueNoWorkError

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
        if self.initialTask.siteWhitelist() and type(self.initialTask.siteWhitelist()) in types.StringTypes:
            error = WorkQueueWMSpecError(self.wmspec, 'Invalid site whitelist: Must be tuple/list but is %s' % type(self.initialTask.siteWhitelist()))
            raise error

        if self.initialTask.siteBlacklist() and type(self.initialTask.siteBlacklist()) in types.StringTypes:
            error = WorkQueueWMSpecError(self.wmspec, 'Invalid site blacklist: Must be tuple/list but is %s' % type(self.initialTask.siteBlacklist()))
            raise error

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
        
        if not self.workQueueElements:
            msg = """data: %s, mask: %s.""" % (str(task.inputDataset().pythonise_()), str(mask))
            error = WorkQueueNoWorkError(self.wmspec, msg)
            raise error
        return self.workQueueElements

    def dbs(self):
        """Get DBSReader"""
        from WMCore.Services.DBS.DBSReader import DBSReader
        dbs_url = self.initialTask.dbsUrl()
        if not self.dbs_pool.has_key(dbs_url):
            self.dbs_pool[dbs_url] = DBSReader(dbs_url)
        return self.dbs_pool[dbs_url]
