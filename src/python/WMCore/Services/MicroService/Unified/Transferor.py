"""
File       : UnifiedTransferorManager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: UnifiedTransferorManager class provides full functionality of the UnifiedTransferor service.
"""

# futures
from __future__ import print_function, division

# system modules

# WMCore modules
from WMCore.Services.MicroService.Unified.RequestInfo import requestsInfo


class Progress(object):
    "Class to keep track of transfer progress in PhEDEx for a given task"
    def __init__(self):
        self.tasks = {}
        self.progress = {}

    def add(self, task):
        "Add task into internal dict"
        self.progress[task] = {} # may replace with actual Phedex info

    def status(self, task):
        "Return progress status for a given task"
        return self.progress.get(task, {})

class UnifiedTransferorManager(object):
    """
    Initialize UnifiedTransferorManager class
    """
    def __init__(self, config=None):
        self.config = config
        self.requests = {}
        self.progress = Progress()

    def status(self):
        "Return current status about UnifiedTransferor"
        sdict = {}
        return sdict

    def request(self, **kwargs):
        "Process request given to UnifiedTransferor"
        state = kwargs.get('state', 'assignment-approved')
        return {'state': state}

    def process(self, state='assignment-approved'):
        "Process request for a given state"
        # get list of requests
        # process requests and obtain their info, including list of sites where to place them
        # submit PhEDEx request to place requests at sites
        requests = requestsInfo(state)
        for key, rdict in requests.items():
            if key not in self.requests:
                self.requests[key] = rdict
                self.progress.add(key)
        # check PhEDEx to all tasks
        # if transfer request is finished move task into assignment-staged (ReqMgr API call)
        #    and remove task from self.task and self.requests
        # if transfer is started change state to assignment-staging (ReqMgr API call)
        #    and update task progress
