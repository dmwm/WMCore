"""
File       : UnifiedTransferorManager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: UnifiedTransferorManager class provides full functionality of the UnifiedTransferor service.
"""

# futures
from __future__ import print_function, division

# system modules
import time
import shelve
import tempfile

# WMCore modules
from WMCore.MicroService.Unified.RequestInfo import requestsInfo
from WMCore.MicroService.Unified.TaskManager import start_new_thread, TaskManager
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription
from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux


class RequestStore(object):
    "RequestStore class handles persistent storage of requests"
    def __init__(self, fname):
        self.db = shelve.open(fname, writeback=True)

    def exists(self, request):
        "Check if request existing in backend"
        return str(request) in self.db

    def get(self, request):
        "Return request info from the storage"
        if str(request) in self.db:
            return self.db[str(request)]
        return {}

    def update(self, request, rdict):
        """
        Update request info in backend, request is a look-up key and rdict its meta-data.
        If request key does exists in internal store do nothing.
        """
        if str(request) in self.db:
            meta_data = self.db[str(request)]
            meta_data.update(rdict)
            self.db[str(request)] = meta_data
            self.db.sync()

    def add(self, request, rdict):
        "Add request to backend, request is a look-up key and rdict its meta-data"
        self.db[str(request)] = rdict
        self.db.sync()

    def delete(self, request):
        "Delete request in backend"
        if str(request) in self.db:
            del self.db[str(request)]
            self.db.sync()

    def info(self, request=None):
        "Return information about given request"
        data = {}
        if request:
            if str(request) in self.db:
                data = self.db[str(request)]
        else:
            for key in self.db.keys():
                data.update({key: self.db[key]})
        return data

    def close(self):
        "Close backend store"
        self.db.close()

def checkRequests(func, store, interval):
    "Demon to perform given function action for all request in our store"
    while True:
        try:
            requests = store.info().keys()
        except ValueError: # store is closed
            requests = []
        for request in requests:
            func(request)
        time.sleep(interval)

class RequestManager(object):
    "Class to keep track of transfer progress in PhEDEx for a given task"
    def __init__(self, group='DataOps', dbFileName=None, interval=10, verbose=False):
        self.verbose = verbose
        if not dbFileName:
            fobj = tempfile.NamedTemporaryFile()
            dbFileName = '%s.db' % fobj.name
        self.store = RequestStore(dbFileName)
        self.phedex = PhEDEx()
        self.group = group
        thname = 'RequestManager monitor'
        self.thr = start_new_thread(thname, checkRequests, \
                (self.checkStatus, self.store, interval))
        if verbose:
            print("### Running %s thread, running: %s" % (thname, self.thr.running()))
        print("RequestManager, group=%s, db=%s, interval=%s" % (group, dbFileName, interval))

    def stop(self):
        "Stop RequestManager"
        self.thr.stop() # stop checkStatus thread
        status = self.thr.running()
        return status

    def checkPhedex(self, request):
        "Send request to Phedex and return status of request subscription"
        sdict = {}
        rdict = self.store.get(request)
        if not rdict: # request is gone
            return 100
        for dataset in rdict.get('datasets'):
            data = self.phedex.subscriptions(dataset=dataset, group=self.group)
            if self.verbose:
                print("### dataset", dataset, "group", self.group)
                print("### subscription", data)
            for row in data['phedex']['dataset']:
                if row['name'] != dataset:
                    continue
                nodes = [s['node'] for s in row['subscription']]
                rNodes = rdict.get('sites')
                if self.verbose:
                    print("### nodes", nodes, rNodes)
                subset = set(nodes) & set(rNodes)
                if subset == set(rNodes):
                    sdict[dataset] = 1
                else:
                    pct = float(len(subset))/float(len(set(rNodes)))
                    sdict[dataset] = pct
        if self.verbose:
            print("### sdict", sdict)
        tot = len(sdict.keys())
        if not tot:
            return -1
        # return percentage of completion
        return round(float(sum(sdict.values()))/float(tot), 2) * 100

    def checkStatus(self, request):
        "Check status of request in local storage"
        if self.verbose:
            print("### checkStatus of request: %s" % request)
        if self.store.exists(request):
            completed = self.checkPhedex(request)
            if completed == 100: # all data are staged
                if self.verbose:
                    print("### request is completed, change its status and remove it from the store")
                # call ReqMgr2 API to change status of the request
                # self.reqmgr.changeStatus(status)
                self.store.delete(request)
            else:
                if self.verbose:
                    print("### request %s, completed %s" % (request, completed))
                self.store.update(request, {'PhedexStatus': completed})

    def add(self, requests):
        "Add requests to task manager"
        # loop over requests: for non-existing pid submit phedex subscription for existing one check their status
        for request, rdict in requests.items():
            if self.store.exists(request):
                self.checkStatus(request)
            else:
                # if request does not exist in backend submit its subscription and add it to backend
                rdict = requests[request]
                self.store.add(request, rdict)
                datasets = rdict.get('datasets')
                sites = rdict.get('sites')
                subscription = PhEDExSubscription(datasets, sites, self.group)
                if self.verbose:
                    print("### add subscription", subscription)
                # TODO: when ready enable submit subscription step
                # self.phedex.subscribe(subscription)

    def info(self, request=None):
        "Return info about given request"
        if not request:
            return self.store.info()
        completed = self.checkPhedex(request)
        idict = self.store.info(request)
        idict.update({'completed': completed})
        return idict

    def delete(self, request):
        "Delete request in backend"
        return self.store.delete(request)

class UnifiedTransferorManager(object):
    """
    Initialize UnifiedTransferorManager class
    """
    def __init__(self, config=None):
        self.config = config
        self.reqmgrAux = ReqMgrAux(self.config.reqmgr2_url)
        self.requests = {}
        self.reqManager = RequestManager()
        self.taskManager = TaskManager(nworkers=3)

    def status(self):
        "Return current status about UnifiedTransferor"
        sdict = {}
        return sdict

    def request(self, **kwargs):
        "Process request given to UnifiedTransferor"
        process = kwargs.get('process', None)
        if process:
            self.taskManager.spawn(self.process, (process,))
            return {'status':'processing'}
        task = kwargs.get('task', None)
        rdict = {}
        if not task:
            return self.reqManager.info()
        rdict[task] = self.reqManager.info(task)
        return rdict

    def process(self, state='assignment-approved'):
        "Process request for a given state"
        reqmgrAuxSvc = self.reqmgrAux
        requests = requestsInfo(reqmgrAuxSvc, state)
        self.reqManager.add(requests)
