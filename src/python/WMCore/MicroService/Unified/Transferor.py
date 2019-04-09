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
import logging
import hashlib
import tempfile
import traceback

from httplib import HTTPException

# WMCore modules
from WMCore.MicroService.Unified.Common import uConfig
from WMCore.MicroService.Unified.RequestInfo import requestsInfo
from WMCore.MicroService.Unified.TaskManager import start_new_thread, TaskManager
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription
from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr

class RequestStore(object):
    "RequestStore class handles persistent storage of requests"
    def __init__(self, fname):
        self.db = shelve.open(fname, writeback=True)

    def exists(self, request):
        "Check if request existing in backend"
        return str(request) in self.db

    def get(self, request):
        "Return request info from the storage"
        req = str(request)
        if req in self.db:
            return self.db[req]
        return {}

    def update(self, request, rdict):
        """
        Update request info in backend, request is a look-up key and rdict its meta-data.
        If request key does exists in internal store do nothing.
        """
        req = str(request)
        if req in self.db:
            meta_data = self.db[req]
            meta_data.update(rdict)
            self.db[req] = meta_data
            self.db.sync()

    def add(self, request, rdict):
        "Add request to backend, request is a look-up key and rdict its meta-data"
        self.db[str(request)] = rdict
        self.db.sync()

    def delete(self, request):
        "Delete request in backend"
        req = str(request)
        if req in self.db:
            del self.db[req]
            self.db.sync()

    def records(self):
        "Return all records from the store"
        return [self.db[k] for k in self.db.keys()]

    def info(self, request=None):
        "Return information about given request"
        data = {}
        req = str(request)
        if request: # check for None
            if req in self.db:
                data = self.db[req]
        else:
            for key in self.db.keys():
                data.update({key: self.db[key]})
        return data

    def close(self):
        "Close backend store"
        self.db.close()

def daemon(func, req_status, interval):
    "Daemon to perform given function action for all request in our store"
    while True:
        try:
            func(req_status)
        except Exception as exc:
            pass
        time.sleep(interval)

class MSManager(object):
    "Class to keep track of transfer progress in PhEDEx for a given task"
    def __init__(self, svc, group='DataOps', dbFileName=None, interval=60, logger=None, verbose=False):
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('reqmgr2ms:MSManager')
            logging.basicConfig()
        self.verbose = verbose
        if verbose:
            self.logger.setLevel(logging.DEBUG)
        if not dbFileName:
            fobj = tempfile.NamedTemporaryFile()
            dbFileName = '%s.db' % fobj.name
        self.store = RequestStore(dbFileName)
        self.logger.debug("dbFileName %s" % dbFileName)
        self.phedex = PhEDEx() # eventually will change to Rucio
        self.group = group
        self.svc = svc # Services: ReqMgr, ReqMgrAux
        thname = 'MSTransferor'
        self.thr = start_new_thread(thname, daemon, (self.transferor, 'assigned', interval))
        self.logger.debug("### Running %s thread %s" % (thname, self.thr.running()))
        thname = 'MSTransferorMonit'
        self.ms_monit = start_new_thread(thname, daemon, (self.monit, 'staging', interval))
        self.logger.debug("+++ Running %s thread %s" % (thname, self.ms_monit.running()))
        self.logger.info("MSManager, group=%s, db=%s, interval=%s" % (group, dbFileName, interval))

    def monit(self, req_status='staging'):
        """
        MSManager monitoring function.
        It performs transfer requests from staging to staged state of ReqMgr2.
        For references see
        https://github.com/dmwm/WMCore/wiki/ReqMgr2-MicroService-Transferor
        """
        try:
            # get requests from ReqMgr2 data-service for given statue
            requestSpecs = self.svc.reqmgr.getRequestByStatus([req_status], detail=False)
            requests = [r for item in requestSpecs for r in item.keys()]
            self.logger.debug('+++ monit found %s requests in %s state' % (len(requests), req_status))

            for req in requests:
                # get transfer IDs
                tids = self.getTransferIDs()
                # get transfer status
                transferStatuses = self.getTransferStatuses(tids)
                # get campaing and unified configuration
                campain = self.requestCampaign(req)
                conf = self.requestConfiguration(req)

                # if all transfers are completed, move the request status staging -> staged
                # completed = self.checkSubscription(request)
                completed = 100 # TMP
                if completed == 100: # all data are staged
                    self.logger.debug("+++ request %s all transfers are completed" % req)
	            self.change(req, 'staged', '+++ monit')
                # if pileup transfers are completed AND some input blocks are completed, move the request status staging -> staged
                elif self.pileupTransfersCompleted(tids):
                    self.logger.debug("+++ request %s pileup transfers are completed" % req)
	            self.change(req, 'staged', '+++ monit')
                # transfers not completed, just update the database with their completion
                else:
                    self.logger.debug("+++ request %s transfers are not completed" % req)
                    self.store.update(req, {'status': completed})
	except Exception as err: # general error
            self.logger.error(err)

    def transferor(self, req_status='assigned'):
        """
        MSManager transferor function.
        It performs Unified logic for data subscription and 
        transfers requests from assigned to staging/staged state of ReqMgr2.
        For references see
        https://github.com/dmwm/WMCore/wiki/ReqMgr2-MicroService-Transferor
        """
        try:
            # get complete requests information (based on Unified Transferor logic)
            requests = requestsInfo(self.svc, req_status, None, self.verbose)
            # peform transfers for our list of requests
            trecords = []
            for rspec in requests:
                for req, rdict in rspec.items():
                    # transfer request
                    tid = self.transferRequest(req, rdict)
                    record = {'request': str(req), 'tid': tid, 'status': 0, 'reqStatus': req_status}
                    trecords.append(record)
                    # add request to internal store
                    self.logger.debug('### add request %s to the store' % record)
                    self.store.add(str(req), record)
            self.updateTransferIDs(trecords)
            requests = self.store.info().keys()
            self.logger.debug('### number of request in store: %s' % len(requests))
        except ValueError as err: # store is closed
            self.logger.error(err)
            requests = []
	except Exception as err: # general error
            self.logger.error(err)
            requests = []
        # perform check of requests
        for request in requests:
            # Once all transfer requests were successfully made, update: assigned -> staging
            status = self.checkTransferRequests(request)
            if status:
                self.logger.debug("### all transfer requests were successfull")
                self.change(request, 'staging', '### transferor')
            # if there is nothing to be transferred (no input at all), then update: staging -> staged
            # uncomment when we'll deal with real transfers
            #self.checkStatus(request)

    def stop(self):
        "Stop MSManager"
        # stop MSTransferorMonit thread
        self.ms_monit.stop()
        # stop MSTransferor thread
        self.thr.stop() # stop checkStatus thread
        status = self.thr.running()
        return status

    def transferRequest(self, request, rdict):
        "Send request to Phedex and return status of request subscription"
	datasets = rdict.get('datasets', [])
	sites = rdict.get('sites', [])
        if datasets and sites:
	    subscription = PhEDExSubscription(datasets, sites, self.group)
	    self.logger.debug("### add subscription %s for request %s" % (subscription, request))
            # TODO: implement how to get transfer id
            tid = hashlib.md5(str(subscription)).hexdigest()
	    # TODO: when ready enable submit subscription step
	    # self.phedex.subscribe(subscription)
            return tid

    def getTransferIDsDoc(self):
        """
        Get transfer ids document from backend. The document has the following form:
        https://gist.github.com/amaltaro/72599f995b37a6e33566f3c749143154
	{"wf_A": {"timestamp": 0000
		  "primary": {"dset_1": ["list of transfer ids"]},
		  "secondary": {"PU_dset_1": ["list of transfer ids"]},
	 "wf_B": {"timestamp": 0000
		  "primary": {"dset_1": ["list of transfer ids"],
			      "parent_dset_1": ["list of transfer ids"]},
		  "secondary": {"PU_dset_1": ["list of transfer ids"],
				"PU_dset_2": ["list of transfer ids"]},
         ...
	}
        """
        doc = {}
        return doc

    def updateTransferIDs(self, trecords):
        "Update transfer ids in backend"
        # TODO/Wait: https://github.com/dmwm/WMCore/issues/9198
        doc = self.getTransferIDsDoc()
        pass

    def getTransferIDs(self):
        "Get transfer ids from backend"
        # TODO/Wait: https://github.com/dmwm/WMCore/issues/9198
        # meanwhile return transfer ids from internal store 
        tids = [r['tid'] for r in self.store.records()]
        return tids

    def getTransferStatuses(self, tids):
        "get transfer statuses for given transfer IDs from backend"
        # transfer docs on backend has the following form
        # https://gist.github.com/amaltaro/72599f995b37a6e33566f3c749143154
        statuses = []
        records = self.store.records()
        store_tids = [r.get('tid', 0) for r in records]
        for tid in tids:
            if tid in store_tids:
                for rec in records:
                    if rec.get('tid', 0) == tid:
                       statuses.append(rec['status'])
                       break
            else:
                # TODO: I need to find request name from transfer ID
                #status = self.checkSubscription(request)
                status = 100
                statuses.append(status)
        return statuses

    def requestCampaign(self, req):
        "Return request campaign"
        pass

    def requestConfiguration(self, req):
        "Return request configuration"
        pass

    def pileupTransfersCompleted(self, tids):
        "Check if pileup transfers are completed"
        # TODO: add implementation
        return False

    def checkSubscription(self, request):
        "Send request to Phedex and return status of request subscription"
        sdict = {}
        rdict = self.store.get(request)
        if not rdict: # request is gone
            return 100
        for dataset in rdict.get('datasets'):
            data = self.phedex.subscriptions(dataset=dataset, group=self.group)
            self.logger.debug("### dataset %s group %s" % (dataset, self.group))
            self.logger.debug("### subscription %s" % data)
            for row in data['phedex']['dataset']:
                if row['name'] != dataset:
                    continue
                nodes = [s['node'] for s in row['subscription']]
                rNodes = rdict.get('sites')
                self.logger.debug("### nodes %s %s" % (nodes, rNodes))
                subset = set(nodes) & set(rNodes)
                if subset == set(rNodes):
                    sdict[dataset] = 1
                else:
                    pct = float(len(subset))/float(len(set(rNodes)))
                    sdict[dataset] = pct
        self.logger.debug("### sdict %s" % sdict)
        tot = len(sdict.keys())
        if not tot:
            return -1
        # return percentage of completion
        return round(float(sum(sdict.values()))/float(tot), 2) * 100

    def checkTransferRequests(self, request):
        "Check all transfer requests for given request"
        # put logic to find out all transfer requests
        return True

    def checkStatus(self, request):
        "Check status of request in local storage"
        self.logger.debug("### checkStatus of request: %s" % request)
        if self.store.exists(request):

            # check subscription status of the request
            # completed = self.checkSubscription(request)
            completed = 100
            if completed == 100: # all data are staged
                self.logger.debug("### request is completed, change its status and remove it from the store")
                self.change(request, 'staged', '### transferor')
            else:
                self.logger.debug("### request %s, completed %s" % (request, completed))
                self.store.update(request, {'status': completed})

    def change(self, request, req_status, prefix='###'):
        """
        Change request status, internally it is done via PUT request to ReqMgr2:
        curl -X PUT -H "Content-Type: application/json" \
             -d '{"RequestStatus":"staging", "RequestName":"bla-bla"}' \
             https://xxx.yyy.zz/reqmgr2/data/request
        """
        try:
            rec = self.store.get(request)
            self.logger.debug('%s changing %s status of record %s to req_status=%s' \
                % (prefix, request, rec, req_status))
            if rec.get('reqStatus', None) != req_status:
                self.svc.reqmgr.updateRequestStatus(request, req_status)
                self.store.update(request, {'reqStatus': req_status})
            if req_status == 'staged':
                self.store.delete(request)
        except HTTPException as err:
            traceback.print_exc()
            self.logger.error('%s change: url=%s headers=%s status=%s reason=%s' \
                % (prefix, err.url, err.headers, err.status, err.reason))
        except Exception as err:
            traceback.print_exc()

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
                self.logger.debug("### add subscription %s" % subscription)
                # TODO: when ready enable submit subscription step
                # self.phedex.subscribe(subscription)

		# TODO: when we made subscription we change status of request in ReqMgr2

    def info(self, request):
        "Return info about given request"
        if not request:
            return self.store.info()
        completed = self.checkSubscription(request)
        idict = self.store.info(request)
        idict.update({'status': completed})
        return idict

    def delete(self, request):
        "Delete request in backend"
        return self.store.delete(request)

class Services(object):
    "Services class provides access to reqmgr2 services: ReqMgr, ReqMgrAux"
    __slots__ = ('reqmgrAux', 'reqmgr', 'workqueue')
    def __init__(self, reqmgrUrl, logger=None):
        self.reqmgrAux = ReqMgrAux(reqmgrUrl, logger=logger)
        self.reqmgr = ReqMgr(reqmgrUrl, logger=logger)

class UnifiedTransferorManager(object):
    """
    UnifiedTransferorManager class provides an REST interface to reqmgr2ms service.
    """
    def __init__(self, config=None, logger=None):
        self.config = config
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('reqmgr2ms:%s' % self.__class__.__name__)
            logging.basicConfig()
        reqmgrUrl = getattr(config, 'reqmgrUrl', 'https://cmsweb.cern.ch/reqmgr2')
        self.requests = {}
        self.svc = Services(reqmgrUrl, self.logger)
        group = getattr(config, 'group', 'DataOps')
        dbFileName = getattr(config, 'dbFileName', None)
        interval = getattr(config, 'interval', 3600)
        self.verbose = getattr(config, 'verbose', False)
        # update uConfig urls according reqmgr2ms configuration
        uConfig.set('reqmgrUrl', reqmgrUrl)
        uConfig.set('reqmgrCacheUrl', getattr(config, 'reqmgrCacheUrl', 'https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache'))
        self.msManager = MSManager(self.svc, group, dbFileName, interval, logger, self.verbose)
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
            return self.msManager.info()
        rdict[task] = self.msManager.info(task)
        return rdict

    def process(self, req_status='assigned'):
        "Process request for a given state"
        requests = transferor(self.svc, req_status, self.logger, self.verbose)
        self.msManager.add(requests)
