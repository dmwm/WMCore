"""
File       : UnifiedTransferorManager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: UnifiedTransferorManager class provides full functionality of the UnifiedTransferor service.
"""

# futures
from __future__ import division

# system modules
import time
import logging
import hashlib
from pprint import pformat

# WMCore modules
from WMCore.MicroService.Unified.Common import uConfig
from WMCore.MicroService.Unified.RequestInfo import requestsInfo, requestRecord
from WMCore.MicroService.Unified.TaskManager import start_new_thread
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription
from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr


def daemon(func, reqStatus, interval, logger):
    "Daemon to perform given function action for all request in our store"
    while True:
        try:
            func(reqStatus)
        except Exception as exc:
            logger.exception("MS daemon error: %s", str(exc))
        time.sleep(interval)


class MSManager(object):
    "Class to keep track of transfer progress in PhEDEx for a given task"
    def __init__(self, svc, group='DataOps', readOnly=True, interval=60, logger=None):
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('reqmgr2ms:MSManager')
            self.logger.setLevel(logging.DEBUG)
            logging.basicConfig()
        self.phedex = PhEDEx()  # eventually will change to Rucio
        self.group = group
        self.readOnly = readOnly
        self.svc = svc  # Services: ReqMgr, ReqMgrAux
        thname = 'MSTransferor'
        self.thr = start_new_thread(thname, daemon,
                (self.transferor, 'assigned', interval, self.logger))
        self.logger.debug("### Running %s thread %s", thname, self.thr.running())
        thname = 'MSTransferorMonit'
        self.ms_monit = start_new_thread(thname, daemon,
                (self.monit, 'staging', interval, self.logger))
        self.logger.debug("+++ Running %s thread %s", thname, self.ms_monit.running())
        self.logger.info("MSManager, group=%s, interval=%s", group, interval)

    def monit(self, reqStatus='staging'):
        """
        MSManager monitoring function.
        It performs transfer requests from staging to staged state of ReqMgr2.
        For references see
        https://github.com/dmwm/WMCore/wiki/ReqMgr2-MicroService-Transferor
        """
        try:
            # get requests from ReqMgr2 data-service for given statue
            # here with detail=False we get back list of records
            requests = self.svc.reqmgr.getRequestByStatus([reqStatus], detail=False)
            self.logger.debug('+++ monit found %s requests in %s state', len(requests), reqStatus)

            requestStatus = {}  # keep track of request statuses
            for reqName in requests:
                req = {'name':reqName, 'reqStatus': reqStatus}
                # get transfer IDs
                tids = self.getTransferIDs()
                # get transfer status
                transferStatuses = self.getTransferStatuses(tids)
                # get campaing and unified configuration
                campaign = self.requestCampaign(reqName)
                conf = self.requestConfiguration(reqName)
                self.logger.debug("+++ request %s campaing %s conf %s", req, campaign, conf)

                # if all transfers are completed, move the request status staging -> staged
                # completed = self.checkSubscription(request)
                completed = 100  # TMP
                if completed == 100:  # all data are staged
                    self.logger.debug("+++ request %s all transfers are completed", req)
                    self.change(req, 'staged', '+++ monit')
                # if pileup transfers are completed AND some input blocks are completed, move the request status staging -> staged
                elif self.pileupTransfersCompleted(tids):
                    self.logger.debug("+++ request %s pileup transfers are completed", req)
                    self.change(req, 'staged', '+++ monit')
                # transfers not completed, just update the database with their completion
                else:
                    self.logger.debug("+++ request %s transfers are not completed", req)
                    requestStatus[req] = transferStatuses  # TODO: implement update of transfer ids
            self.updateTransferIDs(requestStatus)
        except Exception as err:  # general error
            self.logger.exception('+++ monit error: %s', str(err))

    def transferor(self, reqStatus='assigned'):
        """
        MSManager transferor function.
        It performs Unified logic for data subscription and 
        transfers requests from assigned to staging/staged state of ReqMgr2.
        For references see
        https://github.com/dmwm/WMCore/wiki/ReqMgr2-MicroService-Transferor
        """
        requestRecords = []
        try:
            # get requests from ReqMgr2 data-service for given statue
            requestSpecs = self.svc.reqmgr.getRequestByStatus([reqStatus], detail=True)
            if requestSpecs:
                for _, wfData in requestSpecs[0].items():
                    requestRecords.append(requestRecord(wfData, reqStatus))
            self.logger.debug('### transferor found %s requests in %s state', len(requestRecords), reqStatus)
            # get complete requests information (based on Unified Transferor logic)
            requestRecords = requestsInfo(requestRecords, self.svc, self.logger)
        except Exception as err:  # general error
            self.logger.exception('### transferor error: %s', str(err))

        # process all requests
        for req in requestRecords:
            reqName = req['name']
            # perform transfer
            tid = self.transferRequest(req)
            if tid:
                # Once all transfer requests were successfully made, update: assigned -> staging
                self.logger.debug("### transfer request for %s successfull", reqName)
                self.change(req, 'staging', '### transferor')
            # if there is nothing to be transferred (no input at all),
            # then update the request status once again staging -> staged
            # self.change(req, 'staged', '### transferor')

    def stop(self):
        "Stop MSManager"
        # stop MSTransferorMonit thread
        self.ms_monit.stop()
        # stop MSTransferor thread
        self.thr.stop()  # stop checkStatus thread
        status = self.thr.running()
        return status

    def transferRequest(self, req):
        "Send request to Phedex and return status of request subscription"
        datasets = req.get('datasets', [])
        sites = req.get('sites', [])
        if datasets and sites:
            self.logger.debug("### creating subscription for: %s", pformat(req))
            subscription = PhEDExSubscription(datasets, sites, self.group)
            # TODO: implement how to get transfer id
            tid = hashlib.md5(str(subscription)).hexdigest()
            # TODO: when ready enable submit subscription step
            # self.phedex.subscribe(subscription)
            return tid

    def getTransferIDsDoc(self):
        """
        Get transfer ids document from backend. The document has the following form:
        {
          "wf_A": [record1, record2, ...],
          "wf_B": [....],
        }
        where each record has the following format:
        {"timestamp":000, "dataset":"/a/b/c", "type": "primary", "trainsferIDs": [1,2,3]}
        """
        doc = {}
        return doc

    def updateTransferIDs(self, requestStatus):
        "Update transfer ids in backend"
        # TODO/Wait: https://github.com/dmwm/WMCore/issues/9198
        # doc = self.getTransferIDsDoc()

    def getTransferIDs(self):
        "Get transfer ids from backend"
        # TODO/Wait: https://github.com/dmwm/WMCore/issues/9198
        # meanwhile return transfer ids from internal store 
        return []

    def getTransferStatuses(self, tids):
        "get transfer statuses for given transfer IDs from backend"
        # transfer docs on backend has the following form
        # https://gist.github.com/amaltaro/72599f995b37a6e33566f3c749143154
        statuses = {}
        for tid in tids:
            # TODO: I need to find request name from transfer ID
            #status = self.checkSubscription(request)
            status = 100
            statuses[tid] = status
        return statuses

    def requestCampaign(self, req):
        "Return request campaign"
        return 'campaign_TODO' # TODO

    def requestConfiguration(self, req):
        "Return request configuration"
        return {}

    def pileupTransfersCompleted(self, tids):
        "Check if pileup transfers are completed"
        # TODO: add implementation
        return False

    def checkSubscription(self, req):
        "Send request to Phedex and return status of request subscription"
        sdict = {}
        for dataset in req.get('datasets', []):
            data = self.phedex.subscriptions(dataset=dataset, group=self.group)
            self.logger.debug("### dataset %s group %s", dataset, self.group)
            self.logger.debug("### subscription %s", data)
            for row in data['phedex']['dataset']:
                if row['name'] != dataset:
                    continue
                nodes = [s['node'] for s in row['subscription']]
                rNodes = req.get('sites')
                self.logger.debug("### nodes %s %s", nodes, rNodes)
                subset = set(nodes) & set(rNodes)
                if subset == set(rNodes):
                    sdict[dataset] = 1
                else:
                    pct = float(len(subset))/float(len(set(rNodes)))
                    sdict[dataset] = pct
        self.logger.debug("### sdict %s", sdict)
        tot = len(sdict.keys())
        if not tot:
            return -1
        # return percentage of completion
        return round(float(sum(sdict.values()))/float(tot), 2) * 100

    def checkStatus(self, req):
        "Check status of request in local storage"
        self.logger.debug("### checkStatus of request: %s", req['name'])
        # check subscription status of the request
        # completed = self.checkSubscription(req)
        completed = 100
        if completed == 100:  # all data are staged
            self.logger.debug("### request is completed, change its status and remove it from the store")
            self.change(req, 'staged', '### transferor')
        else:
            self.logger.debug("### request %s, completed %s", req, completed)

    def change(self, req, reqStatus, prefix='###'):
        """
        Change request status, internally it is done via PUT request to ReqMgr2:
        curl -X PUT -H "Content-Type: application/json" \
             -d '{"RequestStatus":"staging", "RequestName":"bla-bla"}' \
             https://xxx.yyy.zz/reqmgr2/data/request
        """
        self.logger.debug('%s updating %s status to %s', prefix, req['name'], reqStatus)
        try:
            if req.get('reqStatus', None) != reqStatus:
                if not self.readOnly:
                    self.svc.reqmgr.updateRequestStatus(req['name'], reqStatus)
        except Exception as err:
            self.logger.exception("Failed to change request status. Error: %s", str(err))

    def info(self, req):
        "Return info about given request"
        completed = self.checkSubscription(req)
        return {'request': req, 'status': completed}

    def delete(self, request):
        "Delete request in backend"
        pass

class Services(object):
    "Services class provides access to reqmgr2 services: ReqMgr, ReqMgrAux"
    __slots__ = ('reqmgrAux', 'reqmgr')
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
            loggerName = 'reqmgr2ms:%s' % self.__class__.__name__
            self.logger = logging.getLogger(loggerName)
            self.logger.setLevel(logging.DEBUG)
            logging.basicConfig()

        self.logger.info("Using the following config: %s", config)
        group = getattr(config, 'group', 'DataOps')
        interval = getattr(config, 'interval', 3600)
        readOnly = getattr(config, 'readOnly', True)
        # update uConfig urls according to reqmgr2ms configuration
        reqmgrUrl = getattr(config, 'reqmgr2Url', 'https://cmsweb.cern.ch/reqmgr2')
        uConfig.set('reqmgrUrl', reqmgrUrl)
        uConfig.set('reqmgrCacheUrl', getattr(config, 'reqmgrCacheUrl', 'https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache'))
        uConfig.set('dbsUrl', getattr(config, 'dbsUrl', 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'))
        self.svc = Services(reqmgrUrl, self.logger)
        self.msManager = MSManager(self.svc, group, readOnly, interval, logger)

    def status(self):
        "Return current status about UnifiedTransferor"
        sdict = {}
        return sdict
