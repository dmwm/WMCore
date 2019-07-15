"""
File       : UnifiedTransferorManager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: UnifiedTransferorManager class provides full functionality of the UnifiedTransferor service.
"""

# futures
from __future__ import division

# system modules
import time
import json
import hashlib
from pprint import pformat

# WMCore modules
from WMCore.MicroService.Unified.Common import getMSLogger
from WMCore.MicroService.Unified.RequestInfo import requestsInfo, requestRecord
from WMCore.MicroService.Unified.TaskManager import start_new_thread
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription
from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.pycurl_manager import RequestHandler


def daemon(func, reqStatus, interval, logger):
    "Daemon to perform given function action for all request in our store"
    while True:
        try:
            func(reqStatus)
        except Exception as exc:
            logger.exception("MS daemon error: %s", str(exc))
        time.sleep(interval)


class MSManager(object):
    """
    Entry point for the MicroServices.
    This class manages both transferor and monitor services/threads.
    """
    def __init__(self, config=None, logger=None):
        """
        Setup a bunch of things, like:
         * logger for this service
         * initialize all the necessary service helpers
         * fetch the unified configuration from central couch
         * update the unified configuration with some deployment and default settings
         * start both transfer and monitor threads
        :param config: reqmgr2ms service configuration
        :param logger:
        """
        self.config = config
        verbose = getattr(config, 'verbose', False)
        self.logger = getMSLogger(verbose, logger)
        self.logger.info("Using the following config:\n%s", config)

        self.group = getattr(config, 'group', 'DataOps')
        self.interval = getattr(config, 'interval', 5 * 60)
        self.readOnly = getattr(config, 'readOnly', True)
        self.uConfigUrl = getattr(config, 'uConfigUrl',
                                  'https://raw.githubusercontent.com/CMSCompOps/WmAgentScripts/master/unifiedConfiguration.json')
        reqmgrUrl = getattr(config, 'reqmgr2Url', 'https://cmsweb.cern.ch/reqmgr2')
        reqmgrCacheUrl = getattr(config, 'reqmgrCacheUrl', 'https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache')
        phedexUrl = getattr(config, 'phedexUrl', 'https://cmsweb.cern.ch/phedex/datasvc/json/prod')
        dbsUrl = getattr(config, 'dbsUrl', 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
        self.logger.info("MSManager set to group=%s, interval=%s and readOnly=%s", self.group,
                         self.interval, self.readOnly)

        ### Initialise basic services
        self.reqmgrAux = ReqMgrAux(reqmgrUrl, httpDict={'cacheduration': 60}, logger=self.logger)
        self.reqmgr2 = ReqMgr(reqmgrUrl, logger=self.logger)
        # eventually will change it to Rucio
        self.phedex = PhEDEx(httpDict={'cacheduration': 10 * 60}, dbsUrl=dbsUrl, logger=self.logger)

        ### Fetch the unified configuration from reqmgr_aux db
        self.uConfig = self.reqmgrAux.getUnifiedConfig()

        ### Now update the Unified configuration with some default values
        self.uConfig.setdefault('reqmgrUrl', reqmgrUrl)
        self.uConfig.setdefault('reqmgrCacheUrl', reqmgrCacheUrl)
        self.uConfig.setdefault('dbsUrl', dbsUrl)
        self.uConfig.setdefault('phedexUrl', phedexUrl)

        ### Last but not least, get the threads started
        thname = 'MSTransferor'
        self.ms_transf = start_new_thread(thname, daemon,
                                          (self.transferor, 'assigned', self.interval, self.logger))
        self.logger.debug("### Running %s thread %s", thname, self.ms_transf.running())

        thname = 'MSTransferorMonit'
        self.ms_monit = start_new_thread(thname, daemon,
                (self.monitor, 'staging', self.interval, self.logger))
        self.logger.debug("+++ Running %s thread %s", thname, self.ms_monit.running())

    def updateUnifiedConfig(self):
        """
        Fetch the unified configuration directly from github, check whether there
        are any changes compared to what we have in memory, and if needed update
        the in-memory unified configuration and also persist it to couchdb
        """
        headers = {'Accept': 'application/json'}
        mgr = RequestHandler()
        try:
            data = mgr.getdata(self.uConfigUrl, params={}, headers=headers)
        except Exception as ex:
            msg = "Failed to retrieve unified configuration from github. Error: %s" % str(ex)
            msg += "\nRetrying again in the next cycle"
            self.logger.error(msg)
            return

        oldData = json.loads(self.uConfig)
        # FIXME: it won't work! We add data to the unified config
        if oldData == data:
            self.logger.debug("Unified configuration hasn't changed compared to the last cycle")
            return

        newData = json.dumps(data)
        # now post the new up-to-date config to Couch as well
        if self.reqmgrAux.updateUnifiedConfig(newData):
            self.uConfig = newData
            self.logger.info("Unified configuration has been updated")
        return

    def monitor(self, reqStatus='staging'):
        """
        MSManager monitoring function.
        It performs transfer requests from staging to staged state of ReqMgr2.
        For references see
        https://github.com/dmwm/WMCore/wiki/ReqMgr2-MicroService-Transferor
        """
        self.logger.info("Starting the monitor thread...")
        try:
            # get requests from ReqMgr2 data-service for given statue
            # here with detail=False we get back list of records
            requests = self.reqmgr2.getRequestByStatus([reqStatus], detail=False)
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
        self.logger.info("Starting the transferor thread...")
        requestRecords = []
        try:
            # first, check if there is a newer unified configuration file and use it if needed
            self.updateUnifiedConfig()
            # get requests from ReqMgr2 data-service for given statue
            requestSpecs = self.reqmgr2.getRequestByStatus([reqStatus], detail=True)
            if requestSpecs:
                for _, wfData in requestSpecs[0].items():
                    requestRecords.append(requestRecord(wfData, reqStatus))
            self.logger.debug('### transferor found %s requests in %s state', len(requestRecords), reqStatus)
            # get complete requests information (based on Unified Transferor logic)
            requestRecords = requestsInfo(requestRecords, self.reqmgrAux, self.uConfig, self.logger)
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
        self.ms_transf.stop()  # stop checkStatus thread
        status = self.ms_transf.running()
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
                    self.reqmgr2.updateRequestStatus(req['name'], reqStatus)
        except Exception as err:
            self.logger.exception("Failed to change request status. Error: %s", str(err))

    def info(self, req):
        "Return info about given request"
        completed = self.checkSubscription(req)
        return {'request': req, 'status': completed}

    def delete(self, request):
        "Delete request in backend"
        pass
