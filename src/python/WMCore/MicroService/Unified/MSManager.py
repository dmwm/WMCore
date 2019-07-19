"""
File       : UnifiedTransferorManager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: UnifiedTransferorManager class provides full functionality of the UnifiedTransferor service.
"""

# futures
from __future__ import division

# system modules
import time

# WMCore modules
from WMCore.MicroService.Unified.Common import getMSLogger
from WMCore.MicroService.Unified.MSTransferor import MSTransferor
from WMCore.MicroService.Unified.TaskManager import start_new_thread
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux


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
        self.uConfig = {}
        self.config = config
        self.logger = getMSLogger(getattr(config, 'verbose', False), logger)
        self._parseConfig(config)
        self.logger.info("Configuration including default values:\n%s", self.msConfig)

        self.reqmgr2 = ReqMgr(self.msConfig['reqmgrUrl'], logger=self.logger)
        self.reqmgrAux = ReqMgrAux(self.msConfig['reqmgrUrl'], httpDict={'cacheduration': 60}, logger=self.logger)

        # transferor has to look at workflows in assigned status
        self.msTransferor = MSTransferor(self.msConfig, "assigned", logger=self.logger)

        ### Last but not least, get the threads started
        thname = 'MSTransferor'
        self.transfThread = start_new_thread(thname, daemon,
                                             (self.transferor, 'assigned', self.msConfig['interval'], self.logger))
        self.logger.debug("### Running %s thread %s", thname, self.transfThread.running())

        thname = 'MSTransferorMonit'
        self.monitThread = start_new_thread(thname, daemon,
                                            (self.monitor, 'staging', self.msConfig['interval'] * 2, self.logger))
        self.logger.debug("+++ Running %s thread %s", thname, self.monitThread.running())

    def _parseConfig(self, config):
        """
        __parseConfig_
        Parse the MicroService configuration and set any default values.
        :param config: config as defined in the deployment
        """
        self.logger.info("Using the following config:\n%s", config)

        self.msConfig = {}
        self.msConfig['verbose'] = getattr(config, 'verbose', False)
        self.msConfig['group'] = getattr(config, 'group', 'DataOps')
        self.msConfig['interval'] = getattr(config, 'interval', 5 * 60)
        self.msConfig['readOnly'] = getattr(config, 'readOnly', True)

        self.msConfig['reqmgrUrl'] = getattr(config, 'reqmgr2Url', 'https://cmsweb.cern.ch/reqmgr2')
        self.msConfig['reqmgrCacheUrl'] = self.msConfig['reqmgrUrl'].replace('reqmgr2', 'couchdb/reqmgr_workload_cache')
        self.msConfig['phedexUrl'] = getattr(config, 'phedexUrl', 'https://cmsweb.cern.ch/phedex/datasvc/json/prod')
        self.msConfig['dbsUrl'] = getattr(config, 'dbsUrl', 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader')

    def transferor(self, reqStatus):
        """
        MSManager transferor function.
        It performs Unified logic for data subscription and
        transfers requests from assigned to staging/staged state of ReqMgr2.
        For references see
        https://github.com/dmwm/WMCore/wiki/ReqMgr2-MicroService-Transferor
        """
        startT = time.time()
        self.logger.info("Starting the transferor thread...")
        self.msTransferor.execute()
        self.logger.info("Total transferor execution time: %.2f secs", time.time() - startT)

    def monitor(self, reqStatus='staging'):
        """
        MSManager monitoring function.
        It performs transfer requests from staging to staged state of ReqMgr2.
        For references see
        https://github.com/dmwm/WMCore/wiki/ReqMgr2-MicroService-Transferor
        """
        startT = time.time()
        self.logger.info("Starting the monitor thread...")
        # First, fetch/update our unified configuration from reqmgr_aux db
        # Keep our own copy of the unified config to avoid race conditions
        self.uConfig = self.reqmgrAux.getUnifiedConfig(docName="config")
        if not self.uConfig:
            self.logger.warning("Monitor failed to fetch the unified config. Skipping this cycle.")
            return
        self.uConfig = self.uConfig[0]

        try:
            # get requests from ReqMgr2 data-service for given statue
            # here with detail=False we get back list of records
            requests = self.reqmgr2.getRequestByStatus([reqStatus], detail=False)
            self.logger.debug('+++ monit found %s requests in %s state', len(requests), reqStatus)

            requestStatus = {}  # keep track of request statuses
            for reqName in requests:
                req = {'name': reqName, 'reqStatus': reqStatus}
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
        self.logger.info("Total monitor execution time: %.2f secs", time.time() - startT)

    def stop(self):
        "Stop MSManager"
        # stop MSTransferorMonit thread
        self.monitThread.stop()
        # stop MSTransferor thread
        self.transfThread.stop()  # stop checkStatus thread
        status = self.transfThread.running()
        return status

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
            # status = self.checkSubscription(request)
            status = 100
            statuses[tid] = status
        return statuses

    def requestCampaign(self, req):
        "Return request campaign"
        return 'campaign_TODO'  # TODO

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
            data = self.phedex.subscriptions(dataset=dataset, group=self.msConfig['group'])
            self.logger.debug("### dataset %s group %s", dataset, self.msConfig['group'])
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
                    pct = float(len(subset)) / float(len(set(rNodes)))
                    sdict[dataset] = pct
        self.logger.debug("### sdict %s", sdict)
        tot = len(sdict.keys())
        if not tot:
            return -1
        # return percentage of completion
        return round(float(sum(sdict.values())) / float(tot), 2) * 100

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

    def info(self, req):
        "Return info about given request"
        completed = self.checkSubscription(req)
        return {'request': req, 'status': completed}

    def delete(self, request):
        "Delete request in backend"
        pass

    def change(self, req, reqStatus, prefix='###'):
        """
        Change request status, internally it is done via PUT request to ReqMgr2:
        curl -X PUT -H "Content-Type: application/json" \
             -d '{"RequestStatus":"staging", "RequestName":"bla-bla"}' \
             https://xxx.yyy.zz/reqmgr2/data/request
        """
        self.logger.debug('%s updating %s status to %s', prefix, req['name'], reqStatus)
        try:
            if not self.msConfig['readOnly']:
                self.reqmgr2.updateRequestStatus(req['name'], reqStatus)
        except Exception as err:
            self.logger.exception("Failed to change request status. Error: %s", str(err))
