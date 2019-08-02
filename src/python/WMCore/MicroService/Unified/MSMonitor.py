"""
File       : MSMonitor.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
             Alan Malta <alan dot malta AT cern dot ch >
Description: MSMonitor class provide whole logic behind
the transferor monitoring module.
"""
# futures
from __future__ import division, print_function

# system modules
import json
import time

# WMCore modules
from WMCore.MicroService.Unified.MSCore import MSCore


class MSMonitor(MSCore):
    """
    MSMonitor class provide whole logic behind
    the transferor monitoring module.
    """
    def __init__(self, msConfig, logger=None):
        super(MSMonitor, self).__init__(msConfig, logger)
        # update interval is used to check records in CouchDB and update them
        # after this interval, default 6h
        self.updateInterval = self.msConfig.get('updateInterval', 6*60*60)

    def updateCaches(self):
        """
        Fetch some data required for the monitoring logic, e.g.:
         * all campaign configuration
         * all transfer records from backend DB
        :return: True if all of them succeeded, else False
        """
        campaigns = self.reqmgrAux.getCampaignConfig("ALL_DOCS")
        transferRecords = [d for d in self.getTransferInfo('ALL_DOCS')]
        cdict = {}
        if not campaigns:
            self.logger.warning("Failed to fetch campaign configurations")
        if not transferRecords:
            self.logger.warning("Failed to fetch transfer records")
        else:
            for camp in campaigns:
                if 'CampaignName' not in camp:
                    self.logger.warning(
                        'No CampaignName attribute in campaign dict: %s',
                        json.dumps(camp))
                    continue
                cdict[camp['CampaignName']] = camp
        return cdict, transferRecords

    def execute(self, reqStatus):
        """
        Executes the MS monitoring logic, see
        https://github.com/dmwm/WMCore/wiki/ReqMgr2-MicroService-Monitor

        :param reqStatus: request statue to process
        :return:
        """
        try:
            # get requests from ReqMgr2 data-service for given statue
            # here with detail=False we get back list of records
            requests = self.reqmgr2.getRequestByStatus([reqStatus], detail=False)
            self.logger.debug('+++ monit found %s requests in %s state',
                              len(requests), reqStatus)

            campaigns, transferRecords = self.updateCaches()
            if not campaigns or not transferRecords:
                # then wait until the next cycle
                msg = "Failed to fetch data from one of the data sources. Retrying again in the next cycle"
                self.logger.error(msg)
                return

            # keep track of request and their new statuses
            requestsToStage = []
            # main logic
            for reqName in requests:
                self.logger.debug("+++ request %s", reqName)
                # obtain transfer records for our request
                transfers = []
                for rec in transferRecords:
                    if reqName == rec['workflowName']:
                        transfers = rec['transfers']
                        break
                if not transfers:
                    continue
                # if all transfers are completed
                # move the request status staging -> staged
                if self.completion(transfers, campaigns):
                    self.logger.debug(
                        "+++ request %s all transfers are completed", reqName)
                    requestsToStage.append(reqName)
                # if pileup transfers are completed AND
                # some input blocks are completed,
                # move the request status staging -> staged
                elif self.completion(transfers, campaigns, pileup=True):
                    self.logger.debug(
                        "+++ request %s pileup transfers are completed", reqName)
                    requestsToStage.append(reqName)
                # transfers not completed
                # just update the database with their completion
                else:
                    self.logger.debug(
                        "+++ request %s transfers are completed", reqName)
            self.updateTransferInfo(transferRecords)
            # finally, update statuses for requests
            for reqName in requestsToStage:
                self.change(reqName, 'staged', '+++ monit')
        except Exception as err:  # general error
            self.logger.exception('+++ monit error: %s', str(err))

    def completion(self, transfers, campaigns, pileup=None):
        """
        Helper function to calculate completion of given records
        :param transfers: list of transfers records
        :param pileup: check pileup completion (optional)
        :return: completion status
        """
        # check completion of all transfers
        statuses = []
        transferTypes = ['primary', 'secondary']
        for rec in transfers:
            campaign = rec['CampaignName']
            cdict = campaigns[campaign]
            thr = cdict.get('PartialCopy', 0)
            if pileup and rec['dataType'] not in transferTypes:
                continue
            if rec['completion'] >= thr:
                status = 1
            else:
                status = 0
            statuses.append(status)
        return True if sum(statuses) == len(transfers) else False

    def updateTransferInfo(self, transferRecords):
        """
        Update transfer records in backend
        :param transferRecords: list of transfer records
        """
        tstamp = time.time()
        for doc in transferRecords:
            transfers = []
            for rec in doc.get('transfers', []):
                # obtain new transfer ids and completion for given dataset
                _, completion = self.getTransferIds(rec['dataset'])
                # Per Alan request, we'll update only completion and not tids
                rec.update({'completion': completion})
                transfers.append(rec)
            wname = doc['workflowName']
            doc['lastUpdate'] = tstamp
            doc['transfers'] = transfers
            self.reqmgrAux.updateTransferInfo(wname, doc, inPlace=True)

    def getTransferIds(self, dataset):
        """
        Get transfer ids document for given request name and datasets.
        :param dataset: dataset name
        :return: a list of transfer ids and completion value
        """
        # phedex implementation, TODO: implement Rucio logic when it is ready
        data = self.phedex.subscriptions(
            dataset=dataset, group=self.msConfig['group'])
        self.logger.debug(
            "### dataset %s group %s", dataset, self.msConfig['group'])
        self.logger.debug("### subscription %s", data)
        tids = []
        vals = []
        for row in data['phedex']['dataset']:
            if row['name'] == dataset:
                for rec in row['subscription']:
                    vals.append(int(rec['percent_files']))
                    tids.append(int(rec['request']))
        if not vals:
            return tids, 0
        return tids, float(sum(vals))/len(vals)

    def getTransferInfo(self, wname):
        """
        Get transfer document from backend. The document has the following form:

        .. doctest::

            {"workflowName": "bla",
             "lastUpdate": 123,
             "transfers": [rec1, rec2, ... ]
            }
            where each record has the following format:
            {"dataset":"/a/b/c", "dataType": "primary", "transferIDs": [1,2], "completion": 0}


        :param wname: workflow name
        :return: a generator of transfer records
        """
        records = self.reqmgrAux.getTransferInfo(wname)
        # according to https://github.com/dmwm/WMCore/wiki/ReqMgr2-MicroService-Monitor
        # we may need to select only record since last updated according
        # to configuration UpdateInterval setting
        for rec in records:
            if self.updateInterval:
                if time.time()-rec['lastUpdate'] > self.updateInterval:
                    yield rec
            else:
                yield rec
