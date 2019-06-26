"""
File       : MSMonitor.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
             Alan Malta <alan dot malta AT cern dot ch >
Description: MSMonitor class provide whole logic behind
the transferor monitoring module.
"""
# futures
from __future__ import division, print_function

# WMCore modules
from WMCore.MicroService.Unified.MSCore import MSCore


class MSMonitor(MSCore):
    """
    MSMonitor class provide whole logic behind
    the transferor monitoring module.
    """
    def execute(self, reqStatus):
        """
        Executes the MS monitoring logic
        :param reqStatus: request statue to process
        :return:
        """
        try:
            # get requests from ReqMgr2 data-service for given statue
            # here with detail=False we get back list of records
            requests = self.reqmgr2.getRequestByStatus([reqStatus], detail=False)
            self.logger.debug('+++ monit found %s requests in %s state',
                              len(requests), reqStatus)

            # FIXME: completion threshold should come either from unified
            # or campaign configuration, e.g.
            # thr = self.unifiedConfig().get('completedThreshold', 100)
            thr = 100

            requestStatuses = {}  # keep track of request statuses
            for reqName in requests:
                req = {'name': reqName, 'reqStatus': reqStatus}
                self.logger.debug("+++ request %s", req)
                # obtain status records from couchdb for given request
                transferRecords = self.getTransferRecords(reqName)
                if not transferRecords:
                    continue
                completion = self.completion(transferRecords)
                # if all transfers are completed
                # move the request status staging -> staged
                if completion == thr:
                    self.logger.debug(
                        "+++ request %s all transfers are completed", req)
                    self.change(req, 'staged', '+++ monit')
                # if pileup transfers are completed AND
                # some input blocks are completed,
                # move the request status staging -> staged
                elif self.pileupTransfersCompleted(transferRecords):
                    self.logger.debug(
                        "+++ request %s pileup transfers are completed", req)
                    self.change(req, 'staged', '+++ monit')
                # transfers not completed
                # just update the database with their completion
                else:
                    self.logger.debug(
                        "+++ request %s transfers are %s completed",
                        req, completion)
                    # for us workflow name is the same as request name
                    wname = reqName
                    requestStatuses[wname] = transferRecords
            self.updateTransferInfo(requestStatuses)
        except Exception as err:  # general error
            self.logger.exception('+++ monit error: %s', str(err))

    def getTransferRecords(self, reqName):
        """
        Get transfer records for given request name from CouchDB.
        Transfer records on backend has the following form
        https://gist.github.com/amaltaro/72599f995b37a6e33566f3c749143154
        So far logic is based on option D of the records

        .. doctest::

            {"workflowName": "bla",
             "timestamp": 123,
             "transfers": [rec1, rec2, ... ]
            }
            where each record has the following format:
            {"dataset":"/a/b/c", "dataType": "primary", "transferIDs": [1,2], "completion":0}

        :param reqName: name of the request
        :return: list of of status records
        """
        transferRecords = []

        # in our case workflow name is the same as request name
        wname = reqName
        # get existing transfer IDs record
        doc = self.getTransferInfo(wname)
        if not doc:
            self.logger.error("Failed to find a transfer document for request: %s", wname)
            return transferRecords

        # loop over all records and obtain new transfer IDs
        for rec in doc['transfers']:
            dataset = rec['dataset']
            dtype = rec['dataType']
            # obtain new transfer ids for given request name and dataset
            tids, completion = self.getTransferIds(dataset)
            rec = {'dataset': dataset, 'dataType': dtype,
                   'transferIDs': tids, "completion": completion}
            transferRecords.append(rec)
        return transferRecords

    def pileupTransfersCompleted(self, transferRecords):
        "Check if pileup transfers are completed for given transfer records"
        # FIXME: completion threshold should come either from unified
        # or campaign configuration, e.g.
        # thr = self.unifiedConfig().get('completedThreshold', 100)
        thr = 100

        records = 0
        transferTypes = ['primary', 'secondary']
        # loop over all records and obtain new transfer IDs
        for rec in transferRecords:
            ctype = rec['dataType']
            completion = rec['completion']
            # count records above threshold
            if ctype in transferTypes and completion > thr:
                records += 1
        return True if records == len(transferRecords) else False

    def completion(self, records):
        """
        Helper function to calculate completion of given records
        :param records: list of status records
        :return: completion value
        """
        # may need to implement proper algorithm, so far we take average
        val = [r['completion'] for r in records]/len(records)
        return val
