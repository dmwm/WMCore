"""
File       : MSCore.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: MSCore class provides core functionality of the MS.
"""
# futures
from __future__ import division, print_function

# system modules
import time

# WMCore modules
from WMCore.MicroService.Unified.Common import getMSLogger
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx


class MSCore(object):
    """
    This class provides core functionality for both
    MSTransferor and MSMonitor classes.
    """

    def __init__(self, msConfig, logger=None):
        """
        Provides setup for MSTransferor and MSMonitor classes

        :param config: MS service configuration
        :param logger: logger object (optional)
        """
        self.logger = getMSLogger(getattr(msConfig, 'verbose', False), logger)
        self.msConfig = msConfig
        self.logger.info(
            "Configuration including default values:\n%s", self.msConfig)

        self.reqmgr2 = ReqMgr(self.msConfig['reqmgrUrl'], logger=self.logger)
        self.reqmgrAux = ReqMgrAux(self.msConfig['reqmgrUrl'],
                                   httpDict={'cacheduration': 60},
                                   logger=self.logger)

        # hard code it to production DBS otherwise PhEDEx subscribe API fails to match TMDB data
        dbsUrl = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
        # eventually will change it to Rucio
        self.phedex = PhEDEx(httpDict={'cacheduration': 10 * 60},
                             dbsUrl=dbsUrl, logger=self.logger)

    def unifiedConfig(self):
        """
        Fetches the unified configuration
        :return: unified configuration content
        """
        res = self.reqmgrAux.getUnifiedConfig(docName="config")
        if res:
            if isinstance(res, list):
                return res[0]
            return res
        else:
            return {}

    def change(self, reqName, reqStatus, prefix='###'):
        """
        Update the request status in ReqMgr2
        """
        self.logger.info('%s updating %s status to %s', prefix, reqName, reqStatus)
        try:
            if not self.msConfig['readOnly']:
                self.reqmgr2.updateRequestStatus(reqName, reqStatus)
        except Exception as err:
            self.logger.exception("Failed to change request status. Error: %s", str(err))

    def updateTransferInfo(self, requestStatuses):
        "Update transfer ids in backend"
        tstamp = time.time()
        # Alan's suggestion to use bulk query
        # get all docs from CouchDB
        docs = self.getTransferInfo('ALL_DOCS')
        for wname, statusRecord in requestStatuses:
            # obtain records from CouchDB
            # doc = self.getTransferInfo(wname)
            # find record in our list of docs
            # VK: I don't know if it will be faster then fetching the record
            #     in CouchDB, we need to loop every time to find proper record
            doc = {}
            for doc in docs:
                if wname in doc:
                    break
            records = doc.get('transfers', [])
            records += statusRecord
            doc['workflowName'] = wname
            doc['timestamp'] = tstamp
            doc['transfers'] = records
            self.reqmgrAux.postTransferInfo(wname, doc)

    def getTransferInfo(self, wname):
        """
        Get transfer document from backend. The document has the following form:

        .. doctest::

            {"workflowName": "bla",
             "timestamp": 123,
             "transfers": [rec1, rec2, ... ]
            }
            where each record has the following format:
            {"dataset":"/a/b/c", "dataType": "primary", "transferIDs": [1,2], "completion": 0}


        :param wname: workflow name
        :return: a tranfer ids document
        """
        return self.reqmgrAux.getTransferInfo(wname)

    def getTransferIds(self, dataset):
        """
        Get transfer ids document for given request name and datasets.
        :param dataset: dataset name
        :return: a list of transfer ids
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
        return tids, float(sum(vals))/len(vals)
