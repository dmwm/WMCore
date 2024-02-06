"""
File       : MSPileup.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: MSPileup provides logic behind the pileup WMCore module.
"""

# system modules
from threading import current_thread

# WMCore modules
from WMCore.MicroService.MSCore.MSAuth import MSAuth
from WMCore.MicroService.MSCore.MSCore import MSCore
from WMCore.MicroService.DataStructs.DefaultStructs import PILEUP_REPORT
from WMCore.MicroService.MSPileup.MSPileupData import MSPileupData


class MSPileup(MSCore):
    """
    MSPileup provides whole logic behind the pileup WMCore module.
    """

    def __init__(self, msConfig, **kwargs):
        super().__init__(msConfig)
        self.dataMgr = MSPileupData(msConfig)
        self.authMgr = MSAuth(msConfig)
        # Get the RSE expression for Disk RSEs from the configuration
        self.diskRSEExpr = msConfig.get("rucioDiskExpression", "")

    @staticmethod
    def status():
        """
        Provide MSPileup status API. We should extend it to check DB connection, etc.

        :return: status dictionary
        """
        summary = dict(PILEUP_REPORT)
        summary.update({'thread_id': current_thread().name})
        return summary

    def userDN(self):
        """
        Return user DN from authentication manager
        :return: string
        """
        user = self.authMgr.userInfo()
        dn = user.get('dn', 'Unknown')
        return dn

    def getPileup(self, **kwargs):
        """
        MSPileup get API fetches the data from underlying database layer

        :param **kwargs: provide key=value (or spec) input
        :return: results of MSPileup data layer (list of dicts)
        """
        spec = {}  # (get all full docs) retrieve a list with the full documentation of all the pileups
        if 'query' in kwargs:
            # use specific spec (JSON query)
            spec = kwargs['query']
        elif 'pileupName' in kwargs:
            # get docs for given pileupName
            spec = {'pileupName': kwargs['pileupName']}
        elif 'campaign' in kwargs:
            # get docs for given campaign
            spec = {'campaign': kwargs['campaign']}
        else:
            for key, val in kwargs.items():
                spec[key] = val

        # check if filters are present and use it as projection fields
        projection = {}
        filters = kwargs.get('filters', [])
        # convert a single filters to a list
        if isinstance(filters, str):
            filters = [filters]
        for key in kwargs.get('filters', []):
            # filter out empty strings
            if key:
                projection[key] = 1
        results = self.dataMgr.getPileup(spec, projection)

        return results

    def queryDatabase(self, query, projection=None):
        """
        MSPileup query database API querying the data in underlying data layer.

        :param query: provide query JSON spec to MSPileup data layer
        :return: results of MSPileup data layer (list of dicts)
        """
        spec = {}  # (get all full docs) retrieve a list with the full documentation of all the pileups
        if 'query' in query:
            # use specific spec (JSON query)
            spec = query['query']
        # check if filters are present and use it as projection fields
        projection = {}
        for key in query.get('filters', []):
            projection[key] = 1
        return self.dataMgr.getPileup(spec, projection)

    def createPileup(self, pdict):
        """
        MSPileup create pileup API to create appropriate pileup document
        in underlying database.

        :param pdict: input MSPileup data dictionary
        :return: results of MSPileup data layer (list of dicts)
        """
        self.authMgr.authorizeApiAccess('ms-pileup', 'create')
        rseNames = self.rucio.evaluateRSEExpression(self.diskRSEExpr, useCache=True)
        return self.dataMgr.createPileup(pdict, rseNames, userDN=self.userDN())

    def updatePileup(self, pdict):
        """
        MSPileup update API to update corresponding pileup document in data layer.

        :param pdict: input MSPileup data dictionary, e.g. partial pileup spec
        {"containerFraction": float, "pileupName": string}.
        :return: results of MSPileup data layer (list of dicts)
        """
        self.authMgr.authorizeApiAccess('ms-pileup', 'update')
        rseNames = self.rucio.evaluateRSEExpression(self.diskRSEExpr, useCache=True)
        return self.dataMgr.updatePileup(pdict, rseNames, validate=True, userDN=self.userDN())

    def deletePileup(self, spec):
        """
        MSPileup delete API to delete corresponding pileup document in data layer.

        :param pdict: input MSPileup data dictionary
        :return: results of MSPileup data layer (list of dicts)
        """
        self.authMgr.authorizeApiAccess('ms-pileup', 'delete')
        return self.dataMgr.deletePileup(spec)
