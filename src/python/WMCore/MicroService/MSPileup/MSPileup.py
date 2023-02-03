"""
File       : MSPileup.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: MSPileup provides logic behind the pileup WMCore module.
"""

# system modules
from threading import current_thread

# 3rd party modules
import cherrypy

# WMCore modules
from WMCore.REST.Auth import authz_match
from WMCore.MicroService.MSCore.MSCore import MSCore
from WMCore.MicroService.DataStructs.DefaultStructs import PILEUP_REPORT
from WMCore.MicroService.MSPileup.MSPileupData import MSPileupData


class MSPileup(MSCore):
    """
    MSPileup provides whole logic behind the pileup WMCore module.
    """

    def __init__(self, msConfig, **kwargs):
        # TODO: so far we are testing this service w/o Rucio access
        super(MSPileup, self).__init__(msConfig, skipRucio=True)
        # TODO: if more generic approach will be required we'll need to switch to
        # super(MSPileup, self).__init__(msConfig, **kwargs)

        # for authz we should provide relevant section in MS configuration
        # if no role/group is provided then authz_match used in HTTP APIs
        # will always return valid state. Therefore, for test purposes we can
        # use empty role and group, while for production setup we should set thenm up
        authzDefaults = msConfig.get('authz_defaults', {})
        self.role = authzDefaults.get('role', [])
        self.group = authzDefaults.get('group', [])
        self.mgr = MSPileupData(msConfig)

    def status(self):
        """
        Provide MSPileup status API. We should extend it to check DB connection, etc.

        :return: status dictionary
        """
        summary = dict(PILEUP_REPORT)
        summary.update({'thread_id': current_thread().name})
        return summary

    def getPileup(self, **kwargs):
        """
        MSPileup get API fetches the data from underlying database layer

        :param **kwargs: provide key=value (or spec) input
        :return: results of MSPileup data layer (list of dicts)
        """
        spec = {}  # (get all full docs) retrieve a list with the full documentation of all the pileups
        if 'spec' in kwargs:
            # use specific spec (JSON query)
            spec = kwargs['spec']
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
        for key in kwargs.get('filters', []):
            projection[key] = 1
        results = self.mgr.getPileup(spec, projection)

        return results

    def queryDatabase(self, spec, projection=None):
        """
        MSPileup query database API querying the data in underlying data layer.

        :param spec: provide query JSON spec to MSPileup data layer
        :return: results of MSPileup data layer (list of dicts)
        """
        return self.mgr.getPileup(spec, projection)

    def createPileup(self, pdict):
        """
        MSPileup create pileup API to create appropriate pileup document
        in underlying database.

        :param pdict: input MSPilup data dictionary
        :return: results of MSPileup data layer (list of dicts)
        """
        authz_match(self.role, self.group)
        return self.mgr.createPileup(pdict)

    def updatePileup(self, pdict):
        """
        MSPileup update API to update correspoding pileup document in data layer.

        :param pdict: input MSPilup data dictionary
        :return: results of MSPileup data layer (list of dicts)
        """
        authz_match(self.role, self.group)
        return self.mgr.updatePileup(pdict)

    def deletePileup(self, spec):
        """
        MSPileup delete API to delete corresponding pileup document in data layer.

        :param pdict: input MSPilup data dictionary
        :return: results of MSPileup data layer (list of dicts)
        """
        authz_match(self.role, self.group)
        return self.mgr.deletePileup(spec)
