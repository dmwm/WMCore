#!/usr/bin/env python
"""Map data to locations for WorkQueue"""

from collections import defaultdict
import time

from WMCore.WMConnectionBase import WMConnectionBase

#TODO: Combine with existing dls so DBSreader can do this kind of thing transparently
#TODO: Known Issue: Can't have same item in multiple dbs's at the same time.


# round update times. Avoid cache misses from too precise time's
UPDATE_INTERVAL_COARSENESS = 5 * 60


def isGlobalDBS(dbs):
    """Is this the global dbs"""
    # try to determine from name - save a trip to server
    # fragile but if this url changes many other things will break also...
    from urlparse import urlparse
    url = urlparse(dbs.dbs.getServerUrl()) #DBSApi has url not DBSReader
    if url.hostname.startswith('cmsdbsprod.cern.ch') and url.path.startswith('/cms_dbs_prod_global'):
        return True
    info = dbs.dbs.getServerInfo()
    if info and info.get('InstanceName') == 'GLOBAL':
        return True
    return False

def timeFloor(number, interval = UPDATE_INTERVAL_COARSENESS):
    """Get numerical floor of time to given interval"""
    from math import floor
    return floor(number / interval) * interval

class DataLocationMapper():
    """Map data to locations for WorkQueue"""
    def __init__(self, **kwargs):
        self.params = kwargs
        self.params.setdefault('locationFrom', 'subscription')
        self.params.setdefault('incompleteBlocks', False)
        self.params.setdefault('requireBlocksSubscribed', True)
        self.params.setdefault('fullRefreshInterval', 7200)
        self.params.setdefault('updateIntervalCoarseness', UPDATE_INTERVAL_COARSENESS)

        self.lastFullResync = 0
        self.lastLocationUpdate = 0

        validLocationFrom = ('subscription', 'location')
        if self.params['locationFrom'] not in validLocationFrom:
            raise ValueError, "Invalid value for locationFrom '%s' valid values %s" % (self.params['locationFrom'], validLocationFrom)

        if self.params.get('phedex'):
            self.phedex = self.params['phedex']
        if self.params.get('dbses'):
            self.dbses = self.params['dbses']
        if self.params.get('sitedb'):
            self.sitedb = self.params['sitedb']


    def __call__(self, dataItems, newDataOnly = False, fullResync = False, dbses = {}):
        result = {}
        if dbses:
            self.dbses.update(dbses)

        # do a full resync every fullRefreshInterval interval
        now = time.time()
        if not fullResync and now > (self.lastFullResync + self.params['fullRefreshInterval']):
            fullResync = True

        dataByDbs = self.organiseByDbs(dataItems)

        for dbs, dataItems in dataByDbs.items():

            # if global use phedex (not dls yet), else use dbs
            if isGlobalDBS(dbs):
                output, fullResync = self.locationsFromPhEDEx(dataItems, fullResync)
            else:
                output, fullResync = self.locationsFromDBS(dbs, dataItems)

            result.update(output)
        if fullResync:
            self.lastFullResync = now

        return result, fullResync


    def locationsFromPhEDEx(self, dataItems, fullResync = False):
        """Get data location from phedex"""
        if self.params['locationFrom'] == 'subscription':
            # subscription api doesn't support partial update
            result, fullResync = self.phedex.getSubscriptionMapping(*dataItems), True

        elif self.params['locationFrom'] == 'location':
            result = defaultdict(set)
            args = {}
            args['block'] = dataItems
            if not self.params['incompleteBlocks']:
                args['complete'] = 'y'
            if not self.params['requireBlocksSubscribed']:
                args['subscribed'] = 'y'
            if not fullResync and self.lastLocationUpdate:
                args['update_since'] = timeFloor(self.lastLocationUpdate, self.params['updateIntervalCoarseness'])
            response = self.phedex.getReplicaInfoForBlocks(**args)['phedex']

            for block in response['block']:
                nodes = [se['node'] for se in block['replica']]
                result[block['name']].update(nodes)
        else:
            raise RuntimeError, "shouldn't get here"

        # convert from PhEDEx name to cms site name
        for name, nodes in result.items():
            result[name] = [self.sitedb.phEDExNodetocmsName(x) for x in nodes]

        return result, fullResync


    def locationsFromDBS(self, dbs, dataItems):
        """Get data location from dbs"""
        result = defaultdict(set)
        for item in dataItems:
            result.update[item](dbs.listFileBlockLocation(item))
        return result, True # partial dbs updates not supported


    def organiseByDbs(self, dataItems):
        """Sort items by dbs instances - return dict with DBSReader as key & data items as values"""
        itemsByDbs = defaultdict(list)
        for item in dataItems:
            if item['dbs_url'] not in self.dbses:
                raise RuntimeError, 'No DBSReader for %s' % item['dbs_url']
            itemsByDbs[self.dbses[item['dbs_url']]].append(item['name'])
        return itemsByDbs


class WorkQueueDataLocationMapper(WMConnectionBase, DataLocationMapper):
    """WorkQueue data location functionality"""
    def __init__(self, logger, dbi, **kwargs):
        WMConnectionBase.__init__(self, 'WMCore.WorkQueue.Database', logger, dbi)
        DataLocationMapper.__init__(self, **kwargs)

        # dao actions
        self.actions = {}
        self.actions['ActiveData'] = self.daofactory(classname = "Data.GetActiveData")
        self.actions['GetDataWithoutSite'] = self.daofactory(classname = "Data.GetDataWithoutSite")
        self.actions['UpdateDataSiteMapping'] = self.daofactory(classname = "Site.UpdateDataSiteMapping")
        self.actions['NewSite'] = self.daofactory(classname = "Site.New")

    def __call__(self, newDataOnly = False, fullResync = False, dbses = {}):

        if newDataOnly:
            dataItems = self.actions['GetDataWithoutSite'].execute(conn = self.getDBConn(),
                                                                   transaction = self.existingTransaction())
        else:
            dataItems = self.actions['ActiveData'].execute(conn = self.getDBConn(),
                                                           transaction = self.existingTransaction())

        dataLocations, fullResync = DataLocationMapper.__call__(self, dataItems, newDataOnly, fullResync, dbses)

        if dataLocations:
            with self.transactionContext() as trans:
                uniqueLocations = set()
                for locations in dataLocations.values():
                    uniqueLocations.update(locations)
                self.actions['NewSite'].execute(list(uniqueLocations), conn = self.getDBConn(),
                                                transaction = trans)
                # This doesn't allow data to be in multiple dbs's
                self.actions['UpdateDataSiteMapping'].execute(dataLocations, fullResync,
                                                              conn = self.getDBConn(),
                                                              transaction = trans)

        return len(dataLocations) # probably not quite what we want, but will indicate whether some mappings were added or not
