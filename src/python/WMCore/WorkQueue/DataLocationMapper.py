#!/usr/bin/env python
"""Map data to locations for WorkQueue"""

from collections import defaultdict
import time
import logging

from WMCore.WorkQueue.WorkQueueUtils import get_dbs
from WMCore.WorkQueue.DataStructs.ACDCBlock import ACDCBlock

#TODO: Combine with existing dls so DBSreader can do this kind of thing transparently
#TODO: Known Issue: Can't have same item in multiple dbs's at the same time.


# round update times. Avoid cache misses from too precise time's
UPDATE_INTERVAL_COARSENESS = 5 * 60


def isGlobalDBS(dbs):
    """Is this the global dbs"""
    try:
        # try to determine from name - save a trip to server
        # fragile but if this url changes many other things will break also...
        from urlparse import urlparse
        url = urlparse(dbs.dbs.getServerUrl()) #DBSApi has url not DBSReader
        if url.hostname.startswith('cmsweb.cern.ch') and url.path.startswith('/dbs/prod/global'):
            return True
        info = dbs.dbs.getServerInfo()
        if info and info.get('InstanceName') == 'GLOBAL':
            return True
        return False
    except Exception as ex:
        # determin whether this is dbs3
        dbs.dbs.serverinfo()
        
        # hacky way to check whether it is global or local dbs.
        # issue is created, when it is resolved. use serverinfo() for that.
        # https://github.com/dmwm/DBS/issues/355
        url = dbs.dbs.url
        if url.find("/global") != -1:
            return True
        else:
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
        if self.params.get('sitedb'):
            self.sitedb = self.params['sitedb']


    def __call__(self, dataItems, fullResync = False, dbses = {},
                 datasetSearch = False):
        result = {}

        # do a full resync every fullRefreshInterval interval
        now = time.time()
        if not fullResync and now > (self.lastFullResync + self.params['fullRefreshInterval']):
            fullResync = True

        dataByDbs = self.organiseByDbs(dataItems)

        for dbs, dataItems in dataByDbs.items():
            # if global use phedex, else use dbs            
            if isGlobalDBS(dbs):
                output, fullResync = self.locationsFromPhEDEx(dataItems, fullResync,
                                                              datasetSearch)
                
            else:
                output, fullResync = self.locationsFromDBS(dbs, dataItems,
                                                           datasetSearch)
            result[dbs] = output
        if fullResync:
            self.lastFullResync = now

        return result, fullResync
    
    def locationsFromPhEDEx(self, dataItems, fullResync = False,
                            datasetSearch = False):
        """Get data location from phedex"""
        if self.params['locationFrom'] == 'subscription':
            # subscription api doesn't support partial update
            result, fullResync = self.phedex.getSubscriptionMapping(*dataItems), True
        elif self.params['locationFrom'] == 'location':
            result = defaultdict(set)
            args = {}
            if not self.params['incompleteBlocks']:
                args['complete'] = 'y'
            if not self.params['requireBlocksSubscribed']:
                args['subscribed'] = 'y'
            if not fullResync and self.lastLocationUpdate:
                args['update_since'] = timeFloor(self.lastLocationUpdate, self.params['updateIntervalCoarseness'])
            for dataItem in dataItems:
                try:
                    if datasetSearch:
                        response = self.phedex.getReplicaInfoForBlocks(dataset = [dataItem], **args)['phedex']
                    else:
                        response = self.phedex.getReplicaInfoForBlocks(block = [dataItem], **args)['phedex']
                    for block in response['block']:
                        nodes = [se['node'] for se in block['replica']]
                        if datasetSearch:
                            result[dataItem].update(nodes)
                        else:
                            result[block['name']].update(nodes)
                except Exception as ex:
                    logging.error('Error getting block location from phedex for %s: %s' % (dataItem, str(ex)))
        else:
            raise RuntimeError, "shouldn't get here"

        # convert from PhEDEx name to cms site name
        for name, nodes in result.items():
            result[name] = list(set([self.sitedb.phEDExNodetocmsName(x) for x in nodes]))

        return result, fullResync

    def locationsFromDBS(self, dbs, dataItems,
                         datasetSearch = False):
        """Get data location from dbs"""
        result = defaultdict(set)
        for item in dataItems:
            try:
                if datasetSearch:
                    seNames = dbs.listDatasetLocation(item, dbsOnly = True)
                else:
                    seNames = dbs.listFileBlockLocation(item, dbsOnly = True)
                for se in seNames:
                    result[item].update(self.sitedb.seToCMSName(se))
            except Exception as ex:
                logging.error('Error getting block location from dbs for %s: %s' % (item, str(ex)))

        # convert the sets to lists
        for name, nodes in result.items():
            result[name] = list(nodes)

        return result, True # partial dbs updates not supported


    def organiseByDbs(self, dataItems):
        """Sort items by dbs instances - return dict with DBSReader as key & data items as values"""
        itemsByDbs = defaultdict(list)
        for item in dataItems:
            if ACDCBlock.checkBlockName(item['name']):
                # if it is acdc block don't update location. location should be
                # inserted when block is queued and not supposed to change
                continue
            itemsByDbs[get_dbs(item['dbs_url'])].append(item['name'])
        return itemsByDbs


class WorkQueueDataLocationMapper(DataLocationMapper):
    """WorkQueue data location functionality"""
    def __init__(self, logger, backend, **kwargs):
        self.backend = backend
        self.logger = logger
        DataLocationMapper.__init__(self, **kwargs)

    def __call__(self, fullResync = False):
        dataItems = self.backend.getActiveData()

        # fullResync incorrect with multiple dbs's - fix!!!
        dataLocations, fullResync = DataLocationMapper.__call__(self, dataItems, fullResync)

        # elements with multiple changed data items will fail fix this, or move to store data outside element
        for dbs, dataMapping in dataLocations.items():
            modified = []
            for data, locations in dataMapping.items():
                elements = self.backend.getElementsForData(dbs, data)
                for element in elements:
                    if element.get('NoLocationUpdate', False):
                        continue
                    if sorted(locations) != sorted(element['Inputs'][data]):
                        if fullResync:
                            self.logger.info(data + ': Setting locations to: ' + ', '.join(locations))
                            element['Inputs'][data] = locations
                        else:
                            self.logger.info(data + ': Adding locations: ' + ', '.join(locations))
                            element['Inputs'][data] = list(set(element['Inputs'][data]) | set(locations))
                        modified.append(element)
            self.backend.saveElements(*modified)

        numOfParentLocations = self.updateParentLocation(fullResync)
        numOfPileupLocations = self.updatePileupLocation(fullResync)

        return len(dataLocations) + numOfParentLocations + numOfPileupLocations # probably not quite what we want, but will indicate whether some mappings were added or not

    def updateParentLocation(self, fullResync = False):
        dataItems = self.backend.getActiveParentData()

        # fullResync incorrect with multiple dbs's - fix!!!
        dataLocations, fullResync = DataLocationMapper.__call__(self, dataItems, fullResync)

        # elements with multiple changed data items will fail fix this, or move to store data outside element
        for dataMapping in dataLocations.values():
            modified = []
            for data, locations in dataMapping.items():
                elements = self.backend.getElementsForParentData(data)
                for element in elements:
                    if element.get('NoLocationUpdate', False):
                        continue
                    for pData in element['ParentData']:
                        if pData == data:
                            if sorted(locations) != sorted(element['ParentData'][pData]):
                                if fullResync:
                                    self.logger.info(data + ': Setting locations to: ' + ', '.join(locations))
                                    element['ParentData'][pData] = locations
                                else:
                                    self.logger.info(data + ': Adding locations: ' + ', '.join(locations))
                                    element['ParentData'][pData] = list(set(pData['Sites']) | set(locations))
                                modified.append(element)
                                break
            self.backend.saveElements(*modified)

        return len(dataLocations)

    def updatePileupLocation(self, fullResync = False):
        dataItems = self.backend.getActivePileupData()

        # fullResync incorrect with multiple dbs's - fix!!!
        dataLocations, fullResync = DataLocationMapper.__call__(self, dataItems, fullResync,
                                                                datasetSearch = True)

        # elements with multiple changed data items will fail fix this, or move to store data outside element
        for dataMapping in dataLocations.values():
            modified = []
            for data, locations in dataMapping.items():
                elements = self.backend.getElementsForPileupData(data)
                for element in elements:
                    for pData in element['PileupData']:
                        if pData == data:
                            if sorted(locations) != sorted(element['PileupData'][pData]):
                                if fullResync:
                                    self.logger.info(data + ': Setting locations to: ' + ', '.join(locations))
                                    element['PileupData'][pData] = locations
                                else:
                                    self.logger.info(data + ': Adding locations: ' + ', '.join(locations))
                                    element['PileupData'][pData] = list(set(element['PileupData'][pData]) | set(locations))
                                modified.append(element)
                                break
            self.backend.saveElements(*modified)

        return len(dataLocations)
