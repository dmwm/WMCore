#!/usr/bin/env python
"""Map data to locations for WorkQueue"""

from collections import defaultdict
import logging
try:
    from urlparse import urlparse
except ImportError:
    # PY3
    from urllib.parse import urlparse

from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.WorkQueue.DataStructs.ACDCBlock import ACDCBlock

# TODO: Combine with existing dls so DBSreader can do this kind of thing transparently
# TODO: Known Issue: Can't have same item in multiple dbs's at the same time.



def isGlobalDBS(dbs):
    """
    Receives a DBSReader object and finds out whether it's
    pointing to Global DBS (no matter whether it's production
    or the pre-production instance).
    """
    try:
        url = urlparse(dbs.dbsURL)
        if url.hostname.startswith('cmsweb'):
            if url.path.startswith('/dbs/prod/global') or url.path.startswith('/dbs/int/global'):
                return True
    except Exception as ex:
        logging.error("Failed to find out whether DBS is Global or not. Error: %s", str(ex))
    return False


def isDataset(inputData):
    """Check whether we're handling a block or a dataset"""
    if '#' in inputData.split('/')[-1]:
        return False
    return True


class DataLocationMapper(object):
    """Map data to locations for WorkQueue"""

    def __init__(self, logger=None, **kwargs):
        self.params = kwargs
        self.logger = logger or logging.getLogger()
        self.params.setdefault('locationFrom', 'subscription')
        self.params.setdefault('incompleteBlocks', False)
        self.params.setdefault('requireBlocksSubscribed', True)

        validLocationFrom = ('subscription', 'location')
        if self.params['locationFrom'] not in validLocationFrom:
            msg = "Invalid value for locationFrom: '%s'. Valid values are: %s" % (
                self.params['locationFrom'], validLocationFrom)
            raise ValueError(msg)

        if self.params.get('phedex'):
            self.phedex = self.params['phedex']  # NOTE: this might be a Rucio instance
        if self.params.get('cric'):
            self.cric = self.params['cric']

        # save each DBSReader instance in the class object, such that
        # the same object is not shared amongst multiple threads
        self.dbses = {}

    def __call__(self, dataItems):
        result = {}

        dataByDbs = self.organiseByDbs(dataItems)

        for dbs, dataItems in dataByDbs.items():
            # if global use phedex, else use dbs
            if isGlobalDBS(dbs):
                output = self.locationsFromPhEDEx(dataItems)
            else:
                output = self.locationsFromDBS(dbs, dataItems)
            result[dbs] = output

        return result

    def locationsFromPhEDEx(self, dataItems):
        """Get data location from phedex"""
        result = defaultdict(set)
        if hasattr(self.phedex, "getBlocksInContainer"):
            ### It's RUCIO!!!
            self.logger.info("Fetching location from Rucio...")
            for dataItem in dataItems:
                try:
                    if isDataset(dataItem):
                        response = self.phedex.getReplicaInfoForBlocks(dataset=dataItem)
                        for item in response:
                            result[dataItem].update(item['replica'])
                    else:
                        response = self.phedex.getReplicaInfoForBlocks(block=dataItem)
                        for item in response:
                            result[item['name']].update(item['replica'])
                except Exception as ex:
                    self.logger.error('Error getting block location from Rucio for %s: %s', dataItem, str(ex))
        elif self.params['locationFrom'] == 'subscription':
            self.logger.info("Fetching subscription data from PhEDEx")
            # subscription api doesn't support partial update
            result = self.phedex.getSubscriptionMapping(*dataItems)
        elif self.params['locationFrom'] == 'location':
            args = {}
            if not self.params['incompleteBlocks']:
                args['complete'] = 'y'
            if not self.params['requireBlocksSubscribed']:
                args['subscribed'] = 'y'
            self.logger.info("Fetching location data from PhEDEx with args: %s", args)

            for dataItem in dataItems:
                try:
                    if isDataset(dataItem):
                        response = self.phedex.getReplicaInfoForBlocks(dataset=[dataItem], **args)['phedex']
                    else:
                        response = self.phedex.getReplicaInfoForBlocks(block=[dataItem], **args)['phedex']
                    for block in response['block']:
                        nodes = [replica['node'] for replica in block['replica']]
                        if isDataset(dataItem):
                            result[dataItem].update(nodes)
                        else:
                            result[block['name']].update(nodes)
                except Exception as ex:
                    self.logger.error('Error getting block location from phedex for %s: %s', dataItem, str(ex))
        else:
            raise RuntimeError("shouldn't get here")

        # convert from PhEDEx name to cms site name
        for item in result:
            psns = set()
            psns.update(self.cric.PNNstoPSNs(result[item]))
            result[item] = list(psns)

        return result

    def locationsFromDBS(self, dbs, dataItems):
        """Get data location from dbs"""
        result = defaultdict(set)
        for dataItem in dataItems:
            try:
                if isDataset(dataItem):
                    phedexNodeNames = dbs.listDatasetLocation(dataItem, dbsOnly=True)
                else:
                    phedexNodeNames = dbs.listFileBlockLocation(dataItem, dbsOnly=True)
                result[dataItem].update(phedexNodeNames)
            except Exception as ex:
                self.logger.error('Error getting block location from dbs for %s: %s', dataItem, str(ex))

        # convert the sets to lists
        for name, nodes in result.items():
            psns = set()
            psns.update(self.cric.PNNstoPSNs(nodes))
            result[name] = list(psns)

        return result

    def organiseByDbs(self, dataItems):
        """Sort items by dbs instances - return dict with DBSReader as key & data items as values"""
        itemsByDbs = defaultdict(list)
        for item in dataItems:
            if ACDCBlock.checkBlockName(item['name']):
                # if it is acdc block don't update location. location should be
                # inserted when block is queued and not supposed to change
                continue

            if item['dbs_url'] not in self.dbses:
                self.dbses[item['dbs_url']] = DBSReader(item['dbs_url'])
            itemsByDbs[self.dbses[item['dbs_url']]].append(item['name'])

        return itemsByDbs


class WorkQueueDataLocationMapper(DataLocationMapper):
    """WorkQueue data location functionality"""

    def __init__(self, logger, backend, **kwargs):
        self.backend = backend
        self.logger = logger
        super(WorkQueueDataLocationMapper, self).__init__(logger, **kwargs)

    def __call__(self):
        dataItems = self.backend.getActiveData()

        dataLocations = super(WorkQueueDataLocationMapper, self).__call__(dataItems)
        self.logger.info("Found %d unique input data to update location", len(dataItems))

        # elements with multiple changed data items will fail fix this, or move to store data outside element
        modified = []
        for _, dataMapping in dataLocations.items():
            for data, locations in dataMapping.items():
                elements = self.backend.getElementsForData(data)
                for element in elements:
                    if element.get('NoInputUpdate', False):
                        continue
                    if sorted(locations) != sorted(element['Inputs'][data]):
                        self.logger.info("%s, setting location to: %s", data, locations)
                        element['Inputs'][data] = locations
                        modified.append(element)
        self.logger.info("Updating %d elements for Input location update", len(modified))
        self.backend.saveElements(*modified)

        numParents = self.updateParentLocation()
        numPileups = self.updatePileupLocation()

        return len(modified) + numParents + numPileups

    def updateParentLocation(self):
        dataItems = self.backend.getActiveParentData()

        # fullResync incorrect with multiple dbs's - fix!!!
        dataLocations = DataLocationMapper.__call__(self, dataItems)
        self.logger.info("Found %d unique parent data to update location", len(dataItems))

        # Given that there might be multiple data items to be updated
        # handle it like a dict such that element lookup becomes easier
        modified = {}
        for _, dataMapping in dataLocations.items():
            for data, locations in dataMapping.items():
                elements = self.backend.getElementsForParentData(data)
                for element in elements:
                    if element.get('NoInputUpdate', False):
                        continue
                    if element.id in modified:
                        element = modified[element.id]
                    for pData in element['ParentData']:
                        if pData == data:
                            if sorted(locations) != sorted(element['ParentData'][pData]):
                                self.logger.info("%s, setting location to: %s", data, locations)
                                element['ParentData'][pData] = locations
                                modified[element.id] = element
                                break
        self.logger.info("Updating %d elements for Parent location update", len(modified))
        self.backend.saveElements(*modified.values())

        return len(modified)

    def updatePileupLocation(self):
        dataItems = self.backend.getActivePileupData()

        # fullResync incorrect with multiple dbs's - fix!!!
        dataLocations = DataLocationMapper.__call__(self, dataItems)
        self.logger.info("Found %d unique pileup data to update location", len(dataItems))

        # Given that there might be multiple data items to be updated
        # handle it like a dict such that element lookup becomes easier
        modified = {}
        for _, dataMapping in dataLocations.items():
            for data, locations in dataMapping.items():
                elements = self.backend.getElementsForPileupData(data)
                for element in elements:
                    if element.get('NoPileupUpdate', False):
                        continue
                    if element.id in modified:
                        element = modified[element.id]
                    for pData in element['PileupData']:
                        if pData == data:
                            if sorted(locations) != sorted(element['PileupData'][pData]):
                                self.logger.info("%s, setting location to: %s", data, locations)
                                element['PileupData'][pData] = locations
                                modified[element.id] = element
                                break
        self.logger.info("Updating %d elements for Pileup location update", len(modified))
        self.backend.saveElements(*modified.values())

        return len(modified)
