#!/usr/bin/env python
"""Map data to locations for WorkQueue"""

from future.utils import viewitems, viewvalues, listvalues
from builtins import object
from future import standard_library
standard_library.install_aliases()

from collections import defaultdict
import logging

from urllib.parse import urlparse

from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Services.MSUtils.MSUtils import getPileupDocs
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
        self.params.setdefault('rucioAccount', "wmcore_transferor")
        self.params.setdefault('rucioAccountPU', "wmcore_pileup")

        validLocationFrom = ('subscription', 'location')
        if self.params['locationFrom'] not in validLocationFrom:
            msg = "Invalid value for locationFrom: '%s'. Valid values are: %s" % (
                self.params['locationFrom'], validLocationFrom)
            raise ValueError(msg)

        self.rucio = self.params.get('rucio')
        if self.params.get('cric'):
            self.cric = self.params['cric']

        # save each DBSReader instance in the class object, such that
        # the same object is not shared amongst multiple threads
        self.dbses = {}

    def __call__(self, dataItems, rucioAcct=None):
        rucioAcct = rucioAcct or self.params['rucioAccount']
        result = {}

        dataByDbs = self.organiseByDbs(dataItems)

        for dbs, dataItems in viewitems(dataByDbs):
            # if global use Rucio, else use dbs
            if "pileup" in rucioAcct:
                output = self.locationsFromMSPileup(dataItems, dbs.dbsURL)
            elif isGlobalDBS(dbs):
                output = self.locationsFromRucio(dataItems, rucioAcct)
            else:
                output = self.locationsFromDBS(dbs, dataItems)
            result[dbs] = output

        return result

    def locationsFromRucio(self, dataItems, rucioAcct):
        """
        Get data location from Rucio. Location is mapped to the actual
        sites associated with them, so PSNs are actually returned
        :param dataItems: list of datasets/blocks names
        :param rucioAcct: string with the Rucio account name to check the rules against
        :return: dictionary key'ed by the dataset/block, with a list of PSNs as value
        """
        result = defaultdict(set)
        self.logger.info("Fetching location from Rucio for account: %s", rucioAcct)
        for dataItem in dataItems:
            try:
                dataLocations = self.rucio.getDataLockedAndAvailable(name=dataItem,
                                                                     account=rucioAcct)
                # resolve the PNNs into PSNs
                result[dataItem] = self.cric.PNNstoPSNs(dataLocations)
            except Exception as ex:
                self.logger.error('Error getting block location from Rucio for %s: %s', dataItem, str(ex))

        return result

    def locationsFromDBS(self, dbs, dataItems):
        """Get data location from dbs"""
        result = defaultdict(set)
        for dataItem in dataItems:
            try:
                if isDataset(dataItem):
                    phedexNodeNames = dbs.listDatasetLocation(dataItem)
                else:
                    phedexNodeNames = dbs.listFileBlockLocation(dataItem)
                result[dataItem].update(phedexNodeNames)
            except Exception as ex:
                self.logger.error('Error getting block location from dbs for %s: %s', dataItem, str(ex))

        # convert the sets to lists
        for name, nodes in viewitems(result):
            psns = set()
            psns.update(self.cric.PNNstoPSNs(nodes))
            result[name] = list(psns)

        return result

    def locationsFromMSPileup(self, dataItems, dbsUrl):
        """
        Get data location from MSPileup.

        :param dataItems: list, list of pileup names to query
        :param dbsUrl: str, dbs url to check which dbs server
        :return: dict, dict of pileup name keys with location set values
        """
        self.logger.info(f'Fetching locations from MSPileup for {len(dataItems)} containers')

        result = defaultdict(set)
        # TODO: Fetch multiple pileups in single request
        for dataItem in dataItems:
            try:
                queryDict = {'query': {'pileupName': dataItem},
                             'filters': ['currentRSEs', 'pileupName', 'containerFraction', 'ruleIds']}
                pileupInstance = '-testbed' if 'cmsweb-testbed' in dbsUrl else '-prod'
                msPileupUrl = f"https://cmsweb{pileupInstance}.cern.ch/ms-pileup/data/pileup"
                doc = getPileupDocs(msPileupUrl, queryDict, method='POST')[0]
                self.logger.info(f'locationsFromPileup - name: {dataItem}, currentRSEs: {doc["currentRSEs"]}, containerFraction: {doc["containerFraction"]}')
                # resolve PNNs into PSNs
                result[dataItem] = self.cric.PNNstoPSNs(doc['currentRSEs'])
            except IndexError:
                self.logger.error('Did not find any pileup document for query: %s', queryDict['query'])
            except Exception as ex:
                self.logger.error('Error getting block location from MSPileup for %s: %s', dataItem, str(ex))

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
        elemUpdated = 0
        elemUpdated += self.updatePrimaryLocation()
        elemUpdated += self.updateParentLocation()
        elemUpdated += self.updatePileupLocation()

        return elemUpdated

    def updatePrimaryLocation(self):
        dataItems = self.backend.getActiveData()
        dataLocations = super(WorkQueueDataLocationMapper, self).__call__(dataItems)
        self.logger.info("Found %d unique input data to update location", len(dataItems))

        # elements with multiple changed data items will fail fix this, or move to store data outside element
        modified = []
        for dataMapping in viewvalues(dataLocations):
            for data, locations in viewitems(dataMapping):
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

        return len(modified)

    def updateParentLocation(self):
        dataItems = self.backend.getActiveParentData()

        # fullResync incorrect with multiple dbs's - fix!!!
        dataLocations = DataLocationMapper.__call__(self, dataItems)
        self.logger.info("Found %d unique parent data to update location", len(dataItems))

        # Given that there might be multiple data items to be updated
        # handle it like a dict such that element lookup becomes easier
        modified = {}
        for dataMapping in viewvalues(dataLocations):
            for data, locations in viewitems(dataMapping):
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
        self.backend.saveElements(*listvalues(modified))

        return len(modified)

    def updatePileupLocation(self):
        dataItems = self.backend.getActivePileupData()

        # fullResync incorrect with multiple dbs's - fix!!!
        dataLocations = DataLocationMapper.__call__(self, dataItems, self.params['rucioAccountPU'])
        self.logger.info("Found %d unique pileup data to update location", len(dataItems))

        # Given that there might be multiple data items to be updated
        # handle it like a dict such that element lookup becomes easier
        modified = {}
        for dataMapping in listvalues(dataLocations):
            for data, locations in viewitems(dataMapping):
                elements = self.backend.getElementsForPileupData(data)
                self.logger.info("Found %d elements using pileup: %s", len(elements), data)
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
        self.backend.saveElements(*listvalues(modified))

        return len(modified)
