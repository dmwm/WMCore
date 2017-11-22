#!/usr/bin/env python

"""
Version of Services/PhEDEx intended to be used with mock or unittest.mock
Parts of this code are cribbed directly from
https://github.com/dmwm/WMCore/blob/a22bbd2a62c08eff50ea26f3886266a5b25963b1/src/python/WMQuality/Emulators/PhEDExClient/PhEDEx.py
which should be consulted for a more complete implementation
"""

from __future__ import (division, print_function)

from RestClient.ErrorHandling.RestClientExceptions import HTTPError

from WMCore.Services.DBS.DBS3Reader import (DBS3Reader, DBSReaderError)
from WMQuality.Emulators.DataBlockGenerator.DataBlockGenerator import DataBlockGenerator

PROD_DBS = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'

NOT_EXIST_DATASET = 'thisdoesntexist'
PILEUP_DATASET = '/HighPileUp/Run2011A-v1/RAW'

SITES = ['T2_XX_SiteA', 'T2_XX_SiteB', 'T2_XX_SiteC']
_BLOCK_LOCATIONS = {}

BLOCKS_PER_DATASET = 2
FILES_PER_BLOCK = 5
FILES_PER_DATASET = BLOCKS_PER_DATASET * FILES_PER_BLOCK

MOCK_DATA = {}


class MockPhEDExApi(object):
    """
    Version of Services/PhEDEx intended to be used with mock or unittest.mock
    """

    def __init__(self, dict=None, responseType="json", logger=None,
                 dbsUrl='https://cmsweb.cern.ch/dbs/prod/global/DBSReader'):
        print("Using MockPhEDExApi")
        self.dbsUrl = dbsUrl
        dict = dict or {}
        self.dataBlocks = DataBlockGenerator()
        self.subRequests = {}

    def sitesByBlock(self, block):
        """
        Centralize the algorithm to decide where a block is based on the hash name

        Args:
            block: the name of the block

        Returns:
            sites: a fake list of sites where the data is

        """

        if hash(block) % 3 == 0:
            sites = ['T2_XX_SiteA']
        elif hash(block) % 3 == 1:
            sites = ['T2_XX_SiteA', 'T2_XX_SiteB']
        else:
            sites = ['T2_XX_SiteA', 'T2_XX_SiteB', 'T2_XX_SiteC']

        return sites

    def getReplicaPhEDExNodesForBlocks(self, block=None, dataset=None, complete='y'):
        """

        Args:
            block: the name of the block
            dataset: the name of the dataset
            complete: ??

        Returns:
            a fake list of blocks and the fakes sites they are at
        """
        if isinstance(dataset, list):
            dataset = dataset[0]  # Dataset is a list in these tests
        if dataset:
            # TODO: Generalize this and maybe move dataset detection into sitesByBlock
            if dataset == PILEUP_DATASET:
                return {
                    '%s#0fcb2b12-d27e-11e0-91b1-003048caaace' % dataset: ['T2_XX_SiteA', 'T2_XX_SiteB', 'T2_XX_SiteC']}
            else:
                try:
                    DBS3Reader(PROD_DBS).checkDatasetPath(dataset)
                    blocks = DBS3Reader(PROD_DBS).dbs.listBlocks(dataset=dataset)
                    singleBlock = blocks[0]['block_name']
                    return {singleBlock: self.sitesByBlock(singleBlock)}
                except DBSReaderError:
                    return {'%s#0fcb2b12-d27e-11e0-91b1-003048caaace' % dataset: []}

        replicas = {}
        for oneBlock in block:
            if oneBlock.split('#')[0] == PILEUP_DATASET:
                # Pileup is at a single site
                sites = ['T2_XX_SiteC']
                _BLOCK_LOCATIONS[oneBlock] = sites
            else:
                sites = self.sitesByBlock(block=oneBlock)
                _BLOCK_LOCATIONS[oneBlock] = sites
            replicas.update({oneBlock: sites})
        return replicas

    def getReplicaInfoForBlocks(self, **args):
        """
        Where are blocks located
        """

        data = {"phedex": {"request_timestamp": 1254762796.13538, "block": []}}

        for block in args['block']:
            blocks = data['phedex']['block']
            # files = self.dataBlocks.getFiles(block)
            # locations = self.dataBlocks.getLocation(block)
            sites = self.sitesByBlock(block=block)
            blocks.append({'files': 1, 'name': block, 'replica': [{'node': x} for x in sites]})
        return data

    def getSubscriptionMapping(self, *dataItems, **kwargs):
        """
        Fake version of the existing PhEDEx method
        """

        dataItems = list(set(dataItems))  # force unique items
        locationMap = {}

        for dataItem in dataItems:
            sites = self.sitesByBlock(block=dataItem)
            locationMap.update({dataItem: sites})

        return locationMap

    def getNodeMap(self):

        nodeMappings = {"phedex": {"node": []}}

        nodes = [{"name": "T1_US_FNAL_MSS", "kind": "MSS", "se": "cmssrm.fnal.gov", "technology": "dCache", "id": 1},
                 {"name": "T1_US_FNAL_Buffer", "kind": "Buffer", "se": "cmssrm.fnal.gov", "technology": "dCache",
                  "id": 2},
                 {"name": "T0_CH_CERN_MSS", "kind": "MSS", "se": "srm-cms.cern.ch", "technology": "Castor", "id": 3},
                 {"name": "T0_CH_CERN_Buffer", "kind": "Buffer", "se": "srm-cms.cern.ch", "technology": "Castor",
                  "id": 4},
                 {"name": "T1_UK_RAL_MSS", "kind": "MSS", "se": "srm-cms.gridpp.rl.ac.uk", "technology": "Castor",
                  "id": 5},
                 {"name": "T1_UK_RAL_Buffer", "kind": "Buffer", "se": "srm-cms.gridpp.rl.ac.uk", "technology": "Castor",
                  "id": 6},
                 {"name": "T1_UK_RAL_Disk", "kind": "Disk", "se": "srm-cms-disk.gridpp.rl.ac.uk", "technology": "Disk",
                  "id": 7},
                 {"name": "T2_CH_CERN", "kind": "Disk", "se": "srm-eoscms.cern.ch", "technology": "Disk", "id": 8},
                 {"name": "T3_CO_Uniandes", "kind": "Disk", "se": "moboro.uniandes.edu.co", "technology": "DPM",
                  "id": 9}
                ]

        for node in nodes:
            nodeMappings["phedex"]["node"].append(node)

        return nodeMappings

    def subscriptions(self, **args):
        """
        Where is data subscribed - for now just replicate blockreplicas
        """

        def _blockInfoGenerator(blockList):

            for block in blockList:
                if isinstance(block, dict):
                    block = block['Name']
                dataset = self.dataBlocks.getDatasetName(block)
                # TODO needs to add correct file numbers
                datasetList = data['phedex']['dataset']
                if datasetList:
                    find = False
                    for dataItem in datasetList:
                        if dataItem['name'] == dataset:
                            datasetSelected = dataItem
                            find = True
                            break
                if not (datasetList and find):
                    data['phedex']['dataset'].append({'name': dataset, 'files': FILES_PER_DATASET,
                                                      'block': []})

                    datasetSelected = data['phedex']['dataset'][-1]
                subs = []
                subs.append({'node': 'T2_XX_SiteA_MSS', 'custodial': 'n', 'suspend_until': None,
                             'level': 'dataset', 'move': 'n', 'request': '47983',
                             'time_created': '1232989000', 'priority': 'low',
                             'time_update': None, 'node_id': '781',
                             'suspended': 'n', 'group': None})

                if dataset in self.subRequests:
                    subs.extend(self.subRequests[dataset])
                datasetSelected['subscription'] = subs
                for sub in subs:
                    if sub['level'] == 'block':
                        subs.remove(sub)

                blocks = datasetSelected['block']
                locations = self.dataBlocks.getLocation(block)

                blocks.append({"bytes": "10438786614", "files": FILES_PER_BLOCK, "is_open": "n", "name": block,
                               "id": "454370",
                               "subscription": [{'node': x + '_MSS', "suspended": "n"} for x in locations]
                              })

        data = {'phedex': {"request_timestamp": 1254850198.15418,
                           'dataset': []}}

        # Different structure depending on whether we ask for dataset or blocks
        if 'dataset' in args and args['dataset']:
            blockList = self.dataBlocks.getBlocks(args['dataset'])
            _blockInfoGenerator(blockList)
        elif 'block' in args and args['block']:
            _blockInfoGenerator(args['block'])
        elif 'group' in args and args['group']:
            blockList = self.dataBlocks.getBlocks('/a/b-%s/c' % args['group'])
            _blockInfoGenerator(blockList)

        return data

    def getRequestList(self, **kwargs):
        """
        _getRequestList_
        Emulated request list, for now it does nothing
        """

        goldenResponse = {"phedex": {"request": [], "request_timestamp": 1368636296.94707,
                                     "request_version": "2.3.15-comp", "request_call": "requestlist",
                                     "call_time": 0.34183, "request_date": "2013-05-15 16:44:56 UTC"}}
        return goldenResponse

    def __getattr__(self, item):
        """
        __getattr__ gets called in case lookup of the actual method fails. We use this to return data based on
        a lookup table

        :param item: The method name the user is trying to call
        :return: The generic lookup function
        """

        def genericLookup(*args, **kwargs):
            """
            This function returns the mocked DBS data

            :param args: positional arguments it was called with
            :param kwargs: named arguments it was called with
            :return: the dictionary that DBS would have returned
            """

            if kwargs:
                signature = '%s:%s' % (item, sorted(kwargs.iteritems()))
            else:
                signature = item

            try:
                if MOCK_DATA[self.url][signature] == 'Raises HTTPError':
                    raise HTTPError
                else:
                    return MOCK_DATA[self.url][signature]
            except KeyError:
                raise KeyError("PhEDEx mock API could not return data for method %s, args=%s, and kwargs=%s (URL %s)." %
                               (item, args, kwargs, self.url))

        return genericLookup
