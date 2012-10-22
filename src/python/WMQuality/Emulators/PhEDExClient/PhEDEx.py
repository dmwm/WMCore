#!/usr/bin/env python
"""
_PhEDEx_

PhEDEx Emulator
"""




# need to clean this up at some point

#//     - ignore some params in dbs spec - silence pylint warnings
# pylint: disable-msg=W0613,R0201
from WMQuality.Emulators.DataBlockGenerator.Globals import GlobalParams
from WMQuality.Emulators.DataBlockGenerator.DataBlockGenerator import DataBlockGenerator

from xml.dom.minidom import parseString



filesInDataset = GlobalParams.numOfFilesPerBlock() * GlobalParams.numOfBlocksPerDataset()
filesInBlock = GlobalParams.numOfFilesPerBlock()

class PhEDEx(dict):
    """
    """
    def __init__(self, *args, **kwargs):
        # add the end point to prevent the existence check fails.
        self['endpoint'] = "phedex_emulator"
        self.dataBlocks = DataBlockGenerator()
        self.subRequests = {}
        
    def injectBlocks(self, node, xmlData, verbose = 0, strict = 1):

        """
        do nothing don't inject block.
        """

        return None

    def getNodeSE(self, value):
        return 'dummy.se.from.emulator'

    def subscribe(self, subscription, xmlData):
        """
        Store the subscription information in the object,
        tests can retrieve it and verify it
        """

        args = {}

        args['node'] = []
        for node in subscription.nodes:
            args['node'].append(node)

        document = parseString(xmlData)
        datasets = document.getElementsByTagName("dataset")
        for dataset in datasets:
            datasetName = dataset.getAttribute("name")

        if datasetName not in self.subRequests:
            self.subRequests[datasetName] = []

        args['data'] = xmlData
        args['level'] = subscription.level
        args['priority'] = subscription.priority
        args['move'] = subscription.move
        args['static'] = subscription.static
        args['custodial'] = subscription.custodial
        args['group'] = subscription.group
        args['request_only'] = subscription.request_only

        self.subRequests[datasetName].append(args)

        return

    def getReplicaInfoForFiles(self, **args):
        """
        _getReplicaInfoForFiles_
        TODO: Need to be implemented correctly,
        Currently not used

        Retrieve file replica information from PhEDEx.

        block          block name, with '*' wildcards, can be multiple (*).  required when no lfn is specified.
        node           node name, can be multiple (*)
        se             storage element name, can be multiple (*)
        update_since   unix timestamp, only return replicas updated since this
                    time
        create_since   unix timestamp, only return replicas created since this
                    time
        complete       y or n. if y, return only file replicas from complete block
                    replicas.  if n only return file replicas from incomplete block
                    replicas.  default is to return either.
        dist_complete  y or n.  if y, return only file replicas from blocks
                    where all file replicas are available at some node. if
                    n, return only file replicas from blocks which have
                    file replicas not available at any node.  default is
                    to return either.
        subscribed     y or n, filter for subscription. default is to return either.
        custodial      y or n. filter for custodial responsibility.  default is
                    to return either.
        group          group name.  default is to return replicas for any group.
        lfn            logical file nam
        """
        return None

    def getNodeMap(self):
        """
        _getNodeMap_

        Retrieve information about nodes known to this PhEDEx instance.  Each
        node entry will have the following keys:
          name       - PhEDEx node name
          se         - Storage element name
          kind       - Node type, e.g. 'Disk' or 'MSS'
          technology - Node technology, e.g. 'Castor'
          id         - Node id

        Return some MSS, Buffer and Disk nodes
        """

        nodeMappings = {"phedex" : {"node" : []}}

        nodeMappings["phedex"]["node"].append({"name" : "T1_US_FNAL_MSS",
                                               "kind" : "MSS",
                                               "se"   : "cmssrm.fnal.gov",
                                               "technology" : "dCache",
                                               "id" : 1})
        nodeMappings["phedex"]["node"].append({"name" : "T1_US_FNAL_Buffer",
                                               "kind" : "Buffer",
                                               "se"   : "cmssrm.fnal.gov",
                                               "technology" : "dCache",
                                               "id" : 2})
        nodeMappings["phedex"]["node"].append({"name" : "T1_UK_RAL_MSS",
                                               "kind" : "MSS",
                                               "se"   : "srm-cms.gridpp.rl.ac.uk",
                                               "technology" : "Castor",
                                               "id" : 3})
        nodeMappings["phedex"]["node"].append({"name" : "T1_UK_RAL_Buffer",
                                               "kind" : "Buffer",
                                               "se"   : "srm-cms.gridpp.rl.ac.uk",
                                               "technology" : "Castor",
                                               "id" : 4})
        nodeMappings["phedex"]["node"].append({"name" : "T1_UK_RAL_Disk",
                                               "kind" : "Disk",
                                               "se"   : "srm-cms-disk.gridpp.rl.ac.uk",
                                               "technology" : "Disk",
                                               "id" : 5})
        nodeMappings["phedex"]["node"].append({"name" : "T2_CH_CERN",
                                               "kind" : "Disk",
                                               "se"   : "srm-eoscms.cern.ch",
                                               "technology" : "Disk",
                                               "id" : 6})
        nodeMappings["phedex"]["node"].append({"name" : "T3_CO_Uniandes",
                                               "kind" : "Disk",
                                               "se"   : "moboro.uniandes.edu.co",
                                               "technology" : "DPM",
                                               "id" : 7})
        return nodeMappings

    def getReplicaInfoForBlocks(self, **args):
        """
        Where are blocks located
        """
        data = {"phedex":{"request_timestamp":1254762796.13538, "block" : []}}
        for block in args['block']:
            blocks = data['phedex']['block']
            files = self.dataBlocks.getFiles(block)
            locations = self.dataBlocks.getLocation(block)
            blocks.append({"files": len(files), "name": block,
                           'replica' : [{'node' : x + '_MSS' } for x in locations]})
        return data

    def subscriptions(self, **args):
        """
        Where is data subscribed - for now just replicate blockreplicas
        """
        def _blockInfoGenerator(blockList):

            for block in blockList:
                if type(block) == dict:
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

                if not datasetList or find:
                    data['phedex']['dataset'].append({'name' : dataset, 'files' : filesInDataset,
                                                      'block' : []})


                    datasetSelected = data['phedex']['dataset'][-1]
                subs = []
                subs.append({'node': 'T2_XX_SiteA_MSS', 'custodial': 'n', 'suspend_until': None,
                                 'level': 'dataset', 'move': 'n', 'request': '47983',
                                 'time_created': '1232989000', 'priority': 'low',
                                 'time_update': None, 'node_id': '781',
                                 'suspended': 'n', 'group': None})
#                subs.append({'node': 'T2_XX_SiteB', 'custodial': 'n', 'suspend_until': None,
#                                 'level': 'dataset', 'move': 'n', 'request': '47983',
#                                 'time_created': '1232989000', 'priority': 'low',
#                                 'time_update': None, 'node_id': '781',
#                                 'suspended': 'n', 'group': None})
                datasetSelected['subscription'] = subs

                blocks = datasetSelected['block']
                locations= self.dataBlocks.getLocation(block)

                blocks.append({"bytes":"10438786614",
                               "files":filesInBlock,
                               "is_open":"n",
                               "name": block,
                               "id":"454370", "subscription"
                                                  :[ {'node' : x + '_MSS', "suspended" : "n"} for x in locations]
                                                        #{"priority":"normal", "request":"51253", "time_created":"1245165314",
                                                        #   "move":"n", "suspend_until":None, "node":"T2_XX_SiteA",
                                                        #   "time_update":"1228905272", "group":None, "level":"block",
                                                        #   "node_id":"641", "custodial":"n", "suspended":"n"}]
                                                    })

        data = {'phedex' : {"request_timestamp" : 1254850198.15418,
                            'dataset' : []}}
        # different structure depending on whether we ask for dataset or blocks

        if args.has_key('dataset') and args['dataset']:
            for dataset in args['dataset']:
                blockList = self.dataBlocks.getBlocks(dataset)
                _blockInfoGenerator(blockList)
        elif args.has_key('block') and args['block']:
            _blockInfoGenerator(args['block'])

        return data

    def getSubscriptionMapping(self, *dataItems, **kwargs):
        """
        Similar basic functionality as self.subscriptions()
        however: dataItems may be a combination of blocks or datasets and
        kwargs is passed to PhEDEx; output is parsed and returned in the form
        { 'dataItem1' : [Node1, Node2] } where dataItem is a block or dataset

        The following cases are handled:
          o Input is a block and subscription is a dataset
          o Input is a block and subscription is a block
          o Input is a dataset and subscription is a dataset

        Not supported:
          o Input is a dataset but only block subscriptions exist
        """
        from collections import defaultdict
        result = defaultdict(set)
        kwargs.setdefault('suspended', 'n') # require active subscription

        dataItems = list(set(dataItems)) # force unique items

        # Hard to query all at once in one GET call, POST not cacheable
        # hence, query individually - use httplib2 caching to protect service
        for item in dataItems:

            # First query for a dataset level subscription (most common)
            # this returns block level subscriptions also.
            # Rely on httplib2 caching to not resend on every block in dataset
            kwargs['dataset'], kwargs['block'] = [item.split('#')[0]], []
            response = self.subscriptions(**kwargs)['phedex']

            # iterate over response as can't jump to specific datasets
            for dset in response['dataset']:
                if dset['name'] != item.split('#')[0]:
                    continue
                if dset.has_key('subscription'):
                    # dataset level subscription
                    nodes = [x['node'] for x in dset['subscription']
                             if x['suspended'] == 'n']
                    result[item].update(nodes)

                #if we have a block we must check for block level subscription also
                # combine with original query when can give both dataset and block
                if item.find('#') > -1 and dset.has_key('block'):
                    for block in dset['block']:
                        if block['name'] == item:
                            nodes = [x['node'] for x in block['subscription']
                                     if x['suspended'] == 'n']
                            result[item].update(nodes)
                            break
        return result


    def emulator(self):
        return "PhEDEx emulator ...."
# pylint: enable-msg=W0613,R0201
