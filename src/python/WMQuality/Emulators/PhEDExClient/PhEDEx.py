#!/usr/bin/env python
"""
    Mocked Phedex interface
"""




# need to clean this up at some point

#//     - ignore some params in dbs spec - silence pylint warnings
# pylint: disable-msg=W0613,R0201
from WMQuality.Emulators.DataBlockGenerator.DataBlockGenerator import DataBlockGenerator
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx as RealPhEDEx

class PhEDEx(RealPhEDEx):
    """
    """
    def __init__(self, *args, **kwargs):
        print "Using PhEDEx Emulator ...."
        # add the end point to prevent the existence check fails.
        self['endpoint'] = "phedex_emulator"
        self.dataBlocks = DataBlockGenerator()

    def injectBlocks(self, node, xmlData, verbose = 0, strict = 1):

        """
        do nothing don't inject block.
        """

        return None

    def subscribe(self, subscription, xmlData):
        """
        do nothing don't subscribe.
        """

        return None

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

        TODO: Need to be implemented correctly,
        Currently not used

        Retrieve information about nodes known to this PhEDEx instance.  Each
        node entry will have the following keys:
          name       - PhEDEx node name
          se         - Storage element name
          kind       - Node type, e.g. 'Disk' or 'MSS'
          technology - Node technology, e.g. 'Castor'
          id         - Node id
        """

        return None

    def getReplicaInfoForBlocks(self, **args):
        """
        Where are blocks located
        """
        for block in args['block']:
            data = {"phedex":{"request_timestamp":1254762796.13538, "block" : []}}
            blocks = data['phedex']['block']
            files = self.dataBlocks.getFiles(block)
            locations = self.dataBlocks.getLocation(block)
            blocks.append({"files": len(files), "name": block,
                           'replica' : [{'node' : x } for x in locations]})
        return data

    def subscriptions(self, **args):
        """
        Where is data subscribed - for now just replicate blockreplicas
        """
        data = {'phedex' : {"request_timestamp" : 1254850198.15418,
                            'dataset' : []}}
        # different structure depending on whether we ask for dataset or blocks
        
        if args.has_key('dataset') and args['dataset']:
            for dataset in args['dataset']:
                # TODO needs to add correct file numbers
                data['phedex']['dataset'].append({'name' : dataset, 'files' : 5,
                                                  'subscription' : []})
                subs = data['phedex']['dataset'][-1]['subscription']
                    #FIXME: Take from self.locations
                subs.append({'node': 'SiteA', 'custodial': 'n', 'suspend_until': None,
                             'level': 'dataset', 'move': 'n', 'request': '47983',
                             'time_created': '1232989000', 'priority': 'low',
                             'time_update': None, 'node_id': '781',
                             'suspended': 'n', 'group': None})
            return data
        elif args.has_key('block') and args['block']:
            
            for block in args['block']:
                dataset = self.dataBlocks.getDatasetName('block')
                # TODO needs to add correct file numbers
                data['phedex']['dataset'].append({'name' : dataset, 'files' : 5,
                                              'block' : []})
                blocks = data['phedex']['dataset'][-1]['block']
                locations= self.dataBlocks.getLocation(block)
                        
                blocks.append({"bytes":"10438786614", "files":"5", "is_open":"n",
                               "name": args['block'],
                               "id":"454370", "subscription"
                                                  :[ {'node' : x } for x in locations]
                                                        #{"priority":"normal", "request":"51253", "time_created":"1245165314",
                                                        #   "move":"n", "suspend_until":None, "node":"SiteA",
                                                        #   "time_update":"1228905272", "group":None, "level":"block",
                                                        #   "node_id":"641", "custodial":"n", "suspended":"n"}]
                                                    })
            return data
        


    def emulator(self):
        return "PhEDEx emulator ...."
# pylint: enable-msg=W0613,R0201
