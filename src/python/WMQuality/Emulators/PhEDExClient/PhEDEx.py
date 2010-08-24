#!/usr/bin/env python
"""
    Mocked Phedex interface
"""




# need to clean this up at some point

#//     - ignore some params in dbs spec - silence pylint warnings
# pylint: disable-msg=W0613,R0201
from WMQuality.Emulators.DataBlockGenerator.DataBlockGenerator import DataBlockGenerator

class PhEDEx:
    """
    """
    def __init__(self, *args, **kwargs):
        print "Using PhEDEx Emulator ...."
        self.dataBlocks = DataBlockGenerator()

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
                dataset = self.dataBlocks.getDataset('block')
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
