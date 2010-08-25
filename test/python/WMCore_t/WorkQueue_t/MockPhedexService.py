#!/usr/bin/env python
"""
    Mocked Phedex interface
"""

__revision__ = "$Id: MockPhedexService.py,v 1.2 2010/03/19 17:29:47 swakef Exp $"
__version__ = "$Revision: 1.2 $"

# need to clean this up at some point

#//     - ignore some params in dbs spec - silence pylint warnings
# pylint: disable-msg=W0613,R0201
class MockPhedexService:
    """
    TODO: Move to a proper mocking libs for this
    """
    def __init__(self, *datasets):
        self.datasets = datasets
        for dataset in self.datasets:
            self.locations = {dataset + '#1' : ['SiteA'],
                              dataset + '#2' : ['SiteA', 'SiteB']}

    def getReplicaInfoForBlocks(self, **args):
        """
        Where are blocks located
        """
        data = {"phedex":{"request_timestamp":1254762796.13538, "block" : []}}
        blocks = data['phedex']['block']
        for dataset in self.datasets:
            blocks.append({"files":"5", "name": dataset + '#1',
                          'replica' : [{'se' : x } for x in self.locations[dataset + '#1']]})
            blocks.append({"files":"10", "name": dataset + '#2',
                          'replica' : [{'se' : x } for x in self.locations[dataset + '#2']]})
        return data

    def subscriptions(self, **args):
        """
        Where is data subscribed - for now just replicate blockreplicas
        """
        data = {'phedex' : {"request_timestamp" : 1254850198.15418,
                            'dataset' : []}}
        # different structure depending on whether we ask for dataset or blocks
        if args.has_key('dataset') and args['dataset']:
            datasets = data['phedex']['dataset']
            for dataset in self.datasets:
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
        else:
            datasets = data['phedex']['dataset']
            for dataset in self.datasets:
                data['phedex']['dataset'].append({'name' : dataset, 'files' : 5,
                                              'block' : []})
                blocks = data['phedex']['dataset'][-1]['block']
                blocks.append({"bytes":"10438786614", "files":"5", "is_open":"n",
                                                    "name": dataset + '#1',
                                                    "id":"454370", "subscription"
                                                                                :[ {'node' : x,
                                                                                    "suspended" : "n"} for x in self.locations[dataset + '#1']]
                                                                                #{"priority":"normal", "request":"51253", "time_created":"1245165314",
                                                                                #   "move":"n", "suspend_until":None, "node":"SiteA",
                                                                                #   "time_update":"1228905272", "group":None, "level":"block",
                                                                                #   "node_id":"641", "custodial":"n", "suspended":"n"}]
                                                    })
                blocks.append({"bytes":"10438786614", "files":"10", "is_open":"n",
                                                    "name": dataset + '#2',
                                                    "id":"454370", "subscription"
                                                                                :[ {'node' : x,
                                                                                    "suspended" : "n" } for x in self.locations[dataset + '#2']]
                                                                                #{"priority":"normal", "request":"51253", "time_created":"1245165314",
                                                                                #   "move":"n", "suspend_until":None, "node":"SiteA",
                                                                                #   "time_update":"1228905272", "group":None, "level":"block",
                                                                                #   "node_id":"641", "custodial":"n", "suspended":"n"}]
                                                    })
            return data
# pylint: enable-msg=W0613,R0201
