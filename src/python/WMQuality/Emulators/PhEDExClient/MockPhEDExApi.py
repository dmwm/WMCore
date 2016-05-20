#! /usr/bin/env python
"""
Version of Services/PhEDEx intended to be used with mock or unittest.mock
"""

from __future__ import (division, print_function)

from RestClient.ErrorHandling.RestClientExceptions import HTTPError

NOT_EXIST_DATASET = 'thisdoesntexist'
PILEUP_DATASET = '/mixing/pileup/DATASET'

SITES = ['T2_XX_SiteA', 'T2_XX_SiteB', 'T2_XX_SiteC']
_BLOCK_LOCATIONS = {}

mockData = {}


class MockPhEDExApi(object):
    def __init__(self, responseType="json"):
        pass

    def sitesByBlock(self, block):
        """
        Centralize the algorithm to decide where a block is based on the hash name

        Args:
            block: the name of the block

        Returns:

        """
        if hash(block) % 3 == 0:
            sites = ['T2_XX_SiteA']
        elif hash(block) % 3 == 1:
            sites = ['T2_XX_SiteA', 'T2_XX_SiteB']
        else:
            sites = ['T2_XX_SiteA', 'T2_XX_SiteB', 'T2_XX_SiteC']

        return sites

    def getReplicaPhEDExNodesForBlocks(self, block=None, dataset=None, complete='y'):

        if dataset:
            return {'%s#1' % dataset: ['T2_XX_SiteA', 'T2_XX_SiteB', 'T2_XX_SiteC']}

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
        dataItems = list(set(dataItems))  # force unique items
        locationMap = {}

        for dataItem in dataItems:
            sites = self.sitesByBlock(block=dataItem)
            locationMap.update({dataItem: sites})

        return locationMap

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
                if mockData[self.url][signature] == 'Raises HTTPError':
                    raise HTTPError
                else:
                    return mockData[self.url][signature]
            except KeyError:
                raise KeyError("PhEDEx mock API could not return data for method %s, args=%s, and kwargs=%s (URL %s)." %
                               (item, args, kwargs, self.url))

        return genericLookup
