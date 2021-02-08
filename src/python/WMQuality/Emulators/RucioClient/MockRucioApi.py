#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Version of WMCore/Services/Rucio intended to be used with mock or unittest.mock
"""
from __future__ import print_function, division

from RestClient.ErrorHandling.RestClientExceptions import HTTPError

from WMCore.Services.DBS.DBS3Reader import DBS3Reader, DBSReaderError
from WMCore.Services.Rucio.Rucio import WMRucioException, WMRucioDIDNotFoundException
from WMQuality.Emulators.DataBlockGenerator.DataBlockGenerator import DataBlockGenerator

PROD_DBS = 'https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader'

NOT_EXIST_DATASET = 'thisdoesntexist'
PILEUP_DATASET = '/HighPileUp/Run2011A-v1/RAW'

SITES = ['T2_XX_SiteA', 'T2_XX_SiteB', 'T2_XX_SiteC']
_BLOCK_LOCATIONS = {}

BLOCKS_PER_DATASET = 2
FILES_PER_BLOCK = 5
FILES_PER_DATASET = BLOCKS_PER_DATASET * FILES_PER_BLOCK

MOCK_DATA = {}


class MockRucioApi(object):
    """
    Version of WMCore/Services/Rucio intended to be used with mock or unittest.mock
    """

    def __init__(self, acct, hostUrl=None, authUrl=None, configDict=None):
        print("Using MockRucioApi: acct={}, url={}, authUrl={}".format(acct, hostUrl, authUrl))
        configDict = configDict or {}
        self.dataBlocks = DataBlockGenerator()
        self.subRequests = {}

    def sitesByBlock(self, block):
        """
        Centralize the algorithm to decide where a block is based on the hash name
        :param block: the name of the block
        :return: a fake list of sites where the data is
        """
        if hash(block) % 3 == 0:
            sites = ['T2_XX_SiteA']
        elif hash(block) % 3 == 1:
            sites = ['T2_XX_SiteA', 'T2_XX_SiteB']
        else:
            sites = ['T2_XX_SiteA', 'T2_XX_SiteB', 'T2_XX_SiteC']
        return sites

    # TODO: not sure this wrapper is actually needed
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
                signature = '%s:%s' % (item, sorted(kwargs.items()))
            else:
                signature = item

            try:
                if MOCK_DATA[self.url][signature] == 'Raises HTTPError':
                    raise HTTPError
                else:
                    return MOCK_DATA[self.url][signature]
            except KeyError:
                raise KeyError("Rucio mock API could not return data for method %s, args=%s, and kwargs=%s (URL %s)." %
                               (item, args, kwargs, self.url))

        return genericLookup

    def getDataLockedAndAvailable(self, **kwargs):
        """
        Mock the method to discover where data is locked and available.
        Note that, by default, it will not return any Tape RSEs.
        :return: a unique list of RSEs
        """
        if 'name' not in kwargs:
            raise WMRucioException("A DID name must be provided to the getBlockLockedAndAvailable API")
        if self.isContainer(kwargs['name']):
            # then resolve it at container level and all its blocks
            return self.getContainerLockedAndAvailable(**kwargs)

        if 'grouping' in kwargs:
            # long strings seem not to be working, like ALL / DATASET. Make it short!
            kwargs['grouping'] = kwargs['grouping'][0]
        # TODO: either grouping or returnTape should change this response...
        returnTape = kwargs.pop("returnTape", False)

        rses = set()
        if kwargs['name'].split('#')[0] == PILEUP_DATASET:
            # Pileup is at a single site
            sites = ['T2_XX_SiteC']
            _BLOCK_LOCATIONS[kwargs['name']] = sites
        else:
            sites = self.sitesByBlock(block=kwargs['name'])
            _BLOCK_LOCATIONS[kwargs['name']] = sites
        rses.update(sites)
        return list(rses)


    def getContainerLockedAndAvailable(self, **kwargs):
        """
        Mock the method to discover where container data is locked and available.
        Note that, by default, it will not return any Tape RSEs.
        :return: a unique list of RSEs
        """
        if 'name' not in kwargs:
            raise WMRucioException("A DID name must be provided to the getContainerLockedAndAvailable API")
        kwargs.setdefault("scope", "cms")
        if 'grouping' in kwargs:
            # long strings seem not to be working, like ALL / DATASET. Make it short!
            kwargs['grouping'] = kwargs['grouping'][0]
        # TODO: either grouping or returnTape should change this response...
        returnTape = kwargs.pop("returnTape", False)

        if kwargs['name'] == PILEUP_DATASET:
            return ['T2_XX_SiteA', 'T2_XX_SiteB', 'T2_XX_SiteC']
        try:
            DBS3Reader(PROD_DBS).checkDatasetPath(kwargs['name'])
            blocks = DBS3Reader(PROD_DBS).dbs.listBlocks(dataset=kwargs['name'])
            singleBlock = blocks[0]['block_name']
            return self.sitesByBlock(singleBlock)
        except DBSReaderError:
            return []

    def isContainer(self, didName, scope='cms'):
        """
        Checks whether the DID name corresponds to a container type or not.
        :param didName: string with the DID name
        :param scope: string containing the Rucio scope (defaults to 'cms')
        :return: True if the DID is a container, else False
        """
        # TODO: figure use cases where we need to raise this exception
        if didName == "a bad DID name yet to be defined":
            msg = "Data identifier not found in MockRucio: {}".format(didName)
            raise WMRucioDIDNotFoundException(msg)
        return "#" not in didName
