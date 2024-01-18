#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Version of WMCore/Services/Rucio intended to be used with mock or unittest.mock
"""
from __future__ import print_function, division
# from builtins import object # avoid importing this, it beraks things

import json
import logging
import os
import hashlib

from WMCore.Services.DBS.DBS3Reader import DBS3Reader, DBSReaderError
from WMCore.Services.Rucio.Rucio import WMRucioException, WMRucioDIDNotFoundException
from WMCore.WMBase import getTestBase
from WMQuality.Emulators.DataBlockGenerator.DataBlockGenerator import DataBlockGenerator

from Utils.PythonVersion import PY2, PY3
from Utils.Utilities import encodeUnicodeToBytesConditional

PROD_DBS = 'https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader'

NOT_EXIST_DATASET = 'thisdoesntexist'
# PILEUP_DATASET = '/HighPileUp/Run2011A-v1/RAW'
PILEUP_DATASET = '/GammaGammaToEE_Elastic_Pt15_8TeV-lpair/Summer12-START53_V7C-v1/GEN-SIM'

SITES = ['T2_XX_SiteA', 'T2_XX_SiteB', 'T2_XX_SiteC']
_BLOCK_LOCATIONS = {}

BLOCKS_PER_DATASET = 2
FILES_PER_BLOCK = 5
FILES_PER_DATASET = BLOCKS_PER_DATASET * FILES_PER_BLOCK

mockFile = os.path.join(getTestBase(), '..', 'data', 'Mock', 'RucioMockData.json')
try:
    with open(mockFile, 'r') as jsonObj:
        MOCK_DATA = json.load(jsonObj)
except IOError:
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
        self.rucioParams = {}

    def sitesByBlock(self, block):
        """
        Centralize the algorithm to decide where a block is based on the hash name
        :param block: the name of the block
        :return: a fake list of sites where the data is
        """
        logging.info("%s: Calling mock sitesByBlock", self.__class__.__name__)
        block = encodeUnicodeToBytesConditional(block, condition=PY3)
        # this algorithm gives same results in both python versions
        blockHash = int(hashlib.sha1(block).hexdigest()[:8], 16)
        if blockHash % 3 == 0:
            sites = ['T2_XX_SiteA']
        elif blockHash % 3 == 1:
            sites = ['T2_XX_SiteA', 'T2_XX_SiteB']
        else:
            sites = ['T2_XX_SiteA', 'T2_XX_SiteB', 'T2_XX_SiteC']
        return sites

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
            logging.info("%s: Calling mock genericLookup", self.__class__.__name__)
            if kwargs:
                for k in kwargs:
                    if isinstance(kwargs[k], (list, tuple)):
                        kwargs[k] = [encodeUnicodeToBytesConditional(x, condition=PY2) for x in kwargs[k]]
                    else:
                        kwargs[k] = encodeUnicodeToBytesConditional(kwargs[k], condition=PY2)
                signature = '%s:%s' % (item, sorted(kwargs.items()))
            else:
                signature = item

            try:
                return MOCK_DATA[signature]
            except KeyError:
                msg = "Rucio mock API failed to find key for signature: {}".format(signature)
                raise KeyError(msg)

        return genericLookup

    def getBlocksInContainer(self, container, scope='cms'):
        """
        Returns list of block names for given container
        """
        cname = self.__class__.__name__
        blockNames = [container + '#123', container + '#456']
        logging.info("%s getBlocksInContainer %s", cname, blockNames)
        return blockNames

    def listDataRules(self, name, **kwargs):
        """
        Emulate listDataRules Rucio API
        :return: list of dictionary records
        """
        cname = self.__class__.__name__
        logging.info("%s listDataRules name=%s kwargs=%s", cname, name, kwargs)
        return [{'rse_expression': 'T1_XX_ABC', 'id': 123, 'state': 'OK'}]

    def createContainer(self, name, scope='cms', **kwargs):
        """
        Create a CMS dataset (Rucio container) in a given scope.
        """
        cname = self.__class__.__name__
        logging.info("%s createContainer for name=%s scope=%s", cname, name, scope)
        return True

    def attachDIDs(self, rse, superDID, portion, scope='cms'):
        """
        Emulate attachDIDs Rucio API
        """
        cname = self.__class__.__name__
        logging.info("%s attachDID rse=%s, suportDID=%s, portion=%s, scope=%s", cname, rse, superDID, portion, scope)
        return True

    def createReplicationRule(self, portion, rseExpression):
        """
        Emulate createReplicationRule Rucio API
        """
        cname = self.__class__.__name__
        logging.info("%s createReplicationRule portion=%s, rseExpression=%s", cname, portion, rseExpression)
        return [rseExpression]

    def updateRule(self, rid, opts):
        """
        Emulate updateRule(rid, opts) Rucio API
        """
        cname = self.__class__.__name__
        logging.info("%s updateRule rid=%s, opts=%s", cname, rid, opts)
        return True

    def getReplicaInfoForBlocks(self, **args):
        """
        Returns a mocked location for data.
        In the same format as the real `getReplicaInfoForBlocks` from the main module.
        """
        logging.info("%s: Calling mock getReplicaInfoForBlocks", self.__class__.__name__)
        result = []
        for blockName in args['block']:
            rses = self.sitesByBlock(block=blockName)
            result.append(dict(name=blockName, replica=rses))
        return result

    def getDataLockedAndAvailable(self, **kwargs):
        """
        Mock the method to discover where data is locked and available.
        Note that, by default, it will not return any Tape RSEs.
        :return: a unique list of RSEs
        """
        logging.info("%s: Calling mock getDataLockedAndAvailable", self.__class__.__name__)
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

    def getPileupLockedAndAvailable(self, container, account, scope="cms"):
        """
        Mock method to resolve where the pileup container (and all its blocks)
        is locked and available.
        """
        logging.info("%s: calling mock getPileupLockedAndAvailable", self.__class__.__name__)
        result = dict()
        if not self.isContainer(container):
            raise WMRucioException("Pileup location needs to be resolved for a container DID type")

        kwargs = dict(name=container, account=account, scope=scope)

        try:
            DBS3Reader(PROD_DBS).checkDatasetPath(kwargs['name'])
            blocks = DBS3Reader(PROD_DBS).listFileBlocks(dataset=kwargs['name'])
            for block in blocks:
                result[block] = self.sitesByBlock(block)
        except DBSReaderError:
            logging.error("%s: Failed to fetch blocks from DBS", self.__class__.__name__)
        return result

    def getContainerLockedAndAvailable(self, **kwargs):
        """
        Mock the method to discover where container data is locked and available.
        Note that, by default, it will not return any Tape RSEs.
        :return: a unique list of RSEs
        """
        logging.info("%s: Calling mock getContainerLockedAndAvailable", self.__class__.__name__)
        if 'name' not in kwargs:
            raise WMRucioException("A DID name must be provided to the getContainerLockedAndAvailable API")
        kwargs.setdefault("scope", "cms")

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
        Mock check for whether a DID name corresponds to a container type or not,
        by simply relying on the naming convention
        :param didName: string with the DID name
        :param scope: string containing the Rucio scope (defaults to 'cms')
        :return: True if the DID is a container, else False
        """
        # TODO: figure use cases where we need to raise this exception
        if didName == "a bad DID name yet to be defined":
            msg = "Data identifier not found in MockRucio: {}".format(didName)
            raise WMRucioDIDNotFoundException(msg)
        return "#" not in didName
