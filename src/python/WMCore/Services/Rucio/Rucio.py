#!/usr/bin/env python
# coding=utf-8
"""
Rucio Service class developed on top of the native Rucio Client
APIs providing custom output and handling as necessary for the
CMS Workload Management system (and migration from PhEDEx).
"""
from __future__ import division, print_function, absolute_import

import logging

from rucio.client import Client
from rucio.common.exception import AccountNotFound, DataIdentifierNotFound,\
    AccessDenied

from WMCore.WMException import WMException


class WMRucioException(WMException):
    """
    _WMRucioException_
    Generic WMCore exception for Rucio
    """
    pass


class Rucio(object):
    """
    Service class providing additional Rucio functionality on top of the
    Rucio client APIs.
    """

    def __init__(self, acct, hostUrl=None, authUrl=None, configDict=None):
        """
        Constructs a Rucio object with the Client object embedded.
        In order to instantiate a Rucio Client object, it assumes the host has
        a proper rucio configuration file, where the default host and authentication
        host URL come from, as well as the X509 certificate information.
        :param acct: rucio account to be used
        :param hostUrl: defaults to the rucio config one
        :param authUrl: defaults to the rucio config one
        :param configDict: dictionary with extra parameters
        """
        configDict = configDict or {}
        params = configDict.copy()
        params.setdefault('ca_cert', None)
        params.setdefault('auth_type', None)
        params.setdefault('creds', None)
        params.setdefault('timeout', 600)
        params.setdefault('user_agent', 'wmcore-client')

        self.logger = params.get("logger", logging.getLogger())
        # yield output compatible with the PhEDEx service class
        self.phedexCompat = params.get("phedexCompatible", True)

        msg = "WMCore Rucio initialization with acct: %s, host: %s, auth: %s" % (acct, hostUrl, authUrl)
        msg += " and these extra parameters: %s" % params
        self.logger.info(msg)
        self.cli = Client(rucio_host=hostUrl, auth_host=authUrl, account=acct,
                          ca_cert=params['ca_cert'], auth_type=params['auth_type'],
                          creds=params['creds'], timeout=params['timeout'],
                          user_agent=params['user_agent'])
        clientParams = {}
        for k in ("host", "auth_host", "auth_type", "account", "user_agent",
                  "ca_cert", "creds", "timeout", "request_retries"):
            clientParams[k] = getattr(self.cli, k)
        self.logger.info("Rucio client initialization with: %s", clientParams)

    def pingServer(self):
        """
        _pingServer_

        Ping the rucio server to see whether it's alive
        :return: a dict with the server version
        """
        return self.cli.ping()

    def whoAmI(self):
        """
        _whoAmI_

        Get information about account whose token is used
        :return: a dict with the account information. None in case of failure
        """
        return self.cli.whoami()

    def getAccount(self, acct):
        """
        _getAccount_

        Gets information about a specific account
        :return: a dict with the account information. None in case of failure
        """
        res = None
        try:
            res = self.cli.get_account(acct)
        except (AccountNotFound, AccessDenied) as ex:
            self.logger.error("Failed to get account information from Rucio. Error: %s", str(ex))
        return res

    def getAccountUsage(self, acct, rse=None):
        """
        _getAccountUsage_

        Provided an account name, gets the storage usage for it against
        a given RSE (or all RSEs)
        :param acct: a string with the rucio account name
        :param rse: an optional string with the RSE name
        :return: a list of dictionaries with the account usage information.
          None in case of failure
        """
        res = None
        try:
            res = list(self.cli.get_account_usage(acct, rse=rse))
        except (AccountNotFound, AccessDenied) as ex:
            self.logger.error("Failed to get account usage information from Rucio. Error: %s", str(ex))
        return res

    def getBlocksInContainer(self, container, scope='cms'):
        """
        _getBlocksInContainer_

        Provided a Rucio container - CMS dataset - retrieve all blocks in it.
        :param container: a CMS dataset string
        :param scope: string containing the Rucio scope (defaults to 'cms')
        :return: a list of block names
        """
        blockNames = []
        try:
            response = self.cli.get_did(scope=scope, name=container)
        except DataIdentifierNotFound:
            self.logger.warning("Cannot find a data identifier for: %s", container)
            return blockNames

        if response['type'].upper() != 'CONTAINER':
            # input container wasn't really a container
            return blockNames

        response = self.cli.list_content(scope=scope, name=container)
        for item in response:
            if item['type'].upper() == 'DATASET':
                blockNames.append(item['name'])

        return blockNames

    def getReplicaInfoForBlocks(self, **kwargs):
        """
        _getReplicaInfoForBlocks_

        Get block replica information.
        It mimics the same API available in the PhEDEx Service module.

        kwargs originally available for PhEDEx are:
        - dataset       dataset name, can be multiple (*)
        - block         block name, can be multiple (*)
        - node          node name, can be multiple (*)
        - se            storage element name, can be multiple (*)
        - update_since  unix timestamp, only return replicas updated since this time
        - create_since  unix timestamp, only return replicas created since this time
        - complete      y or n, whether or not to require complete or incomplete blocks.
                        Default is to return either
        - subscribed    y or n, filter for subscription. default is to return either.
        - custodial     y or n. filter for custodial responsibility.
                        Default is to return either.
        - group         group name. Default is to return replicas for any group.

        kwargs supported by Rucio are:
        - dids             The list of data identifiers (DIDs) like : [{'scope': <scope1>, 'name': <name1>},
                           {'scope': <scope2>, 'name': <name2>}, ...]
        - schemes          A list of schemes to filter the replicas. (e.g. file, http, ...)
        - unavailable      Also include unavailable replicas in the list. Default to False
        - metalink         False (default) retrieves as JSON, True retrieves as metalink4+xml.
        - rse_expression   The RSE expression to restrict replicas on a set of RSEs.
        - client_location  Client location dictionary for PFN modification {'ip', 'fqdn', 'site'}
        - sort             Sort the replicas:
                           geoip - based on src/dst IP topographical distance
                           closeness - based on src/dst closeness
                           dynamic - Rucio Dynamic Smart Sort (tm)
        - domain           Define the domain. None is fallback to 'wan', otherwise 'wan', 'lan', or 'all'
        - resolve_archives When set to True, find archives which contain the replicas.
        - resolve_parents  When set to True, find all parent datasets which contain the replicas.

        :kwargs: either a dataset or a block name has to be provided. Not both!
        :return: a list of dictionaries with replica information; or a dictionary
        compatible with PhEDEx.
        """
        kwargs.setdefault("scope", "cms")
        kwargs.setdefault("deep", False)  # lookup at the file level, probably not needed...

        blockNames = []
        result = []

        if isinstance(kwargs.get('block', None), (list, set)):
            blockNames = kwargs['block']
        elif 'block' in kwargs:
            blockNames = [kwargs['block']]

        # FIXME: make bulk requests once https://github.com/rucio/rucio/issues/2459 gets fixed
        if isinstance(kwargs.get('dataset', None), (list, set)):
            for datasetName in kwargs['dataset']:
                blockNames.extend(self.getBlocksInContainer(datasetName, scope=kwargs['scope']))
        elif 'dataset' in kwargs:
            blockNames.extend(self.getBlocksInContainer(kwargs['dataset'], scope=kwargs['scope']))

        for blockName in blockNames:
            replicas = []
            response = self.cli.list_dataset_replicas(kwargs['scope'], blockName,
                                                      deep=kwargs['deep'])
            for item in response:
                # same as complete='y' used for PhEDEx (which is always set within WMCore)
                if item['state'].upper() == 'AVAILABLE':
                    replicas.append(item['rse'])
            result.append({'name': blockName, 'replica': list(set(replicas))})

        if self.phedexCompat:
            # convert plain node list to list of nodes dict
            for block in result:
                replicas = []
                for node in block['replica']:
                    replicas.append({'node': node})
                block['replica'] = replicas
            result = {'phedex': {'block': result}}

        return result

    def getPFN(self, nodes=None, lfns=None, destination=None, protocol='srmv2', custodial='n'):
        """
        Copy of the method from the PhEDEx service class
        Used by CRABServer
        """
        raise NotImplementedError("Apparently not available in the Rucio client as well")
