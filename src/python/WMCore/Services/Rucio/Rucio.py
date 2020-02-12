#!/usr/bin/env python
# coding=utf-8
"""
Rucio Service class developed on top of the native Rucio Client
APIs providing custom output and handling as necessary for the
CMS Workload Management system (and migration from PhEDEx).
"""
from __future__ import division, print_function, absolute_import

import logging
from copy import deepcopy
from rucio.client import Client
from rucio.common.exception import (AccountNotFound, DataIdentifierNotFound, AccessDenied,
                                    DataIdentifierAlreadyExists, DuplicateContent,
                                    UnsupportedOperation, FileAlreadyExists, RuleNotFound)
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

    Just a reminder, we have different naming convention between Rucio
    and CMS, where:
     * CMS dataset corresponds to a Rucio container
     * CMS block corresponds to a Rucio dataset
     * CMS file corresponds to a Rucio file
    We will try to use Container -> CMS dataset, block -> CMS block.
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
        self.logger = configDict.pop("logger", logging.getLogger())

        self.rucioParams = deepcopy(configDict)
        self.rucioParams.setdefault('account', acct)
        self.rucioParams.setdefault('rucio_host', hostUrl)
        self.rucioParams.setdefault('auth_host', authUrl)
        self.rucioParams.setdefault('ca_cert', None)
        self.rucioParams.setdefault('auth_type', None)
        self.rucioParams.setdefault('creds', None)
        self.rucioParams.setdefault('timeout', 600)
        self.rucioParams.setdefault('user_agent', 'wmcore-client')

        # yield output compatible with the PhEDEx service class
        self.phedexCompat = self.rucioParams.get("phedexCompatible", True)

        msg = "WMCore Rucio initialization with acct: %s, host: %s, auth: %s" % (acct, hostUrl, authUrl)
        msg += " and these extra parameters: %s" % self.rucioParams
        self.logger.info(msg)
        self.cli = Client(rucio_host=hostUrl, auth_host=authUrl, account=acct,
                          ca_cert=self.rucioParams['ca_cert'], auth_type=self.rucioParams['auth_type'],
                          creds=self.rucioParams['creds'], timeout=self.rucioParams['timeout'],
                          user_agent=self.rucioParams['user_agent'])
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

    def createContainer(self, name, scope='cms', **kwargs):
        """
        _createContainer_

        Create a CMS dataset (Rucio container) in a given scope.
        :param name: string with the container name
        :param scope: optional string with the scope name
        :param kwargs:  supported keyword arguments (from the Rucio CLI API documentation):
           * statuses:  Dictionary with statuses, e.g.g {"monotonic":True}.
           * meta:      Meta-data associated with the data identifier is represented using key/value
                        pairs in a dictionary.
           * rules:     Replication rules associated with the data identifier. A list of dictionaries,
                        e.g., [{"copies": 2, "rse_expression": "TIERS1"}, ].
           * lifetime:  DID's lifetime (in seconds).
        :return: a boolean to represent whether it succeeded or not
        """
        response = False
        try:
            # add_container(scope, name, statuses=None, meta=None, rules=None, lifetime=None)
            response = self.cli.add_container(scope, name, **kwargs)
        except DataIdentifierAlreadyExists:
            self.logger.debug("Container name already exists in Rucio: %s", name)
            response = True
        except Exception as ex:
            self.logger.error("Exception creating container: %s. Error: %s", name, str(ex))
        return response

    def createBlock(self, name, scope='cms', attach=True, **kwargs):
        """
        _createBlock_

        Create a CMS block (Rucio dataset) in a given scope. It also associates
        the block to its container
        :param name: string with the block name
        :param scope: optional string with the scope name
        :param attach: boolean whether to attack this block to its container or not
        :param kwargs:  supported keyword arguments (from the Rucio CLI API documentation):
           * statuses:  Dictionary with statuses, e.g. {"monotonic":True}.
           * lifetime:  DID's lifetime (in seconds).
           * files:     The content.
           * rse:       The RSE name when registering replicas.
           * meta:      Meta-data associated with the data identifier. Represented
                        using key/value pairs in a dictionary.
           * rules:     replication rules associated with the data identifier. A list of
                        dictionaries, e.g., [{"copies": 2, "rse_expression": "TIERS1"}, ].
        :return: a boolean with the outcome of the operation

        NOTE: we will very likely define a rule saying: keep this block at the location it's
              being produced
        """
        response = False
        try:
            # add_dataset(scope, name, statuses=None, meta=None, rules=None, lifetime=None, files=None, rse=None)
            response = self.cli.add_dataset(scope, name, **kwargs)
        except DataIdentifierAlreadyExists:
            self.logger.debug("Block name already exists in Rucio: %s", name)
            response = True
        except Exception as ex:
            self.logger.error("Exception creating block: %s. Error: %s", name, str(ex))

        # then attach this block recently created to its container
        if response and attach:
            container = name.split('#')[0]
            response = self.attachDIDs(kwargs.get('rse'), container, name, scope)
        return response

    def attachDIDs(self, rse, superDID, dids, scope='cms'):
        """
        _attachDIDs_

        Create a list of files - in bulk - to a RSE. Then attach them to the block name.
        :param rse: string with the RSE name
        :param superDID: upper structure level (can be a container or block. If attaching blocks,
             then it's a container name; if attaching files, then it's a block name)
        :param dids: either a string or a list of data identifiers (can be block or files)
        :param scope: string with the scope name
        :return: a boolean to represent whether it succeeded or not
        """
        if not isinstance(dids, list):
            dids = [dids]
        dids = [{'scope': scope, 'name': did} for did in dids]

        response = False
        try:
            response = self.cli.attach_dids(scope, superDID, dids=dids, rse=rse)
        except DuplicateContent:
            self.logger.warning("Dids: %s already attached to: %s", dids, superDID)
            response = True
        except FileAlreadyExists:
            self.logger.warning("Failed to attach files already existent on block: %s", superDID)
            response = True
        except DataIdentifierNotFound:
            self.logger.error("Failed to attach dids: %s. Parent DID %s does not exist.", dids, superDID)
        except Exception as ex:
            self.logger.error("Exception attaching dids: %s to: %s. Error: %s",
                              dids, superDID, str(ex))
        return response

    def createReplicas(self, rse, files, block, scope='cms', ignoreAvailability=True):
        """
        _createReplicas_

        Create a list of files - in bulk - to a RSE. Then attach them to the block name.
        :param rse: string with the RSE name
        :param files: list of dictionaries with the file names and some of its meta data.
            E.g.: {'name': lfn, 'bytes': size, 'scope': scope, 'adler32': checksum, 'state': 'A'}
            State 'A' means that the replica is available
        :param block: string with the block name
        :param scope: string with the scope name
        :param ignore_availability: boolean to ignore the RSE blacklisting
        :return: a boolean to represent whether it succeeded or not
        """
        if isinstance(files, dict):
            files = [files]
        for item in files:
            item['scope'] = scope

        # TODO: test to make sure 'state' is a valid argument
        response = False
        try:
            # add_replicas(rse, files, ignore_availability=True)
            response = self.cli.add_replicas(rse, files, ignoreAvailability)
        except Exception as ex:
            self.logger.error("Failed to add replicas for: %s and block: %s. Error: %s", files, block, str(ex))

        if response:
            files = [item['name'] for item in files]
            response = self.attachDIDs(rse, block, files, scope)

        return response

    def closeBlockContainer(self, name, scope='cms'):
        """
        _closeBlockContainer_

        Method to close a block or container, such that it doesn't get any more
        blocks and/or files inserted.
        :param name: data identifier (either a block or a container name)
        :param scope: string with the scope name
        :return: a boolean to represent whether it succeeded or not
        """
        response = False
        try:
            response = self.cli.close(scope, name)
        except UnsupportedOperation:
            self.logger.warning("Container/block has been closed already: %s", name)
            response = True
        except DataIdentifierNotFound:
            self.logger.error("Failed to close DID: %s; it does not exist.", name)
        except Exception as ex:
            self.logger.error("Exception closing container/block: %s. Error: %s", name, str(ex))
        return response

    def createReplicationRule(self, names, rseExpression, scope='cms', copies=1, **kwargs):
        """
        _createReplicationRule_

        Creates a replication rule against a list of data identifiers.
        :param names: either a string with a did or a list of dids of the same type
          (either a block or a container name)
        :param rseExpression: boolean string expression to give the list of RSEs.
           Full documentation on: https://rucio.readthedocs.io/en/latest/rse_expressions.html
           E.g.: "tier=2&US=true"
        :param scope: string with the scope name
        :param kwargs:  supported keyword arguments (from the Rucio CLI API documentation):
           * weight:    If the weighting option of the replication rule is used,
             the choice of RSEs takes their weight into account.
           * lifetime:  The lifetime of the replication rules (in seconds).
           * grouping:  Decides how the replication is going to happen; where:
                ALL: All files will be replicated to the same RSE.
                DATASET: All files in the same block will be replicated to the same RSE.
                NONE: Files will be completely spread over all allowed RSEs without any
                grouping considerations at all.
                E.g.: ALL grouping for 3 blocks against 3 RSEs. All 3 blocks go to one of the RSEs
                      DATASET grouping for 3 blocks against 3 RSEs, each block gets fully replicated
                      to one of those random RSEs
           * account:   The account owning the rule.
           * locked:    If the rule is locked, it cannot be deleted.
           * source_replica_expression: RSE Expression for RSEs to be considered for source replicas.
           * activity:  Transfer Activity to be passed to FTS.
           * notify:    Notification setting for the rule (Y, N, C).
           * purge_replicas: When the rule gets deleted purge the associated replicas immediately.
           * ignore_availability: Option to ignore the availability of RSEs.
           * ask_approval: Ask for approval of this replication rule.
           * asynchronous: Create rule asynchronously by judge-injector.
           * priority:  Priority of the transfers.
           * comment:   Comment about the rule.
           * meta:      Metadata, as dictionary.
        :return: it returns either an empty list or a list with a string id for the rule created
        """
        kwargs.setdefault('grouping', 'ALL')
        kwargs.setdefault('account', self.rucioParams.get('account'))
        kwargs.setdefault('locked', False)
        kwargs.setdefault('notify', 'N')
        kwargs.setdefault('purge_replicas', False)
        kwargs.setdefault('ignore_availability', False)
        kwargs.setdefault('ask_approval', False)
        kwargs.setdefault('asynchronous', False)
        kwargs.setdefault('priority', 3)

        if not isinstance(names, list):
            names = [names]
        dids = [{'scope': scope, 'name': did} for did in names]

        response = []
        try:
            response = self.cli.add_replication_rule(dids, copies, rseExpression, **kwargs)
        except Exception as ex:
            self.logger.error("Exception creating replica for data: %s. Error: %s", names, str(ex))
        return response

    def listContent(self, name, scope='cms'):
        """
        _listContent_

        List the content of the data identifier, returning some very basic information.
        :param name: data identifier (either a block or a container name)
        :param scope: string with the scope name
        :return: a list with dictionary items
        """
        res = []
        try:
            res = self.cli.list_content(scope, name)
        except Exception as ex:
            self.logger.error("Exception listing content of: %s. Error: %s", name, str(ex))
        return list(res)

    def listDataRules(self, name, scope='cms'):
        """
        _listDataRules_

        List all rules associated to the data identifier provided.
        :param name: data identifier (either a block or a container name)
        :param scope: string with the scope name
        :return: a list with dictionary items
        """
        res = []
        try:
            res = self.cli.list_did_rules(scope, name)
        except Exception as ex:
            self.logger.error("Exception listing rules for data: %s. Error: %s", name, str(ex))
        return list(res)

    def getRule(self, ruleId, estimatedTtc=False):
        """
        _getRule_

        Retrieve rule information for a given rule id
        :param ruleId: string with the rule id
        :param estimatedTtc: bool, if rule_info should return ttc information
        :return: a dictionary with the rule data (or empty if no rule can be found)
        """
        res = {}
        try:
            res = self.cli.get_replication_rule(ruleId, estimate_ttc=estimatedTtc)
        except RuleNotFound:
            self.logger.error("Cannot find any information for rule id: %s", ruleId)
        except Exception as ex:
            self.logger.error("Exception getting rule id: %s. Error: %s", ruleId, str(ex))
        return res
