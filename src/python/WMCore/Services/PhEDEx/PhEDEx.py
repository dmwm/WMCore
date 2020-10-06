import json
import logging
from xml.dom.minidom import parseString

from WMCore.Services.PhEDEx import XMLDrop
from WMCore.Services.Service import Service


class PhEDEx(Service):
    """
    API for dealing with retrieving information from PhEDEx DataService

    according to documentation
    https://cmsweb.cern.ch/phedex/datasvc/doc
    """

    def __init__(self, httpDict=None, responseType="json", logger=None,
                 dbsUrl='https://cmsweb.cern.ch/dbs/prod/global/DBSReader'):
        """
        responseType will be either xml or json
        """
        self.dbsUrl = dbsUrl
        httpDict = httpDict or {}
        self.responseType = responseType.lower()

        httpDict['logger'] = logger if logger else logging.getLogger()
        httpDict["timeout"] = 300
        if 'endpoint' not in httpDict:
            httpDict['endpoint'] = "https://cmsweb.cern.ch/phedex/datasvc/%s/prod/" % self.responseType
        httpDict.setdefault('cacheduration', 0)

        Service.__init__(self, httpDict)
        # NOTE: it looks like PhEDEx returns these weird data locations since ever.
        # Why don't we deal with it as close as possible to the PhEDEx service then...
        self.nodeFilter = set(['UNKNOWN', None])

    def _getResult(self, callname, clearCache=False,
                   args=None, verb="POST"):
        """
        _getResult_

        retrieve JSON/XML formatted information given the service name and the
        argument dictionaries

        TODO: Probably want to move this up into Service
        """
        result = ''
        # make base file name from call name.
        ifile = callname.replace("/", "_")
        if clearCache:
            self.clearCache(ifile, args, verb=verb)

        fobj = self.refreshCache(ifile, callname, args, verb=verb)
        result = fobj.read()
        fobj.close()

        if self.responseType == "json":
            return json.loads(result)

        return result

    def injectBlocks(self, node, xmlData, strict=1):

        """
        _injectBlocksToPhedex_

        xmlData = XMLDrop.makePhEDExDrop(self.dbsUrl, datasetPath, *blockNames)

        node: node name for injection
        strict: throw an error if it can't insert the data exactly as
                requested. Otherwise simply return the statistics. The
                default is to be strict, 1, you can turn it off with 0.
        """

        callname = 'inject'
        args = {}

        args['node'] = node
        args['data'] = xmlData
        args['strict'] = strict

        return self._getResult(callname, args=args, verb="POST")

    def subscribe(self, subscription):
        """
        _subscribe_

        xmlData = XMLDrop.makePhEDExXMLForDatasets(dbsUrl, subscription.getDatasetPaths())
        Subscription is PhEDEX subscription structure
        """

        callname = 'subscribe'
        args = {}

        args['node'] = []
        for node in subscription.nodes:
            args['node'].append(node)

        args['comments'] = subscription.comments
        args['level'] = subscription.level
        args['priority'] = subscription.priority
        args['move'] = subscription.move
        args['static'] = subscription.static
        args['custodial'] = subscription.custodial
        args['group'] = subscription.group
        args['request_only'] = subscription.request_only
        if args['level'] == 'dataset':
            xmlData = XMLDrop.makePhEDExXMLForDatasets(self.dbsUrl, list(subscription.datasetPaths))
        else:  # block
            xmlData = XMLDrop.makePhEDExXMLForBlocks(self.dbsUrl, subscription.getDatasetsAndBlocks())
        args['data'] = xmlData

        return self._getResult(callname, args=args, verb="POST")

    def delete(self, deletion):
        """
        _delete_

        Deletion is a PhEDEX deletion structure
        """
        callname = 'delete'
        args = {}

        args['node'] = []
        for node in deletion.nodes:
            args['node'].append(node)

        xmlData = XMLDrop.makePhEDExXMLForBlocks(self.dbsUrl, deletion.getDatasetsAndBlocks())
        args['data'] = xmlData
        args['level'] = deletion.level
        args['rm_subscriptions'] = deletion.subscriptions
        args['comments'] = deletion.comments

        return self._getResult(callname, args=args, verb="POST")

    def updateRequest(self, requestId, decision, nodes):
        """
        _updateRequest_

        Update a request approving/disapproving it.
        """
        if isinstance(nodes, basestring):
            nodes = [nodes]
        args = {}
        args['decision'] = decision.lower()
        args['request'] = requestId
        args['node'] = nodes

        return self._getResult('updaterequest', args=args, verb="POST")

    def getReplicaInfoForBlocks(self, **kwargs):
        """
        _blockreplicas_

        Get replicas for given blocks
        kwargs are options passed through to phedex

        dataset        dataset name, can be multiple (*)
        block          block name, can be multiple (*)
        node           node name, can be multiple (*)
        se             storage element name, can be multiple (*)
        update_since  unix timestamp, only return replicas updated since this
                time
        create_since   unix timestamp, only return replicas created since this
                time
        complete       y or n, whether or not to require complete or incomplete
                blocks. Default is to return either
        subscribed     y or n, filter for subscription. default is to return either.
        custodial      y or n. filter for custodial responsibility.  default is
                to return either.
        group          group name.  default is to return replicas for any group.
        """

        callname = 'blockreplicas'
        return self._getResult(callname, args=kwargs)

    def getReplicaInfoForFiles(self, **args):
        """
        _getReplicaInfoForFiles_

        TODO: this is only used for the unittest for other API's doesn't need to have RUCIO equvalent
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
        return self._getResult("filereplicas", args=args)

    def subscriptions(self, **kwargs):
        """
        _subscriptions_

        Get subscriptios for blocks and datasets
        kwargs are options passed through to phedex

        dataset          dataset name (wildcards)
        block            block name (wildcards)
        node             node name (wildcards)
        se               storage element
        create_since     timestamp. only subscriptions created after.
        request          request number which created the subscription.
        custodial        y or n to filter custodial/non subscriptions.
                           default is null (either)
        group            group name filter
        priority         priority, one of "low", "normal" and "high"
        move             y (move) or n (replica)
        suspended        y or n, default is either
        collapse         y or n, default y. If y, do not show block level
                           subscriptions of a dataset
        """

        callname = 'subscriptions'
        return self._getResult(callname, args=kwargs, verb="GET")

    def getSubscriptionMapping(self, *dataItems, **kwargs):
        """
        Similar basic functionality as self.subscriptions()
        however: dataItems may be a combination of blocks or datasets and
        kwargs is passed to PhEDEx; output is parsed and returned in the form
        { 'dataItem1' : [Node1, Node2] } where dataItem is a block or dataset
        If an error is encountered for a dataItem that item will be missing
        from the returned dictionary.

        The following cases are handled:
          o Input is a block and subscription is a dataset
          o Input is a block and subscription is a block
          o Input is a dataset and subscription is a dataset
          o Input is a dataset but only block subscriptions exist
        """
        from collections import defaultdict
        inputs = defaultdict(set)
        result = defaultdict(set)

        kwargs.setdefault('suspended', 'n')  # active subscriptions by default
        kwargs['collapse'] = 'n'  # queries for block level subscriptions as well

        dataItems = list(set(dataItems))  # force unique items
        datasetsOnly = set()
        # get dict of dataset : (blocks or dataset)
        for item in dataItems:
            if len(item.split('#')) > 1:
                inputs[item.split('#')[0]].add(item)
            else:
                datasetsOnly.add(item)

        # Hard to query all at once in one GET call, POST not cacheable
        # Query each dataset and record relevant dataset or block location
        for dsname, items in inputs.items():
            try:
                # query for all blocks in dataset
                kwargs['block'] = dsname + '#%'
                response = self.subscriptions(**kwargs)['phedex']

                # iterate over response as can't jump to specific datasets
                for dset in response['dataset']:
                    if dset['name'] != dsname:
                        continue
                    if 'subscription' in dset:
                        # dataset level subscription
                        nodes = [x['node'] for x in dset['subscription']
                                 if kwargs['suspended'] == 'either' or \
                                 x['suspended'] == kwargs['suspended']]
                        # update locations for all items in this dataset
                        for item in items:
                            result[item].update(nodes)
                        if dsname in datasetsOnly:
                            result[dsname].update(nodes)

                    # if we have a block we must check for block level subscription also
                    # combine with original query when can give both dataset and block
                    if 'block' in dset:
                        for block in dset['block']:
                            nodes = [x['node'] for x in block['subscription']
                                     if kwargs['suspended'] == 'either' or \
                                     x['suspended'] == kwargs['suspended']]
                            # update locations for this block and/or dataset
                            if dsname in datasetsOnly:
                                result[dsname].update(nodes)
                            if block['name'] in items:
                                result[block['name']].update(nodes)
            except Exception as ex:
                logging.error('Error looking up phedex subscription for %s: %s', dsname, str(ex))
        return result

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
        """
        return self._getResult("nodes", args=None)

    def getPFN(self, nodes=None, lfns=None, destination=None, protocol='srmv2', custodial='n'):
        """
        Get the PFN for an LFN on a node. Return a dict with a tuple of the input as the key
        and the pfn as the value.
        """
        nodes = nodes or []
        lfns = lfns or []

        input_dict = {'node': nodes, 'lfn': lfns, 'protocol': protocol, 'custodial': custodial}
        if destination:
            input_dict['destination'] = destination

        data = self._getResult('lfn2pfn', args=input_dict, verb='GET')
        result_dict = {}

        if self.responseType == "json":
            for mapping in data['phedex']['mapping']:
                key = (mapping['node'], mapping['lfn'])
                result_dict[key] = mapping['pfn']
        else:
            phedex_dom = parseString(data)
            for mapping in phedex_dom.getElementsByTagName("mapping"):
                key = (mapping.getAttribute('node'), mapping.getAttribute('lfn'))
                result_dict[key] = mapping.getAttribute('pfn')

        return result_dict

    def getRequestList(self, **kwargs):
        """
        _getRequestList_

        Get the list of requests in the system according to the given options:

        request *        request id
        type             request type, 'xfer' (default) or 'delete'
        approval         approval state, 'approved', 'disapproved', 'mixed', or 'pending'
        requested_by *   requestor's name
        node *           name of the destination node
                         (show requests in which this node is involved)
        decision         decision at the node, 'approved', 'disapproved' or 'pending'
        group *          user group
        create_since     created since this time
        create_until     created until this time
        decide_since     decided since this time
        decide_until     decided until this time
        dataset *        dataset is part of request, or a block from this dataset
        block *          block is part of request, or part of a dataset in request
        decided_by *     name of person who approved the request
        * could be multiple and/or with wildcard
        ** when both 'block' and 'dataset' are present, they form a logical disjunction (ie. or)
        """
        callname = 'requestlist'
        return self._getResult(callname, args=kwargs, verb="GET")

    def getTransferRequests(self, **kwargs):
        """
        _getTransferRequests_

        Get the detailed information about transfer requests, options are:

        request          request number, may be multiple
        node             name of the destination node, may be multiple
        group            name of the group, may be multiple
        limit            maximal number of records returned
        create_since     created after this time
        approval         approval state: approved, disapproved, pending or mixed
                         default is all, may be multiple
        requested_by *   human name of the requestor, may be multiple
        * requested_by only works with approval option
        ** without any input, the default "create_since" is set to 24 hours ago
        """
        callname = 'transferrequests'
        return self._getResult(callname, args=kwargs, verb="GET")

    def _testNonExistentInEmulator(self):
        # This is a dummy function to use in unittests to make sure the right class is
        # instantiated
        pass

    def getInjectedFiles(self, blockFileDict):
        """
        take dict of the input
        {'block_name1': [file_lfn1, file_lfn2, ....],
         'block_name2': [file_lfn1, file_lfn2, ....],
        }
        and returns
        list of file injected
        """
        injectedFiles = []
        for block in blockFileDict:
            result = self._getResult('data', args={'block': block}, verb='GET')
            for dbs in result['phedex']['dbs']:
                for dataset in dbs['dataset']:
                    blockChunk = dataset['block']
                    for blockInfo in blockChunk:
                        for fileInfo in blockInfo['file']:
                            if fileInfo['lfn'] in blockFileDict[block]:
                                injectedFiles.append(fileInfo['lfn'])
        return injectedFiles

    def getReplicaPhEDExNodesForBlocks(self, **kwargs):
        """
        _blockreplicasPNN_

        Get replicas PNN for given blocks
        kwargs are options passed through to phedex

        dataset        dataset name, can be multiple (*)
        block          block name, can be multiple (*)
        node           node name, can be multiple (*)
        se             storage element name, can be multiple (*)
        update_since  unix timestamp, only return replicas updated since this
                time
        create_since   unix timestamp, only return replicas created since this
                time
        complete       y or n, whether or not to require complete or incomplete
                blocks. Default is to return either
        subscribed     y or n, filter for subscription. default is to return either.
        custodial      y or n. filter for custodial responsibility.  default is
                to return either.
        group          group name.  default is to return replicas for any group.

        Returns a dictionary with se names per block
        """
        callname = 'blockreplicas'
        response = self._getResult(callname, args=kwargs)

        blockNodes = dict()

        blocksInfo = response['phedex']['block']
        if not blocksInfo:
            return {}

        for blockInfo in blocksInfo:
            nodes = set()
            for replica in blockInfo['replica']:
                nodes.add(replica['node'])
            blockNodes[blockInfo['name']] = list(nodes - self.nodeFilter)

        return blockNodes

    def getGroupUsage(self, **kwargs):
        """
        _getGroupUsage_

        Get storage statistics node per group, like data already
        stored and data subscribed
        :param kwargs: accepts the optional parameters, as defined by PhEDEx:
            node    node name, could be multiple
            se      storage element name, could be multiple
            group   group name, could be multiple
        :return: a dictionary if `json` response type is defined, otherwise it's XML
        """
        callname = 'groupusage'
        return self._getResult(callname, clearCache=True, args=kwargs, verb="GET")
