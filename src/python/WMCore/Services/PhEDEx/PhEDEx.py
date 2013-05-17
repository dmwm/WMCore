from xml.dom.minidom import parseString
import logging
from WMCore.Services.Service import Service
from WMCore.Wrappers import JsonWrapper
from WMCore.Services.EmulatorSwitch import emulatorHook

# emulator hook is used to swap the class instance
# when emulator values are set.
# Look WMCore.Services.EmulatorSwitch module for the values
@emulatorHook
class PhEDEx(Service):

    """
    API for dealing with retrieving information from PhEDEx DataService

    according to documentation
    https://cmsweb.cern.ch/phedex/datasvc/doc
    """

    def __init__(self, dict = None, responseType = "json", secure = True):
        """
        responseType will be either xml or json
        """
        if not dict:
            dict = {}
        self.responseType = responseType.lower()

        dict["timeout"] = 300

        if not dict.has_key('endpoint'):
            dict['endpoint'] = "https://cmsweb.cern.ch/phedex/datasvc/%s/prod/" % self.responseType

        dict.setdefault('cacheduration', 0)
        Service.__init__(self, dict)

    def _getResult(self, callname, clearCache = False,
                   args = None, verb="POST"):
        """
        _getResult_

        retrieve JSON/XML formatted information given the service name and the
        argument dictionaries

        TODO: Probably want to move this up into Service
        """
        result = ''
        # make base file name from call name.
        file = callname.replace("/", "_")
        if clearCache:
            self.clearCache(file, args, verb = verb)

        f = self.refreshCache(file, callname, args, verb = verb)
        result = f.read()
        f.close()

        if self.responseType == "json":
            return JsonWrapper.loads(result)

        return result

    def injectBlocks(self, node, xmlData, strict = 1):

        """
        _injectBlocksToPhedex_

        xmlData = XMLDrop.makePhEDExDrop(dbsUrl, datasetPath, *blockNames)

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

        return self._getResult(callname, args = args, verb="POST")

    def subscribe(self, subscription, xmlData):
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

        args['data'] = xmlData
        args['level'] = subscription.level
        args['priority'] = subscription.priority
        args['move'] = subscription.move
        args['static'] = subscription.static
        args['custodial'] = subscription.custodial
        args['group'] = subscription.group
        args['request_only'] = subscription.request_only

        return self._getResult(callname, args = args, verb="POST")


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
        return self._getResult(callname, args = kwargs)

    def getReplicaInfoForFiles(self, **args):
        """
        _getReplicaInfoForFiles_

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
        return self._getResult("filereplicas", args = args)

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
        """

        callname = 'subscriptions'
        return self._getResult(callname, args = kwargs, verb="GET")

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

        kwargs.setdefault('suspended', 'n') # active subscriptions by default

        dataItems = list(set(dataItems)) # force unique items
        datasetsOnly = set()
        # get dict of dataset : (blocks or dataset)
        for item in dataItems:
            if len(item.split('#')) > 1:
                inputs[item.split('#')[0]].add(item)
            else:
                inputs[item.split('#')[0]]
                datasetsOnly.add(item)

        # Hard to query all at once in one GET call, POST not cacheable
        # Query each dataset and record relevant dataset or block location
        for dsname, items in inputs.items():
            try:
                # query for all blocks in dataset
                # returns both dataset and block level subscriptions.
                kwargs['block'] = dsname + '#%'
                response = self.subscriptions(**kwargs)['phedex']

                # iterate over response as can't jump to specific datasets
                for dset in response['dataset']:
                    if dset['name'] != dsname:
                        continue
                    if dset.has_key('subscription'):
                        # dataset level subscription
                        nodes = [x['node'] for x in dset['subscription']
                                 if kwargs['suspended'] == 'either' or \
                                            x['suspended'] == kwargs['suspended']]
                        # update locations for all items in this dataset
                        for item in items:
                            result[item].update(nodes)
                        if dsname in datasetsOnly:
                            result[dsname].update(nodes)

                    #if we have a block we must check for block level subscription also
                    # combine with original query when can give both dataset and block
                    if dset.has_key('block'):
                        for block in dset['block']:
                            nodes = [x['node'] for x in block['subscription']
                                     if kwargs['suspended'] == 'either' or \
                                        x['suspended'] == kwargs['suspended']]
                            # update locations for this block and/or dataset
                            if dsname in datasetsOnly:
                                result[dsname].update(nodes)
                            if block['name'] in items:
                                result[block['name']].update(nodes)
            except Exception, ex:
                logging.error('Error looking up phedex subscription for %s: %s' % (dsname, str(ex)))
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
        return self._getResult("nodes", args = None)


    def getBestNodeName(self, se, nodeNameMap=None):
        """
        _getBestNodeName_

        Convert SE to Name giving back one of the following types:
        Buffer, MSS, and Disk (in order). See 2817
        """
        if nodeNameMap==None:
            nodeNameMap = self.getNodeMap()
        nodeList = nodeNameMap['phedex']['node']
        ret = None
        for node in nodeList:
            if node['se'] == unicode(se):
                if node['kind'] == 'Buffer':
                    return node['name']
                elif node['kind'] == 'MSS':
                    ret = node['name']
                elif node['kind'] == 'Disk' and ret == None:
                    ret = node['name']
        return ret


    def getNodeNames(self, se):
        """
        _getNodeName_

        Convert SE to Name
        """
        names = []
        output = self.getNodeMap()
        nodeList = output['phedex']['node']
        for node in nodeList:
            if node['se'] == se:
                names.append(node['name'])
        return names

    def getNodeSE(self, name):
        """
        _getNodeSE_

        Convert Name to SE
        """
        output = self.getNodeMap()
        nodeList = output['phedex']['node']
        for node in nodeList:
            if node['name'] == name:
                return node['se']
        return None

    def getNodeTFC(self, node):
        data = self._getResult('tfc', args = {'node':node}, verb="GET")
        return data

    def getAuth(self, ability):
        """
        _getAuth_

        Determine whether or not the users has permissions to perform the
        given ability.
        """
        data = self._getResult('auth', args = {'ability':ability}, verb="GET")
        node = data['phedex']['auth'][0].get('node', None)

        if node:
            return True
        else:
            return False

    def getPFN(self, nodes=[], lfns=[], destination=None, protocol='srmv2', custodial='n'):
        """
        Get the PFN for an LFN on a node. Return a dict with a tuple of the input as the key
        and the pfn as the value.
        """
        input_dict = {'node': nodes, 'lfn': lfns, 'protocol': protocol, 'custodial': custodial}
        if destination:
            input_dict['destination'] = destination

        data = self._getResult('lfn2pfn', args = input_dict, verb = 'GET')
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
        return self._getResult(callname, args = kwargs, verb = "GET")

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
        return self._getResult(callname, args = kwargs, verb = "GET")

    def _testNonExistentInEmulator(self):
        # This is a dummy function to use in unittests to make sure the right class is
        # instantiated
        pass
