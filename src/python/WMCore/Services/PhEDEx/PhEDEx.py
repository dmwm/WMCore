import urllib
import logging
import os
import pwd

from WMCore.Services.Service import Service
from WMCore.Wrappers import JsonWrapper

class PhEDEx(Service):

    """
    API for dealing with retrieving information from PhEDEx DataService
    
    according to documentation
    http://cmsweb.cern.ch/phedex/datasvc/doc
    """

    def __init__(self, dict = {}, responseType = "json", secure = False):
        """
        responseType will be either xml or json
        """
        self.responseType = responseType.lower()

        dict["timeout"] = 300

        if not dict.has_key('endpoint'):
            dict['endpoint'] = "%scmsweb.cern.ch/phedex/datasvc/%s/prod/" % \
                                ((secure and "https://" or "http://"),
                                 self.responseType)

        if dict.has_key('cachepath'):
            pass
        elif os.getenv('CMS_PHEDEX_CACHE_DIR'):
            dict['cachepath'] = os.getenv('CMS_PHEDEX_CACHE_DIR') + '/.cms_phedexcache'
        elif os.getenv('HOME'):
            dict['cachepath'] = os.getenv('HOME') + '/.cms_phedexcache'
        else:
            dict['cachepath'] = '/tmp/phedex_' + pwd.getpwuid(os.getuid())[0]
        if not os.path.isdir(dict['cachepath']):
            os.makedirs(dict['cachepath'])
        if 'logger' not in dict.keys():
            logging.basicConfig(level = logging.DEBUG,
                    format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt = '%m-%d %H:%M',
                    filename = dict['cachepath'] + '/phedexdbjsonparser.log',
                    filemode = 'w')
            dict['logger'] = logging.getLogger('PhEDExParser')

        Service.__init__(self, dict)

    def _getResult(self, callname, clearCache = True,
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
        try:
            f = self.refreshCache(file, callname, args, verb = verb)
            result = f.read()
            f.close()

        except IOError, ex:
            raise RuntimeError("URL not available: %s" % callname)

        if self.responseType == "json":
            return JsonWrapper.loads(result)

        return result

    def injectBlocks(self, node, xmlData, verbose = 0, strict = 1):

        """
        _injectBlocksToPhedex_

        xmlData = XMLDrop.makePhEDExDrop(dbsUrl, datasetPath, *blockNames)

        node: node name for injection
        verbose: 1 for being verbose, 0 for not
        strict: throw an error if it can't insert the data exactly as
                requested. Otherwise simply return the statistics. The
                default is to be strict, 1, you can turn it off with 0.
        """

        callname = 'inject'
        args = {}

        args['node'] = node
        args['data'] = xmlData
        args['verbose'] = verbose
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

        The following cases are handled:
          o Input is a block and subscription is a dataset
          o Input is a block and subscription is a block
          o Input is a dataset and subscription is a dataset

        Not supported:
          o Input is a dataset but only block subscriptions exist
        """
        from collections import defaultdict
        result = defaultdict(set)

        # bug in phedex api means we can't query multiple datasets/blocks
        # remove looping construct when fix deployed
        for item in dataItems:

            # TODO: Bug in which this option causes no results to be returned
            # uncomment when https://savannah.cern.ch/bugs/index.php?64617 deployed
            # kwargs['suspended'] = 'n' # require subscription to be active

            # First query for a dataset level subscription (most common)
            # then if its a block, query for a block level subscription
            kwargs['dataset'], kwargs['block'] = [item.split('#')[0]], []
            response = self.subscriptions(**kwargs)['phedex']

            # iterate over response as can't jump to specific datasets
            for dset in response['dataset']:
                if dset['subscription']:
                    # dataset level subscription
                    if dset['name'] == item.split('#')[0]:
                        nodes = [x['node'] for x in dset['subscription']
                                 if x['suspended'] == 'n']
                        result[item].update(nodes)
                        break

            #if we have a block we must check for block level subscription also
            # combine with original query when can give both dataset and block
            if item.find('#') > -1:
                kwargs['dataset'], kwargs['block'] = [], [item]
                response = self.subscriptions(**kwargs)['phedex']
                for dset in response['dataset']:
                    for block in dset['block']:
                        if block['name'] == item:
                            nodes = [x['node'] for x in block['subscription']
                                     if x['suspended'] == 'n']
                            result[item].update(nodes)
                            break
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

# TODO: find the better way to handle emulation:
# hacky code: swap the namespace if emulator config is set 
from WMQuality.Emulators import emulatorSwitch
if emulatorSwitch("PhEDEx"):
    from WMQuality.Emulators.PhEDExClient.PhEDEx import PhEDEx
    
