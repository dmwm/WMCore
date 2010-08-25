import urllib
import logging
import os
import pwd

# this is temporary library until ProdCommon is ported to WMCore 
#from ProdCommon.DataMgmt.PhEDEx.DropMaker import DropMaker
from WMCore.Services.PhEDEx import PhEDExXMLDrop
from WMCore.Services.Service import Service

try:
    # Python 2.6
    import json
except ImportError:
    # Prior to 2.6 requires simplejson
    import simplejson as json

class PhEDEx(Service):

    """
    API for dealing with retrieving information from PhEDEx DataService
    """

    def __init__(self, dict = {}, responseType = "json", secure = False):
        """
        responseType will be either xml or json
        """
        self.responseType = responseType.lower()

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

    def _getResult(self, callname, file = 'result', clearCache = True, 
                   args = None, verb="GET"):
        """
        _getResult_

        retrieve JSON/XML formatted information given the service name and the
        argument dictionaries

        TODO: Probably want to move this up into Service
        """
        result = ''
        if clearCache:
            self.clearCache(file, args)
        try:
            orignalVerb = self["method"]
            
            self["method"] = verb     
            f = self.refreshCache(file, callname, args)
            result = f.read()
            f.close()
            self["method"] = orignalVerb
            
        except IOError, ex:
            raise RuntimeError("URL not available: %s" % callname)

        if self.responseType == "json":
            decoder = json.JSONDecoder()
            return decoder.decode(result)

        return result

    def injectBlocks(self, dbsUrl, node, datasetPath = None, 
                     verbose = 0, strict = 1, *blockNames):

        """
        _injectBlocksToPhedex_
    
        dbsUrl is global dbs url
        node: node name for injection 
        verbose: 1 for being verbose, 0 for not
        strict: throw an error if it can't insert the data exactly as
                requested. Otherwise simply return the statistics. The
                default is to be strict, 1, you can turn it off with 0.
        """

        callname = 'inject'
        args = {}

        args['node'] = node

        xml = PhEDExXMLDrop.makePhEDExDrop(dbsUrl, datasetPath, *blockNames)
        
        args['data'] = xml
        args['verbose'] = verbose
        args['strict'] = strict

        return self._getResult(callname, args = args, verb="POST")


    def subscribe(self, dbsUrl, subscription):
        """
        _subscribe_
        
        Subscription is PhEDEX subscription structure
        """

        callname = 'subscribe'
        args = {}

        args['node'] = []
        for node in subscription.nodes:
            args['node'].append(node)

        xml = PhEDExXMLDrop.makePhEDExXMLForDatasets(dbsUrl, subscription.getDatasetPaths())

        args['data'] = xml
        args['level'] = subscription.level
        args['priority'] = subscription.priority
        args['move'] = subscription.move
        args['static'] = subscription.static
        args['custodial'] = subscription.custodial
        args['group'] = subscription.group
        args['request_only'] = subscription.request_only

        return self._getResult(callname, args = args)


    def getReplicaInfoForBlocks(self, **kwargs):
        """
        _blockreplicas_
        
        Get replicas for given blocks
        kwargs are options passed through to phedex
        """

        callname = 'blockreplicas'
        return self._getResult(callname, args = kwargs)


    def subscriptions(self, **kwargs):
        """
        _subscriptions_
        
        Get subscriptios for blocks and datasets
        kwargs are options passed through to phedex
        """

        callname = 'subscriptions'
        return self._getResult(callname, args = kwargs)
