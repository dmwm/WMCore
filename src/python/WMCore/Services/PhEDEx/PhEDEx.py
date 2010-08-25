import urllib
import logging
import os
import pwd

# this is temporary library until ProdCommon is ported to WMCore 
#from ProdCommon.DataMgmt.PhEDEx.DropMaker import DropMaker
from WMCore.Services.PhEDEx import PhEDExXMLDrop
from WMCore.Services.Service import Service
from WMCore.Services.AuthorisedService import AuthorisedService
# This should be deprecated in preference to simplejson once SiteDB spits out
# correct json
from WMCore.Services.JSONParser.JSONParser import JSONParser

try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json

#TODO: this should move to the AuthorisedService class
class PhEDEx(AuthorisedService):

    """
    API for dealing with retrieving information from PhEDEx DataService
    """

    def __init__(self, dict={}, responseType="xml"):
        """
        responseType will be either xml or json
        """
        self.responseType = responseType.lower()
        
        if not dict.has_key('endpoint'):
            dict['endpoint'] = "https://cmsweb.cern.ch/phedex/datasvc/%s/test/" % \
                                self.responseType
        
        #PhEDEx Service default is using POST 
        self.setdefault("method", 'POST')    

        if os.getenv('CMS_PHEDEX_CACHE_DIR'):
            dict['cachepath'] = os.getenv('CMS_PHEDEX_CACHE_DIR') + '/.cms_phedexcache'
        elif os.getenv('HOME'):
            dict['cachepath'] = os.getenv('HOME') + '/.cms_phedexcache'
        else:
            dict['cachepath'] = '/tmp/phedex_' + pwd.getpwuid(os.getuid())[0]
        if not os.path.isdir(dict['cachepath']):
            os.mkdir(dict['cachepath'])
        if 'logger' not in dict.keys():
            logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=dict['cachepath'] + '/phedexdbjsonparser.log',
                    filemode='w')
            dict['logger'] = logging.getLogger('PhEDExParser')
        
        #TODO if service doesn't need to be authorized, have switch to use Service
        AuthorisedService.__init__(self, dict)

    def _getResult(self, callname, file='result', clearCache=True, args=None):
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
            f = self.refreshCache(file, callname, args)
            result = f.read()
            f.close()
        except IOError, ex:
            raise RuntimeError("URL not available: %s" % callname )

        if self.responseType == "json":
            decoder = json.JSONDecoder()
            return decoder.decode(result)
        
        return result 

    def injectBlocks(self, dbsUrl, node, datasetPath = None, verbose = 0,
                     strict = 1, *blockNames):
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

        print "XML: %s" % xml

        args['data'] = xml
        args['verbose'] = verbose
        args['strict'] = strict
        
        #return self._getResult(callname, args=args)
        return None

    def injectBlocksFromDB(self, dbsUrl, injectionData, nodeName, verbose = 0,
                           strict = 0):
        """
        _injectBlocksFromDB_

        Inject blocks into PhEDEx without querying local DBS.  The injectionData
        parameter must be a dictionary keyed by dataset path.  Each dataset path
        will map to a list of blocks, each block being a dict.  The block dicts
        will have three keys: name, is-open and files.  The files key will be a
        list of dicts, each of which have the following keys: lfn, size and
        checksum.  The following is an example object:

        {"dataset1":
          {"block1": {"is-open": "y", "files":
                        [{"lfn": "lfn1", "size": 10, "checksum": "cksum1234"},
                         {"lfn": "lfn2", "size": 20, "checksum": "cksum4321"}]}}}

        The verbose and strict parameters are passed to the PhEDEx data service.
        A verbose setting of 1 will enable verbose output, a strict setting 1 of
        will cause the data service to throw an error if it can't insert the
        data exactly as requested.
        """
        injectionSpec = PhEDExXMLDrop.XMLInjectionSpec(dbsUrl)

        for datasetPath in injectionData:
            datasetSpec = injectionSpec.getDataset(datasetPath)

            for fileBlockName, fileBlock in injectionData[datasetPath].iteritems():
                blockSpec = datasetSpec.getFileblock(fileBlockName,
                                                     fileBlock["is-open"])

                for file in fileBlock["files"]:
                    blockSpec.addFile(file["lfn"], file["checksum"],
                                      file["size"])

        improv = injectionSpec.save()
        xmlString = improv.makeDOMElement().toprettyxml()

        args = {}
        args["node"] = nodeName
        args["data"] = xmlString
        args["verbose"] = verbose
        args["strict"] = strict
        
        return self._getResult("inject", args = args)

    def getReplicaInfoForBlock(self, blockName):
        """
        _getReplicaInfoForBlock_

        Retrieve replica information from PhEDEx for a particular block.
        """
        args = {"block": blockName}
        return self._getResult("filereplicas", args = args)        
        pass
    
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
        
        return self._getResult(callname, args=args)
     
        
