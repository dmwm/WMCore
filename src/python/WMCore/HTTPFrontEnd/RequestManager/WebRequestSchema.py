import WMCore.RequestManager.RequestMaker.Production
import WMCore.RequestManager.RequestMaker.Processing.StoreResultsRequest
from WMCore.RequestManager.RequestMaker.Registry import  retrieveRequestMaker
from WMCore.Services.Requests import JSONRequests
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import parseRunList, parseBlockList
from WMCore.HTTPFrontEnd.RequestManager.CmsDriverWebRequest import CmsDriverWebRequest
import WMCore.HTTPFrontEnd.RequestManager.Sites
from httplib import HTTPException
import cherrypy
import time
from WMCore.WebTools.Page import TemplatedPage


class WebRequestSchema(TemplatedPage):
    """ Allows the user to submit a request to the RequestManager through a web interface """
    def __init__(self, config):
        TemplatedPage.__init__(self, config)
        # set the database to whatever the environment defines
        self.templatedir = __file__.rsplit('/', 1)[0]
        self.requestor = config.requestor
        self.cmsswInstallation = config.cmsswInstallation
        self.cmsswVersion = config.cmsswDefaultVersion
        self.reqMgrHost = config.reqMgrHost
        self.jsonSender = JSONRequests(config.reqMgrHost)
        self.cmsDriver = CmsDriverWebRequest(config)
        self.couchUrl = config.configCacheUrl
        self.couchDBName = config.configCacheDBName
        cherrypy.config.update({'tools.sessions.on': True, 'tools.encode.on':True, 'tools.decode.on':True})

        self.defaultSkimConfig = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/DataOps/python/prescaleskimmer.py?revision=1.1"    

    def index(self):
        """ Main web page for creating requests """
        self.versions = self.jsonSender.get('/reqMgr/version')[0]
        self.versions.sort()

        if not self.requestor in self.jsonSender.get('/reqMgr/user')[0]:
            return "User " + self.requestor + " is not registered.  Contact a ReqMgr administrator."

        groups = self.jsonSender.get('/reqMgr/group?user='+self.requestor)[0]
        if groups == []:
            return "User " + self.requestor + " is not in any groups.  Contact a ReqMgr administrator."
        #reqTypes = TypesList
        reqTypes = ["ReReco"]
        return self.templatepage("WebRequestSchema", requestor=self.requestor,
          groups=groups, reqTypes=reqTypes, 
          versions=self.versions, defaultVersion=self.cmsswVersion,
          defaultSkimConfig=self.defaultSkimConfig)
    index.exposed = True

    @cherrypy.expose
    def makeSchema(self, **kwargs):
        """ Handles the submission of requests """
        current_time = time.strftime('%y%m%d_%H%M%S',
                                 time.localtime(time.time()))
        # make sure no extra spaces snuck in
        for k, v in kwargs.iteritems():
            kwargs[k] = v.strip()
        maker = retrieveRequestMaker(kwargs["RequestType"])
        schema = maker.newSchema()
        print str(kwargs)
        schema.update(kwargs)        
        schema['Requestor'] = self.requestor
        schema['RequestName'] = self.requestor + '_' + current_time
        schema['CouchURL'] = self.couchUrl
        schema['CouchDBName'] = self.couchDBName

        if 'Scenario' in kwargs and 'ProdConfigCacheID' in kwargs:
            # Use input mode to delete the unused one
            inputMode = kwargs['inputMode']
            inputValues = {'scenario':'Scenario',
                           'couchDB':'ProdConfigCacheID'}
            for n, v in inputValues.iteritems():
                if n != inputMode:
                    schema[v] = ""

        if kwargs.has_key("InputDataset"):
            schema["InputDatasets"] = [kwargs["InputDataset"]]
        skimNumber = 1
        # a list of dictionaries
        schema["SkimConfigs"] = []
        while kwargs.has_key("SkimName%s" % skimNumber):
            d = {}
            d["SkimName"] = kwargs["SkimName%s" % skimNumber]
            d["SkimInput"] = kwargs["SkimInput%s" % skimNumber]
            d["Scenario"] = kwargs["Scenario"]

            if kwargs.get("SkimConfig%s" % skimNumber, None) != None:
                d["ConfigCacheID"] = kwargs["SkimConfig%s" % skimNumber]
       
            schema["SkimConfigs"].append(d)
            skimNumber += 1

        if kwargs.has_key("RunWhitelist"):
            schema["RunWhitelist"] = parseRunList(kwargs["RunWhitelist"])
        if kwargs.has_key("RunBlacklist"):
            schema["RunBlacklist"] = parseRunList(kwargs["RunBlacklist"])
        if kwargs.has_key("BlockWhitelist"):
            schema["BlockWhitelist"] = parseBlockList(kwargs["BlockWhitelist"])
        if kwargs.has_key("BlockBlacklist"):
            schema["BlockBlacklist"] = parseBlockList(kwargs["BlockBlacklist"])

        schema['CmsPath'] = self.cmsswInstallation
        schema["CouchUrl"] = self.couchUrl
        schema["CouchDBName"] = self.couchDBName
        #cherrypy.session['schema'] = schema
        try:
            self.jsonSender.put('/reqMgr/request/'+schema['RequestName'], schema)
        except HTTPException, ex:
            return ex.reason+' '+ex.result
        raise cherrypy.HTTPRedirect('/reqMgrBrowser/requestDetails/'+schema['RequestName'])
