import WMCore.RequestManager.RequestMaker.Production
import WMCore.RequestManager.RequestMaker.Processing.StoreResultsRequest
from WMCore.RequestManager.DataStructs.RequestSchema import RequestSchema
from WMCore.RequestManager.RequestDB.Settings.RequestTypes import TypesList
from WMCore.RequestManager.RequestMaker.Registry import  retrieveRequestMaker
from WMCore.Services.Requests import JSONRequests
from WMCore.HTTPFrontEnd.RequestManager.CmsDriverWebRequest import CmsDriverWebRequest
import WMCore.HTTPFrontEnd.RequestManager.Sites
from httplib import HTTPException
import cherrypy
import os
import time
from WMCore.WebTools.Page import TemplatedPage


class WebRequestSchema(TemplatedPage):
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

        self.sites = WMCore.HTTPFrontEnd.RequestManager.Sites.sites()
        self.defaultProcessingConfig = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/rereco_FirstCollisions_MinimumBias_35X.py?revision=1.8"
        self.defaultSkimConfig = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/DataOps/python/prescaleskimmer.py?revision=1.1"    

    def index(self, requestType=None):
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
          versions=self.versions, defaultVersion=self.cmsswVersion,sites=self.sites, 
          defaultProcessingConfig=self.defaultProcessingConfig, defaultSkimConfig=self.defaultSkimConfig)
    index.exposed = True


    def makeSchema(self, **kwargs):
        current_time = time.strftime('%y%m%d_%H%M%S',
                                 time.localtime(time.time()))

        maker = retrieveRequestMaker(kwargs["RequestType"])
        schema = maker.newSchema()
        print str(kwargs)
        schema.update(kwargs)        
        schema['Requestor'] = self.requestor
        schema['RequestName'] = self.requestor + '_' + current_time

        if kwargs.has_key("InputDataset"):
            schema["InputDatasets"] = [kwargs["InputDataset"]]
        # hack until makers accept numbered skims
        if kwargs.has_key("SkimInput1"):
            schema["SkimInput"] = schema["SkimInput1"]
            schema["SkimConfig"] = schema["SkimConfig1"]
        self.parseList("RunWhitelist", kwargs, schema)
        self.parseList("RunBlacklist", kwargs, schema)
        self.parseList("BlockWhitelist", kwargs, schema)
        self.parseList("BlockBlacklist", kwargs, schema)
        self.parseSite("SiteWhitelist", kwargs, schema)
        self.parseSite("SiteBlacklist", kwargs, schema)

        schema['CmsPath'] = self.cmsswInstallation
        schema["CouchURL"] = self.couchUrl
        schema["CouchDBName"] = self.couchDBName
        print "SCHEMA " + str(schema)

        if kwargs.has_key("StdJobSplitAlgo"):            
            schema["StdJobSplitAlgo"] = splitAlgo
            d = {}
            if splitAlgo == "FileBased":
                 d = {'files_per_job' : kwargs["filesPerJob"] }                 
            elif splitAlgo == "LumiBased":
                 d = {'lumis_per_job' : kwargs["lumisPerJob"],
                      'split_files_between_job':kwargs["splitFilesBetweenJob"]}                 
            elif splitAlgo == "EventBased":        
                 d = {'events_per_job': kwargs["eventsPerJob"]}                 
            else:
                  raise RuntimeError("Cannot find splitting algo " + splitAlgo)
            schema["StdJobSplitArgs"] = d

        if kwargs.has_key("SkimJobSplitAlgo"):
            skimSplitAlgo = kwargs["SkimJobSplitAlgo"]            
            schema["SkimJobSplitAlgo"] = skimSplitAlgo
            files_per_job = 0
            if skimSplitAlgo == "FileBased":
               files_per_job = kwargs["skimFilesPerJob"]               
            elif skimSplitAlgo == "TwoFileBased":
               files_per_job = kwargs["skimTwoFilesPerJob"]               
            else:
                  raise RuntimeError("Cannot find splitting algo " + skimSplitAlgo)
            schema["SkimJobSplitArgs"] = {'files_per_job': files_per_job}

        #delete unnecessary parameters.
        # is there a way to make these fields never appear?
        inputMode = kwargs['inputMode']
        inputValues = {'scenario':'Scenario', 'url':'ProcessingConfig',
                       'couchDB':'ConfigCacheDoc'}
        for n,v in inputValues.iteritems():
            if n != inputMode:
                schema[v] = ""

        cherrypy.session['schema'] = schema
        return self.submit()
    makeSchema.exposed = True

    def parseList(self, name, kwargs, schema):
        """ For a given run or block list, put it in as a list """
        if kwargs.has_key(name):
            schema[name] = eval("[%s]"%kwargs[name])
        else:
            schema[name] = []

    def parseSite(self, name, kwargs, schema):
        """ puts site whitelist & blacklists into nice format"""
        if kwargs.has_key(name):
            value = kwargs[name]
            if value == None:
                value = []
            if not isinstance(value, list):
                value = [value]
            schema[name] = value

    def submit(self):
        schema = cherrypy.session.get('schema', None)
        if schema == None:
           return "Where did that darn schema go?"
        schema['PSetHash'] = cherrypy.session.get('PSetHash', None)
        newLabel = cherrypy.session.get('Label', None)
        if newLabel != None:
           schema['Label'] = newLabel
        schema['ProductionChannel'] = cherrypy.session.get('ProductionChannel', None)
        try:
            result = self.jsonSender.put('/reqMgr/request/'+schema['RequestName'], schema)
        except HTTPException, ex:
            return ex.reason+' '+ex.result
        raise cherrypy.HTTPRedirect('/reqMgrBrowser/requestDetails/'+schema['RequestName'])
    submit.exposed = True
