import WMCore.RequestManager.RequestMaker.Production
import WMCore.RequestManager.RequestMaker.Processing.StoreResultsRequest
from WMCore.RequestManager.DataStructs.RequestSchema import RequestSchema
from WMCore.RequestManager.RequestDB.Settings.RequestTypes import TypesList
from WMCore.RequestManager.RequestMaker.Registry import  retrieveRequestMaker
from WMCore.Services.Requests import JSONRequests
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import parseRunList, parseBlockList
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
          versions=self.versions, defaultVersion=self.cmsswVersion,
          defaultSkimConfig=self.defaultSkimConfig)
    index.exposed = True


    def makeSchema(self, **kwargs):
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
            for n,v in inputValues.iteritems():
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
                  raise cherrypy.HTTPError(400, "Cannot find splitting algo " + splitAlgo)
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
                  raise cherrypy.HTTPError(400, "Cannot find splitting algo " + skimSplitAlgo)
            schema["SkimJobSplitArgs"] = {'files_per_job': files_per_job}


        cherrypy.session['schema'] = schema
        return self.submit()
    makeSchema.exposed = True

    def submit(self):
        schema = cherrypy.session.get('schema', None)
        if schema == None:
           return "Where did that darn schema go?"
        try:
            result = self.jsonSender.put('/reqMgr/request/'+schema['RequestName'], schema)
        except HTTPException, ex:
            return ex.reason+' '+ex.result
        raise cherrypy.HTTPRedirect('/reqMgrBrowser/requestDetails/'+schema['RequestName'])
    submit.exposed = True
