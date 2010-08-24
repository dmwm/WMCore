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


    def makeSchema(self, cmsswVersion=None, scramArch=None, requestType=None,
            requestPriority=None, requestSizeEvents=None, requestSizeFiles=None,
            acquisitionEra=None, scenario=None, globalTag=None, processingVersion=None,
            group=None, requestor=None, filein=None, inputMode=None, couchDBConfig = None,
            dbs=None, lfnCategory=None,
            processingConfig=None,
            skimInput1= None, skimConfig1= None,
            mergedLFNBase = None, unmergedLFNBase = None,
            splitAlgo=None, filesPerJob=None, lumisPerJob=None, eventsPerJob=None, splitFilesBetweenJob=False,
            skimSplitAlgo=None, skimFilesPerJob=None, skimTwoFilesPerJob=None,
            runWhitelist=None, runBlacklist=None, blockWhitelist=None, blockBlacklist=None,
            siteWhitelist=None, siteBlacklist=None, RECO=None, ALCA=None, AOD=None,
            minMergeSize=None, maxMergeSize=None, maxMergeEvents=None):
        current_time = time.strftime('%y%m%d_%H%M%S',
                                 time.localtime(time.time()))

        maker = retrieveRequestMaker(requestType)
        schema = maker.newSchema()
        schema['Requestor'] = self.requestor
        schema['RequestName'] = self.requestor + '_' + current_time
        schema["RequestType"] = requestType
        schema["RequestPriority"] = requestPriority
        schema["RequestSizeEvents"] = requestSizeEvents
        schema["RequestSizeFiles"] = requestSizeFiles
        schema["AcquisitionEra"] = acquisitionEra
        schema["GlobalTag"] = globalTag
        schema["Group"] = group
        schema["CMSSWVersion"] = cmsswVersion
        schema["ScramArch"] = scramArch
        schema["InputDataset"] = filein
        schema["InputDatasets"] = [filein]
        schema["SkimInput"] = skimInput1
        schema["DbsUrl"] = dbs
        # FIXME duplicate
        schema["LFNCategory"] = lfnCategory
        schema["UnmergedLFNBase"] = mergedLFNBase
        schema["UnmergedLFNBase"] = unmergedLFNBase
        schema["RunWhitelist"] = eval("[%s]"%runWhitelist)
        schema["RunBlacklist"] = eval("[%s]"%runBlacklist)
        schema["BlockWhitelist"] = eval("[%s]"%blockWhitelist)
        schema["BlockBlacklist"] = eval("[%s]"%blockBlacklist)
        if siteWhitelist == None:
            siteWhitelist = []
        if siteBlacklist == None:
            siteBlacklist = []
        if not isinstance(siteWhitelist, list):
            siteWhitelist = [siteWhitelist]
        if not isinstance(siteBlacklist, list):
            siteBlacklist = [siteBlacklist]
        schema["SiteWhitelist"] = siteWhitelist
        schema["SiteBlacklist"] = siteBlacklist
        schema['CmsPath'] = self.cmsswInstallation
        schema['ProcessingVersion'] = processingVersion
        schema["CouchUrl"] = self.couchUrl
        schema["CouchDBName"] = self.couchDBName
        print "SCHEMA " + str(schema)

        schema["SkimConfig"] = skimConfig1
        if minMergeSize != None:  
            schema["MinMergeSize"] = minMergeSize
        if maxMergeSize != None:
            schema["MaxMergeSize"] = maxMergeSize
        if maxMergeEvents != None:
            schema["MaxMergeEvents"] = maxMergeEvents

        schema["Label"] = "WHATEVER"
        tiers = []
        if RECO != None:
           tiers.append("RECO")
        if ALCA != None:
           tiers.append("ALCA")
        if AOD != None:
           tiers.append("AOD")
        schema["OutputTiers"] = tiers

        if splitAlgo != None:
            schema["StdJobSplitAlgo"] = splitAlgo
            d = {}
            if splitAlgo == "FileBased":
                 d = {'files_per_job' : filesPerJob }
            elif splitAlgo == "LumiBased":
                 d = {'lumis_per_job' : lumisPerJob, 'split_files_between_job':splitFilesBetweenJob}
            elif splitAlgo == "EventBased":        
                 d = {'events_per_job': eventsPerJob}
            else:
                  raise RuntimeError("Cannot find splitting algo " + splitAlgo)
            schema["StdJobSplitArgs"] = d

        if skimSplitAlgo != None:
            schema["SkimJobSplitAlgo"] = skimSplitAlgo
            files_per_job = 0
            if skimSplitAlgo == "FileBased":
               files_per_job = skimFilesPerJob
            elif skimSplitAlgo == "TwoFileBased":
               files_per_job = skimTwoFilesPerJob
            else:
                  raise RuntimeError("Cannot find splitting algo " + skimSplitAlgo)
            schema["SkimJobSplitArgs"] = {'files_per_job': files_per_job}

        if requestType == "CmsGen":
            # No idea what I'm doing here
            schema['CmsGenParameters'] = {'generator' : 'madgraph'}
            schema['CmsGenConfiguration'] = """madgraph\nttjets\ntarballnamehere"""
        cherrypy.session['schema'] = schema

        schema["Scenario"] = ""
        schema["ProcessingConfig"] = ""
        if inputMode == "scenario":
            schema["Scenario"] = scenario
        elif inputMode == "url":
            schema["ProcessingConfig"] = processingConfig
        elif inputMode == "couchDB":
            schema["ProcessingConfig"] = couchDBConfig
        elif inputMode == "cmsDriver":
            # FIXME output dataset isn't just LFNCategory
            url = 'cmsDriver?'
            # add a few options
            if requestType in ['Reco',  'ReReco']:
               url += '&reco=True'
            if requestType in ['MonteCarlo', 'CmsGen']:
               if schema["RequestSizeEvents"] == -1:
                   raise RuntimeError("Must set the number of events to generate")
               url += '&gen=True'
            raise cherrypy.HTTPRedirect(url)
        else:
            print "Warning: bad configuration option"

        return self.submit()
    makeSchema.exposed = True


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
