import WMCore.RequestManager.RequestMaker.Production
import WMCore.RequestManager.RequestMaker.Processing.StoreResultsRequest
import WMCore.RequestManager.RequestDB.Interface.User.Registration as Registration
import WMCore.RequestManager.RequestDB.Interface.Admin.SoftwareManagement as SoftwareAdmin
import WMCore.RequestManager.RequestDB.Interface.Group.Information as GroupInfo
from WMCore.RequestManager.RequestMaker.Registry import  retrieveRequestMaker
from WMCore.Services.Requests import JSONRequests
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import parseRunList, parseBlockList
import WMCore.HTTPFrontEnd.RequestManager.Sites
from httplib import HTTPException
import cherrypy
import time
from WMCore.WebTools.WebAPI import WebAPI
import threading


class WebRequestSchema(WebAPI):
    """ Allows the user to submit a request to the RequestManager through a web interface """
    def __init__(self, config):
        WebAPI.__init__(self, config)
        # set the database to whatever the environment defines
        self.templatedir = __file__.rsplit('/', 1)[0]
        self.requestor = config.requestor
        self.cmsswVersion = config.cmsswDefaultVersion
        self.reqMgrHost = config.reqMgrHost
        self.jsonSender = JSONRequests(config.reqMgrHost)
        self.couchUrl = config.couchUrl
        self.couchDBName = config.configCacheDBName
        #cherrypy.config.update({'tools.sessions.on': True, 'tools.encode.on':True, 'tools.decode.on':True})

        self.defaultSkimConfig = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/DataOps/python/prescaleskimmer.py?revision=1.1"    
        cherrypy.engine.subscribe('start_thread', self.initThread)

    def initThread(self, thread_index):
        """ The ReqMgr expects the DBI to be contained in the Thread  """
        myThread = threading.currentThread()
        #myThread = cherrypy.thread_data
        # Get it from the DBFormatter superclass
        myThread.dbi = self.dbi

    @cherrypy.expose
    def index(self):
        """ Main web page for creating requests """
        self.versions = SoftwareAdmin.listSoftware().keys()
        self.versions.sort()

        if not self.requestor in Registration.listUsers():
            return "User " + self.requestor + " is not registered.  Contact a ReqMgr administrator."

        groups = GroupInfo.groupsForUser(self.requestor).keys()
        if groups == []:
            return "User " + self.requestor + " is not in any groups.  Contact a ReqMgr administrator."
        return self.templatepage("WebRequestSchema", requestor=self.requestor,
                                 groups=groups, 
                                 versions=self.versions, defaultVersion=self.cmsswVersion,
                                 defaultSkimConfig=self.defaultSkimConfig)

    @cherrypy.expose
    def makeSchema(self, **kwargs):
        """ Handles the submission of requests """
        # make sure no extra spaces snuck in
        for k, v in kwargs.iteritems():
            kwargs[k] = v.strip()
        maker = retrieveRequestMaker(kwargs["RequestType"])
        schema = maker.newSchema()
        print str(kwargs)
        schema.update(kwargs)
        current_time = time.strftime('%y%m%d_%H%M%S',
                                 time.localtime(time.time()))

        if schema.has_key('RequestString') and schema['RequestString'] != "":
            schema['RequestName'] = "%s_%s_%s" % (self.requestor, schema['RequestString'],
                                                  current_time)
        else:
            schema['RequestName'] = "%s_%s" % (self.requestor, current_time)
            
        schema['Requestor'] = self.requestor
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

        try:
            self.jsonSender.put('/reqMgr/request/'+schema['RequestName'], schema)
        except HTTPException, ex:
            return ex.reason+' '+ex.result
        raise cherrypy.HTTPRedirect('/reqMgrBrowser/details/'+schema['RequestName'])
