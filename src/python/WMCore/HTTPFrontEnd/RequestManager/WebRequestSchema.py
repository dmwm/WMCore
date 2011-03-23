""" Pages for the creation of requests """
import WMCore.RequestManager.RequestMaker.Production
import WMCore.RequestManager.RequestDB.Interface.User.Registration as Registration
import WMCore.RequestManager.RequestDB.Interface.Admin.SoftwareManagement as SoftwareAdmin
import WMCore.RequestManager.RequestDB.Interface.Group.Information as GroupInfo
from WMCore.RequestManager.RequestMaker.Registry import  retrieveRequestMaker
import WMCore.RequestManager.RequestMaker.Processing
import WMCore.RequestManager.RequestMaker.Production
import WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools as Utilities
import cherrypy
import time
from WMCore.WebTools.WebAPI import WebAPI
import WMCore.Database.CMSCouch
import threading


class WebRequestSchema(WebAPI):
    """ Allows the user to submit a request to the RequestManager through a web interface """
    def __init__(self, config):
        WebAPI.__init__(self, config)
        self.templatedir = config.templates
        self.requestor = config.requestor
        self.cmsswVersion = config.cmsswDefaultVersion
        self.couchUrl = config.couchUrl
        self.componentDir = config.componentDir
        self.configDBName = config.configDBName
        self.workloadDBName = config.workloadDBName
        self.defaultSkimConfig = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/DataOps/python/prescaleskimmer.py?revision=1.1"    
        self.yuiroot = config.yuiroot
        cherrypy.engine.subscribe('start_thread', self.initThread)

    def initThread(self, thread_index):
        """ The ReqMgr expects the DBI to be contained in the Thread  """
        myThread = threading.currentThread()
        #myThread = cherrypy.thread_data
        # Get it from the DBFormatter superclass
        myThread.dbi = self.dbi

    def allDocs(self):
        server = WMCore.Database.CMSCouch.CouchServer(self.couchUrl)
        database = server.connectDatabase(self.configDBName)
        docs = database.allDocs()
        result = []
        for row in docs["rows"]:
           if row["id"].startswith('user') or row["id"].startswith('group'):
               pass
           else:
               result.append(row["id"]) 
        return result

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def index(self):
        """ Main web page for creating requests """
        self.versions = SoftwareAdmin.listSoftware().keys()
        self.versions.sort()
        # see if this was configured with a hardcoded user.  If not, take from the request header 
        requestor = self.requestor
        if not requestor:
            requestor = cherrypy.request.user["login"]
        if not requestor in Registration.listUsers():
            return "User " + requestor + " is not registered.  Contact a ReqMgr administrator."
        groups = GroupInfo.groupsForUser(requestor).keys()
        if groups == []:
            return "User " + requestor + " is not in any groups.  Contact a ReqMgr administrator."
        return self.templatepage("WebRequestSchema", yuiroot=self.yuiroot,
            requestor=requestor,
            groups=groups, 
            versions=self.versions, 
            alldocs = self.allDocs(),
            defaultVersion=self.cmsswVersion,
            defaultSkimConfig=self.defaultSkimConfig)

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def makeSchema(self, **kwargs):
        """ Handles the submission of requests """
        # make sure no extra spaces snuck in
        for k, v in kwargs.iteritems():
            kwargs[k] = str(v).strip()
        maker = retrieveRequestMaker(kwargs["RequestType"])
        schema = maker.newSchema()
        schema.update(kwargs)
        currentTime = time.strftime('%y%m%d_%H%M%S',
                                 time.localtime(time.time()))
        if schema.has_key('RequestString') and schema['RequestString'] != "":
            schema['RequestName'] = "%s_%s_%s" % (
                schema['Requestor'], schema['RequestString'], currentTime)
        else:
            schema['RequestName'] = "%s_%s" % (schema['Requestor'], currentTime)
            
        schema['CouchURL'] = self.couchUrl
        schema['CouchDBName'] = self.configDBName

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

        if kwargs.has_key("DataPileup") or kwargs.has_key("MCPileup"):
            schema["PileupConfig"] = {}
            if kwargs.has_key("DataPileup") and kwargs["DataPileup"] != "":
                schema["PileupConfig"]["data"] = [kwargs["DataPileup"]]
            if kwargs.has_key("MCPileup") and kwargs["MCPileup"] != "":
                schema["PileupConfig"]["mc"] = [kwargs["MCPileup"]]
                
        for runlist in ["RunWhitelist", "RunBlacklist"]:
            if runlist in kwargs:
                schema[runlist] = Utilities.parseRunList(kwargs[runlist])
        for blocklist in ["BlockWhitelist", "BlockBlacklist"]:
            if blocklist in kwargs:
                schema[blocklist] = Utilities.parseBlockList(kwargs[blocklist])

        schema = Utilities.unidecode(schema)
        request = Utilities.makeRequest(schema, self.couchUrl, self.workloadDBName)
        baseURL = cherrypy.request.base
        raise cherrypy.HTTPRedirect('%s/reqmgr/view/details/%s' % (baseURL, schema['RequestName']))
