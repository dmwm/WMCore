""" Pages for the creation of requests """
import WMCore.RequestManager.RequestMaker.Production
import WMCore.RequestManager.RequestDB.Interface.User.Registration as Registration
import WMCore.RequestManager.RequestDB.Interface.Admin.SoftwareManagement as SoftwareAdmin
import WMCore.RequestManager.RequestDB.Interface.Group.Information as GroupInfo
import WMCore.RequestManager.RequestDB.Interface.Request.Campaign as Campaign
from WMCore.RequestManager.RequestMaker.Registry import  retrieveRequestMaker
import WMCore.RequestManager.RequestMaker.Processing
import WMCore.RequestManager.RequestMaker.Production
import WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools as Utilities
from WMCore.Wrappers import JsonWrapper
import cherrypy
import time
from WMCore.WebTools.WebAPI import WebAPI
import WMCore.Database.CMSCouch
import threading
import os.path


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

    @cherrypy.expose
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
    def javascript(self, *args):
        if args[0] == "external":
            return Utilities.serveFile('application/javascript',
                             os.path.join(self.config.javascript), *args)
        return Utilities.serveFile('application/javascript',
                          os.path.join(self.config.javascript,
                                    'WMCore', 'WebTools'), *args)

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
        if not requestor:
            return "No username found in your certificate"
        if not requestor in Registration.listUsers():
            return "User %s is not registered.  Contact a ReqMgr administrator." % requestor
        groups = GroupInfo.groupsForUser(requestor).keys()
        if groups == []:
            return "User " + requestor + " is not in any groups.  Contact a ReqMgr administrator."
        campaigns = Campaign.listCampaigns()
        return self.templatepage("WebRequestSchema", yuiroot=self.yuiroot,
            requestor=requestor,
            groups=groups, 
            versions=self.versions, 
            alldocs = Utilities.unidecode(self.allDocs()),
            allcampaigns = campaigns,                     
            defaultVersion=self.cmsswVersion,
            defaultSkimConfig=self.defaultSkimConfig)

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def makeSchema(self, **schema):
        schema.setdefault('CouchURL', Utilities.removePasswordFromUrl(self.couchUrl))
        schema.setdefault('CouchDBName', self.configDBName)

        decodedSchema = {}
        for key in schema.keys():
            decodedSchema[key] = JsonWrapper.loads(schema[key])

        try:
            request = Utilities.makeRequest(decodedSchema, self.couchUrl, self.workloadDBName)
        except RuntimeError, e:
            raise cherrypy.HTTPError(400, "Error creating request: %s" % e)
        except KeyError, e:
            raise cherrypy.HTTPError(400, "Error creating request: %s" % e)        
        baseURL = cherrypy.request.base
        raise cherrypy.HTTPRedirect('%s/reqmgr/view/details/%s' % (baseURL, request['RequestName']))
