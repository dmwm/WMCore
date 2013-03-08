""" Pages for the creation of requests """
import WMCore.RequestManager.RequestDB.Interface.User.Registration as Registration
import WMCore.RequestManager.RequestDB.Interface.Admin.SoftwareManagement as SoftwareAdmin
import WMCore.RequestManager.RequestDB.Interface.Group.Information as GroupInfo
import WMCore.RequestManager.RequestDB.Interface.Request.Campaign as Campaign
import WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools as Utilities
from WMCore.Wrappers import JsonWrapper
import cherrypy
import time
from WMCore.WebTools.WebAPI import WebAPI
import WMCore.Database.CMSCouch
import threading
import os.path
from WMCore.Database.CMSCouch import CouchUnauthorisedError


class WebRequestSchema(WebAPI):
    """ Allows the user to submit a request to the RequestManager through a web interface """
    def __init__(self, config):
        WebAPI.__init__(self, config)
        self.templatedir = config.templates
        self.requestor = config.requestor
        self.cmsswVersion = config.cmsswDefaultVersion
        self.defaultArch  = getattr(config, 'defaultScramArch', "slc5_amd64_gcc434")
        self.couchUrl = config.couchUrl
        self.componentDir = config.componentDir
        self.configDBName = config.configDBName
        self.workloadDBName = config.workloadDBName
        self.wmstatWriteURL = "%s/%s" % (self.couchUrl.rstrip('/'), config.wmstatDBName)
        self.defaultSkimConfig = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/DataOps/python/prescaleskimmer.py?revision=1.1"
        self.yuiroot = config.yuiroot
        cherrypy.engine.subscribe('start_thread', self.initThread)
        self.scramArchs = []

    def initThread(self, thread_index):
        """ The ReqMgr expects the DBI to be contained in the Thread  """
        myThread = threading.currentThread()
        #myThread = cherrypy.thread_data
        # Get it from the DBFormatter superclass
        myThread.dbi = self.dbi

    @cherrypy.expose
    def allDocs(self):
        server = WMCore.Database.CMSCouch.CouchServer(self.couchUrl)
        try:
            database = server.connectDatabase(self.configDBName)
        except CouchUnauthorisedError, ex:
            # We can't talk to couch...it's not authorized
            raise cherrypy.HTTPError(400, "Couch has raised an authorisation error on pulling allDocs!")
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
        # Get the scram Architecture from the keys and the
        # CMSSW versions from the values
        self.scramArchs = SoftwareAdmin.listSoftware().keys()
        versionLists = SoftwareAdmin.listSoftware().values()
        self.versions = []
        for l in versionLists:
            for v in l:
                if not v in self.versions:
                    self.versions.append(v)
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
                                 archs = self.scramArchs,
                                 alldocs = Utilities.unidecode(self.allDocs()),
                                 allcampaigns = campaigns,
                                 defaultVersion=self.cmsswVersion,
                                 defaultArch = self.defaultArch,
                                 defaultSkimConfig=self.defaultSkimConfig)

    @cherrypy.expose
    @cherrypy.tools.secmodv2()
    def makeSchema(self, **schema):
        schema.setdefault('CouchURL', Utilities.removePasswordFromUrl(self.couchUrl))
        # wrong naming ... but it's all over the place, it's the config cache DB name
        schema.setdefault('CouchDBName', self.configDBName)
        schema.setdefault('CouchWorkloadDBName', self.workloadDBName)

        decodedSchema = {}
        for key in schema.keys():
            try:
                decodedSchema[key] = JsonWrapper.loads(schema[key])
            except:
                # We don't know what kind of exception we'll get, so ignore them all
                # If it does except, it probably wasn't in JSON to begin with.
                # Anything else should be caught by the parsers and the validation
                decodedSchema[key] = schema[key]
        try:
            self.info("Creating a request for: '%s'\n\tworkloadDB: '%s'\n\twmstatUrl: "
                      "'%s' ..." % (decodedSchema, self.workloadDBName,
                                    Utilities.removePasswordFromUrl(self.wmstatWriteURL)))
            request = Utilities.makeRequest(self, decodedSchema, self.couchUrl, self.workloadDBName, self.wmstatWriteURL)
            # catching here KeyError is just terrible
        except (RuntimeError, KeyError, Exception) as ex:
            # TODO problem not to expose logs to the client
            # e.g. on ConfigCacheID not found, the entire CouchDB traceback is sent in ex_message
            self.error("Create request failed, reason: %s" % ex)
            if hasattr(ex, "name"):
                detail = ex.name
            else:
                detail = "check logs." 
            msg = "Create request failed, %s" % detail
            raise cherrypy.HTTPError(400, msg)            
        baseURL = cherrypy.request.base
        raise cherrypy.HTTPRedirect('%s/reqmgr/view/details/%s' % (baseURL, request['RequestName']))