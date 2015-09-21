"""
Main Module for browsing and modifying requests
as done by the dataOps operators to assign requests
to processing sites.

Handles site whitelist/blacklist info as well.

"""

import cherrypy
import threading

import WMCore.RequestManager.RequestDB.Interface.Admin.ProdManagement as ProdManagement
import WMCore.RequestManager.RequestDB.Interface.Request.ChangeState as ChangeState
import WMCore.RequestManager.RequestDB.Interface.Request.GetRequest as GetRequest
import WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools as Utilities
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrAuth import ReqMgrAuth
from WMCore.Database.CMSCouch import Database
import WMCore.Lexicon
from WMCore.Wrappers import JsonWrapper
from WMCore.WebTools.WebAPI import WebAPI
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from WMCore.WMSpec.WMWorkloadTools import strToBool
from WMCore.Services.DBS.DBSReader import DBSReader

class Assign(WebAPI):
    """ Used by data ops to assign requests to processing sites"""
    def __init__(self, config, noSiteDB=False):
        """
        _init_

        Note, noSiteDB added for TESTING PURPOSED ONLY!
        """
        WebAPI.__init__(self, config)
        ReqMgrAuth.assign_roles = config.security_roles
        # Take a guess
        self.templatedir = config.templates
        self.couchUrl = config.couchUrl
        self.configDBName = config.configDBName
        self.workloadDBName = config.workloadDBName
        self.configDBName = config.configDBName
        self.wmstatWriteURL = "%s/%s" % (self.couchUrl.rstrip('/'), config.wmstatDBName)
        if not noSiteDB:
            try:
                # Download a list of all the sites from SiteDB, uses v2 API.
                sitedb = SiteDBJSON()
                self.sites = sitedb.getAllCMSNames()
                self.sites.sort()
                self.phedexNodes = sitedb.getAllPhEDExNodeNames(excludeBuffer=True)
                self.phedexNodes.sort()
            except Exception as ex:
                msg = "ERROR: Could not retrieve sites from SiteDB, reason: %s" % ex
                cherrypy.log(msg)
                raise
        else:
            self.sites = []

        #store result lfn base with all Physics group
        storeResultLFNBase = ["/store/results/analysisops",
                              "/store/results/b_physics",
                              "/store/results/b_tagging",
                              "/store/results/b2g",
                              "/store/results/e_gamma_ecal",
                              "/store/results/ewk",
                              "/store/results/exotica",
                              "/store/results/forward",
                              "/store/results/heavy_ions",
                              "/store/results/higgs",
                              "/store/results/jets_met_hcal",
                              "/store/results/muon",
                              "/store/results/qcd",
                              "/store/results/susy",
                              "/store/results/tau_pflow",
                              "/store/results/top",
                              "/store/results/tracker_dpg",
                              "/store/results/tracker_pog",
                              "/store/results/trigger"]
        # yet 0.9.40 had also another self.mergedLFNBases which was differentiating
        # list of mergedLFNBases based on type of request, removed and all bases
        # will be displayed regardless of the request type (discussion with Edgar)
        self.allMergedLFNBases = [
            "/store/backfill/1",
            "/store/backfill/2",
            "/store/data",
            "/store/mc",
            "/store/generator",
            "/store/relval",
            "/store/hidata",
            "/store/himc"]

        self.allMergedLFNBases.extend(storeResultLFNBase)

        self.allUnmergedLFNBases = ["/store/unmerged", "/store/temp"]

        self.yuiroot = config.yuiroot
        cherrypy.engine.subscribe('start_thread', self.initThread)

        self.wildcardKeys = getattr(config, 'wildcardKeys', {'T1*': 'T1_*',
                                                             'T2*': 'T2_*',
                                                             'T3*': 'T3_*'})
        self.wildcardSites = {}
        Utilities.addSiteWildcards(self.wildcardKeys, self.sites, self.wildcardSites)

    def initThread(self, thread_index):
        """ The ReqMgr expects the DBI to be contained in the Thread  """
        myThread = threading.currentThread()
        #myThread = cherrypy.thread_data
        # Get it from the DBFormatter superclass
        myThread.dbi = self.dbi

    def validate(self, v, name = ""):
        """
        _validate_

        Checks different fields with different Lexicon methods,
        if not the field name is not known then apply the identifier check
        """

        #Make sure the value is a string, otherwise the Lexicon complains
        strValue = str(v)
        try:
            if name == "ProcessingVersion":
                WMCore.Lexicon.procversion(strValue)
            elif name == "AcquisitionEra":
                WMCore.Lexicon.acqname(strValue)
            elif name == "ProcessingString":
                WMCore.Lexicon.procstring(strValue)
            else:
                WMCore.Lexicon.identifier(strValue)
        except AssertionError as ex:
            raise cherrypy.HTTPError(400, "Bad input: %s" % str(ex))
        return v

    def validateDatatier(self, datatier, dbsUrl):
        """
        _validateDatatier_

        Provided a list of datatiers extracted from the outputDatasets, checks
        whether they all exist in DBS already.
        """
        dbsReader = DBSReader(dbsUrl)
        dbsTiers = dbsReader.listDatatiers()
        badTiers = list(set(datatier) - set(dbsTiers))
        if badTiers:
            raise cherrypy.HTTPError(400, "Bad datatier(s): %s not available in DBS." % badTiers)

    @cherrypy.expose
    @cherrypy.tools.secmodv2(role=ReqMgrAuth.assign_roles)
    def one(self, requestName):
        """ Assign a single request """
        self.validate(requestName)
        request = GetRequest.getRequestByName(requestName)
        request = Utilities.prepareForTable(request)
        # get assignments
        teams = ProdManagement.listTeams()
        assignments = GetRequest.getAssignmentsByName(requestName)
        # might be a list, or a dict team:priority
        if isinstance(assignments, dict):
            assignments = assignments.keys()

        procVer = ""
        acqEra = ""
        procString = ""
        helper = Utilities.loadWorkload(request)
        if helper.getAcquisitionEra() != None:
            acqEra = helper.getAcquisitionEra()
        if helper.getProcessingVersion() != None:
            procVer = helper.getProcessingVersion()
        if helper.getProcessingString():
            procString = helper.getProcessingString()
        dashboardActivity = helper.getDashboardActivity()
        blockCloseMaxWaitTime = helper.getBlockCloseMaxWaitTime()
        blockCloseMaxFiles = helper.getBlockCloseMaxFiles()
        blockCloseMaxEvents = helper.getBlockCloseMaxEvents()
        blockCloseMaxSize = helper.getBlockCloseMaxSize()
        (reqMergedBase, reqUnmergedBase) = helper.getLFNBases()

        return self.templatepage("Assign", requests = [request], teams = teams,
                                 assignments = assignments, sites = self.sites,
                                 phedexNodes = self.phedexNodes,
                                 mergedLFNBases = self.allMergedLFNBases,
                                 reqMergedBase = reqMergedBase,
                                 unmergedLFNBases = self.allUnmergedLFNBases,
                                 reqUnmergedBase = reqUnmergedBase,
                                 acqEra = acqEra, procVer = procVer,
                                 procString = procString,
                                 dashboardActivity = dashboardActivity,
                                 badRequests = [],
                                 blockCloseMaxWaitTime = blockCloseMaxWaitTime,
                                 blockCloseMaxFiles = blockCloseMaxFiles,
                                 blockCloseMaxSize = blockCloseMaxSize,
                                 blockCloseMaxEvents = blockCloseMaxEvents)

    @cherrypy.expose
    @cherrypy.tools.secmodv2(role=ReqMgrAuth.assign_roles)
    def index(self, all=0):
        """ Main page """
        # returns dict of  name:id
        allRequests = Utilities.requestsWithStatus('assignment-approved')
        teams = ProdManagement.listTeams()

        procVer = ""
        acqEra = ""
        procString = ""
        dashboardActivity = None
        badRequestNames = []
        goodRequests = allRequests
        reqMergedBase = None
        reqUnmergedBase = None
        blockCloseMaxWaitTime = 66400
        blockCloseMaxFiles = 500
        blockCloseMaxEvents = 250000000
        blockCloseMaxSize = 5000000000000
#         for request in allRequests:
#             # make sure there's a workload attached
#             try:
#                 helper = Utilities.loadWorkload(request)
#             except Exception, ex:
#                 logging.error("Assign error: %s " % str(ex))
#                 badRequestNames.append(request["RequestName"])
#             else:
#                 # get defaults from the first good one
#                 if not goodRequests:
#                     # forget it if it fails.
#                     try:
#                         if helper.getAcquisitionEra() != None:
#                             acqEra = helper.getAcquisitionEra()
#                         if helper.getProcessingVersion() != None:
#                             procVer = helper.getProcessingVersion()
#                         if helper.getProcessingString() != None:
#                             procString = helper.getProcessingString()
#                         blockCloseMaxWaitTime = helper.getBlockCloseMaxWaitTime()
#                         blockCloseMaxFiles = helper.getBlockCloseMaxFiles()
#                         blockCloseMaxEvents = helper.getBlockCloseMaxEvents()
#                         blockCloseMaxSize = helper.getBlockCloseMaxSize()
#                         (reqMergedBase, reqUnmergedBase) = helper.getLFNBases()
#                         dashboardActivity = helper.getDashboardActivity()
#                         goodRequests.append(request)
#                     except Exception, ex:
#                         logging.error("Assign error: %s " % str(ex))
#                         badRequestNames.append(request["RequestName"])
#                 else:
#                     goodRequests.append(request)

        return self.templatepage("Assign", all = all, requests = goodRequests, teams = teams,
                                 assignments = [], sites = self.sites,
                                 phedexNodes = self.phedexNodes,
                                 mergedLFNBases = self.allMergedLFNBases,
                                 reqMergedBase = reqMergedBase,
                                 unmergedLFNBases = self.allUnmergedLFNBases,
                                 reqUnmergedBase = reqUnmergedBase,
                                 acqEra = acqEra, procVer = procVer,
                                 procString = procString,
                                 dashboardActivity = dashboardActivity,
                                 badRequests = badRequestNames,
                                 blockCloseMaxWaitTime = blockCloseMaxWaitTime,
                                 blockCloseMaxFiles = blockCloseMaxFiles,
                                 blockCloseMaxSize = blockCloseMaxSize,
                                 blockCloseMaxEvents = blockCloseMaxEvents)

    @cherrypy.expose
    #@cherrypy.tools.secmodv2(role=ReqMgrAuth.assign_roles) security issue fix
    @cherrypy.tools.secmodv2(role=Utilities.security_roles(), group = Utilities.security_groups())
    def handleAssignmentPage(self, **kwargs):
        """ handler for the main page """
        #Accept Json encoded strings
        decodedArgs = {}
        for key in kwargs.keys():
            try:
                decodedArgs[key] = JsonWrapper.loads(kwargs[key])
            except Exception:
                #Probably wasn't JSON
                decodedArgs[key] = kwargs[key]
        kwargs = decodedArgs
        # handle the checkboxes
        teams = []
        requestNames = []
        for key, value in kwargs.iteritems():
            if isinstance(value, basestring):
                kwargs[key] = value.strip()
            if key.startswith("Team"):
                teams.append(key[4:])
            if key.startswith("checkbox"):
                requestName = key[8:]
                self.validate(requestName)
                requestNames.append(key[8:])

        for requestName in requestNames:
            if kwargs['action'] == 'Reject':
                ChangeState.changeRequestStatus(requestName, 'rejected', wmstatUrl = self.wmstatWriteURL)
            else:
                assignments = GetRequest.getAssignmentsByName(requestName)
                if teams == [] and assignments == []:
                    raise cherrypy.HTTPError(400, "Must assign to one or more teams")
                kwargs["Teams"] = teams
                self.assignWorkload(requestName, kwargs)
                for team in teams:
                    if not team in assignments:
                        ChangeState.assignRequest(requestName, team, wmstatUrl = self.wmstatWriteURL)
                priority = kwargs.get(requestName+':priority', '')
                if priority != '':
                    Utilities.changePriority(requestName, priority, self.wmstatWriteURL)
        participle = kwargs['action']+'ed'
        return self.templatepage("Acknowledge", participle=participle, requests=requestNames)


    def assignWorkload(self, requestName, kwargs):
        """ Make all the necessary changes in the Workload to reflect the new assignment """
        request = GetRequest.getRequestByName(requestName)
        helper = Utilities.loadWorkload(request)

        #Validate the different parts of the processed dataset
        processedDatasetParts = {"AcquisitionEra": helper.getAcquisitionEra(),
                                 "ProcessingString": helper.getProcessingString(),
                                 "ProcessingVersion": helper.getProcessingVersion()}
        for field, origValue in processedDatasetParts.iteritems():
            if field in kwargs and isinstance(kwargs[field], dict):
                for value in kwargs[field].values():
                    self.validate(value, field)
            else:
                self.validate(kwargs.get(field, origValue))

        # Set white list and black list
        whiteList = kwargs.get("SiteWhitelist", [])
        blackList = kwargs.get("SiteBlacklist", [])
        if not isinstance(whiteList, list):
            whiteList = [whiteList]
        if not isinstance(blackList, list):
            blackList = [blackList]
        helper.setSiteWildcardsLists(siteWhitelist = whiteList, siteBlacklist = blackList,
                                     wildcardDict = self.wildcardSites)
        res = set(whiteList) & set(blackList)
        if len(res):
            raise cherrypy.HTTPError(400, "White and blacklist the same site is not allowed %s" % list(res))
        # Set AcquisitionEra, ProcessingString and ProcessingVersion
        # which could be json encoded dicts
        if 'AcquisitionEra' in kwargs:
            helper.setAcquisitionEra(kwargs["AcquisitionEra"])
        if 'ProcessingString' in kwargs:
            helper.setProcessingString(kwargs["ProcessingString"])
        if 'ProcessingVersion' in kwargs:
            helper.setProcessingVersion(kwargs["ProcessingVersion"])

        # Now verify the output datasets
        datatier = []
        outputDatasets = helper.listOutputDatasets()
        for dataset in outputDatasets:
            tokens = dataset.split("/")
            procds = tokens[2]
            datatier.append(tokens[3])
            try:
                WMCore.Lexicon.procdataset(procds)
            except AssertionError as ex:
                raise cherrypy.HTTPError(400,
                            "Bad output dataset name, check the processed dataset.\n %s" % 
                            str(ex))

        # Verify whether the output datatiers are available in DBS
        self.validateDatatier(datatier, dbsUrl=helper.getDbsUrl())

        #FIXME not validated
        helper.setLFNBase(kwargs["MergedLFNBase"], kwargs["UnmergedLFNBase"])
        helper.setMergeParameters(int(kwargs.get("MinMergeSize", 2147483648)),
                                  int(kwargs.get("MaxMergeSize", 4294967296)),
                                  int(kwargs.get("MaxMergeEvents", 50000)))
        helper.setupPerformanceMonitoring(kwargs.get("MaxRSS", None),
                                          kwargs.get("MaxVSize", None),
                                          kwargs.get("SoftTimeout", None),
                                          kwargs.get("GracePeriod", None))

        # Check whether we should check location for the data
        useAAA = strToBool(kwargs.get("useSiteListAsLocation", False))
        if useAAA:
            helper.setLocationDataSourceFlag(flag=useAAA)

        # Set phedex subscription information
        custodialList = kwargs.get("CustodialSites", [])
        nonCustodialList = kwargs.get("NonCustodialSites", [])
        autoApproveList = kwargs.get("AutoApproveSubscriptionSites", [])
        for site in autoApproveList:
            if site.endswith('_MSS'):
                raise cherrypy.HTTPError(400, "Auto-approval to MSS endpoint not allowed %s" % autoApproveList)
        subscriptionPriority = kwargs.get("SubscriptionPriority", "Low")
        if subscriptionPriority not in ["Low", "Normal", "High"]:
            raise cherrypy.HTTPError(400, "Invalid subscription priority %s" % subscriptionPriority)
        custodialType = kwargs.get("CustodialSubType", "Replica")
        if custodialType not in ["Move", "Replica"]:
            raise cherrypy.HTTPError(400, "Invalid custodial subscription type %s" % custodialType)
        nonCustodialType = kwargs.get("NonCustodialSubType", "Replica")
        if nonCustodialType not in ["Move", "Replica"]:
            raise cherrypy.HTTPError(400, "Invalid noncustodial subscription type %s" % nonCustodialType)

        helper.setSubscriptionInformationWildCards(wildcardDict = self.wildcardSites,
                                                   custodialSites = custodialList,
                                                   nonCustodialSites = nonCustodialList,
                                                   autoApproveSites = autoApproveList,
                                                   custodialSubType = custodialType,
                                                   nonCustodialSubType = nonCustodialType,
                                                   priority = subscriptionPriority)

        # Block closing information
        blockCloseMaxWaitTime = int(kwargs.get("BlockCloseMaxWaitTime", helper.getBlockCloseMaxWaitTime()))
        blockCloseMaxFiles = int(kwargs.get("BlockCloseMaxFiles", helper.getBlockCloseMaxFiles()))
        blockCloseMaxEvents = int(kwargs.get("BlockCloseMaxEvents", helper.getBlockCloseMaxEvents()))
        blockCloseMaxSize = int(kwargs.get("BlockCloseMaxSize", helper.getBlockCloseMaxSize()))

        helper.setBlockCloseSettings(blockCloseMaxWaitTime, blockCloseMaxFiles,
                                     blockCloseMaxEvents, blockCloseMaxSize)

        helper.setDashboardActivity(kwargs.get("Dashboard", ""))
        # set Task properties if they are exist
        # TODO: need to define the task format (maybe kwargs["tasks"]?)
        helper.setTaskProperties(kwargs)

        Utilities.saveWorkload(helper, request['RequestWorkflow'], self.wmstatWriteURL)

        # update AcquisitionEra in the Couch document (#4380)
        # request object returned above from Oracle doesn't have information Couch
        # database
        reqDetails = Utilities.requestDetails(request["RequestName"])
        couchDb = Database(reqDetails["CouchWorkloadDBName"], reqDetails["CouchURL"])
        couchDb.updateDocument(request["RequestName"], "ReqMgr", "updaterequest",
                               fields={"AcquisitionEra": reqDetails["AcquisitionEra"],
                                       "ProcessingVersion": reqDetails["ProcessingVersion"],
                                       "CustodialSites": custodialList,
                                       "NonCustodialSites": nonCustodialList,
                                       "AutoApproveSubscriptionSites": autoApproveList,
                                       "SubscriptionPriority": subscriptionPriority,
                                       "CustodialSubType": custodialType,
                                       "NonCustodialSubType": nonCustodialType,
                                       "Teams": kwargs["Teams"],
                                       "OutputDatasets": outputDatasets,
                                       "SiteWhitelist": whiteList,
                                       "SiteBlacklist": blackList},
                               useBody=True)
