""" Functions external to the web interface classes"""
import urllib
import time
import logging
import re
import WMCore.Wrappers.JsonWrapper as JsonWrapper
import cherrypy
from os import path
from xml.dom.minidom import parse as parseDOM
from xml.parsers.expat import ExpatError
from cherrypy import HTTPError
from cherrypy.lib.static import serve_file
import WMCore.Lexicon
from WMCore.ACDC.CouchService import CouchService
from WMCore.Database.CMSCouch import Database
import cgi
import WMCore.RequestManager.RequestDB.Settings.RequestStatus             as RequestStatus
import WMCore.RequestManager.RequestDB.Interface.Request.ChangeState      as ChangeState
import WMCore.RequestManager.RequestDB.Interface.Request.GetRequest       as GetRequest
import WMCore.RequestManager.RequestDB.Interface.Admin.ProdManagement     as ProdManagement
import WMCore.RequestManager.RequestDB.Interface.Request.ListRequests     as ListRequests
import WMCore.RequestManager.RequestDB.Interface.Admin.SoftwareManagement as SoftwareAdmin
import WMCore.Services.WorkQueue.WorkQueue as WorkQueue
import WMCore.RequestManager.RequestMaker.CheckIn as CheckIn
from WMCore.RequestManager.RequestMaker.Registry import buildWorkloadForRequest
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WMSpec.StdSpecs.StdBase import WMSpecFactoryException
from WMCore.RequestManager.DataStructs.RequestSchema import RequestSchema
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter

def addSiteWildcards(wildcardKeys, sites, wildcardSites):
    """
    _addSiteWildcards_

    Add site wildcards to the self.sites list
    These wildcards should allow you to whitelist/blacklist a
    large number of sites at once.

    Expects a dictionary for wildcardKeys where the key:values are
    key = Label to be displayed as
    value = Regular expression
    """

    for k in wildcardKeys.keys():
        reValue = wildcardKeys.get(k)
        found   = False
        for s in sites:
            if re.search(reValue, s):
                found = True
                if not k in wildcardSites.keys():
                    wildcardSites[k] = []
                wildcardSites[k].append(s)
        if found:
            sites.append(k)


def parseRunList(l):
    """ Changes a string into a list of integers """
    result = None
    if isinstance(l, list):
        result = l
    elif isinstance(l, basestring):
        toks = l.lstrip(' [').rstrip(' ]').split(',')
        if toks == ['']:
            return []
        result = [int(tok) for tok in toks]
    elif isinstance(l, int):
        result = [l]
    else:
        raise cherrypy.HTTPError(400, "Bad Run list of type " + type(l).__name__)

    # If we're here, we have a list of runs
    for r in result:
        try:
            tmp = int(r)
        except ValueError:
            raise cherrypy.HTTPError(400, "Given runList without integer run numbers")
        if not tmp == r:
            raise cherrypy.HTTPError(400, "Given runList without valid integer run numbers")
    return result
    #raise RuntimeError, "Bad Run list of type " + type(l).__name__

def parseBlockList(l):
    """ Changes a string into a list of strings """
    result = None
    if isinstance(l, list):
        result = l
    elif isinstance(l, basestring):
        toks = l.lstrip(' [').rstrip(' ]').split(',')
        if toks == ['']:
            return []
        # only one set of quotes
        result = [str(tok.strip(' \'"')) for tok in toks]
    else:
        raise cherrypy.HTTPError(400, "Bad Run list of type " + type(l).__name__)

    # If we've gotten here we've got a list of blocks
    # Hopefully they pass validation
    for block in result:
        try:
            WMCore.Lexicon.block(candidate = block)
        except AssertionError, ex:
            raise cherrypy.HTTPError(400, "Block in blockList has invalid name")
    return result

def parseStringListWithoutValidation(l):
    """
    _parseStringListWithoutValidation

    Changes a string into a list of  generic strings,
    this doesn't validate the strings in the list
    against the Lexicon
    """
    result = None
    if isinstance(l, list):
        result = l
    elif isinstance(l, basestring):
        toks = l.lstrip(' [').rstrip(' ]').split(',')
        if toks == ['']:
            return []
        # only one set of quotes
        result = [str(tok.strip(' \'"')) for tok in toks]
    else:
        raise cherrypy.HTTPError(400, "Bad list of type " + type(l).__name__)
    return result

def parseSite(kw, name):
    """ puts site whitelist & blacklists into nice format"""
    value = kw.get(name, [])
    if value == None:
        value = []
    if not isinstance(value, list):
        value = [value]
    return value

def allScramArchsAndVersions():
    """
    _allScramArchs_

    Downloads a list of all ScramArchs and Versions from the tag collector
    """
    result = {}
    try:
        f = urllib.urlopen("https://cmstags.cern.ch/tc/ReleasesXML/?anytype=1")
        domDoc   = parseDOM(f)
    except ExpatError, ex:
        logging.error("Could not connect to tag collector!")
        logging.error("Not changing anything!")
        return {}
    archDOMs = domDoc.firstChild.getElementsByTagName("architecture")
    for archDOM in archDOMs:
        arch = archDOM.attributes.item(0).value
        releaseList = []
        for node in archDOM.childNodes:
            # Somehow we can get extraneous ('\n') text nodes in
            # certain versions of Linux
            if str(node.__class__) == "xml.dom.minidom.Text":
                continue
            if not node.hasAttributes():
                # Then it's an empty random node created by the XML
                continue
            for i in range(node.attributes.length):
                attr = node.attributes.item(i)
                if str(attr.name) == 'label':
                    releaseList.append(str(attr.value))
        result[str(arch)] = releaseList
    return result

def updateScramArchsAndCMSSWVersions():
    """
    _updateScramArchsAndCMSSWVersions_

    Update both the scramArchs and their associated software versions to
    the current tag collector standard.
    """

    allArchsAndVersions = allScramArchsAndVersions()
    if allArchsAndVersions == {}:
        # The tag collector is probably down
        # NO valid CMSSW Versions is not a valid use case!
        # Do nothing.
        logging.error("Handed blank list of scramArchs/versions.  Ignoring for this cycle.")
        return
    for scramArch in allArchsAndVersions.keys():
        SoftwareAdmin.updateSoftware(scramArch = scramArch,
                                     softwareNames = allArchsAndVersions[scramArch])

def allSoftwareVersions():
    """ Downloads a list of all software versions from the tag collector """
    result = []
    f = urllib.urlopen("https://cmstags.cern.ch/tc/ReleasesXML/?anytype=1")
    for line in f:
        for tok in line.split():
            if tok.startswith("label="):
                release = tok.split("=")[1].strip('"')
                result.append(release)
    return result

def loadWorkload(request):
    """ Returns a WMWorkloadHelper for the workload contained in the request """
    url = request['RequestWorkflow']
    helper = WMWorkloadHelper()
    try:
        WMCore.Lexicon.couchurl(url)
    except Exception:
        raise cherrypy.HTTPError(400, "Invalid workload "+urllib.quote(url))
    helper = WMWorkloadHelper()
    try:
        helper.load(url)
    except Exception:
        raise cherrypy.HTTPError(404, "Cannot find workload "+removePasswordFromUrl(url))
    return helper

def saveWorkload(helper, workload, wmstatUrl = None):
    """ Saves the changes to this workload """
    if workload.startswith('http'):
        helper.saveCouchUrl(workload)
        if wmstatUrl:
            wmstatSvc = WMStatsWriter(wmstatUrl)
            wmstatSvc.updateFromWMSpec(helper)
    else:
        helper.save(workload)

def removePasswordFromUrl(url):
    """ Gets rid of the stuff before the @ sign """
    result = url
    atat = url.find('@')
    slashslashat = url.find('//')
    if atat != -1 and slashslashat != -1 and slashslashat < atat:
        result = url[:slashslashat+2] + url[atat+1:]
    return result


def changePriority(requestName, priority, wmstatUrl = None):
    """
    Changes the priority that's stored in the workload.
    Takes the current priority stored in the workload and adds
    to it the input priority value. 
    
    """
    request = requestDetails(requestName)
    # change in Oracle
    newPrior = int(priority)
    ChangeState.changeRequestPriority(requestName, newPrior)
    # change in workload (spec)
    helper = loadWorkload(request)
    helper.data.request.priority = newPrior
    saveWorkload(helper, request['RequestWorkflow'], wmstatUrl)
    # change priority in CouchDB
    couchDb = Database(request["CouchWorkloadDBName"], request["CouchURL"])
    fields = {"RequestPriority": newPrior}
    couchDb.updateDocument(requestName, "ReqMgr", "updaterequest", fields=fields)
    # push the change to the WorkQueue
    response = ProdManagement.getProdMgr(requestName)
    if response == [] or response[0] is None or response[0] == "":
        # Request must not be assigned yet, we are safe here
        return
    workqueue = WorkQueue.WorkQueue(response[0])
    workqueue.updatePriority(requestName, priority)
    return

def abortRequest(requestName):
    """ Changes the state of the request to "aborted", and asks the work queue
    to cancel its work """
    response = ProdManagement.getProdMgr(requestName)
    if response == [] or response[0] == None or response[0] == "":
        msg =  "Cannot find ProdMgr for request %s\n " % requestName
        msg += "Request may not be known to WorkQueue.  If aborted immediately after assignment, ignore this."
        raise cherrypy.HTTPError(400, msg)
    workqueue = WorkQueue.WorkQueue(response[0])
    workqueue.cancelWorkflow(requestName)
    return

def insecure():
    return "user" not in cherrypy.request.__dict__ or cherrypy.request.user['dn'] == 'None'

def ownsRequest(request):
    # let it slide if there's no authentication
    if insecure():
        return True
    return cherrypy.request.user['login'] == request['Requestor']

def security_roles():
    return ['Developer', 'Admin',  'Data Manager', 'developer', 'admin', 'data-manager']

def security_groups():
    """
    A list of groups which have security access
    """
    return ['ReqMgr', 'reqmgr']

def privileged():
    """ whether this user has roles that overlap with security_roles """
    # let it slide if there's no authentication
    if insecure():
        return True

    # Check and see if we have a valid group
    groups = []
    for role in cherrypy.request.user['roles'].values():
        for group in role['group']:
            # This should be a set
            if group in security_groups():
                groups.append(group)
    if len(groups) < 1:
        return False


    #FIXME doesn't check role in this specific site
    secure_roles = [role for role in cherrypy.request.user['roles'].keys() if role in security_roles()]
    # and maybe we're running without security, in which case dn = 'None'
    return secure_roles != []

def changeStatus(requestName, status, wmstatUrl, acdcUrl):
    """ Changes the status for this request """
    request = GetRequest.getRequestByName(requestName)
    if not status in RequestStatus.StatusList:
        raise RuntimeError, "Bad status code " + status
    if not request.has_key('RequestStatus'):
        raise RuntimeError, "Cannot find status for request " + requestName
    oldStatus = request['RequestStatus']
    if not status in RequestStatus.NextStatus[oldStatus]:
        raise RuntimeError, "Cannot change status from %s to %s.  Allowed values are %s" % (
           oldStatus, status,  RequestStatus.NextStatus[oldStatus])

    if status == 'aborted':
        # delete from the workqueue
        if not privileged() and not ownsRequest(request):
            raise cherrypy.HTTPError(403, "You are not allowed to abort this request")
        elif not privileged():
            raise cherrypy.HTTPError(403, "You are not allowed to change the state for this request")
        # delete from the workqueue if it's been assigned to one
        if oldStatus in ["acquired", "running", "running-closed", "running-open"]:
            abortRequest(requestName)
        else:
            raise cherrypy.HTTPError(400, "You cannot abort a request in state %s" % oldStatus)
        
    if status == 'announced':
        # cleanup acdc database, if possible
        if acdcUrl:
            url, database = WMCore.Lexicon.splitCouchServiceURL(acdcUrl)
            acdcService = CouchService(url = url, database = database)
            acdcService.removeFilesetsByCollectionName(requestName)

    # finally, perform the transition, have to do it in both Oracle and CouchDB
    # and in WMStats
    ChangeState.changeRequestStatus(requestName, status, wmstatUrl=wmstatUrl)

def prepareForTable(request):
    """ Add some fields to make it easier to display a request """
    if 'InputDataset' in request and request['InputDataset'] != '':
        request['Input'] = request['InputDataset']
    elif 'InputDatasets' in request and len(request['InputDatasets']) != 0:
        request['Input'] = str(request['InputDatasets']).strip("[]'")
    else:
        request['Input'] = "Total Events: %s" % request.get('RequestNumEvents', 0)
    if len(request.get('SoftwareVersions', [])) > 0:
        # only show one version
        request['SoftwareVersions'] = request['SoftwareVersions'][0]
    request['PriorityMenu'] = priorityMenu(request)
    return request

def requestsWithStatus(status):
    requestIds = theseIds = ListRequests.listRequestsByStatus(status).values()
    requests = []
    for requestId in requestIds:
        request = GetRequest.getRequest(requestId)
        request = prepareForTable(request)
        requests.append(request)
    return requests

def requestsWhichCouldLeadTo(newStatus):
    """ returns a list of all statuses which can lead to the new status """
    requests = []
    for status, nextStatus in RequestStatus.NextStatus.iteritems():
        # don't allow same->same transition
        if status != newStatus and newStatus in nextStatus:
            requests.extend(requestsWithStatus(status))
    return requests

def priorityMenu(request):
    """ Returns HTML for a box to set priority """
    return ' %i &nbsp<input type="text" size=2 name="%s:priority" />' % (
            request['RequestPriority'],
            request['RequestName'])

def sites(siteDbUrl):
    """ download a list of all the sites from siteDB """
    data = JsonWrapper.loads(urllib.urlopen(siteDbUrl).read().replace("'", '"'))
    # kill duplicates, then put in alphabetical order
    siteset = set([d['name'] for d in data.values()])
    # warning: alliteration
    sitelist = list(siteset)
    sitelist.sort()
    return sitelist

def quote(data):
    """
    Sanitize the data using cgi.escape.
    """
    if  isinstance(data, int) or isinstance(data, float):
        res = data
    else:
        res = cgi.escape(str(data), quote=True)
    return res

def unidecode(data):
    if isinstance(data, unicode):
        return str(data)
    elif isinstance(data, dict):
        return dict(map(unidecode, data.iteritems()))
    elif isinstance(data, (list, tuple, set, frozenset)):
        return type(data)(map(unidecode, data))
    else:
        return data

def validate(schema):
    schema.validate()
    for field in ['RequestName', 'Requestor', 'RequestString',
        'Campaign', 'ProcScenario', 'ConfigCacheID', 'inputMode',
        'CouchDBName', 'Group']:
        value = schema.get(field, '')
        if value and value != '':
            WMCore.Lexicon.identifier(value)
    for field in ['CouchURL']:
        value = schema.get(field, '')
        if value and value != '':
            WMCore.Lexicon.couchurl(schema[field])
            schema[field] = removePasswordFromUrl(value)
    for field in ['InputDatasets', 'OutputDatasets']:
        for dataset in schema.get(field, []):
            if dataset and dataset != '':
                WMCore.Lexicon.dataset(dataset)
    for field in ['InputDataset', 'OutputDataset']:
        value = schema.get(field, '')
        if value and value != '':
            WMCore.Lexicon.dataset(schema[field])
    for field in ['SoftwareVersion']:
        value = schema.get(field, '')
        if value and value != '':
            WMCore.Lexicon.cmsswversion(schema[field])
            
            
def getNewRequestSchema(reqInputArgs):
    """
    Create a new schema
    
    """
    reqSchema = RequestSchema()
    reqSchema.update(reqInputArgs)
    
    currentTime = time.strftime('%y%m%d_%H%M%S',
                             time.localtime(time.time()))
    secondFraction = int(10000 * (time.time()%1.0))
    requestString = reqSchema.get('RequestString', "")
    if requestString != "":
        reqSchema['RequestName'] = "%s_%s_%s_%s" % (
        reqSchema['Requestor'], requestString, currentTime, secondFraction)
    else:
        reqSchema['RequestName'] = "%s_%s_%s" % (reqSchema['Requestor'], currentTime, secondFraction)
    return reqSchema


def buildWorkloadAndCheckIn(webApi, reqSchema, couchUrl, couchDB, wmstatUrl, clone=False):
    """
    If clone is True, the function is called on a cloned request in which
    case no modification of the reqSchema shall happen and should be checked in
    as is.
    
    """
    try:
        request = buildWorkloadForRequest(typename = reqSchema["RequestType"], 
                                          schema = reqSchema)
    except WMSpecFactoryException, ex:
        raise HTTPError(400, "Error in Workload Validation: %s" % ex._message)
    
    helper = WMWorkloadHelper(request['WorkloadSpec'])
    
    #4378 - ACDC (Resubmission) requests should inherit the Campaign ...
    # for Resubmission request, there already is previous Campaign set
    # this call would override it with initial request arguments where
    # it is not specified, so would become ''
    if not helper.getCampaign():
        helper.setCampaign(reqSchema["Campaign"])
    
    # update request as well for wmstats update
    # there is a better way to do this (passing helper to request but make sure all the information is there) 
    request["Campaign"] = helper.getCampaign()
        
    if "RunWhitelist" in reqSchema:
        helper.setRunWhitelist(reqSchema["RunWhitelist"])
        
    # can't save Request object directly, because it makes it hard to retrieve the _rev
    metadata = {}
    metadata.update(request)    
    
    # Add the output datasets if necessary
    # for some bizarre reason OutpuDatasets is list of lists, when cloning
    # [['/MinimumBias/WMAgentCommissioning10-v2/RECO'], ['/MinimumBias/WMAgentCommissioning10-v2/ALCARECO']]
    # #3743
    if not clone:
        for ds in helper.listOutputDatasets():
            if ds not in request['OutputDatasets']:
                request['OutputDatasets'].append(ds)
                
    # don't want to JSONify the whole workflow
    del metadata['WorkloadSpec']
    workloadUrl = helper.saveCouch(couchUrl, couchDB, metadata=metadata)
    request['RequestWorkflow'] = removePasswordFromUrl(workloadUrl)
    try:
        CheckIn.checkIn(request, reqSchema['RequestType'])
    except CheckIn.RequestCheckInError, ex:
        msg = ex._message
        raise HTTPError(400, "Error in Request check-in: %s" % msg)
        
    # Inconsistent request parameters between Oracle and Couch (#4380, #4388)
    # metadata above is what is saved into couch to represent a request document.
    # Number of request arguments on a corresponding couch document
    # is not set, has default null/None values, update those accordingly now.
    # It's a mess to have two mutually inconsistent database backends.
    # Not easy to handle this earlier since couch is stored first and
    # some parameters are worked out later when storing into Oracle.
    reqDetails = requestDetails(request["RequestName"])
    # couchdb request parameters which are null at the injection time and remain so
    paramsToUpdate = ["RequestStatus",
                      "RequestSizeFiles",
                      "AcquisitionEra",
                      "RequestWorkflow",
                      "RequestType",
                      "RequestStatus",
                      "RequestPriority",
                      "Requestor",
                      "Group",
                      "SizePerEvent",
                      "PrepID",
                      "RequestNumEvents",
                      ]
    
    couchDb = Database(reqDetails["CouchWorkloadDBName"], reqDetails["CouchURL"])
    fields = {}
    for key in paramsToUpdate:
        fields[key] = reqDetails[key]
    couchDb.updateDocument(request["RequestName"], "ReqMgr", "updaterequest", fields=fields) 
        
    try:
        wmstatSvc = WMStatsWriter(wmstatUrl)
        wmstatSvc.insertRequest(request)
    except Exception as ex:
        webApi.error("Could not update WMStats, reason: %s" % ex)
        raise HTTPError(400, "Creating request failed, could not update WMStats.")

    return request
    
        
def makeRequest(webApi, reqInputArgs, couchUrl, couchDB, wmstatUrl):
    """
    Handles the submission of requests.
    
    """

    # make sure no extra spaces snuck in
    for k, v in reqInputArgs.iteritems():
        if isinstance(v, str):
            reqInputArgs[k] = v.strip()
            
    webApi.info("makeRequest(): reqInputArgs: '%s'" %  reqInputArgs)
    reqSchema = getNewRequestSchema(reqInputArgs)
            
    # TODO
    # the request arguments below shall be handled automatically by either
    # being specified in the input or already have correct default
    # values in the schema definition
    
    reqSchema["Campaign"] = reqInputArgs.get("Campaign", "")
    
    if 'ProcScenario' in reqInputArgs and 'ConfigCacheID' in reqInputArgs:
        # Use input mode to delete the unused one
        inputMode = reqInputArgs.get('inputMode', None)
        if inputMode == 'scenario':
            del reqSchema['ConfigCacheID']

    if 'EnableDQMHarvest' not in reqInputArgs:
        reqSchema["EnableHarvesting"] = False

    if reqInputArgs.has_key("InputDataset"):
        reqSchema["InputDatasets"] = [reqInputArgs["InputDataset"]]
    if reqInputArgs.has_key("FilterEfficiency"):
        reqInputArgs["FilterEfficiency"] = float(reqInputArgs["FilterEfficiency"])
    skimNumber = 1
    # a list of dictionaries
    reqSchema["SkimConfigs"] = []
    while reqInputArgs.has_key("SkimName%s" % skimNumber):
        d = {}
        d["SkimName"] = reqInputArgs["SkimName%s" % skimNumber]
        d["SkimInput"] = reqInputArgs["SkimInput%s" % skimNumber]
        d["Scenario"] = reqInputArgs["Scenario"]
        d["TimePerEvent"] = reqInputArgs.get("SkimTimePerEvent%s" % skimNumber, None)
        d["SizePerEvent"] = reqInputArgs.get("SkimSizePerEvent%s" % skimNumber, None)
        d["Memory"] = reqInputArgs.get("SkimMemory%s" % skimNumber, None)


        if reqInputArgs.get("Skim%sConfigCacheID" % skimNumber, None) != None:
            d["ConfigCacheID"] = reqInputArgs["Skim%sConfigCacheID" % skimNumber]

        reqSchema["SkimConfigs"].append(d)
        skimNumber += 1

    if reqInputArgs.has_key("DataPileup") or reqInputArgs.has_key("MCPileup"):
        reqSchema["PileupConfig"] = {}
        if reqInputArgs.has_key("DataPileup") and reqInputArgs["DataPileup"] != "":
            reqSchema["PileupConfig"]["data"] = [reqInputArgs["DataPileup"]]
        if reqInputArgs.has_key("MCPileup") and reqInputArgs["MCPileup"] != "":
            reqSchema["PileupConfig"]["mc"] = [reqInputArgs["MCPileup"]]

    for runlist in ["RunWhitelist", "RunBlacklist"]:
        if runlist in reqInputArgs:
            reqSchema[runlist] = parseRunList(reqInputArgs[runlist])
    for blocklist in ["BlockWhitelist", "BlockBlacklist"]:
        if blocklist in reqInputArgs:
            reqSchema[blocklist] = parseBlockList(reqInputArgs[blocklist])
    for stringList in ["DqmSequences", "IgnoredOutputModules", "TransientOutputModules"]:
        if stringList in reqInputArgs:
            reqSchema[stringList] = parseStringListWithoutValidation(reqInputArgs[stringList])

    validate(reqSchema)

    # Get the DN
    reqSchema['RequestorDN'] = cherrypy.request.user.get('dn', 'unknown')
    
    request = buildWorkloadAndCheckIn(webApi, reqSchema, couchUrl, couchDB, wmstatUrl)
    return request
    

def requestDetails(requestName):
    """ Adds details from the Couch document as well as the database """
    WMCore.Lexicon.identifier(requestName)
    request = GetRequest.getRequestDetails(requestName)
    helper = loadWorkload(request)
    schema = helper.data.request.schema.dictionary_whole_tree_()
    # take the stuff from the DB preferentially
    schema.update(request)
    task = helper.getTopLevelTask()[0]
    schema['Site Whitelist']  = task.siteWhitelist()
    schema['Site Blacklist']  = task.siteBlacklist()
    schema['MergedLFNBase']   = str(helper.getMergedLFNBase())
    schema['UnmergedLFNBase'] = str(helper.getUnmergedLFNBase())
    schema['Campaign']        = str(helper.getCampaign()) 
    schema['AcquisitionEra']  = str(helper.getAcquisitionEra())
    if schema['SoftwareVersions'] == ['DEPRECATED']:
        schema['SoftwareVersions'] = helper.getCMSSWVersions()

    # Check in the CouchWorkloadDBName if not present
    schema.setdefault("CouchWorkloadDBName", "reqmgr_workload_cache")

    # get DbsUrl from CouchDB
    if schema.get("CouchWorkloadDBName", None) and schema.get("CouchURL", None):
        couchDb = Database(schema["CouchWorkloadDBName"], schema["CouchURL"])
        couchReq = couchDb.document(requestName)
        schema["DbsUrl"] = couchReq.get("DbsUrl", None)
        
    return schema


def serveFile(contentType, prefix, *args):
    """Return a workflow from the cache"""
    name = path.normpath(path.join(prefix, *args))
    if path.commonprefix([name, prefix]) != prefix:
        raise cherrypy.HTTPError(403)
    if not path.exists(name):
        raise cherrypy.HTTPError(404, "%s not found" % name)
    return cherrypy.lib.static.serve_file(name, content_type = contentType)

def getOutputForRequest(requestName):
    """Return the datasets produced by this request."""
    request = GetRequest.getRequestByName(requestName)
    if not request:
        return []
    helper = loadWorkload(request)
    return helper.listOutputDatasets()

def associateCampaign(campaign, requestName, couchURL, couchDBName):
    """
    _associateCampaign_

    Associate a campaign and a request inside the workloadSpec
    This is done by loading the workloadSpec from couch, modifying the
    campaign, and saving it again.
    """
    WMCore.Lexicon.identifier(requestName)
    request = GetRequest.getRequestDetails(requestName)
    helper = loadWorkload(request)
    helper.setCampaign(campaign = campaign)
    helper.saveCouch(couchUrl = couchURL, couchDBName = couchDBName)

def retrieveResubmissionChildren(requestName, couchUrl, couchDBName):
    """
    _retrieveResubmissionChildren_

    Construct a list of request names which are the resubmission
    offspring from a request. This is a recursive
    call with a single requestName as input.
    The result only includes the children and not the original request.
    """
    childrenRequestNames = []
    reqmgrDb = Database(couchDBName, couchUrl)
    result = reqmgrDb.loadView('ReqMgr', 'childresubmissionrequests', keys = [requestName])['rows']
    for child in result:
        childrenRequestNames.append(child['id'])
        childrenRequestNames.extend(retrieveResubmissionChildren(child['id'], couchUrl, couchDBName))
    return childrenRequestNames

def updateRequestStats(requestName, stats, couchURL, couchDBName):
    couchDB = Database(couchDBName, couchURL)
    return couchDB.updateDocument(requestName, 'ReqMgr', 'totalstats',
                                         fields=stats)
