""" Functions external to the web interface classes"""
import urllib
import time
import logging
import re
import WMCore.Wrappers.JsonWrapper as JsonWrapper
import cherrypy
from os import path
from cherrypy import HTTPError
from cherrypy.lib.static import serve_file
import WMCore.Lexicon
import cgi
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
import WMCore.RequestManager.RequestDB.Settings.RequestStatus as RequestStatus
import WMCore.RequestManager.RequestDB.Interface.Request.ChangeState as ChangeState
import WMCore.RequestManager.RequestDB.Interface.Request.GetRequest as GetRequest
import WMCore.RequestManager.RequestDB.Interface.Admin.ProdManagement as ProdManagement
import WMCore.RequestManager.RequestDB.Interface.Request.ListRequests as ListRequests
import WMCore.Services.WorkQueue.WorkQueue as WorkQueue
import WMCore.RequestManager.RequestMaker.CheckIn as CheckIn
from WMCore.RequestManager.RequestMaker.Registry import retrieveRequestMaker, buildWorkloadForRequest
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WMSpec.StdSpecs.StdBase import WMSpecFactoryException
from WMCore.RequestManager.DataStructs.RequestSchema import RequestSchema


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

    return


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

def parseSite(kw, name):
    """ puts site whitelist & blacklists into nice format"""
    value = kw.get(name, [])
    if value == None:
        value = []
    if not isinstance(value, list):
        value = [value]
    return value

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
 
def saveWorkload(helper, workload):
    """ Saves the changes to this workload """
    if workload.startswith('http'):
        helper.saveCouchUrl(workload)
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

def changePriority(requestName, priority):
    """ Changes the priority that's stored in the workload """
    # fill in all details
    request = GetRequest.getRequestByName(requestName)
    groupPriority = request.get('ReqMgrGroupBasePriority', 0)
    userPriority  = request.get('ReqMgrRequestorBasePriority', 0)
    ChangeState.changeRequestPriority(requestName, priority)
    helper = loadWorkload(request)
    totalPriority = int(priority + userPriority + groupPriority)
    helper.data.request.priority = totalPriority
    saveWorkload(helper, request['RequestWorkflow'])

def abortRequest(request):
    """ Changes the state of the request to "aborted", and asks the work queue
    to cancel its work """
    response = ProdManagement.getProdMgr(request)
    url = response[0]
    if url == None or url == "":
        raise cherrypy.HTTPError(400, "Cannot find URL for request " + request)
    workqueue = WorkQueue.WorkQueue(url)
    workqueue.cancelWorkflow(request)

def changeStatus(requestName, status):
    """ Changes the status for this request """
    request = GetRequest.getRequestByName(requestName)
    oldStatus = request['RequestStatus']
    if not status in RequestStatus.StatusList:
        raise RuntimeError, "Bad status code " + status
    if not request.has_key('RequestStatus'):
        raise RuntimeError, "Cannot find status for request " + requestName
    if not status in RequestStatus.NextStatus[oldStatus]:
        raise RuntimeError, "Cannot change status from %s to %s.  Allowed values are %s" % (
           oldStatus, status,  RequestStatus.NextStatus[oldStatus])
    ChangeState.changeRequestStatus(requestName, status)

    if status == 'aborted':
        # delete from the workqueue
        abortRequest(requestName)

def prepareForTable(request):
    """ Add some fields to make it easier to display a request """
    if 'InputDataset' in request and request['InputDataset'] != '':
        request['Input'] = request['InputDataset']
    elif 'InputDatasets' in request and len(request['InputDatasets']) != 0:
        request['Input'] = str(request['InputDatasets']).strip("[]'")
    else:
        request['Input'] = "Total Events: %s" % request['RequestSizeEvents']
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
    return '(%iu, %ig) %i &nbsp<input type="text" size=2 name="%s:priority" />' % (
            request['ReqMgrRequestorBasePriority'], request['ReqMgrGroupBasePriority'],
            request['ReqMgrRequestBasePriority'],
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
        'Campaign', 'Scenario', 'ProdConfigCacheID', 'inputMode',
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
        
def makeRequest(kwargs, couchUrl, couchDB):
    logging.info(kwargs)
    """ Handles the submission of requests """
    # make sure no extra spaces snuck in
    for k, v in kwargs.iteritems():
        if isinstance(v, str):
            kwargs[k] = v.strip()
    #maker = retrieveRequestMaker(kwargs["RequestType"])
    #schema = maker.newSchema()
    # Create a new schema
    schema = RequestSchema()
    schema.update(kwargs)
    
    currentTime = time.strftime('%y%m%d_%H%M%S',
                             time.localtime(time.time()))
    secondFraction = int(10000 * (time.time()%1.0))
    requestString = schema.get('RequestString', "")
    if requestString != "":
        schema['RequestName'] = "%s_%s_%s_%s" % (
        schema['Requestor'], requestString, currentTime, secondFraction)
    else:
        schema['RequestName'] = "%s_%s_%s" % (schema['Requestor'], currentTime, secondFraction)
    schema["Campaign"] = kwargs.get("Campaign", "")
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
    if kwargs.has_key("FilterEfficiency"):
        kwargs["FilterEfficiency"] = float(kwargs["FilterEfficiency"])
    skimNumber = 1
    # a list of dictionaries
    schema["SkimConfigs"] = []
    while kwargs.has_key("SkimName%s" % skimNumber):
        d = {}
        d["SkimName"] = kwargs["SkimName%s" % skimNumber]
        d["SkimInput"] = kwargs["SkimInput%s" % skimNumber]
        d["Scenario"] = kwargs["Scenario"]

        if kwargs.get("Skim%sConfigCacheID" % skimNumber, None) != None:
            d["ConfigCacheID"] = kwargs["Skim%sConfigCacheID" % skimNumber]

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
            schema[runlist] = parseRunList(kwargs[runlist])
    for blocklist in ["BlockWhitelist", "BlockBlacklist"]:
        if blocklist in kwargs:
            schema[blocklist] = parseBlockList(kwargs[blocklist])
    validate(schema)
    try:
        request = buildWorkloadForRequest(typename = kwargs["RequestType"],
                                          schema = schema)
    except WMSpecFactoryException, ex:
        msg = ex._message
        raise HTTPError(400, "Error in Workload Validation: %s" % msg)
    helper = WMWorkloadHelper(request['WorkloadSpec'])
    helper.setCampaign(schema["Campaign"])
    if "RunWhitelist" in schema:
        helper.setRunWhitelist(schema["RunWhitelist"])
    # can't save Request object directly, because it makes it hard to retrieve the _rev
    metadata = {}
    metadata.update(request)
    # don't want to JSONify the whole workflow
    del metadata['WorkloadSpec']
    workloadUrl = helper.saveCouch(couchUrl, couchDB, metadata=metadata)
    request['RequestWorkflow'] = removePasswordFromUrl(workloadUrl)
    CheckIn.checkIn(request)
    return request

def requestDetails(requestName):
    """ Adds details from the Couch document as well as the database """
    WMCore.Lexicon.identifier(requestName)
    request = GetRequest.getRequestDetails(requestName)
    helper = loadWorkload(request)
    schema = helper.data.request.schema.dictionary_()
    # take the stuff from the DB preferentially
    schema.update(request)
    task = helper.getTopLevelTask()[0]
    schema['Site Whitelist']  = task.siteWhitelist()
    schema['Site Blacklist']  = task.siteBlacklist()
    schema['MergedLFNBase']   = str(helper.getMergedLFNBase())
    schema['UnmergedLFNBase'] = str(helper.getUnmergedLFNBase())
    schema['Campaign']        = str(helper.getCampaign())
    schema['AcquisitionEra']  = str(helper.getAcquisitionEra())
    return schema

def serveFile(contentType, prefix, *args):
    """Return a workflow from the cache"""
    name = path.normpath(path.join(prefix, *args))
    if path.commonprefix([name, prefix]) != prefix:
        raise HTTPError(403)
    if not path.exists(name):
        raise HTTPError(404, "%s not found" % name)
    return serve_file(name, content_type = contentType)

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
    return   
