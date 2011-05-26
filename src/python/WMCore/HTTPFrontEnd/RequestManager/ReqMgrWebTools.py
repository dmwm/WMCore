""" Functions external to the web interface classes"""
import urllib
import logging
import WMCore.Wrappers.JsonWrapper as JsonWrapper
import cherrypy
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
from WMCore.RequestManager.RequestMaker.Registry import retrieveRequestMaker
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper



def parseRunList(l):
    """ Changes a string into a list of integers """
    toks = l.lstrip(' [').rstrip(' ]').split(',')
    if toks == ['']:
        return []
    return [int(tok) for tok in toks]

def parseBlockList(l):
    """ Changes a string into a list of strings """
    toks = l.lstrip(' [').rstrip(' ]').split(',')
    if toks == ['']:
        return []
    # only one set of quotes
    return [str(tok.strip(' \'"')) for tok in toks]

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
    ChangeState.changeRequestPriority(requestName, priority)
    helper = loadWorkload(request)
    helper.data.request.priority = int(priority)
    saveWorkload(helper, request['RequestWorkflow'])

def abortRequest(request):
    """ Changes the state of the request to "aborted", and asks the work queue
    to cancel its work """
    response = ProdManagement.getProdMgr(request)
    url = response[0]
    if url == None or url == "":
        raise cherrypy.HTTPError(400, "Cannot find URL for request " + request)
    workqueue = WorkQueue.WorkQueue({'endpoint': url})
    workqueue.cancelWork([request], "request_name")

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
        if newStatus in nextStatus:
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

def makeRequest(schema, couchUrl, couchDB):
    logging.info(schema)
    maker = retrieveRequestMaker(schema['RequestType'])
    specificSchema = maker.schemaClass()
    specificSchema.update(schema)
    specificSchema.validate()

    request = maker(specificSchema)
    helper = WMWorkloadHelper(request['WorkflowSpec'])
    # can't save Request object directly, because it makes it hard to retrieve the _rev
    metadata = {}
    metadata.update(request)
    # don't want to JSONify the whole workflow
    del metadata['WorkflowSpec']
    workloadUrl = helper.saveCouch(couchUrl, couchDB, metadata=metadata)
    request['RequestWorkflow'] = removePasswordFromUrl(workloadUrl)
    CheckIn.checkIn(request)

from os import path
from cherrypy import HTTPError
from cherrypy.lib.static import serve_file

def serveFile(contentType, prefix, *args):
    """Return a workflow from the cache"""
    name = path.normpath(path.join(prefix, *args))
    if path.commonprefix([name, prefix]) != prefix:
        raise HTTPError(403)
    if not path.exists(name):
        raise HTTPError(404, "%s not found" % name)
    return serve_file(name, content_type = contentType)

