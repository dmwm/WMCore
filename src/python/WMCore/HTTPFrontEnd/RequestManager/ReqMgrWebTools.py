""" Functions to interpret lists that get sent in as text"""
import urllib
import cherrypy
import WMCore.Lexicon
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
import WMCore.RequestManager.RequestDB.Interface.Request.ChangeState as ChangeState
import WMCore.RequestManager.RequestDB.Interface.Request.GetRequest as GetRequest
import WMCore.RequestManager.RequestDB.Interface.Admin.ProdManagement as ProdManagement
import WMCore.RequestManager.RequestDB.Settings.RequestStatus as RequestStatus


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
    result = []
    f = urllib.urlopen("https://cmstags.cern.ch/cgi-bin/CmsTC/ReleasesXML?anytype=1")
    for line in f:
        for tok in line.split():
            if tok.startswith("label="):
                release = tok.split("=")[1].strip('"')
                result.append(release)
    return result

def loadWorkload(request):
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
    if workload.startswith('http://'):
        helper.saveCouchUrl(workload)
    else:
        helper.save(workload)

def removePasswordFromUrl(url):
    # where the @ symbol is at.
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

