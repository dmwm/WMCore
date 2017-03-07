"""
Unlike ReqMgr1 defining Request and RequestSchema classes,
define just 1 class. Derived from Python dict and implementing
necessary conversion and validation extra methods possibly needed.

TODO/NOTE:
    'inputMode' should be removed by now (2013-07)

    since arguments validation #4705, arguments which are later
        validated during spec instantiation and which are not
        present in the request injection request, can't be defined
        here because their None value is not allowed in the spec.
        This is the case for e.g. DbsUrl, AcquisitionEra
        This module should probably define only absolutely
        necessary request parameters and not any optional ones.

"""
from __future__ import print_function, division
from builtins import range
from builtins import object
import time
import cherrypy
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_START_STATE, ACTIVE_STATUS_FILTER
from WMCore.ReqMgr.DataStructs.RequestError import InvalidSpecParameterValue
from WMCore.Lexicon import identifier

ARGS_TO_REMOVE_FROM_ORIGINAL_REQUEST = \
    ["_id", "_rev", "Requestor", "ReqMgr2Only", "RequestTransition", "RequestStatus",
     "RequestorDN", "MaxRSS", "MaxVSize", "IgnoredOutputModules", "TrustSitelists",
     "TrustPUSitelists", "HardTimeout", "GracePeriod", "SoftTimeout", "MaxWaitTime", "Team",
     "Teams", "SiteWhitelist", "SiteBlacklist", "EnableNewStageout", "DeleteFromSource",
     "OutputDatasets", "Dashboard", "SoftwareVersions", "VoRole", "DN", "TotalEstimatedJobs",
     "TotalInputEvents", "TotalInputLumis", "TotalInputFiles"]

def initialize_request_args(request, config, clone=False):
    """
    Request data class request is a dictionary representing
    a being injected / created request. This method initializes
    various request fields. This should be the ONLY method to
    manipulate request arguments upon injection so that various
    levels or arguments manipulation does not occur across several
    modules and across about 7 various methods like in ReqMgr1.

    request is changed here.
    """

    # user information for cert. (which is converted to cherry py log in)
    request["Requestor"] = cherrypy.request.user["login"]
    request["RequestorDN"] = cherrypy.request.user.get("dn", "unknown")
    # service certificates carry @hostname, remove it if it exists
    request["Requestor"] = request["Requestor"].split('@')[0]

    # assign first starting status, should be 'new'
    request["RequestStatus"] = REQUEST_START_STATE
    request["RequestTransition"] = [{"Status": request["RequestStatus"],
                                     "UpdateTime": int(time.time()), "DN": request["RequestorDN"]}]
    request["RequestDate"] = list(time.gmtime()[:6])

    if clone:
        # if it is clone parameter should contain requestName
        request["OriginalRequestName"] = request["RequestName"]
    # TODO: generate this automatically from the spec
    # generate request name using request
    generateRequestName(request)

    if not clone:
        # update the information from config
        request["CouchURL"] = config.couch_host
        request["CouchWorkloadDBName"] = config.couch_reqmgr_db
        request["CouchDBName"] = config.couch_config_cache_db

        request.setdefault("SoftwareVersions", [])
        if "CMSSWVersion" in request and request["CMSSWVersion"] not in request["SoftwareVersions"]:
            request["SoftwareVersions"].append(request["CMSSWVersion"])

        # TODO
        # do we need InputDataset and InputDatasets? when one is just a list
        # containing the other? ; could be related to #3743 problem
        if "InputDataset" in request:
            request["InputDatasets"] = [request["InputDataset"]]

def initialize_resubmission(request_args, config, reqmgr_db_service):
    request_args["OriginalRequestCouchURL"] = '%s/%s' % (config.couch_host,
                                                         config.couch_reqmgr_db)
    requests = reqmgr_db_service.getRequestByNames(request_args["OriginalRequestName"])
    resubmission_args = list(requests.values())[0]
    for arg in resubmission_args:
        if (arg not in request_args) and (arg not in ARGS_TO_REMOVE_FROM_ORIGINAL_REQUEST):
            request_args[arg] = resubmission_args[arg]
    return request_args

def generateRequestName(request):
    currentTime = time.strftime('%y%m%d_%H%M%S', time.localtime(time.time()))
    seconds = int(10000 * (time.time() % 1.0))

    if "RequestString" not in request:
        raise InvalidSpecParameterValue("RequestString need to be specified")

    request["RequestName"] = "%s_%s" % (request["Requestor"], request["RequestString"])
    # add time info
    request["RequestName"] += "_%s_%s" % (currentTime, seconds)
    # then validate the final request name
    identifier(request["RequestName"])
    
        
def protectedLFNs(requestInfo):
    
    reqData = RequestInfo(requestInfo)
    result = []
    if reqData.andFilterCheck(ACTIVE_STATUS_FILTER):
        outs = requestInfo.get('OutputDatasets', [])
        base= requestInfo.get('UnmergedLFNBase','/store/unmerged')
        for out in outs:
            dsn, ps, tier = out.split('/')[1:]
            acq, rest = ps.split('-',1)
            dirPath = '/'.join([ base, acq, dsn, tier, rest])
            result.append(dirPath)
    return result
            

class RequestInfo(object):
    """
    Wrapper class for Request data
    """
    def __init__(self, requestData):
        self.data = requestData
    
    def _maskTaskStepChain(self, prop, chain_name, default=None):

        propExist = False
        numLoop = self.data["%sChain" % chain_name]
        for i in range(numLoop):
            if prop in self.data["%s%s" % (chain_name, i + 1)]:
                propExist = True
                break
        
        defaultValue = self.data.get(prop, default)
            
        if propExist:
            result = set()
            for i in range(numLoop):
                chain_key = "%s%s" % (chain_name, i + 1)
                chain = self.data[chain_key]
                if prop in chain:
                    result.add(chain[prop])
                else:
                    if isinstance(defaultValue, dict):
                        value = defaultValue.get(chain_key, None)
                    else:
                        value = defaultValue
                    
                    if value is not None:
                        result.add(value)
            return list(result)
        else:
            if isinstance(defaultValue, dict):
                return list(defaultValue.values())
            else:
                return defaultValue
            
        return

    def get(self, prop, default=None):
        """
        gets the value when prop exist as one of the properties in the request document.
        In case TaskChain, StepChain workflow it searches the property in Task/Step level
        """
        
        if "TaskChain" in self.data:
            return self._maskTaskStepChain(prop, "Task")
        elif "StepChain" in self.data:
            return self._maskTaskStepChain(prop, "Step")
        elif prop in self.data:
            return self.data[prop]
        else:
            return default
        
    def andFilterCheck(self, filterDict):
        """
        checks whether filterDict condition met.
        filterDict is the dict of key and value(list) format)
        i.e. 
        {"RequestStatus": ["running-closed", "completed"],}
        If this request's RequestStatus is either "running-closed", "completed",
        return True, otherwise False
        """
        for key, value in filterDict.items():
            if key in self.data:
                if self.data[key] not in value:
                    return False
            else:
                return False  
        return True


    
