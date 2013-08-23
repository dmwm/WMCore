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
import time
import cherrypy
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_START_STATE

def initialize_request_args(request, config, clone = False):
    """
    Request data class request is a dictionary representing
    a being injected / created request. This method initializes
    various request fields. This should be the ONLY method to
    manipulate request arguments upon injection so that various
    levels or arguments manipulation does not occur accros several
    modules and across about 7 various methods like in ReqMgr1.
    
    request is changed here.
    
    """ 
    
    #user information for cert. (which is converted to cherry py log in)
    request["Requestor"] = cherrypy.request.user["login"]
    request["RequestorDN"] = cherrypy.request.user.get("dn", "unknown")
    
    # assign first starting status, should be 'new'
    request["RequestStatus"] = REQUEST_START_STATE 
    request["RequestTransition"] = [{"Status": request["RequestStatus"], "UpdateTime": int(time.time())}]
    request["RequestDate"] = list(time.gmtime()[:6])
    
    #TODO: generate this automatically from the spec
    # generate request name using request
    generateRequestName(request)
    
    if not clone:
        #update the information from config
        request["CouchURL"] = config.couch_host
        request["CouchWorkloadDBName"] = config.couch_reqmgr_db
        request["CouchDBName"] = config.couch_config_cache_db
        
        request.setdefault("SoftwareVersions", [])
        if request["CMSSWVersion"] and (request["CMSSWVersion"] not in request["SoftwareVersions"]):
            request["SoftwareVersions"].append(request["CMSSWVersion"])
            
        # TODO
        # do we need InputDataset and InputDatasets? when one is just a list
        # containing the other? ; could be related to #3743 problem
        if request.has_key("InputDataset"):
            request["InputDatasets"] = [request["InputDataset"]]

def generateRequestName(request):
    
    current_time = time.strftime('%y%m%d_%H%M%S', time.localtime(time.time()))
    seconds = int(10000 * (time.time() % 1.0))
    request_string = request.get("RequestString", "")
    if request_string != "":
        request["RequestName"] = "%s_%s" % (request["Requestor"], request_string)
    else:
        request["RequestName"] = request["Requestor"]
    request["RequestName"] += "_%s_%s" % (current_time, seconds)