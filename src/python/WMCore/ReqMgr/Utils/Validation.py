"""
ReqMgr request handling.

"""
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WMSpec.WMWorkloadTools import loadSpecByType
from WMCore.REST.Auth import authz_match
from WMCore.WMFactory import WMFactory

from WMCore.ReqMgr.Auth import getWritePermission
from WMCore.ReqMgr.DataStructs.Request import initialize_request_args
from WMCore.ReqMgr.DataStructs.RequestStatus import check_allowed_transition
from WMCore.ReqMgr.DataStructs.RequestError import InvalidStateTransition
from WMCore.ReqMgr.Tools.cms import releases, architectures

def workqueue_stat_validation(request_args):
    stat_keys = ['total_jobs', 'input_lumis', 'input_events', 'input_num_files']
    return set(request_args.keys()) == set(stat_keys)
    
def validate_request_update_args(request_args, config, reqmgr_db_service, param):
    """
    param and safe structure is RESTArgs structure: named tuple
    RESTArgs(args=[], kwargs={})
    
    validate post request
    1. read data from body
    2. validate the permission (authentication)
    3. validate state transition (against previous state from couchdb)
    2. validate using workload validation
    3. convert data from body to arguments (spec instance, argument with default setting)
    
    TODO: rasie right kind of error with clear message 
    """

    request_name = request_args["RequestName"]
    # this need to be deleted for validation
    del request_args["RequestName"]
    couchurl =  '%s/%s' % (config.couch_host, config.couch_reqmgr_db)
    workload = WMWorkloadHelper()
    # param structure is RESTArgs structure.
    workload.loadSpecFromCouch(couchurl, request_name)
    
    # first validate the permission by status and request type.
    # if the status is not set only ReqMgr Admin can change the the values
    # TODO for each step, assigned, approved, announce find out what other values
    # can be set
    request_args["RequestType"] = workload.requestType()
    permission = getWritePermission(request_args)
    authz_match(permission['role'], permission['group'])
    del request_args["RequestType"]

    #validate the status
    if request_args.has_key("RequestStatus"):
        validate_state_transition(reqmgr_db_service, request_name, request_args["RequestStatus"])
        # delete request_args since it is not part of spec argument sand validation
        args_without_status = {}
        args_without_status.update(request_args)
        del args_without_status["RequestStatus"]
    else:
        args_without_status = request_args

    if len(args_without_status) > 0 and not workqueue_stat_validation(args_without_status):
        # validate the arguments against the spec argumentSpecdefinition
        #TODO: currently only assigned status allows any update other then Status update
        workload.validateArgumentForAssignment(args_without_status)

    # to update request_args with type conversion
    request_args.update(args_without_status)

    return workload, request_args
        
def validate_request_create_args(request_args, config, *args, **kwargs):
    """
    *arg and **kwargs are only for the interface
    validate post request
    1. read data from body
    2. validate using spec validation
    3. convert data from body to arguments (spec instance, argument with default setting) 
    TODO: rasie right kind of error with clear message 
    """
    
    initialize_request_args(request_args, config)
    #check the permission for creating the request
    permission = getWritePermission(request_args)
    authz_match(permission['role'], permission['group'])
    
    # get the spec type and validate arguments
    spec = loadSpecByType(request_args["RequestType"])
    if request_args["RequestType"] == "Resubmission":
        request_args["OriginalRequestCouchURL"] = '%s/%s' % (config.couch_host, 
                                                             config.couch_reqmgr_db)
    workload = spec.factoryWorkloadConstruction(request_args["RequestName"], 
                                                request_args)
    return workload, request_args
    
def validate_state_transition(reqmgr_db_service, request_name, new_state) :
    """
    validate state transition by getting the current data from
    couchdb
    """
    requests = reqmgr_db_service.getRequestByNames(request_name)
    # generator object can't be subscribed: need to loop.
    # only one row should be returned
    for request in requests.values():
        current_state = request["RequestStatus"]
    if not check_allowed_transition(current_state, new_state):
        raise InvalidStateTransition(current_state, new_state)
    return

def create_json_template_spec(specArgs):
    template = {}
    for key, prop in specArgs.items():
        
        if key == "RequestorDN":
            # this will be automatically collected so skip it.
            continue
        
        if key == "CMSSWVersion":
            # get if from tag collector
            value = releases()
        elif key == "ScramArch":
            value = architectures()
        elif prop.get("optional", True):
            # if optional need to always have default value
            value = prop["default"]
        else:
            value = "REPLACE-%s" % key
        template[key] = value
    return template

def get_request_template_from_type(request_type, loc="WMSpec.StdSpecs"):
    pluginFactory = WMFactory("specArgs", loc)
    alteredClassName = "%sWorkloadFactory" % request_type
    spec = pluginFactory.loadObject(classname = request_type, alteredClassName = alteredClassName)
    specArgs = spec.getWorkloadArguments()

    result = create_json_template_spec(specArgs)
    return result
